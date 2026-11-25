package restore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// _secondaryMirrorTimeout is how long a secondary may take to know the
// mirrored import job at all. The mirror is created when the replication
// stream delivers the import message, which happens around the submit — long
// before the local job reaches Uncommitted — so by the time the wait starts
// the mirror should already be there, and the window covers the stragglers
// and a stream that is catching up after a pause.
const _secondaryMirrorTimeout = 5 * time.Minute

// _secondaryCommitTimeout is how long a secondary's mirror may take to finish
// after the local commit. Nobody drives the secondary's commit: a replication
// target refuses client-side DDL, and the local commit's fence message travels
// the stream and commits the mirror there, so this window covers stream lag
// plus the secondary's own flush-and-commit across every vchannel of the job.
const _secondaryCommitTimeout = 10 * time.Minute

// importViaRESTFul2PCTask imports a set of dirs through the v2 restful bulk
// insert API in two phases: the job is submitted with auto_commit=false and
// held at Uncommitted until every secondary's mirrored job — same job id,
// created by the replication stream — is Uncommitted too; only then is the job
// committed on the local cluster, and the commit fence it broadcasts
// replicates and commits every mirror. Being two-phase is the type's declared
// job, so the secondary clients are a plain field; the orchestration layer is
// what knows which addresses are the secondaries.
type importViaRESTFul2PCTask struct {
	importer    *restfulImporter
	secondaries []importSecondary
}

func (rt *importViaRESTFul2PCTask) Execute(ctx context.Context) error {
	if err := rt.execute(ctx); err != nil {
		rt.importer.job.SetFailed(err)
		return err
	}
	return nil
}

func (rt *importViaRESTFul2PCTask) execute(ctx context.Context) error {
	im := rt.importer
	staging := newImportCopyTask(im.args, im.job)
	defer staging.Cleanup(ctx)

	submit := func(ctx context.Context) (string, error) {
		staged := *im
		if err := staged.stage(ctx, staging); err != nil {
			return "", err
		}
		return submitImport(ctx, im.args.jobs.ResumeEnabled(), staged.submit2PC)
	}
	flow := restImport2PC{
		primary:                im.args.restfulCli,
		db:                     im.args.target.DBName(),
		secondaries:            rt.secondaries,
		job:                    im.job,
		enableResume:           im.args.jobs.ResumeEnabled(),
		logger:                 im.args.logger,
		primaryImportLifecycle: im,
	}
	return flow.execute(ctx, submit)
}

type primaryImportLifecycle interface {
	waitUncommitted(context.Context, string) error
	commit(context.Context, string) error
	abort(context.Context, string) error
	waitCompleted(context.Context, string) error
}

// restImport2PC drives one manual-commit job on the primary and its mirrors.
// The caller prepares and submits its source only when start needs a new job.
type restImport2PC struct {
	primaryImportLifecycle
	primary      milvus.Restful
	db           string
	secondaries  []importSecondary
	job          *jobstate.RestoreJobTracker
	enableResume bool
	logger       *zap.Logger
}

func (rt *restImport2PC) execute(ctx context.Context, submit func(context.Context) (string, error)) error {
	if _, done := rt.job.Current(); done {
		return nil
	}
	jobID, phase, err := rt.start(ctx, submit)
	if err != nil {
		return err
	}
	phase, err = rt.waitReady(ctx, jobID, phase)
	if err == nil && phase == milvus.ImportStateUncommitted {
		// Record the request before sending: a lost reply may hide a commit.
		if rt.enableResume {
			rt.job.MarkCommitRequested()
		}
		err = rt.commit(ctx, jobID)
	}
	if err != nil {
		// A restore without Resume aborts on any failure before commit returns.
		if !rt.enableResume {
			if abortErr := rt.abort(context.WithoutCancel(ctx), jobID); abortErr != nil {
				rt.logger.Warn("abort import job failed", zap.String("job_id", jobID), zap.Error(abortErr))
			}
		}
		return err
	}

	for _, secondary := range rt.secondaries {
		if rt.enableResume && rt.job.MirrorCompleted(secondary.addr) {
			continue
		}
		if err := rt.waitSecondaryCommitted(ctx, secondary, jobID); err != nil {
			return err
		}
		if rt.enableResume {
			rt.job.MarkMirrorCompleted(secondary.addr)
		}
	}
	if rt.enableResume && rt.job.PrimaryCompleted() {
		rt.job.SetCompleted()
		return nil
	}
	return rt.waitCompleted(ctx, jobID)
}

// start adopts the saved job or replaces a confirmed pre-commit failure.
// Once an ID is selected, waiting and committing never submit another job.
func (rt *restImport2PC) start(ctx context.Context, submit func(context.Context) (string, error)) (string, milvus.ImportState, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if id, _ := rt.job.Current(); rt.enableResume && id != "" {
		rt.job.Bind(id)
		if rt.job.PrimaryCompleted() {
			return id, milvus.ImportStateCompleted, nil
		}
		phase, progress, err := rt.primaryState(ctx, id)
		if err == nil {
			rt.job.SetProgress(int32(progress))
			return id, phase, nil
		}
		if rt.job.CommitRequested() || phase != milvus.ImportStateFailed || !errors.Is(err, milvus.ErrRemoteJobFailed) {
			return id, phase, err
		}
		if err := rt.abort(ctx, id); err != nil {
			return id, phase, err
		}
		rt.job.Bind("")
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	id, err := submit(ctx)
	if err != nil {
		return "", "", err
	}
	if rt.enableResume && id == "" {
		return "", "", errors.New("restore: Milvus returned an empty import ID")
	}
	rt.job.Bind(id)
	if rt.enableResume {
		return id, "", ctx.Err()
	}
	// Let the legacy wait report cancellation so its caller still aborts.
	return id, "", nil
}

func (rt *restImport2PC) waitReady(ctx context.Context, jobID string, phase milvus.ImportState) (milvus.ImportState, error) {
	if !rt.enableResume {
		if err := rt.waitUncommitted(ctx, jobID); err != nil {
			return "", err
		}
	} else {
		if phase != milvus.ImportStateUncommitted && phase != milvus.ImportStateCommitting && phase != milvus.ImportStateCompleted {
			var err error
			phase, err = rt.waitPrimaryReady(ctx, jobID)
			if err != nil {
				return "", err
			}
		}
		switch phase {
		case milvus.ImportStateCompleted:
			rt.job.MarkPrimaryCompleted()
			return phase, nil
		case milvus.ImportStateCommitting:
			rt.job.MarkCommitRequested()
			return phase, nil
		}
	}

	for _, secondary := range rt.secondaries {
		if err := rt.waitSecondaryMirrored(ctx, secondary, jobID); err != nil {
			return "", err
		}
	}
	return milvus.ImportStateUncommitted, nil
}

func (rt *restImport2PC) primaryState(ctx context.Context, jobID string) (milvus.ImportState, int, error) {
	resp, err := rt.primary.GetBulkInsertState(ctx, rt.db, jobID)
	if err != nil {
		return "", 0, err
	}
	if resp == nil || (resp.Data.JobId != "" && resp.Data.JobId != jobID) {
		return "", 0, fmt.Errorf("restore: invalid primary import response for job %s", jobID)
	}
	phase := milvus.ImportState(resp.Data.State)
	err = milvus.ImportLookupError(jobID, resp.Code, resp.Message)
	if err == nil {
		switch phase {
		case milvus.ImportStatePending, milvus.ImportStateImporting, milvus.ImportStateUncommitted, milvus.ImportStateCommitting, milvus.ImportStateCompleted:
			return phase, resp.Data.Progress, nil
		case milvus.ImportStateFailed:
			err = milvus.ImportFailure(jobID, resp.Data.Reason)
		default:
			err = fmt.Errorf("restore: unknown state %q for primary import %s", phase, jobID)
		}
	}
	if rt.job.CommitRequested() && (errors.Is(err, milvus.ErrRemoteJobFailed) || errors.Is(err, milvus.ErrImportJobNotFound)) {
		err = fmt.Errorf("restore: primary import %s after commit was requested: %w", jobID, err)
	}
	return phase, 0, err
}

func (rt *restImport2PC) waitPrimaryReady(ctx context.Context, jobID string) (milvus.ImportState, error) {
	var phase milvus.ImportState
	state := func(ctx context.Context) (importJobState, error) {
		var progress int
		var err error
		phase, progress, err = rt.primaryState(ctx, jobID)
		if err != nil {
			if !rt.job.CommitRequested() && phase == milvus.ImportStateFailed && errors.Is(err, milvus.ErrRemoteJobFailed) {
				if abortErr := rt.abort(ctx, jobID); abortErr != nil {
					return importJobState{}, abortErr
				}
			}
			return importJobState{}, err
		}
		ready := phase == milvus.ImportStateUncommitted || phase == milvus.ImportStateCommitting || phase == milvus.ImportStateCompleted
		return importJobState{progress: progress, completed: ready}, nil
	}
	err := waitImportJob(ctx, rt.logger, rt.job, jobID, state)
	return phase, err
}

// mirrorState distinguishes an explicit missing ID (including a GC race in the
// failure reason) from a failed mirror or an inconclusive lookup.
func mirrorState(resp *milvus.GetProcessResp, jobID string) (milvus.ImportState, error) {
	if resp == nil || (resp.Data.JobId != "" && resp.Data.JobId != jobID) {
		return "", fmt.Errorf("invalid mirror response for job %s", jobID)
	}
	if err := milvus.ImportLookupError(jobID, resp.Code, resp.Message); err != nil {
		return "", err
	}
	state := milvus.ImportState(resp.Data.State)
	if state == milvus.ImportStateFailed {
		return state, milvus.ImportFailure(jobID, resp.Data.Reason)
	}
	return state, nil
}

func (rt *restImport2PC) waitSecondaryMirrored(ctx context.Context, secondary importSecondary, jobID string) error {
	if rt.enableResume && rt.job.MirrorCompleted(secondary.addr) {
		return nil
	}
	var missingSince time.Time
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		resp, err := secondary.cli.GetBulkInsertState(ctx, rt.db, jobID)
		if err != nil {
			return fmt.Errorf("restore: get the mirrored import job %s on secondary %s: %w", jobID, secondary.addr, err)
		}
		state, err := mirrorState(resp, jobID)
		rt.logger.Info("secondary mirrored bulk insert task state", zap.String("secondary", secondary.addr),
			zap.String("job_id", jobID), zap.String("state", string(state)), zap.Error(err))
		switch {
		// Legacy restores allow an empty state while replication catches up.
		case errors.Is(err, milvus.ErrImportJobNotFound), err == nil && state == "" && !rt.enableResume:
			if missingSince.IsZero() {
				missingSince = time.Now()
			}
			if time.Since(missingSince) >= _secondaryMirrorTimeout {
				timeout := fmt.Errorf("restore: secondary %s has not reported mirrored import job %s for %s; check the secondaries option and the replication stream", secondary.addr, jobID, _secondaryMirrorTimeout)
				return errors.Join(timeout, err)
			}
		case errors.Is(err, milvus.ErrRemoteJobFailed):
			return rt.abortBeforeCommit(ctx, jobID, fmt.Errorf("restore: mirrored import job %s on secondary %s failed: %w", jobID, secondary.addr, err))
		case err != nil:
			return fmt.Errorf("restore: get the mirrored import job %s on secondary %s: %w", jobID, secondary.addr, err)
		case state == milvus.ImportStateCompleted:
			if rt.enableResume {
				rt.job.MarkMirrorCompleted(secondary.addr)
			}
			return nil
		case state == milvus.ImportStateCommitting:
			if rt.enableResume {
				rt.job.MarkCommitRequested()
			}
			return nil
		case state == milvus.ImportStateUncommitted:
			return nil
		case state == milvus.ImportStatePending, state == milvus.ImportStateImporting, !rt.enableResume:
			missingSince = time.Time{}
		default:
			return fmt.Errorf("restore: unknown state %q for mirrored import job %s on secondary %s", state, jobID, secondary.addr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Resumable jobs retain their ID regardless of the abort outcome. The caller
// of waitReady handles abort for jobs without Resume enabled.
func (rt *restImport2PC) abortBeforeCommit(ctx context.Context, jobID string, cause error) error {
	if rt.enableResume && !rt.job.CommitRequested() {
		return errors.Join(cause, rt.abort(ctx, jobID))
	}
	return cause
}

// waitSecondaryCommitted waits until the secondary's mirrored job is
// Completed. The commit itself is not issued against the secondary — the local
// commit's fence message travels the replication stream and commits the mirror
// — so all this wait can surface is a mirror that failed on its own or a
// commit that never arrived, both of which leave the two ends diverged.
func (rt *restImport2PC) waitSecondaryCommitted(ctx context.Context, secondary importSecondary, jobID string) error {
	// The local commit has just landed, so the mirror is expected to be mid
	// flight; the window starts right away, unlike the mirror wait's.
	pendingSince := time.Now()
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		resp, err := secondary.cli.GetBulkInsertState(ctx, rt.db, jobID)
		if err != nil {
			return fmt.Errorf("restore: get the mirrored import job %s on secondary %s after the local commit: %w", jobID, secondary.addr, err)
		}

		state, err := mirrorState(resp, jobID)
		if errors.Is(err, milvus.ErrRemoteJobFailed) {
			return fmt.Errorf("restore: the mirrored import job %s on secondary %s failed: %w; the local cluster has already committed it, so the two ends diverge", jobID, secondary.addr, err)
		}
		// Legacy polling gives an absent mirror the remaining commit window.
		// A Failed response carrying the same missing-ID reason is terminal.
		waitForMirror := !rt.enableResume && errors.Is(err, milvus.ErrImportJobNotFound) && resp.Data.State == ""
		if err != nil && !waitForMirror {
			return fmt.Errorf("restore: describe mirrored import on secondary %s after commit: %w", secondary.addr, err)
		}
		rt.logger.Info("secondary mirrored bulk insert task state",
			zap.String("secondary", secondary.addr),
			zap.String("job_id", jobID),
			zap.String("state", string(state)),
			zap.Int("progress", resp.Data.Progress))
		switch state {
		case milvus.ImportStateCompleted:
			return nil
		default:
			if time.Since(pendingSince) >= _secondaryCommitTimeout {
				return fmt.Errorf("restore: the mirrored import job %s on secondary %s is still %q "+
					"%s after the local commit — the commit fence travels the replication "+
					"stream and commits the mirror when it lands, so the ends should converge "+
					"on their own; check the replication stream's lag to the secondary",
					jobID, secondary.addr, resp.Data.State, _secondaryCommitTimeout)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
