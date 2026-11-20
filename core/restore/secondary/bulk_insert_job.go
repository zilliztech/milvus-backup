package secondary

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

const (
	_bulkInsertTimeout          = 60 * time.Minute
	_commitImportMessageType    = 45
	_commitImportMessageVersion = 2
)

// Overridden in tests so the unknown-job path does not take a minute to
// exercise.
var (
	_bulkInsertCheckInterval = 3 * time.Second
	// How long an import job may stay unknown to the target before the restore
	// gives up on it. The job id travels inside the import message, so the
	// target only knows the job once that message has been applied; the window
	// covers the message still being in flight.
	_bulkInsertUnknownJobTimeout = 1 * time.Minute
)

// bulkInsertJob is the lifecycle of one import job submitted over the replay
// stream, which both data paths share once their import message is out: the
// job id travels inside that message, so the target learns the job only when
// the message is applied; the restore then watches it through the target's
// import api and commits it with a broadcast commit message if the target
// holds it at Uncommitted. Targets without explicit commit finish automatically.
// What differs between the binlog and the snapshot path is only
// what the import message asks for, never how the job it creates is seen
// through.
type bulkInsertJob struct {
	streamCheckpoint *milvus.StreamCheckpoint
	submit           func(context.Context, int64) error
	dbName           string
	collectionID     int64
	vchannels        []string

	streamCli  milvus.Stream
	restfulCli milvus.Restful

	logger *zap.Logger
}

func importKey(id int64) string { return fmt.Sprintf("import/%d", id) }
func commitKey(id int64) string { return fmt.Sprintf("commit/%d", id) }

// start submits a new import or returns the initial state of an adopted job.
func (j bulkInsertJob) start(ctx context.Context, op *jobstate.SecondaryImportProgress) (milvus.ImportState, error) {
	if op.State == jobstate.SecondaryImportCompleted {
		return milvus.ImportStateCompleted, nil
	}
	if op.JobID != 0 &&
		op.State != jobstate.SecondaryImportFailed &&
		j.streamCheckpoint != nil && j.streamCheckpoint.Sent(importKey(op.JobID)) {
		state, _, err := j.importProgress(ctx, strconv.FormatInt(op.JobID, 10))
		if !errors.Is(err, milvus.ErrRemoteJobFailed) {
			if errors.Is(err, milvus.ErrImportJobNotFound) {
				return "", nil // Keep the ID and wait for the job to become visible.
			}
			return state, err
		}
		op.State = jobstate.SecondaryImportFailed
	}
	if op.JobID == 0 || op.State == jobstate.SecondaryImportFailed {
		// Only a failure confirmed before waiting authorizes a replacement.
		// Replaying a failed import is at-least-once, even after commit:
		// published data or compaction effects may remain on the target.
		op.JobID = rand.Int64N(1<<62) + 1
		op.State = jobstate.SecondaryImportPending
	}
	// The saved ID is Pending before submit; interruption before registration
	// reuses this ID on the next Resume instead of allocating another one.
	return "", j.submit(ctx, op.JobID)
}

func (j bulkInsertJob) wait(ctx context.Context, op *jobstate.SecondaryImportProgress, state milvus.ImportState) error {
	if op.State == jobstate.SecondaryImportCompleted {
		return nil
	}
	err := j.check(ctx, op, state)
	if err == nil {
		op.State = jobstate.SecondaryImportCompleted
	} else if errors.Is(err, milvus.ErrRemoteJobFailed) {
		// A failure while waiting ends this execution. A later explicit Resume
		// can replace it; query errors never authorize a new job ID.
		op.State = jobstate.SecondaryImportFailed
	}
	return err
}

// check sees the job through to Completed: a job the target already finished
// needs nothing more, one held at Uncommitted is committed here, and one
// another coordinator is committing is only waited for.
func (j bulkInsertJob) check(ctx context.Context, op *jobstate.SecondaryImportProgress, state milvus.ImportState) error {
	jobID := op.JobID
	if state != milvus.ImportStateCompleted && state != milvus.ImportStateUncommitted && state != milvus.ImportStateCommitting {
		var err error
		state, err = j.waitReadyToCommit(ctx, jobID)
		if err != nil {
			return err
		}
	}

	switch state {
	case milvus.ImportStateCompleted:
		return nil
	case milvus.ImportStateUncommitted:
		if err := j.sendCommitMsg(ctx, jobID); err != nil {
			return fmt.Errorf("secondary: send commit import msg: %w", err)
		}
	case milvus.ImportStateCommitting:
		// Another coordinator already initiated the commit; only wait for completion.
	default:
		return fmt.Errorf("secondary: unexpected bulk insert state %s", state)
	}

	op.State = jobstate.SecondaryImportCommitting
	return j.waitCompleted(ctx, jobID)
}

func (j bulkInsertJob) waitReadyToCommit(ctx context.Context, jobID int64) (milvus.ImportState, error) {
	return j.waitState(ctx, jobID, func(state milvus.ImportState) bool {
		return state == milvus.ImportStateCompleted ||
			state == milvus.ImportStateUncommitted ||
			state == milvus.ImportStateCommitting
	})
}

func (j bulkInsertJob) waitCompleted(ctx context.Context, jobID int64) error {
	_, err := j.waitState(ctx, jobID, func(state milvus.ImportState) bool {
		return state == milvus.ImportStateCompleted
	})
	return err
}

func (j bulkInsertJob) importProgress(ctx context.Context, jobID string) (milvus.ImportState, int, error) {
	resp, err := j.restfulCli.GetBulkInsertState(ctx, j.dbName, jobID)
	if err != nil {
		return "", 0, fmt.Errorf("secondary: get bulk insert state: %w", err)
	}
	if resp == nil {
		return "", 0, fmt.Errorf("secondary: target returned no usable status for import job %s", jobID)
	}
	if id := resp.Data.JobId; id != "" && id != jobID {
		return "", 0, fmt.Errorf("secondary: queried import job %s but received job %s", jobID, id)
	}
	if err := milvus.ImportLookupError(jobID, resp.Code, resp.Message); err != nil {
		return "", 0, err
	}
	state := milvus.ImportState(resp.Data.State)
	if state == milvus.ImportStateFailed {
		return "", 0, fmt.Errorf("secondary: bulk insert failed: %w", milvus.ImportFailure(jobID, resp.Data.Reason))
	}
	return state, resp.Data.Progress, nil
}

func (j bulkInsertJob) waitState(
	ctx context.Context,
	jobID int64,
	done func(milvus.ImportState) bool,
) (milvus.ImportState, error) {
	jobIDStr := strconv.FormatInt(jobID, 10)

	var lastProgress int
	// Stays zero until the target reports an empty state, so the window only
	// starts once there is something to wait out.
	var unknownSince time.Time
	lastUpdateTime := time.Now()
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		state, currentProgress, err := j.importProgress(ctx, jobIDStr)
		if err != nil && !errors.Is(err, milvus.ErrImportJobNotFound) {
			return "", err
		}
		j.logger.Info("bulk insert task state", zap.Int64("job_id", jobID),
			zap.String("state", string(state)),
			zap.Int("progress", currentProgress))
		// A replicated import can be invisible until its broadcast is applied.
		// An expired record looks the same, so keep its ID across Resume.
		if state == "" {
			if unknownSince.IsZero() {
				unknownSince = time.Now()
			} else if time.Since(unknownSince) >= _bulkInsertUnknownJobTimeout {
				return "", fmt.Errorf("secondary: the target does not know import job %d after %s, "+
					"so this batch cannot be confirmed either way. Two things produce this and "+
					"they are not distinguishable from here: the import message was never "+
					"applied, or the job ran and the target already dropped its record. A record "+
					"is kept only for dataCoord.import.taskRetention past the job finishing, and "+
					"success and failure are dropped alike, so a short retention loses the "+
					"outcome rather than reporting it. Check the target's log for job %d before "+
					"concluding anything about this batch, and raise "+
					"dataCoord.import.taskRetention if it is below the default of 10800",
					jobID, _bulkInsertUnknownJobTimeout, jobID)
			}
		} else {
			unknownSince = time.Time{}
		}
		if done(state) {
			if state == milvus.ImportStateCompleted {
				j.logger.Info("bulk insert task success", zap.Int64("job_id", jobID))
			}
			return state, nil
		}

		if currentProgress > lastProgress {
			lastProgress = currentProgress
			lastUpdateTime = time.Now()
		} else if time.Since(lastUpdateTime) >= _bulkInsertTimeout {
			j.logger.Warn("bulk insert task no progress for too long, may milvus is not healthy",
				zap.Int64("job_id", jobID),
				zap.Duration("timeout", _bulkInsertTimeout))
			lastUpdateTime = time.Now()
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (j bulkInsertJob) sendCommitMsg(ctx context.Context, jobID int64) error {
	header := protowire.AppendTag(nil, 1, protowire.VarintType)
	header = protowire.AppendVarint(header, uint64(j.collectionID))
	header = protowire.AppendTag(header, 2, protowire.VarintType)
	header = protowire.AppendVarint(header, uint64(jobID))

	broadcastHeader, err := message.EncodeProto(&messagespb.BroadcastHeader{
		Vchannels: j.vchannels,
	})
	if err != nil {
		return fmt.Errorf("secondary: encode commit import broadcast header: %w", err)
	}

	properties := map[string]string{
		"_t":  strconv.Itoa(_commitImportMessageType),
		"_v":  strconv.Itoa(_commitImportMessageVersion),
		"_h":  base64.StdEncoding.EncodeToString(header),
		"_bh": broadcastHeader,
	}

	if err := j.streamCli.Send(ctx, commitKey(jobID), func(uint64) []message.MutableMessage {
		broadcast := message.NewBroadcastMutableMessageBeforeAppend(nil, properties).
			WithBroadcastID(rand.Uint64())
		return broadcast.SplitIntoMutableMessage()
	}); err != nil {
		return fmt.Errorf("secondary: broadcast commit import: %w", err)
	}

	return nil
}
