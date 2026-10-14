package restore

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

// _peerMirrorTimeout is how long a peer may take to know the mirrored import
// job at all. The mirror is created when the replication stream delivers the
// import message, which happens around the submit — long before the local job
// reaches Uncommitted — so by the time the wait starts the mirror should
// already be there, and the window covers the stragglers and a stream that is
// catching up after a pause.
const _peerMirrorTimeout = 5 * time.Minute

// _peerCommitTimeout is how long a peer's mirror may take to finish after the
// local commit. Nobody drives the peer's commit: a replication target refuses
// client-side DDL, and the local commit's fence message travels the stream and
// commits the mirror there, so this window covers stream lag plus the peer's
// own flush-and-commit across every vchannel of the job.
const _peerCommitTimeout = 10 * time.Minute

// importViaRESTFul2PCTask imports a set of dirs through the v2 restful bulk
// insert API in two phases: the job is submitted with auto_commit=false and
// held at Uncommitted until every peer cluster's mirrored job — same job id,
// created by the replication stream — is Uncommitted too; only then is the job
// committed on the local cluster, and the commit fence it broadcasts
// replicates and commits every mirror. Being two-phase is the type's declared
// job, so the peer clients are a plain field; the orchestration layer is what
// knows which addresses are the peers.
type importViaRESTFul2PCTask struct {
	importer *restfulImporter
	peers    []importPeer
}

func (rt *importViaRESTFul2PCTask) Execute(ctx context.Context) error {
	defer rt.importer.cleanup(ctx)

	if err := rt.importer.stage(ctx); err != nil {
		return err
	}

	jobID, err := rt.importer.submit2PC(ctx)
	if err != nil {
		return err
	}
	// From here on the job is staged on the local cluster and, once the
	// replication stream delivers it, on the peers. A failure before the commit
	// aborts the local job; the abort's rollback message replicates like the
	// commit would and fails the mirrors, so the peers are never called
	// directly — a replication target refuses client-side DDL anyway. Past the
	// local commit aborting is no longer possible.
	committed := false
	defer func() {
		if !committed {
			rt.importer.abort(context.WithoutCancel(ctx), jobID)
		}
	}()

	if err := rt.importer.waitUncommitted(ctx, jobID); err != nil {
		return err
	}

	for _, peer := range rt.peers {
		if err := rt.waitPeerMirrored(ctx, peer, jobID); err != nil {
			return err
		}
	}

	if err := rt.importer.commit(ctx, jobID); err != nil {
		return err
	}
	committed = true

	for _, peer := range rt.peers {
		if err := rt.waitPeerCommitted(ctx, peer, jobID); err != nil {
			return err
		}
	}

	return rt.importer.waitCompleted(ctx, jobID)
}

// waitPeerMirrored waits until the peer's mirrored job also holds at
// Uncommitted. Committing or Completed mirrors are accepted too: a retried task
// meets them when the commit of the crashed first run already replicated.
func (rt *importViaRESTFul2PCTask) waitPeerMirrored(ctx context.Context, peer importPeer, jobID string) error {
	db := rt.importer.target.DBName()

	// Stays zero until the peer reports an empty state, so the window only
	// starts once there is something to wait out.
	var unknownSince time.Time
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		resp, err := peer.cli.GetBulkInsertState(ctx, db, jobID)
		if err != nil {
			return fmt.Errorf("restore: get the mirrored import job %s on peer %s: %w", jobID, peer.addr, err)
		}

		rt.importer.logger.Info("peer mirrored bulk insert task state",
			zap.String("peer", peer.addr),
			zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateFailed):
			return fmt.Errorf("restore: the mirrored import job %s on peer %s failed: %s",
				jobID, peer.addr, resp.Data.Reason)
		case string(milvus.ImportStateUncommitted),
			string(milvus.ImportStateCommitting),
			string(milvus.ImportStateCompleted):
			return nil
		case "":
			// An empty state means the peer has no such job: the mirror has not
			// been delivered yet. It never turns into anything on its own once
			// the stream is past the message, so the wait is bounded.
			if unknownSince.IsZero() {
				unknownSince = time.Now()
			} else if time.Since(unknownSince) >= _peerMirrorTimeout {
				return fmt.Errorf("restore: the peer %s still does not know the mirrored import "+
					"job %s after %s — the peer is expected to be a replication target of "+
					"the cluster being restored into, mirroring its import jobs under the same "+
					"job id; check that --peers points at that cluster and that the replication "+
					"stream is running", peer.addr, jobID, _peerMirrorTimeout)
			}
		default:
			unknownSince = time.Time{}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// waitPeerCommitted waits until the peer's mirrored job is Completed. The
// commit itself is not issued against the peer — the local commit's fence
// message travels the replication stream and commits the mirror — so all this
// wait can surface is a mirror that failed on its own or a commit that never
// arrived, both of which leave the two ends diverged.
func (rt *importViaRESTFul2PCTask) waitPeerCommitted(ctx context.Context, peer importPeer, jobID string) error {
	db := rt.importer.target.DBName()

	// The local commit has just landed, so the mirror is expected to be mid
	// flight; the window starts right away, unlike the mirror wait's.
	pendingSince := time.Now()
	ticker := time.NewTicker(_bulkInsertCheckInterval)
	defer ticker.Stop()

	for {
		resp, err := peer.cli.GetBulkInsertState(ctx, db, jobID)
		if err != nil {
			return fmt.Errorf("restore: get the mirrored import job %s on peer %s: %w", jobID, peer.addr, err)
		}

		rt.importer.logger.Info("peer mirrored bulk insert task state",
			zap.String("peer", peer.addr),
			zap.String("job_id", jobID),
			zap.String("state", resp.Data.State),
			zap.Int("progress", resp.Data.Progress))
		switch resp.Data.State {
		case string(milvus.ImportStateCompleted):
			return nil
		case string(milvus.ImportStateFailed):
			return fmt.Errorf("restore: the mirrored import job %s on peer %s failed: %s; "+
				"the local cluster has already committed it, and a failed mirror does not "+
				"take the replicated commit, so the two ends diverge", jobID, peer.addr, resp.Data.Reason)
		default:
			if time.Since(pendingSince) >= _peerCommitTimeout {
				return fmt.Errorf("restore: the mirrored import job %s on peer %s is still %q "+
					"%s after the local commit — the commit fence travels the replication "+
					"stream and commits the mirror when it lands, so the ends should converge "+
					"on their own; check the replication stream's lag to the peer",
					jobID, peer.addr, resp.Data.State, _peerCommitTimeout)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
