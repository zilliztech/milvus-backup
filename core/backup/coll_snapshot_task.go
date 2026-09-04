package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

const (
	_snapshotPollInterval = 3 * time.Second

	// A drop right after the export job goes terminal can still be rejected: DataCoord
	// releases the pin it took for the copy on its own reconcile tick, not inline with
	// the state change the poll observed.
	_snapshotDropRetry    = 10
	_snapshotDropInterval = time.Second
)

type collSnapshotTask struct {
	taskID string

	collRef      collref.Name
	snapshotName string
	target       snapshotTarget

	pollInterval time.Duration

	grpc milvus.Grpc

	taskMgr     *taskmgr.Mgr
	metaBuilder *metaBuilder

	logger *zap.Logger
}

func newCollSnapshotTask(collRef collref.Name, snapshotName string, target snapshotTarget, args collTaskArgs) *collSnapshotTask {
	logger := log.L().With(
		zap.String("task_id", args.TaskID),
		zap.String("coll", collRef.String()),
		zap.String("snapshot_name", snapshotName))

	return &collSnapshotTask{
		taskID:       args.TaskID,
		collRef:      collRef,
		snapshotName: snapshotName,
		target:       target,
		pollInterval: _snapshotPollInterval,
		grpc:         args.Grpc,
		taskMgr:      args.TaskMgr,
		metaBuilder:  args.MetaBuilder,
		logger:       logger,
	}
}

// Execute freezes the collection into a snapshot, has Milvus copy it into the backup
// directory, and records where the bundle landed. No bytes move through this process.
func (st *collSnapshotTask) Execute(ctx context.Context) error {
	st.logger.Info("start to backup collection with snapshot")
	st.taskMgr.UpdateBackupTask(st.taskID, taskmgr.SetBackupCollDMLPrepare(st.collRef))

	// Compaction protection is left off: the snapshot holds its files against GC for
	// as long as it exists, and it exists only for this export.
	if err := st.grpc.CreateSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName, 0); err != nil {
		return fmt.Errorf("backup: create snapshot: %w", err)
	}
	defer st.dropSnapshot(ctx)

	// The snapshot's own boundary, read before the export so a failed export still
	// leaves nothing behind to interpret.
	desc, err := st.grpc.DescribeSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName)
	if err != nil {
		return fmt.Errorf("backup: describe snapshot: %w", err)
	}

	jobID, err := st.grpc.ExportSnapshot(ctx, milvus.ExportSnapshotInput{
		DB:             st.collRef.DBName(),
		CollectionName: st.collRef.CollName(),
		SnapshotName:   st.snapshotName,
		TargetPath:     st.target.Path,
		ExternalSpec:   st.target.ExternalSpec,
	})
	if err != nil {
		return fmt.Errorf("backup: export snapshot: %w", err)
	}
	st.logger.Info("snapshot export job accepted", zap.Int64("job_id", jobID))

	info, err := st.waitExport(ctx, jobID)
	if err != nil {
		return err
	}

	metadataPath, err := st.target.metadataPath(info.GetSnapshotMetadataUri())
	if err != nil {
		return err
	}

	snapshotBackup := &backuppb.SnapshotBackupInfo{
		MetadataPath: metadataPath,
		TotalFiles:   info.GetTotalFiles(),
		TotalBytes:   info.GetTotalBytes(),
	}
	if err := st.metaBuilder.addSnapshot(st.collRef, snapshotBackup, uint64(desc.GetCreateTs())); err != nil {
		return fmt.Errorf("backup: add snapshot meta: %w", err)
	}

	st.taskMgr.UpdateBackupTask(st.taskID, taskmgr.SetBackupCollDMLDone(st.collRef))
	st.logger.Info("backup collection with snapshot done",
		zap.String("metadata_path", metadataPath),
		zap.Int64("total_files", info.GetTotalFiles()),
		zap.Int64("total_bytes", info.GetTotalBytes()))

	return nil
}

// waitExport polls the export job until it is done. A job that failed comes back
// through the info rather than as an error from the state query, so both are checked.
func (st *collSnapshotTask) waitExport(ctx context.Context, jobID int64) (*milvuspb.ExportSnapshotInfo, error) {
	ticker := time.NewTicker(st.pollInterval)
	defer ticker.Stop()

	// The job reports files, not bytes: total_bytes is only known once the bundle is
	// published. Progress is a ratio either way, and nothing outside the task manager
	// reads these as a size.
	var reportedTotal, copied int64
	for {
		info, err := st.grpc.GetExportSnapshotState(ctx, jobID)
		if err != nil {
			return nil, fmt.Errorf("backup: get export snapshot state: %w", err)
		}

		if total := info.GetTotalFiles(); total != reportedTotal {
			reportedTotal = total
			st.taskMgr.UpdateBackupTask(st.taskID, taskmgr.SetBackupCollDMLExecuting(st.collRef, total))
		}
		if done := info.GetCopiedFiles(); done > copied {
			st.taskMgr.UpdateBackupTask(st.taskID, taskmgr.IncBackupCollCopiedSize(st.collRef, done-copied, 0))
			copied = done
		}

		switch info.GetState() {
		case milvuspb.ExportSnapshotState_ExportSnapshotCompleted:
			return info, nil
		case milvuspb.ExportSnapshotState_ExportSnapshotFailed:
			return nil, fmt.Errorf("backup: snapshot export job %d failed: %s", jobID, info.GetReason())
		}

		st.logger.Debug("waiting for snapshot export",
			zap.Int64("job_id", jobID),
			zap.Int32("progress", info.GetProgress()),
			zap.Int64("copied_files", info.GetCopiedFiles()),
			zap.Int64("total_files", info.GetTotalFiles()))

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("backup: wait snapshot export job %d: %w", jobID, ctx.Err())
		case <-ticker.C:
		}
	}
}

// dropSnapshot releases the snapshot once the bundle is written. The bundle is
// self-contained, so nothing that was backed up depends on the snapshot surviving —
// a snapshot left behind only holds its collection's files against GC.
func (st *collSnapshotTask) dropSnapshot(ctx context.Context) {
	// The task's own context may already be canceled by the failure that brought us
	// here, and the snapshot still has to go.
	ctx = context.WithoutCancel(ctx)

	for attempt := range _snapshotDropRetry {
		err := st.grpc.DropSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName)
		if err == nil {
			return
		}

		st.logger.Warn("drop snapshot failed, will retry",
			zap.Int("attempt", attempt+1),
			zap.Error(err))

		timer := time.NewTimer(_snapshotDropInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	// Nothing else can clean this up: the export job that pinned it is gone, and the
	// backup meta does not record snapshot names.
	st.logger.Error("could not drop snapshot, it holds its collection's files against gc until removed by hand",
		zap.String("snapshot_name", st.snapshotName))
}
