package backup

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/snapshotbundle"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type collSnapshotTask struct {
	taskID string

	collRef      collref.Name
	snapshotName string
	target       snapshotTarget

	milvusStorage  storage.Client
	milvusRootPath string
	backupStore    bundleStore

	// exportNamespace isolates everything this task writes under the bundle
	// root, one random namespace per collection export — the same shape the
	// server's export flow gives each job.
	exportNamespace string
	exportDir       string
	exportRootURI   string

	streaming bool
	copySem   *semaphore.Weighted

	tracker     *jobstate.BackupCollDMLTracker
	metaBuilder *metaBuilder

	grpc milvus.Grpc

	logger *zap.Logger
}

func newCollSnapshotTask(collRef collref.Name, snapshotName string, target snapshotTarget, args collTaskArgs) *collSnapshotTask {
	return newCollSnapshotTaskWithNamespace(collRef, snapshotName, target, args, uuid.NewString())
}

func newCollSnapshotTaskWithNamespace(collRef collref.Name, snapshotName string, target snapshotTarget, args collTaskArgs, namespace string) *collSnapshotTask {
	return &collSnapshotTask{
		taskID:          args.TaskID,
		collRef:         collRef,
		snapshotName:    snapshotName,
		target:          target,
		milvusStorage:   args.MilvusStorage,
		milvusRootPath:  args.MilvusRootPath,
		backupStore:     bundleStore{cli: args.BackupStorage},
		exportNamespace: namespace,
		exportDir:       snapshotbundle.NamespaceBundleRoot(target.dir, namespace),
		exportRootURI:   target.rootURI + "/" + snapshotbundle.ExportNamespaceSubPath + "/" + namespace,
		streaming:       args.Streaming,
		copySem:         args.Throttling.CopySem,
		tracker:         args.tracker.NewCollDMLTracker(collRef),
		metaBuilder:     args.MetaBuilder,
		grpc:            args.Grpc,
		logger: log.L().With(
			zap.String("task_id", args.TaskID),
			zap.String("coll", collRef.String()),
			zap.String("snapshot_name", snapshotName)),
	}
}

// Execute freezes the collection into a snapshot, copies its files into the
// backup directory through backup's own storage clients, and records where the
// bundle landed. It is the job's data-half task for the snapshot format, so
// the data-half state settles here.
func (st *collSnapshotTask) Execute(ctx context.Context) error {
	st.logger.Info("start to backup collection with snapshot")
	st.tracker.SetPrepare()

	if err := st.execute(ctx); err != nil {
		st.tracker.SetFail(err)
		return err
	}

	st.tracker.SetSuccess()
	return nil
}

func (st *collSnapshotTask) execute(ctx context.Context) error {
	// Compaction protection is left off: the snapshot holds its files against GC for
	// as long as it exists, and it exists only for this export.
	if err := st.grpc.CreateSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName, 0); err != nil {
		return fmt.Errorf("backup: create snapshot: %w", err)
	}
	// A snapshot pins its collection's files in Milvus until dropped, so both ends of
	// its lifetime are logged.
	st.logger.Info("snapshot created")
	defer st.dropSnapshot(ctx)

	// The snapshot's own boundary, read before the copy so a failed copy still
	// leaves nothing behind to interpret. S3Location points at the snapshot's
	// metadata JSON in the milvus bucket, which names every file the snapshot
	// references.
	desc, err := st.grpc.DescribeSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName)
	if err != nil {
		return fmt.Errorf("backup: describe snapshot: %w", err)
	}

	snapshot, err := snapshotbundle.LoadSnapshotData(
		ctx,
		bundleStore{cli: st.milvusStorage},
		desc.GetS3Location(),
		true,
	)
	if err != nil {
		return fmt.Errorf("backup: load snapshot data: %w", err)
	}
	st.logger.Info("snapshot data loaded",
		zap.Int("segment_count", len(snapshot.Segments)),
		zap.Int("index_count", len(snapshot.Indexes)))

	files, err := st.enumerateBundleFiles(ctx, snapshot)
	if err != nil {
		return fmt.Errorf("backup: enumerate snapshot files: %w", err)
	}
	st.tracker.SetExecuting(files.totalBytes)

	opt := storage.CopyObjectsOpt{
		Src:       st.milvusStorage,
		Dest:      st.backupStore.cli,
		Attrs:     files.attrs,
		Streaming: st.streaming,
		Sem:       st.copySem,
		TraceFn:   st.tracker.AddCopiedSize,
	}
	if err := storage.NewCopyObjectsTask(opt).Execute(ctx); err != nil {
		return fmt.Errorf("backup: copy snapshot files: %w", err)
	}

	_, metadataPath, err := st.writeBundle(ctx, snapshot, files)
	if err != nil {
		return err
	}

	snapshotBackup := &backuppb.SnapshotBackupInfo{
		MetadataPath: metadataPath,
		TotalFiles:   int64(len(files.attrs)),
		TotalBytes:   files.totalBytes,
	}
	if err := st.metaBuilder.addSnapshot(st.collRef, snapshotBackup, uint64(desc.GetCreateTs()), snapshot.Collection.GetSchema()); err != nil {
		return fmt.Errorf("backup: record snapshot bundle in meta: %w", err)
	}

	st.logger.Info("backup collection with snapshot done",
		zap.String("metadata_path", metadataPath),
		zap.Int64("total_files", snapshotBackup.GetTotalFiles()),
		zap.Int64("total_bytes", snapshotBackup.GetTotalBytes()))

	return nil
}

// writeBundle rewrites the snapshot's path references into the bundle and
// publishes the bundle metadata. The bundle root URI is the identity restore
// resolves the recorded path against, so it is embedded as the bundle's own
// location.
func (st *collSnapshotTask) writeBundle(ctx context.Context, snapshot *snapshotbundle.SnapshotData, files *bundleFileSet) (string, string, error) {
	_, metadataKey := snapshotbundle.GetSnapshotPaths(
		st.exportDir, snapshot.SnapshotInfo.GetCollectionId(), snapshot.SnapshotInfo.GetId())
	metadataPath := path.Join(mpath.BundleDirName, strings.TrimPrefix(metadataKey, st.target.dir+"/"))
	metadataURI := st.exportRootURI + "/" + strings.TrimPrefix(metadataKey, st.exportDir+"/")

	exported, err := snapshotbundle.RewriteSnapshotWithMapping(snapshot, files.mapping, st.exportDir, metadataURI)
	if err != nil {
		return "", "", fmt.Errorf("backup: rewrite snapshot paths: %w", err)
	}

	_, _, err = snapshotbundle.SaveBundle(
		ctx,
		st.backupStore,
		exported,
		st.exportDir,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	)
	if err != nil {
		return "", "", fmt.Errorf("backup: write bundle metadata: %w", err)
	}
	return metadataURI, metadataPath, nil
}

// dropSnapshot releases the snapshot once the bundle is written. The bundle is
// self-contained, so nothing that was backed up depends on the snapshot surviving —
// a snapshot left behind only holds its collection's files against GC.
func (st *collSnapshotTask) dropSnapshot(ctx context.Context) {
	// The task's own context may already be canceled by the failure that brought us
	// here, and the snapshot still has to go.
	ctx = context.WithoutCancel(ctx)

	if err := st.grpc.DropSnapshot(ctx, st.collRef.DBName(), st.collRef.CollName(), st.snapshotName); err != nil {
		// Nothing else can clean this up: the backup meta does not record snapshot names.
		st.logger.Error("could not drop snapshot, it holds its collection's files against gc until removed by hand",
			zap.String("snapshot_name", st.snapshotName),
			zap.Error(err))
		return
	}

	st.logger.Info("snapshot dropped")
}
