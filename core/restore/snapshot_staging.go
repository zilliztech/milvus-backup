package restore

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// snapshotStaging stages snapshot bundles into the milvus storage: the restore
// api moves data with a server-side copy, a primitive that exists only inside
// one storage service, so a backup held on another service is streamed over by
// milvus-backup first — the same staging the binlog path does, with a whole
// bundle subtree for a unit.
type snapshotStaging struct {
	backupDir     string
	backupStorage storage.Client
	milvusStorage storage.Client
	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath). Empty unless the target provider is local.
	milvusRootPath string

	copySem       *semaphore.Weighted
	keepTempFiles bool
}

// bundleRoot returns the prefix of the bundle subtree holding a snapshot metadata file:
// the path up to the snapshots/<coll>/metadata/<file> anchor the server derives the
// bundle root from. The staged copy must cover everything under it, anchor included.
func bundleRoot(metadataPath string) (string, error) {
	p := strings.Trim(metadataPath, "/")
	idx := strings.Index(p, "snapshots/")
	if idx <= 0 {
		return "", fmt.Errorf("restore: snapshot metadata path %s does not sit under a bundle", metadataPath)
	}

	tail := strings.Split(p[idx:], "/")
	if len(tail) != 4 || tail[2] != "metadata" || !strings.HasSuffix(tail[3], ".json") {
		return "", fmt.Errorf("restore: snapshot metadata path %s does not have the snapshots/<coll>/metadata/<file> shape", metadataPath)
	}

	return p[:idx], nil
}

// stage copies the bundle subtree of the given metadata path into a temp dir on the
// milvus storage, verifies the copy, and returns the metadata's uri there plus a
// cleanup removing the temp dir. The bundle is location-independent: the server
// derives its root from the metadata uri and rebases every reference in it onto that
// root, so the staged copy restores exactly like the original.
func (s *snapshotStaging) stage(ctx context.Context, logger *zap.Logger, taskID string, target collref.Name, metadataPath string) (string, func(context.Context), error) {
	root, err := bundleRoot(metadataPath)
	if err != nil {
		return "", nil, err
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s-%s/", taskID, target.DBName(), target.CollName(), uuid.NewString())
	srcPrefix := path.Join(s.backupDir, root)
	destPrefix := tempDir + root

	// Cross-service there is no server-side copy to ride, so the bytes stream
	// through milvus-backup.
	logger.Info("stage snapshot bundle into the milvus storage",
		zap.String("src", srcPrefix), zap.String("dest", destPrefix))
	copyTask := storage.NewCopyPrefixTask(storage.CopyPrefixOpt{
		Sem:        s.copySem,
		Src:        s.backupStorage,
		Dest:       s.milvusStorage,
		SrcPrefix:  srcPrefix,
		DestPrefix: destKey(s.milvusStorage, s.milvusRootPath, destPrefix),
		Streaming:  true,
	})
	if err := copyTask.Execute(ctx); err != nil {
		return "", nil, fmt.Errorf("restore: stage snapshot bundle: %w", err)
	}

	expected, err := storage.ExpectedDestObjects(ctx, s.backupStorage, srcPrefix, destKey(s.milvusStorage, s.milvusRootPath, destPrefix))
	if err != nil {
		return "", nil, fmt.Errorf("restore: build expected for staged bundle verify: %w", err)
	}
	verifyTask := storage.NewVerifyPrefixTask(storage.VerifyPrefixOpt{
		Cli:      s.milvusStorage,
		Prefix:   destKey(s.milvusStorage, s.milvusRootPath, destPrefix),
		Expected: expected,
	})
	if err := verifyTask.Execute(ctx); err != nil {
		return "", nil, fmt.Errorf("restore: verify staged snapshot bundle: %w", err)
	}
	logger.Info("stage snapshot bundle success", zap.String("dest", destPrefix))

	// The staged copy sits in the milvus storage itself, so the uri needs no
	// external spec: Milvus resolves it with its own storage config.
	uri, err := storage.SnapshotStoreURI(s.milvusStorage.Config(), s.milvusStorage.Config(), tempDir+strings.Trim(metadataPath, "/"))
	if err != nil {
		return "", nil, err
	}

	cleanup := func(ctx context.Context) {
		if s.keepTempFiles {
			logger.Info("skip clean staged snapshot bundle", zap.String("dir", tempDir))
			return
		}
		if err := storage.DeletePrefix(ctx, s.milvusStorage, destKey(s.milvusStorage, s.milvusRootPath, tempDir)); err != nil {
			logger.Warn("clean staged snapshot bundle failed", zap.String("dir", tempDir), zap.Error(err))
		}
	}

	return uri, cleanup, nil
}
