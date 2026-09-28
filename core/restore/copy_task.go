package restore

import (
	"context"
	"fmt"
	"path"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// copyTask stages binlog dirs from the backup storage into a temp dir on the
// target Milvus storage, so the bulk insert API can read them. It is the
// prepare half of an import job: each job gets its own copyTask with a unique
// temp dir and cleans it up itself once the import is over.
type copyTask struct {
	src       storage.Client
	dest      storage.Client
	backupDir string

	tempDir string

	sem *semaphore.Weighted

	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath). Empty unless the target provider is local.
	milvusRootPath string

	logger *zap.Logger
}

// Execute copies every non-empty dir to the temp dir, verifies each copy, and
// returns the dirs rewritten to point at the copies.
func (t *copyTask) Execute(ctx context.Context, dirs []partitionDir) ([]partitionDir, error) {
	copied := make([]partitionDir, len(dirs))
	for i, dir := range dirs {
		if dir.insertLogDir != "" {
			insertLogDir, err := t.copyPrefix(ctx, dir.insertLogDir)
			if err != nil {
				return nil, fmt.Errorf("restore_collection: copy insert log dir: %w", err)
			}
			dir.insertLogDir = insertLogDir
		}

		if dir.deltaLogDir != "" {
			deltaLogDir, err := t.copyPrefix(ctx, dir.deltaLogDir)
			if err != nil {
				return nil, fmt.Errorf("restore_collection: copy delta log dir: %w", err)
			}
			dir.deltaLogDir = deltaLogDir
		}

		copied[i] = dir
	}

	return copied, nil
}

// Cleanup removes the temp dir. It is safe to call after a partial Execute:
// whatever was copied sits under the same temp dir.
func (t *copyTask) Cleanup(ctx context.Context) error {
	t.logger.Info("delete temporary file", zap.String("dir", t.tempDir))
	if err := storage.DeletePrefix(ctx, t.dest, t.destKey(t.tempDir)); err != nil {
		return fmt.Errorf("restore_collection: failed to delete temporary file: %w", err)
	}

	return nil
}

func (t *copyTask) copyPrefix(ctx context.Context, srcPrefix string) (string, error) {
	dest := path.Join(t.tempDir, strings.Replace(srcPrefix, t.backupDir, "", 1)) + "/"
	destKey := t.destKey(dest)
	opt := storage.CopyPrefixOpt{
		Sem:        t.sem,
		Src:        t.src,
		Dest:       t.dest,
		SrcPrefix:  srcPrefix,
		DestPrefix: destKey,
		Streaming:  true,
	}

	t.logger.Info("copy temporary restore file", zap.String("src", srcPrefix), zap.String("dest", destKey))
	task := storage.NewCopyPrefixTask(opt)
	if err := task.Execute(ctx); err != nil {
		return "", fmt.Errorf("restore_collection: copy temporary restore file: %w", err)
	}

	expected, err := storage.ExpectedDestObjects(ctx, t.src, srcPrefix, destKey)
	if err != nil {
		return "", fmt.Errorf("restore_collection: build expected for copy verify: %w", err)
	}
	verifyTask := storage.NewVerifyPrefixTask(storage.VerifyPrefixOpt{Cli: t.dest, Prefix: destKey, Expected: expected})
	if err := verifyTask.Execute(ctx); err != nil {
		return "", fmt.Errorf("restore_collection: verify temporary restore file: %w", err)
	}
	t.logger.Info("copy temporary restore file success", zap.String("src", srcPrefix), zap.String("dest", destKey))

	return dest, nil
}

// isLocal reports whether the restore target keeps its data on the local
// filesystem, where LocalClient keys are absolute paths.
func (t *copyTask) isLocal() bool {
	return t.dest.Config().Provider == v2.ProviderLocal
}

// destKey maps a bucket-relative restore key onto the key LocalClient reads and
// writes. A local target resolves keys against the directory milvus-backup
// reaches the target's storage at (milvus.storage.rootPath); any other provider
// keeps the bucket-relative key as-is. A trailing slash is preserved: the copy
// task maps each object by replacing the source prefix with this key.
func (t *copyTask) destKey(key string) string {
	if !t.isLocal() || key == "" {
		return key
	}
	return joinLocal(t.milvusRootPath, key)
}

// joinLocal prefixes p with base, keeping p's trailing slash and avoiding a
// doubled separator. base is empty only when a config omitted the directory.
func joinLocal(base, p string) string {
	if base == "" {
		return p
	}
	return strings.TrimSuffix(base, "/") + "/" + p
}
