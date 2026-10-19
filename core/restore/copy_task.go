package restore

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// copyTask stages binlog dirs from the backup storage into a temp dir on the
// target Milvus storage, so the bulk insert API can read them. Each import
// task gets its own copyTask with a unique temp dir, and removes the dir
// itself once the import is over: the copy task has no lifecycle of its own.
type copyTask struct {
	src       storage.Client
	dest      storage.Client
	backupDir string

	tempDir string

	sem *semaphore.Weighted

	keepTempFiles bool

	// milvusRootPath is where milvus-backup reaches the target's local storage
	// directory (milvus.storage.rootPath). Empty unless the target provider is local.
	milvusRootPath string

	logger *zap.Logger
}

// newCopyTask builds the staging copy task of one import job, with a temp dir
// unique to the job so the task cleans up after itself. It returns nil when
// the plan decided the backup is imported in place.
func newCopyTask(args importTaskArgs) *copyTask {
	if !args.staging {
		return nil
	}

	tempDir := fmt.Sprintf("restore-temp-%s-%s-%s-%s/",
		args.taskID, args.target.DBName(), args.target.CollName(), uuid.NewString())
	return &copyTask{
		src:       args.backupStorage,
		dest:      args.milvusStorage,
		backupDir: args.backupDir,
		tempDir:   tempDir,
		sem:       args.copySem,

		keepTempFiles: args.keepTempFiles,

		milvusRootPath: args.milvusRootPath,

		logger: args.logger,
	}
}

// Execute copies every non-empty dir to the temp dir, verifies each copy, and
// returns the dirs rewritten to point at the copies. A nil copy task is a
// no-op pass-through: the plan decided the backup is imported in place, so
// the dirs already sit in the shared bucket and are handed over as-is.
func (t *copyTask) Execute(ctx context.Context, dirs []partitionDir) ([]partitionDir, error) {
	if t == nil {
		return dirs, nil
	}

	copied := make([]partitionDir, len(dirs))
	for i, dir := range dirs {
		if dir.insertLogDir != "" {
			insertLogDir, err := t.copyPrefix(ctx, dir.insertLogDir)
			if err != nil {
				return nil, fmt.Errorf("restore: copy insert log dir: %w", err)
			}
			dir.insertLogDir = insertLogDir
		}

		if dir.deltaLogDir != "" {
			deltaLogDir, err := t.copyPrefix(ctx, dir.deltaLogDir)
			if err != nil {
				return nil, fmt.Errorf("restore: copy delta log dir: %w", err)
			}
			dir.deltaLogDir = deltaLogDir
		}

		copied[i] = dir
	}

	return copied, nil
}

func (t *copyTask) copyPrefix(ctx context.Context, srcPrefix string) (string, error) {
	dest := path.Join(t.tempDir, strings.Replace(srcPrefix, t.backupDir, "", 1)) + "/"
	destKey := destKey(t.dest, t.milvusRootPath, dest)
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
		return "", fmt.Errorf("restore: copy temporary restore file: %w", err)
	}

	expected, err := storage.ExpectedDestObjects(ctx, t.src, srcPrefix, destKey)
	if err != nil {
		return "", fmt.Errorf("restore: build expected for copy verify: %w", err)
	}
	verifyTask := storage.NewVerifyPrefixTask(storage.VerifyPrefixOpt{Cli: t.dest, Prefix: destKey, Expected: expected})
	if err := verifyTask.Execute(ctx); err != nil {
		return "", fmt.Errorf("restore: verify temporary restore file: %w", err)
	}
	t.logger.Info("copy temporary restore file success", zap.String("src", srcPrefix), zap.String("dest", destKey))

	return dest, nil
}

// Cleanup removes the staged copy once the import is over, succeeded or not:
// the import has either consumed the data or never will. A copy that failed
// midway is safe too, whatever was copied sits under the same temp dir. A nil
// copy task means the backup was imported in place and there is nothing to
// remove.
func (t *copyTask) Cleanup(ctx context.Context) {
	if t == nil {
		return
	}
	if t.keepTempFiles {
		t.logger.Info("skip clean temporary files")
		return
	}

	t.logger.Info("delete temporary file", zap.String("dir", t.tempDir))
	if err := storage.DeletePrefix(ctx, t.dest, destKey(t.dest, t.milvusRootPath, t.tempDir)); err != nil {
		t.logger.Warn("clean temporary restore files failed", zap.Error(err))
	}
}

// destKey maps a bucket-relative restore key onto the key the target's storage
// client reads and writes. A local target resolves keys against the directory
// milvus-backup reaches the target's storage at (milvus.storage.rootPath); any
// other provider keeps the bucket-relative key as-is. A trailing slash is
// preserved: the copy task maps each object by replacing the source prefix
// with this key.
func destKey(cli storage.Client, rootPath, key string) string {
	if cli.Config().Provider != cfg.ProviderLocal || key == "" {
		return key
	}
	return joinLocal(rootPath, key)
}

// joinLocal prefixes p with base, keeping p's trailing slash and avoiding a
// doubled separator. base is empty only when a config omitted the directory.
func joinLocal(base, p string) string {
	if base == "" {
		return p
	}
	return strings.TrimSuffix(base, "/") + "/" + p
}
