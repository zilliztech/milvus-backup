package check

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

type TaskArgs struct {
	Params *cfg.Config

	Grpc milvus.Grpc

	MilvusStorage storage.Client
	BackupStorage storage.Client

	Output io.Writer
}

type Task struct {
	logger *zap.Logger

	params *cfg.Config

	grpc milvus.Grpc

	milvusStorage storage.Client
	backupStorage storage.Client

	output io.Writer
}

func NewTask(args TaskArgs) *Task {
	return &Task{
		logger:        log.L(),
		params:        args.Params,
		grpc:          args.Grpc,
		milvusStorage: args.MilvusStorage,
		backupStorage: args.BackupStorage,
		output:        args.Output,
	}
}

func (t *Task) checkMilvusConnect(ctx context.Context) (string, error) {
	t.logger.Info("check milvus connect")
	version, err := t.grpc.GetVersion(ctx)
	if err != nil {
		return "", fmt.Errorf("check: get milvus version: %w", err)
	}

	t.logger.Info("check milvus connect success", zap.String("version", version))
	return version, nil
}

func (t *Task) writeResult(version string, milvusEmpty bool) error {
	var buff []byte

	buff = append(buff, []byte("\nMilvus version: "+version+"\n")...)

	buff = append(buff, []byte("Storage:\n")...)
	buff = append(buff, []byte("  milvus-storage-type: "+t.params.Minio.StorageType.Val+"\n")...)
	buff = append(buff, []byte("  milvus-bucket: "+t.params.Minio.BucketName.Val+"\n")...)
	buff = append(buff, []byte("  milvus-rootpath: "+t.params.Minio.RootPath.Val+"\n")...)
	buff = append(buff, []byte("  backup-storage-type: "+t.params.Minio.BackupStorageType.Val+"\n")...)
	buff = append(buff, []byte("  backup-bucket: "+t.params.Minio.BackupBucketName.Val+"\n")...)
	buff = append(buff, []byte("  backup-rootpath: "+t.params.Minio.BackupRootPath.Val+"\n")...)

	if milvusEmpty {
		buff = append(buff, []byte("\n")...)
		buff = append(buff, []byte("!!! Milvus root path is empty !!! \n")...)
		buff = append(buff, []byte("If your Milvus instance is expected to have data,\n")...)
		buff = append(buff, []byte("please check your minio configuration.\n")...)
		buff = append(buff, []byte("(address / bucket / rootPath).\n")...)
	} else {
		buff = append(buff, []byte("\nSuccess!\n")...)
	}

	if _, err := io.Copy(t.output, bytes.NewReader(buff)); err != nil {
		return fmt.Errorf("check: write result %w", err)
	}

	return nil
}

func (t *Task) checkMilvusStorage(ctx context.Context) (bool, error) {
	t.logger.Info("check milvus storage")
	files, _, err := storage.ListPrefixFlat(ctx, t.milvusStorage, mpath.MilvusRootDir(t.params.Minio.RootPath.Val), true)
	if err != nil {
		return false, fmt.Errorf("check: list milvus root dir %w", err)
	}

	empty := len(files) == 0
	if empty {
		t.logger.Warn("check milvus storage: milvus root dir is empty.")
	}

	t.logger.Info("connect to milvus storage success", zap.Int("file_num", len(files)))
	return empty, nil
}

func (t *Task) checkBackupStorage(ctx context.Context) error {
	t.logger.Info("check backup storage")
	_, _, err := storage.ListPrefixFlat(ctx, t.backupStorage, mpath.BackupRootDir(t.params.Minio.BackupRootPath.Val), false)
	if err != nil {
		return fmt.Errorf("check: list backup root dir %w", err)
	}
	t.logger.Info("connect to backup storage success")
	return nil
}

func (t *Task) checkWriteAndCopy(ctx context.Context) error {
	t.logger.Info("check write and copy")
	srcKey := path.Join(t.params.Minio.RootPath.Val, "milvus_backup_check_src_"+uuid.NewString())
	destKey := path.Join(t.params.Minio.BackupRootPath.Val, "milvus_backup_check_dst_"+uuid.NewString())
	if err := storage.Write(ctx, t.milvusStorage, srcKey, []byte{1}); err != nil {
		return fmt.Errorf("check: write to milvus storage %w", err)
	}
	defer func() {
		t.logger.Info("delete src file", zap.String("key", srcKey))
		if err := t.milvusStorage.DeleteObject(ctx, srcKey); err != nil {
			t.logger.Error("failed to delete check file", zap.String("path", srcKey), zap.Error(err))
		}
	}()
	t.logger.Info("write to milvus storage success", zap.String("key", srcKey))

	t.logger.Info("copy from milvus storage to backup storage")
	crossStorage := t.params.Minio.CrossStorage.Val
	if t.backupStorage.Config().Provider != t.milvusStorage.Config().Provider {
		crossStorage = true
	}
	t.logger.Info("try to copy", zap.Bool("cross_storage", crossStorage), zap.String("dest_key", destKey))
	opt := storage.CopyPrefixOpt{
		Src:          t.milvusStorage,
		Dest:         t.backupStorage,
		SrcPrefix:    srcKey,
		DestPrefix:   destKey,
		Sem:          semaphore.NewWeighted(1),
		CopyByServer: crossStorage,
	}
	task := storage.NewCopyPrefixTask(opt)
	if err := task.Execute(ctx); err != nil {
		return fmt.Errorf("check: copy from milvus storage to backup storage %w", err)
	}

	defer func() {
		t.logger.Info("delete dest file", zap.String("key", destKey))
		if err := t.backupStorage.DeleteObject(ctx, destKey); err != nil {
			t.logger.Error("failed to delete check file", zap.String("path", destKey), zap.Error(err))
		}
	}()

	t.logger.Info("copy from milvus storage to backup storage success")
	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	version, err := t.checkMilvusConnect(ctx)
	if err != nil {
		return fmt.Errorf("check: check milvus connect %w", err)
	}

	empty, err := t.checkMilvusStorage(ctx)
	if err != nil {
		return fmt.Errorf("check: check milvus storage %w", err)
	}

	if err := t.checkBackupStorage(ctx); err != nil {
		return fmt.Errorf("check: check backup storage %w", err)
	}

	if err := t.checkWriteAndCopy(ctx); err != nil {
		return fmt.Errorf("check: check write and copy %w", err)
	}

	if err := t.writeResult(version, empty); err != nil {
		return fmt.Errorf("check: write info %w", err)
	}

	return nil
}
