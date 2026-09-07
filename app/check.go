package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// Check verifies that a backup run would work: Milvus answers, both storages
// are reachable, and an object can be copied from the Milvus storage to the
// backup storage.
type Check struct {
	grpc          milvus.Grpc
	milvusStorage storage.Client
	backupStorage storage.Client

	milvusRootPath string
	backupRootPath string
	transferMode   string
}

// NewCheck builds the usecase from config, creating the Milvus gRPC client and
// both storage clients itself so the transports never import internal/storage
// or internal/client/milvus. The clients are created per call; sharing them
// across calls is a lifecycle decision this layer deliberately does not make.
func NewCheck(ctx context.Context, params *v2.Config) (*Check, error) {
	grpc, err := milvus.NewGrpc(&params.Milvus)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	milvusStorage, err := storage.NewMilvusStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	backupStorage, err := storage.NewBackupStorage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("app: %w", err)
	}

	return &Check{
		grpc:           grpc,
		milvusStorage:  milvusStorage,
		backupStorage:  backupStorage,
		milvusRootPath: params.Milvus.Storage.RootPath.Val,
		backupRootPath: params.Backup.Storage.RootPath.Val,
		transferMode:   params.Transfer.Mode.Val,
	}, nil
}

// Execute runs the checks in order and writes the result report to output.
// It stops at the first failed check: a later check against a broken
// dependency would only add noise. The report is plain text by design —
// how a transport presents it (stdout, HTTP body) is the transport's call.
func (uc *Check) Execute(ctx context.Context, output io.Writer) error {
	version, err := uc.checkMilvusConnect(ctx)
	if err != nil {
		return fmt.Errorf("app: check milvus connect %w", err)
	}

	empty, err := uc.checkMilvusStorage(ctx)
	if err != nil {
		return fmt.Errorf("app: check milvus storage %w", err)
	}

	if err := uc.checkBackupStorage(ctx); err != nil {
		return fmt.Errorf("app: check backup storage %w", err)
	}

	if err := uc.checkWriteAndCopy(ctx); err != nil {
		return fmt.Errorf("app: check write and copy %w", err)
	}

	if err := uc.writeResult(output, version, empty); err != nil {
		return fmt.Errorf("app: write info %w", err)
	}

	return nil
}

func (uc *Check) checkMilvusConnect(ctx context.Context) (string, error) {
	log.Info("check milvus connect")
	version, err := uc.grpc.GetVersion(ctx)
	if err != nil {
		return "", fmt.Errorf("app: get milvus version: %w", err)
	}

	log.Info("check milvus connect success", zap.String("version", version))
	return version, nil
}

func (uc *Check) writeResult(output io.Writer, version string, milvusEmpty bool) error {
	var buff []byte

	buff = append(buff, []byte("\nMilvus version: "+version+"\n")...)

	if milvusEmpty {
		buff = append(buff, []byte("\n")...)
		buff = append(buff, []byte("!!! Milvus root path is empty !!! \n")...)
		buff = append(buff, []byte("If your Milvus instance is expected to have data,\n")...)
		buff = append(buff, []byte("please check your minio configuration.\n")...)
		buff = append(buff, []byte("(address / bucket / rootPath).\n")...)
	} else {
		buff = append(buff, []byte("\nSuccess!\n")...)
	}

	if _, err := io.Copy(output, bytes.NewReader(buff)); err != nil {
		return fmt.Errorf("app: write result %w", err)
	}

	return nil
}

func (uc *Check) checkMilvusStorage(ctx context.Context) (bool, error) {
	log.Info("check milvus storage")
	files, _, err := storage.ListPrefixFlat(ctx, uc.milvusStorage, mpath.MilvusRootDir(uc.milvusRootPath), true)
	if err != nil {
		return false, fmt.Errorf("app: list milvus root dir %w", err)
	}

	empty := len(files) == 0
	if empty {
		log.Warn("check milvus storage: milvus root dir is empty.")
	}

	log.Info("connect to milvus storage success", zap.Int("file_num", len(files)))
	return empty, nil
}

func (uc *Check) checkBackupStorage(ctx context.Context) error {
	log.Info("check backup storage")
	_, _, err := storage.ListPrefixFlat(ctx, uc.backupStorage, mpath.BackupRootDir(uc.backupRootPath), false)
	if err != nil {
		return fmt.Errorf("app: list backup root dir %w", err)
	}
	log.Info("connect to backup storage success")
	return nil
}

func (uc *Check) checkWriteAndCopy(ctx context.Context) error {
	log.Info("check write and copy")
	srcKey := path.Join(uc.milvusRootPath, "milvus_backup_check_src_"+uuid.NewString())
	destKey := path.Join(uc.backupRootPath, "milvus_backup_check_dst_"+uuid.NewString())
	if err := storage.Write(ctx, uc.milvusStorage, srcKey, []byte{1}); err != nil {
		return fmt.Errorf("app: write to milvus storage %w", err)
	}
	defer func() {
		log.Info("delete src file", zap.String("key", srcKey))
		if err := uc.milvusStorage.DeleteObject(ctx, srcKey); err != nil {
			log.Error("failed to delete check file", zap.String("path", srcKey), zap.Error(err))
		}
	}()
	log.Info("write to milvus storage success", zap.String("key", srcKey))

	log.Info("copy from milvus storage to backup storage")
	streaming := storage.UseStreaming(uc.transferMode, uc.milvusStorage.Config(), uc.backupStorage.Config())
	log.Info("try to copy",
		zap.String("transfer_mode", uc.transferMode),
		zap.Bool("streaming", streaming),
		zap.String("dest_key", destKey))
	opt := storage.CopyPrefixOpt{
		Src:        uc.milvusStorage,
		Dest:       uc.backupStorage,
		SrcPrefix:  srcKey,
		DestPrefix: destKey,
		Sem:        semaphore.NewWeighted(1),
		Streaming:  streaming,
	}
	task := storage.NewCopyPrefixTask(opt)
	if err := task.Execute(ctx); err != nil {
		return fmt.Errorf("app: copy from milvus storage to backup storage %w", err)
	}

	defer func() {
		log.Info("delete dest file", zap.String("key", destKey))
		if err := uc.backupStorage.DeleteObject(ctx, destKey); err != nil {
			log.Error("failed to delete check file", zap.String("path", destKey), zap.Error(err))
		}
	}()
	log.Info("copy from milvus storage to backup storage success")

	expected, err := storage.ExpectedDestObjects(ctx, uc.milvusStorage, srcKey, destKey)
	if err != nil {
		return fmt.Errorf("app: build expected for copy verify %w", err)
	}
	verifyTask := storage.NewVerifyPrefixTask(storage.VerifyPrefixOpt{Cli: uc.backupStorage, Prefix: destKey, Expected: expected})
	if err := verifyTask.Execute(ctx); err != nil {
		return fmt.Errorf("app: verify copy to backup storage %w", err)
	}
	log.Info("verify copy to backup storage success")

	return nil
}
