package migrate

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/client/cloud"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/core/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type Task struct {
	logger *zap.Logger

	taskID string

	cloudCli  cloud.Client
	clusterID string

	taskMgr *taskmgr.Mgr

	backupDir     string
	backupBucket  string
	backupStorage storage.Client

	copySem *semaphore.Weighted
}

func NewTask(taskID, backupName, clusterID string, params *paramtable.BackupParams) (*Task, error) {
	logger := log.L().With(zap.String("task_id", taskID))
	cloudCli := cloud.NewClient(params.CloudConfig.Address, params.CloudConfig.APIKey)

	backupStorage, err := storage.NewBackupStorage(context.Background(), params)
	if err != nil {
		return nil, fmt.Errorf("migrate: new backup storage %w", err)
	}
	backupDir := mpath.BackupDir(params.MinioCfg.BackupRootPath, backupName)

	return &Task{
		logger: logger,

		taskID: taskID,

		cloudCli:  cloudCli,
		clusterID: clusterID,
		taskMgr:   taskmgr.DefaultMgr,

		backupDir:     backupDir,
		backupBucket:  params.MinioCfg.BackupBucketName,
		backupStorage: backupStorage,

		copySem: semaphore.NewWeighted(params.BackupCfg.BackupCopyDataParallelism),
	}, nil
}

func (t *Task) Prepare(ctx context.Context) error {
	t.logger.Info("try to read backup meta info")
	backupInfo, err := meta.Read(ctx, t.backupDir, t.backupStorage)
	if err != nil {
		return fmt.Errorf("migrate: read backup meta info %w", err)
	}

	t.taskMgr.AddMigrateTask(t.taskID, backupInfo.GetSize())

	return nil
}

type stage struct {
	storage storage.Client

	resp *cloud.ApplyStageResp
}

func (t *Task) applyStage(ctx context.Context) (stage, error) {
	t.logger.Info("apply stage")
	// use taskID as dir
	resp, err := t.cloudCli.ApplyStage(ctx, t.clusterID, t.taskID)
	if err != nil {
		return stage{}, fmt.Errorf("migrate: apply stage %w", err)
	}
	t.logger.Info("apply stage done")

	cfg := &storage.Config{
		Provider: resp.Cloud,
		Endpoint: resp.Endpoint,
		AK:       resp.Credentials.TmpAK,
		SK:       resp.Credentials.TmpSK,
		Token:    resp.Credentials.SessionToken,
		UseSSL:   true,
		Bucket:   resp.BucketName,
	}

	cli, err := storage.NewClient(ctx, *cfg)
	if err != nil {
		return stage{}, fmt.Errorf("migrate: new storage client %w", err)
	}

	return stage{storage: cli, resp: resp}, nil
}

func (t *Task) copyToCloud(ctx context.Context, stage stage) error {
	t.logger.Info("copy backup to cloud")
	opt := storage.CopyPrefixOpt{
		Src:        t.backupStorage,
		Dest:       stage.storage,
		SrcPrefix:  t.backupDir,
		DestPrefix: stage.resp.UploadPath,
		Sem:        t.copySem,
		OnSuccess: func(copyAttr storage.CopyAttr) {
			t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.IncMigrateCopiedSize(copyAttr.Src.Length))
		},
		CopyByServer: true,
	}

	copyTask := storage.NewCopyPrefixTask(opt)
	if err := copyTask.Execute(ctx); err != nil {
		return fmt.Errorf("migrate: copy to cloud %w", err)
	}
	t.logger.Info("copy backup to cloud done")

	return nil
}

func (t *Task) startMigrate(ctx context.Context, stage stage) error {
	src := cloud.Source{
		AccessKey:  stage.resp.Credentials.TmpAK,
		SecretKey:  stage.resp.Credentials.TmpSK,
		Token:      stage.resp.Credentials.SessionToken,
		BucketName: stage.resp.BucketName,
		Cloud:      stage.resp.Cloud,
		Path:       stage.resp.UploadPath,
		Region:     stage.resp.Region,
	}

	dest := cloud.Destination{ClusterID: t.clusterID}
	jobID, err := t.cloudCli.Migrate(ctx, src, dest)
	if err != nil {
		return fmt.Errorf("migrate: start migrate %w", err)
	}
	t.logger.Info("trigger migrate job", zap.String("job_id", jobID))
	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateJobID(jobID))

	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	s, err := t.applyStage(ctx)
	if err != nil {
		return fmt.Errorf("migrate: apply stage %w", err)
	}

	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyStart())
	if err := t.copyToCloud(ctx, s); err != nil {
		t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyComplete())
		return fmt.Errorf("migrate: copy to cloud %w", err)
	}
	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyComplete())

	if err := t.startMigrate(ctx, s); err != nil {
		return fmt.Errorf("migrate: start migrate %w", err)
	}

	return nil
}
