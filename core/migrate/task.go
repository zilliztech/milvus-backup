package migrate

import (
	"context"
	"fmt"
	"time"

	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
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

var _ minioCred.Provider = (*stage)(nil)

type stage struct {
	clusterID string
	prefix    string

	cloudCli cloud.Client

	expiration time.Time
	resp       *cloud.ApplyStageResp

	logger *zap.Logger
}

func newStage(clusterID, prefix string, cloudCli cloud.Client) *stage {
	return &stage{
		clusterID: clusterID,
		prefix:    prefix,

		cloudCli: cloudCli,

		logger: log.L().With(zap.String("cluster_id", clusterID), zap.String("prefix", prefix)),
	}
}

func (s *stage) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return s.Retrieve()
}

func (s *stage) Retrieve() (minioCred.Value, error) {
	s.logger.Debug("retrieve stage credential")
	if !s.IsExpired() {
		s.logger.Debug("stage credential not expired, return cached credential")
		return minioCred.Value{
			AccessKeyID:     s.resp.Credentials.TmpAK,
			SecretAccessKey: s.resp.Credentials.TmpSK,
			SessionToken:    s.resp.Credentials.SessionToken,
			Expiration:      s.expiration,
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.logger.Info("stage credential expired, apply new credential")
	if err := s.apply(ctx); err != nil {
		return minioCred.Value{}, err
	}

	return minioCred.Value{
		AccessKeyID:     s.resp.Credentials.TmpAK,
		SecretAccessKey: s.resp.Credentials.TmpSK,
		SessionToken:    s.resp.Credentials.SessionToken,
		Expiration:      s.expiration,
	}, nil
}

func (s *stage) IsExpired() bool {
	return time.Now().After(s.expiration)
}

func (s *stage) apply(ctx context.Context) error {
	s.logger.Info("apply stage, it will take about 10 seconds")
	start := time.Now()
	resp, err := s.cloudCli.ApplyStage(ctx, s.clusterID, s.prefix)
	if err != nil {
		return fmt.Errorf("migrate: apply stage %w", err)
	}
	s.logger.Info("apply stage done", zap.Duration("cost", time.Since(start)))

	expiration, err := time.Parse(time.RFC3339, resp.Credentials.ExpireTime)
	if err != nil {
		return fmt.Errorf("migrate: parse expire time %w", err)
	}

	s.resp = resp
	s.expiration = expiration

	return nil
}

func newStageStorage(ctx context.Context, stage *stage) (storage.Client, error) {
	cfg := storage.Config{
		Provider:   stage.resp.Cloud,
		Endpoint:   stage.resp.Endpoint,
		UseSSL:     true,
		Credential: storage.Credential{Type: storage.MinioCredProvider, MinioCredProvider: stage},
		Bucket:     stage.resp.BucketName,
	}

	cli, err := storage.NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("migrate: new stage storage %w", err)
	}

	return cli, nil
}

type Task struct {
	logger *zap.Logger

	taskID string

	cloudCli  cloud.Client
	clusterID string

	taskMgr *taskmgr.Mgr

	backupDir     string
	backupStorage storage.Client

	stage   *stage
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
		backupStorage: backupStorage,

		// use taskID as stage prefix
		stage:   newStage(clusterID, taskID, cloudCli),
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

// copyToCloud copy all backup files to cloud.
// We use a separate copy logic here instead of using Storage.CopyPrefixTask
// because the temporary AK/SK credentials obtained from apply stage have an expiration time.
// If the copy fails due to expired credentials, we need to reapply for new credentials.
// Implementing this retry logic in the copy task would make it too complex and specialized.
func (t *Task) copyToCloud(ctx context.Context) error {
	t.logger.Info("copy backup to cloud")

	destCli, err := newStageStorage(ctx, t.stage)
	if err != nil {
		return fmt.Errorf("migrate: new stage storage %w", err)
	}

	opt := storage.CopyPrefixOpt{
		Src:        t.backupStorage,
		Dest:       destCli,
		SrcPrefix:  t.backupDir,
		DestPrefix: t.stage.resp.UploadPath,
		Sem:        t.copySem,

		TraceFn: func(size int64, cost time.Duration) {
			t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.IncMigrateCopiedSize(size, cost))
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

func (t *Task) startMigrate(ctx context.Context) error {
	src := cloud.Source{StageName: t.stage.resp.StageName, DataPath: t.stage.prefix}

	t.logger.Info("trigger migrate job")
	dest := cloud.Destination{ClusterID: t.clusterID}
	jobID, err := t.cloudCli.Migrate(ctx, src, dest)
	if err != nil {
		return fmt.Errorf("migrate: start migrate %w", err)
	}
	t.logger.Info("trigger migrate job done", zap.String("job_id", jobID))
	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateJobID(jobID))

	return nil
}

func (t *Task) Execute(ctx context.Context) error {
	if err := t.stage.apply(ctx); err != nil {
		return fmt.Errorf("migrate: apply stage %w", err)
	}

	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyStart())
	if err := t.copyToCloud(ctx); err != nil {
		t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyComplete())
		return fmt.Errorf("migrate: copy to cloud %w", err)
	}
	t.taskMgr.UpdateMigrateTask(t.taskID, taskmgr.SetMigrateCopyComplete())

	if err := t.startMigrate(ctx); err != nil {
		return fmt.Errorf("migrate: start migrate %w", err)
	}

	return nil
}
