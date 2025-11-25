package migrate

import (
	"context"
	"fmt"
	"sync"
	"time"

	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/client/cloud"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

var (
	_ minioCred.Provider = (*volume)(nil)
	_ oauth2.TokenSource = (*volume)(nil)
)

type volume struct {
	clusterID string
	prefix    string

	cloudCli cloud.Client

	currentResp struct {
		mu         sync.RWMutex
		expiration time.Time
		resp       *cloud.ApplyVolumeResp
	}

	logger *zap.Logger
}

func newVolume(clusterID, prefix string, cloudCli cloud.Client) *volume {
	return &volume{
		clusterID: clusterID,
		prefix:    prefix,

		cloudCli: cloudCli,

		logger: log.L().With(zap.String("cluster_id", clusterID), zap.String("prefix", prefix)),
	}
}

func (v *volume) RetrieveWithCredContext(_ *minioCred.CredContext) (minioCred.Value, error) {
	return v.Retrieve()
}

func (v *volume) minioValue() minioCred.Value {
	return minioCred.Value{
		AccessKeyID:     v.currentResp.resp.Credentials.TmpAK,
		SecretAccessKey: v.currentResp.resp.Credentials.TmpSK,
		SessionToken:    v.currentResp.resp.Credentials.SessionToken,
		Expiration:      v.currentResp.expiration,
	}
}

func (v *volume) oauth2Token() *oauth2.Token {
	return &oauth2.Token{AccessToken: v.currentResp.resp.Credentials.SessionToken, Expiry: v.currentResp.expiration}
}

func (v *volume) Retrieve() (minioCred.Value, error) {
	v.logger.Debug("retrieve stage credential")
	v.currentResp.mu.RLock()
	if !v.IsExpired() {
		v.logger.Debug("stage credential not expired, return cached credential")
		v.currentResp.mu.RUnlock()
		return v.minioValue(), nil
	}
	v.currentResp.mu.RUnlock()

	// double check.
	// maybe other thread has already applied for a new credential
	v.currentResp.mu.Lock()
	defer v.currentResp.mu.Unlock()
	if !v.IsExpired() {
		return v.minioValue(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	v.logger.Info("stage credential expired, apply new credential")
	if err := v.apply(ctx); err != nil {
		return minioCred.Value{}, err
	}

	return v.minioValue(), nil
}

func (v *volume) IsExpired() bool { return time.Now().After(v.currentResp.expiration) }

func (v *volume) Token() (*oauth2.Token, error) {
	v.logger.Debug("get volume token")

	// first check if the token is still valid
	v.currentResp.mu.RLock()
	token := v.oauth2Token()
	if token.Valid() {
		v.currentResp.mu.RUnlock()
		v.logger.Debug("volume token not expired, return cached token")
		return token, nil
	}
	v.currentResp.mu.RUnlock()

	// double check, maybe other thread has already applied for a new credential
	v.currentResp.mu.Lock()
	defer v.currentResp.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token = v.oauth2Token()
	if token.Valid() {
		return token, nil
	}

	if err := v.apply(ctx); err != nil {
		return nil, fmt.Errorf("migrate: get volume token %w", err)
	}
	v.logger.Debug("get volume token done")

	return v.oauth2Token(), nil
}

func (v *volume) apply(ctx context.Context) error {
	v.logger.Info("apply volume, it will take about 10 seconds")
	start := time.Now()
	resp, err := v.cloudCli.ApplyVolume(ctx, v.clusterID, v.prefix)
	if err != nil {
		return fmt.Errorf("migrate: apply stage %w", err)
	}
	v.logger.Info("apply volume done", zap.Duration("cost", time.Since(start)))

	expiration, err := time.Parse(time.RFC3339, resp.Credentials.ExpireTime)
	if err != nil {
		return fmt.Errorf("migrate: parse expire time %w", err)
	}

	v.currentResp.resp = resp
	v.currentResp.expiration = expiration

	return nil
}

func newVolumeStorage(ctx context.Context, vol *volume) (storage.Client, error) {
	var cred storage.Credential
	switch vol.currentResp.resp.Cloud {
	case paramtable.CloudProviderGCP:
		cred = storage.Credential{Type: storage.OAuth2TokenSource, OAuth2TokenSource: vol}
	case paramtable.CloudProviderAWS:
		cred = storage.Credential{Type: storage.MinioCredProvider, MinioCredProvider: vol}
	default:
		return nil, fmt.Errorf("migrate: unsupported cloud provider %s", vol.currentResp.resp.Cloud)
	}

	cfg := storage.Config{
		Provider:   vol.currentResp.resp.Cloud,
		Endpoint:   vol.currentResp.resp.Endpoint,
		UseSSL:     true,
		Credential: cred,
		Bucket:     vol.currentResp.resp.BucketName,
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

	volume  *volume
	copySem *semaphore.Weighted
}

func NewTask(taskID, backupName, clusterID string, params *paramtable.BackupParams) (*Task, error) {
	logger := log.With(zap.String("task_id", taskID))
	cloudCli := cloud.NewClient(params.CloudConfig.Address, params.CloudConfig.APIKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backupStorage, err := storage.NewBackupStorage(ctx, &params.MinioCfg)
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
		volume:  newVolume(clusterID, taskID, cloudCli),
		copySem: semaphore.NewWeighted(params.BackupCfg.BackupCopyDataParallelism),
	}, nil
}

func (t *Task) Prepare(ctx context.Context) error {
	t.logger.Info("try to read backup meta info")
	backupInfo, err := meta.Read(ctx, t.backupStorage, t.backupDir)
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

	destCli, err := newVolumeStorage(ctx, t.volume)
	if err != nil {
		return fmt.Errorf("migrate: new stage storage %w", err)
	}

	opt := storage.CopyPrefixOpt{
		Src:        t.backupStorage,
		Dest:       destCli,
		SrcPrefix:  t.backupDir,
		DestPrefix: t.volume.currentResp.resp.UploadPath,
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
	src := cloud.Source{StageName: t.volume.currentResp.resp.StageName, DataPath: t.volume.prefix}

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
	if err := t.volume.apply(ctx); err != nil {
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
