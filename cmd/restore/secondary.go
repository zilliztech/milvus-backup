package restore

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/restore/secondary"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

type secondaryOption struct {
	backupName string

	sourceClusterID string
	targetClusterID string
}

func (o *secondaryOption) addCmd(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "backup name")

	cmd.Flags().StringVarP(&o.sourceClusterID, "source_cluster_id", "", "", "source cluster id")
	cmd.Flags().StringVarP(&o.targetClusterID, "target_cluster_id", "", "", "target cluster id")
}

func (o *secondaryOption) validate() error {
	if len(o.sourceClusterID) == 0 {
		return errors.New("empty source cluster id")
	}
	if len(o.targetClusterID) == 0 {
		return errors.New("empty target cluster id")
	}

	return nil
}

func (o *secondaryOption) toArgs(params *cfg.Config) (secondary.TaskArgs, error) {

	backupStorage, err := storage.NewBackupStorage(context.Background(), &params.Minio)
	if err != nil {
		return secondary.TaskArgs{}, fmt.Errorf("create backup storage: %w", err)
	}

	backupDir := mpath.BackupDir(params.Minio.BackupRootPath.Value(), o.backupName)
	exist, err := meta.Exist(context.Background(), backupStorage, backupDir)
	if err != nil {
		return secondary.TaskArgs{}, fmt.Errorf("check backup exist: %w", err)
	}
	if !exist {
		return secondary.TaskArgs{}, fmt.Errorf("backup %s not found", o.backupName)
	}

	backup, err := meta.Read(context.Background(), backupStorage, backupDir)
	if err != nil {
		return secondary.TaskArgs{}, fmt.Errorf("read backup meta: %w", err)
	}
	milvusClient, err := milvus.NewGrpc(&params.Milvus)
	if err != nil {
		return secondary.TaskArgs{}, fmt.Errorf("create milvus grpc client: %w", err)
	}

	restfulClient, err := milvus.NewRestful(&params.Milvus)
	if err != nil {
		return secondary.TaskArgs{}, fmt.Errorf("create milvus restful client: %w", err)
	}

	return secondary.TaskArgs{
		TaskID: uuid.NewString(),

		SourceClusterID: o.sourceClusterID,
		TargetClusterID: o.targetClusterID,

		Backup:        backup,
		Params:        params,
		BackupDir:     backupDir,
		BackupStorage: backupStorage,

		Restful: restfulClient,
		Grpc:    milvusClient,

		TaskMgr: taskmgr.DefaultMgr(),
	}, nil
}

func (o *secondaryOption) run(cmd *cobra.Command, params *cfg.Config) error {
	args, err := o.toArgs(params)
	if err != nil {
		return err
	}

	task, err := secondary.NewTask(args)
	if err != nil {
		return err
	}

	if err := task.Execute(context.Background()); err != nil {
		return err
	}

	return nil
}

func newSecondaryCmd(opt *root.Options) *cobra.Command {
	var o secondaryOption
	cmd := &cobra.Command{
		Use:   "secondary",
		Short: "restore backup to secondary cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()

			if err := o.validate(); err != nil {
				return err
			}

			err := o.run(cmd, params)
			cobra.CheckErr(err)

			return nil
		},
	}

	o.addCmd(cmd)

	return cmd
}
