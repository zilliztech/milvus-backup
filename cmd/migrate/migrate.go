package migrate

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core/meta/taskmgr"
	"github.com/zilliztech/milvus-backup/core/migrate"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

type options struct {
	backupName string
	clusterID  string
}

func (o *options) validate() error {
	if o.clusterID == "" {
		return errors.New("cluster id is required")
	}

	if o.backupName == "" {
		return errors.New("backup name is required")
	}

	return nil
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	taskID := uuid.NewString()
	task, err := migrate.NewTask(taskID, o.backupName, o.clusterID, params)
	if err != nil {
		return err
	}

	if err := task.Prepare(context.TODO()); err != nil {
		return err
	}

	if err := task.Execute(context.TODO()); err != nil {
		return err
	}

	taskStat, err := taskmgr.DefaultMgr.GetMigrateTask(taskID)
	if err != nil {
		return err
	}

	cmd.Printf("Successfully triggered migration with backup name: %s target cluster: %s \n", o.backupName, o.clusterID)
	cmd.Printf("migration job id: %s. \n", taskStat.MigrateJobID())
	cmd.Printf("You can check the progress of the migration job in Zilliz Cloud console.\n")

	return nil
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.backupName, "name", "n", "", "need to migrate backup name")
	cmd.Flags().StringVarP(&o.clusterID, "cluster_id", "c", "", "target cluster id")
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate the backup data to zilliz cloud cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			var params paramtable.BackupParams
			params.GlobalInitWithYaml(opt.Config)
			params.Init()

			if err := o.validate(); err != nil {
				return err
			}

			err := o.run(cmd, &params)
			cobra.CheckErr(err)

			return nil
		},
	}

	o.addFlags(cmd)

	return cmd
}
