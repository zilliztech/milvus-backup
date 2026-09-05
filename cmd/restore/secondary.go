package restore

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/root"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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

func (o *secondaryOption) toRequest() app.RestoreSecondaryRequest {
	return app.RestoreSecondaryRequest{
		TaskID: uuid.NewString(),

		BackupName: o.backupName,

		SourceClusterID: o.sourceClusterID,
		TargetClusterID: o.targetClusterID,
	}
}

func (o *secondaryOption) run(cmd *cobra.Command, params *v2.Config) error {
	job, err := app.NewRestoreSecondary(params).Start(context.Background(), o.toRequest())
	if err != nil {
		return err
	}

	if err := job.Execute(context.Background()); err != nil {
		return err
	}

	return nil
}

func newSecondaryCmd(opt *root.Options) *cobra.Command {
	var o secondaryOption
	cmd := &cobra.Command{
		Use:   "secondary",
		Short: "restore a backup to a secondary cluster",
		Long: "Restore a backup to a secondary cluster.\n\n" +
			"Create the backup with --backup_index_extra to restore its indexes too: secondary\n" +
			"restore replays the source cluster DDL verbatim and needs the index attributes that\n" +
			"only that flag reads from etcd. A backup created without it is refused: a collection\n" +
			"without its indexes cannot be loaded, so restoring one would produce a secondary\n" +
			"that can never serve after a failover.",
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
