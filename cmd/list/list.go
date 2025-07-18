package list

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type options struct {
	collectionName string
}

func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.collectionName, "collection", "c", "", "only list backups contains a certain collection")
}

func (o *options) run(cmd *cobra.Command, params *paramtable.BackupParams) error {
	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, params)

	backups := backupContext.ListBackups(ctx, &backuppb.ListBackupsRequest{
		CollectionName: o.collectionName,
	})

	cmd.Println(">> Backups:")
	for _, backup := range backups.GetData() {
		cmd.Println(backup.GetName())
	}

	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	var o options
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list subcommand shows all backup in the cluster.",

		Run: func(cmd *cobra.Command, args []string) {
			params := opt.InitGlobalVars()
			err := o.run(cmd, params)
			cobra.CheckErr(err)
		},
	}

	o.addFlags(cmd)

	return cmd
}
