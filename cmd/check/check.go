package check

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check if the connects is right.",

		Run: func(cmd *cobra.Command, args []string) {
			var params paramtable.BackupParams
			params.GlobalInitWithYaml(opt.Config)
			params.Init()

			ctx := context.Background()
			backupContext := core.CreateBackupContext(ctx, &params)

			resp := backupContext.Check(ctx)
			cmd.Println(resp)
		},
	}

	cmd.AddCommand(newBackupYamlCmd(opt))

	return cmd
}
