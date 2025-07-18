package check

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/core"
)

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check if the connects is right.",

		Run: func(cmd *cobra.Command, args []string) {
			params := opt.InitGlobalVars()

			backupContext := core.CreateBackupContext(context.Background(), params)

			resp := backupContext.Check(context.Background())
			cmd.Println(resp)
		},
	}

	cmd.AddCommand(newBackupYamlCmd(opt))

	return cmd
}
