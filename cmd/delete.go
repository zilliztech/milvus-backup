package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

var (
	deleteBackName string
)

var deleteBackupCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete subcommand delete backup by name.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		resp := backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
			BackupName: deleteBackName,
		})

		fmt.Println(resp.GetMsg())
	},
}

func init() {
	deleteBackupCmd.Flags().StringVarP(&deleteBackName, "name", "n", "", "get backup with this name")

	rootCmd.AddCommand(deleteBackupCmd)
}
