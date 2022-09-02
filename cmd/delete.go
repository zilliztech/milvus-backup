package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

var (
	deleteBackName string
)

var deleteBackupCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete subcommand delete backup by name.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.ComponentParam
		params.GlobalInitWithYaml(milvusConfig)
		params.InitOnce()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		backup, err := backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
			BackupName: deleteBackName,
		})
		if err != nil {
			fmt.Errorf("fail to get backup, %s", err.Error())
		}

		fmt.Println(backup.GetStatus())
	},
}

func init() {
	deleteBackupCmd.Flags().StringVarP(&deleteBackName, "name", "n", "", "get backup with this name")

	rootCmd.AddCommand(deleteBackupCmd)
}
