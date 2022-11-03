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
	getBackName string
)

var getBackupCmd = &cobra.Command{
	Use:   "get",
	Short: "get subcommand get backup by name.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
			BackupName: getBackName,
		})
		if err != nil {
			fmt.Errorf("fail to get backup, %s", err.Error())
		}

		fmt.Println(backup.GetBackupInfo().String())
	},
}

func init() {
	getBackupCmd.Flags().StringVarP(&getBackName, "name", "n", "", "get backup with this name")

	rootCmd.AddCommand(getBackupCmd)
}
