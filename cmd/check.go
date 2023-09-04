package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
)

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "check if the connects is right.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		resp := backupContext.Check(context)
		fmt.Println(resp)
	},
}

func init() {
	rootCmd.AddCommand(checkCmd)
}
