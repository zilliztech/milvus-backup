package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

var (
	getBackName string
	getDetail   bool
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

		resp := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
			BackupName:    getBackName,
			WithoutDetail: !getDetail,
		})

		output, _ := json.MarshalIndent(resp.GetData(), "", "    ")
		fmt.Println(string(output))
		fmt.Println(resp.GetCode())
	},
}

func init() {
	getBackupCmd.Flags().StringVarP(&getBackName, "name", "n", "", "get backup with this name")
	getBackupCmd.Flags().BoolVarP(&getDetail, "detail", "d", false, "get complete backup info")

	rootCmd.AddCommand(getBackupCmd)
}
