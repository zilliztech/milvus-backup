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
	collectionName string
)

var listBackupCmd = &cobra.Command{
	Use:   "list",
	Short: "list subcommand shows all backup in the cluster.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.ComponentParam
		params.GlobalInitWithYaml(milvusConfig)
		params.InitOnce()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		backups, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
			CollectionName: collectionName,
		})
		if err != nil {
			fmt.Errorf("fail to list backup, %s", err.Error())
		}

		fmt.Println(">> Backups:")
		for _, backup := range backups.GetBackupInfos() {
			fmt.Println(backup.GetName())
		}
	},
}

func init() {
	listBackupCmd.Flags().StringVarP(&collectionName, "collection", "c", "", "only list backups contains a certain collection")

	rootCmd.AddCommand(listBackupCmd)
}
