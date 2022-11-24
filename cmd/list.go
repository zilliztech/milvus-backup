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
	collectionName string
)

var listBackupCmd = &cobra.Command{
	Use:   "list",
	Short: "list subcommand shows all backup in the cluster.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		backups := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
			CollectionName: collectionName,
		})

		fmt.Println(">> Backups:")
		for _, backup := range backups.GetData() {
			fmt.Println(backup.GetName())
		}
	},
}

func init() {
	listBackupCmd.Flags().StringVarP(&collectionName, "collection", "c", "", "only list backups contains a certain collection")

	rootCmd.AddCommand(listBackupCmd)
}
