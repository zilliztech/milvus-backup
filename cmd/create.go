package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
)

var (
	backupName      string
	collectionNames string
)

var createBackupCmd = &cobra.Command{
	Use:   "create",
	Short: "create subcommand create a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.ComponentParam
		params.GlobalInitWithYaml(milvusConfig)
		params.InitOnce()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		var collectionNameArr []string
		if collectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(collectionNames, ",")
		}
		backup, err := backupContext.CreateBackup(context, &backuppb.CreateBackupRequest{
			BackupName:      backupName,
			CollectionNames: collectionNameArr,
		})
		if err != nil {
			fmt.Errorf("fail to create backup, %s", err.Error())
		}

		fmt.Println(backup.GetStatus())
	},
}

func init() {
	createBackupCmd.Flags().StringVarP(&backupName, "name", "n", "", "backup name, if unset will generate a name automatically")
	createBackupCmd.Flags().StringVarP(&collectionNames, "collections", "c", "", "collectionNames to backup")

	rootCmd.AddCommand(createBackupCmd)
}
