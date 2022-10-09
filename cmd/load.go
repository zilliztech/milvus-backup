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
	loadBackupName        string
	loadCollectionNames   string
	renameSuffix          string
	renameCollectionNames string
)

var loadBackupCmd = &cobra.Command{
	Use:   "load",
	Short: "load subcommand load a backup.",

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
			collectionNameArr = strings.Split(loadCollectionNames, ",")
		}

		var renameMap map[string]string
		if renameCollectionNames == "" {
			renameMap = map[string]string{}
		} else {
			renameArr := strings.Split(renameCollectionNames, ",")
			if len(renameArr) != len(collectionNameArr) {
				fmt.Errorf("collection_names and renames num dismatch, Forbid to load")
			}
		}

		backup, err := backupContext.LoadBackup(context, &backuppb.LoadBackupRequest{
			BackupName:        loadBackupName,
			CollectionNames:   collectionNameArr,
			CollectionSuffix:  renameSuffix,
			CollectionRenames: renameMap,
		})
		if err != nil {
			fmt.Errorf("fail to load backup, %s", err.Error())
		}

		fmt.Println(backup.GetStatus())
	},
}

func init() {
	loadBackupCmd.Flags().StringVarP(&loadBackupName, "name", "n", "", "backup name to load")
	loadBackupCmd.Flags().StringVarP(&loadCollectionNames, "collections", "c", "", "collectionNames to load")
	loadBackupCmd.Flags().StringVarP(&renameSuffix, "suffix", "s", "", "add a suffix to collection name to load")
	loadBackupCmd.Flags().StringVarP(&renameCollectionNames, "rename", "r", "", "rename collections to new names")

	rootCmd.AddCommand(loadBackupCmd)
}
