package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"

	"go.uber.org/zap"
)

var (
	restoreBackupName      string
	restoreCollectionNames string
	renameSuffix           string
	renameCollectionNames  string
)

var restoreBackupCmd = &cobra.Command{
	Use:   "restore",
	Short: "restore subcommand restore a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)
		log.Info("restore cmd input args", zap.Strings("args", args))
		var collectionNameArr []string
		if restoreCollectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(restoreCollectionNames, ",")
		}

		var renameMap map[string]string
		if renameCollectionNames == "" {
			renameMap = map[string]string{}
		} else {
			renameArr := strings.Split(renameCollectionNames, ",")
			if len(renameArr) != len(collectionNameArr) {
				fmt.Errorf("collection_names and renames num dismatch, Forbid to restore")
			}
		}

		resp := backupContext.RestoreBackup(context, &backuppb.RestoreBackupRequest{
			BackupName:        restoreBackupName,
			CollectionNames:   collectionNameArr,
			CollectionSuffix:  renameSuffix,
			CollectionRenames: renameMap,
		})

		fmt.Println(resp.GetCode(), "\n", resp.GetMsg())
	},
}

func init() {
	restoreBackupCmd.Flags().StringVarP(&restoreBackupName, "name", "n", "", "backup name to restore")
	restoreBackupCmd.Flags().StringVarP(&restoreCollectionNames, "collections", "c", "", "collectionNames to restore")
	restoreBackupCmd.Flags().StringVarP(&renameSuffix, "suffix", "s", "", "add a suffix to collection name to restore")
	restoreBackupCmd.Flags().StringVarP(&renameCollectionNames, "rename", "r", "", "rename collections to new names")

	rootCmd.AddCommand(restoreBackupCmd)
}
