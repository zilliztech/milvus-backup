package cmd

import (
	"context"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
)

var (
	backupName      string
	collectionNames string
	databases       string
	dbCollections   string
	force           bool
)

var createBackupCmd = &cobra.Command{
	Use:   "create",
	Short: "create subcommand create a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		fmt.Println("config:" + config)
		params.GlobalInitWithYaml(config)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, params)

		var collectionNameArr []string
		if collectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(collectionNames, ",")
		}

		if dbCollections == "" && databases != "" {
			dbCollectionDict := make(map[string][]string)
			splits := strings.Split(databases, ",")
			for _, db := range splits {
				dbCollectionDict[db] = []string{}
			}
			completeDbCollections, err := jsoniter.MarshalToString(dbCollectionDict)
			dbCollections = completeDbCollections
			if err != nil {
				fmt.Println("illegal databases input")
				return
			}
		}
		resp := backupContext.CreateBackup(context, &backuppb.CreateBackupRequest{
			BackupName:      backupName,
			CollectionNames: collectionNameArr,
			DbCollections:   utils.WrapDBCollections(dbCollections),
			Force:           force,
		})
		fmt.Println(resp.GetCode(), "\n", resp.GetMsg())
	},
}

func init() {
	createBackupCmd.Flags().StringVarP(&backupName, "name", "n", "", "backup name, if unset will generate a name automatically")
	createBackupCmd.Flags().StringVarP(&collectionNames, "colls", "c", "", "collectionNames to backup, use ',' to connect multiple collections")
	createBackupCmd.Flags().StringVarP(&databases, "databases", "d", "", "databases to backup")
	createBackupCmd.Flags().StringVarP(&dbCollections, "database_collections", "a", "", "databases and collections to backup, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")
	createBackupCmd.Flags().BoolVarP(&force, "force", "f", false, "force backup skip flush, should make sure data has been stored into disk when using it")

	rootCmd.AddCommand(createBackupCmd)
}
