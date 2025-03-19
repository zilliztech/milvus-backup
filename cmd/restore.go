package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/alias"
	"github.com/zilliztech/milvus-backup/internal/log"

	"go.uber.org/zap"
)

var (
	restoreBackupName           string
	restoreCollectionNames      string
	renameSuffix                string
	renameCollectionNames       string
	restoreDatabases            string
	restoreDatabaseCollections  string
	restoreMetaOnly             bool
	restoreRestoreIndex         bool
	restoreUseAutoIndex         bool
	restoreDropExistCollection  bool
	restoreDropExistIndex       bool
	restoreSkipCreateCollection bool
	restoreRBAC                 bool
)

var restoreBackupCmd = &cobra.Command{
	Use:   "restore",
	Short: "restore subcommand restore a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		// Check if backup name contains alias prefix (alias/backup_name format)
		var configFile = config
		var actualBackupName = restoreBackupName
		
		if strings.Contains(restoreBackupName, "/") {
			// Parse alias and backup name
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}
			
			aliasName, parsedBackupName, err := aliasManager.ParseBackupName(restoreBackupName)
			if err != nil {
				PrintError("Failed to parse backup name", err, 1)
			}
			
			if aliasName != "" {
				// Get config file for the alias
				configFile, err = aliasManager.GetConfigPath(aliasName)
				if err != nil {
					PrintError(fmt.Sprintf("Failed to get config for alias '%s'", aliasName), err, 1)
				}
				
				// Use the parsed backup name
				actualBackupName = parsedBackupName
				PrintInfo(fmt.Sprintf("Using config file for alias '%s': %s", aliasName, configFile))
			}
		}
		
		var params paramtable.BackupParams
		fmt.Println("config:" + configFile)
		params.GlobalInitWithYaml(configFile)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, &params)
		log.Info("restore cmd input args", zap.Strings("args", args))
		start := time.Now().Unix()
		var collectionNameArr []string
		if restoreCollectionNames == "" {
			collectionNameArr = []string{}
		} else {
			collectionNameArr = strings.Split(restoreCollectionNames, ",")
		}

		renameMap := make(map[string]string, 0)
		if renameCollectionNames != "" {
			fmt.Println("rename: " + renameCollectionNames)
			renameArr := strings.Split(renameCollectionNames, ",")
			for _, rename := range renameArr {
				if strings.Contains(rename, ":") {
					splits := strings.Split(rename, ":")
					renameMap[splits[0]] = splits[1]
				} else {
					fmt.Println("illegal rename parameter")
					return
				}
			}
		}

		if restoreDatabaseCollections == "" && restoreDatabases != "" {
			dbCollectionDict := make(map[string][]string)
			splits := strings.Split(restoreDatabases, ",")
			for _, db := range splits {
				dbCollectionDict[db] = []string{}
			}
			completeDbCollections, err := jsoniter.MarshalToString(dbCollectionDict)
			restoreDatabaseCollections = completeDbCollections
			if err != nil {
				fmt.Println("illegal databases input")
				return
			}
		}
		// Display restore operation start
		PrintSectionTitle("STARTING RESTORE OPERATION")
		
		resp := backupContext.RestoreBackup(context, &backuppb.RestoreBackupRequest{
			BackupName:           actualBackupName,
			CollectionNames:      collectionNameArr,
			CollectionSuffix:     renameSuffix,
			CollectionRenames:    renameMap,
			DbCollections:        utils.WrapDBCollections(restoreDatabaseCollections),
			MetaOnly:             restoreMetaOnly,
			RestoreIndex:         restoreRestoreIndex,
			UseAutoIndex:         restoreUseAutoIndex,
			DropExistCollection:  restoreDropExistCollection,
			DropExistIndex:       restoreDropExistIndex,
			SkipCreateCollection: restoreSkipCreateCollection,
			Rbac:                 restoreRBAC,
		})

		// Display restore results
		PrintSectionTitle("RESTORE RESULTS")
		
		// Check if restore was successful
		if resp.GetCode() == backuppb.ResponseCode_Success {
			PrintSuccess(resp.GetMsg())
		} else {
			PrintError("Restore operation failed", fmt.Errorf(resp.GetMsg()), 0)
		}
		
		duration := time.Now().Unix() - start
		PrintKeyValue("Duration", fmt.Sprintf("%d seconds", duration))
		
		// If restore was successful, display more detailed information
		if resp.GetCode() == backuppb.ResponseCode_Success {
			// Get backup details to show which collections were restored
			getResp := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
				BackupName: actualBackupName,
			})
			
			if getResp.GetCode() == backuppb.ResponseCode_Success && getResp.GetData() != nil {
				backupInfo := getResp.GetData()
				
				// Display restored collection information
				if len(backupInfo.GetCollectionBackups()) > 0 {
					// Collect database and collection information
					dbCollMap := make(map[string][]string)
					
					// Filter collections based on user selection
					var restoredCollections []*backuppb.CollectionBackupInfo
					if len(collectionNameArr) > 0 {
						// User specified collections to restore
						collMap := make(map[string]bool)
						for _, c := range collectionNameArr {
							collMap[c] = true
						}
						
						for _, coll := range backupInfo.GetCollectionBackups() {
							if collMap[coll.GetCollectionName()] {
								restoredCollections = append(restoredCollections, coll)
							}
						}
					} else {
						// User did not specify collections, restore all
						restoredCollections = backupInfo.GetCollectionBackups()
					}
					
					PrintSubsectionTitle("RESTORED COLLECTIONS")
					
					// Create a table for collections
					table := CreateTable([]string{"Collection Name", "Original Name", "Database", "Collection ID"})
					for _, coll := range restoredCollections {
						dbName := coll.GetDbName()
						if dbName == "" {
							dbName = "default"
						}
						
						// Add to database collection mapping
						if _, exists := dbCollMap[dbName]; !exists {
							dbCollMap[dbName] = []string{}
						}
						
						// Check if renamed
						originalName := coll.GetCollectionName()
						displayName := originalName
						
						// Check if suffix was applied
						if renameSuffix != "" {
							displayName = originalName + renameSuffix
						}
						
						// Check if specific rename was applied
						fullName := dbName + "." + originalName
						if newName, ok := renameMap[fullName]; ok {
							displayName = newName
						}
						
						dbCollMap[dbName] = append(dbCollMap[dbName], displayName)
						
						// Add to table
						originalNameDisplay := originalName
						if displayName == originalName {
							originalNameDisplay = "-"
						}
						
						table.Append([]string{
							displayName,
							originalNameDisplay,
							dbName,
							fmt.Sprintf("%d", coll.GetCollectionId()),
						})
					}
					
					// Render the table
					table.Render()
					
					// Show collections grouped by database
					if len(dbCollMap) > 0 {
						PrintSubsectionTitle("COLLECTIONS GROUPED BY DATABASE")
						for db, collections := range dbCollMap {
							fmt.Printf("  %s %s\n", colorInfo(symbolBullet), colorHeader(db))
							for _, coll := range collections {
								fmt.Printf("    %s %s\n", colorInfo(symbolArrow), coll)
							}
						}
					}
				}
				
				// Display restore options information
				PrintSubsectionTitle("RESTORE OPTIONS USED")
				if restoreMetaOnly {
					fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Metadata only (no data)")
				}
				if restoreRestoreIndex {
					fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Restore indexes")
				}
				if restoreUseAutoIndex {
					fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Use auto-indexing")
				}
				if restoreDropExistCollection {
					fmt.Printf("  %s %s\n", colorWarning(symbolWarning), "Drop existing collections")
				}
				if restoreDropExistIndex {
					fmt.Printf("  %s %s\n", colorWarning(symbolWarning), "Drop existing indexes")
				}
				if restoreSkipCreateCollection {
					fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Skip collection creation")
				}
				if restoreRBAC {
					fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Restore RBAC metadata")
				}
			}
		}
	},
}

func init() {
	restoreBackupCmd.Flags().StringVarP(&restoreBackupName, "name", "n", "", "backup name to restore")
	restoreBackupCmd.Flags().StringVarP(&restoreCollectionNames, "collections", "c", "", "collectionNames to restore")
	restoreBackupCmd.Flags().StringVarP(&renameSuffix, "suffix", "s", "", "add a suffix to collection name to restore")
	restoreBackupCmd.Flags().StringVarP(&renameCollectionNames, "rename", "r", "", "rename collections to new names, format: db1.collection1:db2.collection1_new,db1.collection2:db2.collection2_new")
	restoreBackupCmd.Flags().StringVarP(&restoreDatabases, "databases", "d", "", "databases to restore, if not set, restore all databases")
	restoreBackupCmd.Flags().StringVarP(&restoreDatabaseCollections, "database_collections", "a", "", "databases and collections to restore, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")

	restoreBackupCmd.Flags().BoolVarP(&restoreMetaOnly, "meta_only", "", false, "if true, restore meta only")
	restoreBackupCmd.Flags().BoolVarP(&restoreRestoreIndex, "restore_index", "", false, "if true, restore index")
	restoreBackupCmd.Flags().BoolVarP(&restoreUseAutoIndex, "use_auto_index", "", false, "if true, replace vector index with autoindex")
	restoreBackupCmd.Flags().BoolVarP(&restoreDropExistCollection, "drop_exist_collection", "", false, "if true, drop existing target collection before create")
	restoreBackupCmd.Flags().BoolVarP(&restoreDropExistIndex, "drop_exist_index", "", false, "if true, drop existing index of target collection before create")
	restoreBackupCmd.Flags().BoolVarP(&restoreSkipCreateCollection, "skip_create_collection", "", false, "if true, will skip collection, use when collection exist, restore index or data")
	restoreBackupCmd.Flags().BoolVarP(&restoreRBAC, "rbac", "", false, "whether restore RBAC meta")

	// won't print flags in character order
	restoreBackupCmd.Flags().SortFlags = false

	rootCmd.AddCommand(restoreBackupCmd)
}
