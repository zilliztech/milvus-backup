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
)

var (
	backupName      string
	collectionNames string
	databases       string
	dbCollections   string
	force           bool
	metaOnly        bool
	rbac            bool
)

var createBackupCmd = &cobra.Command{
	Use:   "create",
	Short: "create subcommand create a backup.",

	Run: func(cmd *cobra.Command, args []string) {
		// Check if backup name contains alias prefix (alias/backup_name format)
		var configFile = config
		var actualBackupName = backupName
		
		if strings.Contains(backupName, "/") {
			// Parse alias and backup name
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}
			
			aliasName, parsedBackupName, err := aliasManager.ParseBackupName(backupName)
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

		start := time.Now().Unix()
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
		// Display backup information
		PrintSectionTitle("BACKUP INFORMATION")
		PrintKeyValue("Backup Name", actualBackupName)
		
		if len(collectionNameArr) > 0 {
			PrintSubsectionTitle("COLLECTIONS TO BACKUP")
			for _, coll := range collectionNameArr {
				fmt.Printf("  %s %s\n", colorInfo(symbolBullet), coll)
			}
		}
		
		if dbCollections != "" {
			PrintSubsectionTitle("DATABASES AND COLLECTIONS TO BACKUP")
			fmt.Println("  " + dbCollections)
		}
		
		// Display backup options
		PrintSubsectionTitle("BACKUP OPTIONS")
		if metaOnly {
			fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Metadata only (no data)")
		}
		
		if rbac {
			fmt.Printf("  %s %s\n", colorInfo(symbolBullet), "Include RBAC metadata")
		}
		
		if force {
			fmt.Printf("  %s %s\n", colorWarning(symbolWarning), "Force backup mode enabled")
		}
		
		PrintInfo("Creating backup...")
		
		// Create backup
		resp := backupContext.CreateBackup(context, &backuppb.CreateBackupRequest{
			BackupName:      actualBackupName,
			CollectionNames: collectionNameArr,
			DbCollections:   utils.WrapDBCollections(dbCollections),
			Force:           force,
			MetaOnly:        metaOnly,
			Rbac:            rbac,
		})

		// Display results
		PrintSectionTitle("BACKUP RESULTS")
		
		// Check if backup was successful
		if resp.GetCode() == backuppb.ResponseCode_Success {
			PrintSuccess(resp.GetMsg())
		} else {
			PrintError("Backup operation failed", fmt.Errorf(resp.GetMsg()), 0)
		}
		
		duration := time.Now().Unix() - start
		PrintKeyValue("Duration", fmt.Sprintf("%d seconds", duration))
		
		// If backup was successful, get and display backup details
		if resp.GetCode() == backuppb.ResponseCode_Success {
			// Use the correct backup name to get details
			var getBackupName string
			if strings.Contains(backupName, "/") {
				// If original backup name contains alias, use the actual backup name
				getBackupName = actualBackupName
			} else {
				getBackupName = backupName
			}
			
			getResp := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
				BackupName: getBackupName,
			})
			
			if getResp.GetCode() == backuppb.ResponseCode_Success && getResp.GetData() != nil {
				backupInfo := getResp.GetData()
				PrintSubsectionTitle("BACKUP DETAILS")
				PrintKeyValue("Backup ID", backupInfo.GetId())
				PrintKeyValue("Backup Size", FormatSize(backupInfo.GetSize()))
				
				// Collect database and collection information
				dbCollMap := make(map[string][]string)
				
				if len(backupInfo.GetCollectionBackups()) > 0 {
					PrintSubsectionTitle("BACKED UP COLLECTIONS")
					
					// Create a table for collections
					table := CreateTable([]string{"Collection Name", "Database", "Collection ID"})
					
					for _, coll := range backupInfo.GetCollectionBackups() {
						dbName := coll.GetDbName()
						if dbName == "" {
							dbName = "default"
						}
						
						// Add to database collection mapping
						if _, exists := dbCollMap[dbName]; !exists {
							dbCollMap[dbName] = []string{}
						}
						dbCollMap[dbName] = append(dbCollMap[dbName], coll.GetCollectionName())
						
						// Add to table
						table.Append([]string{
							coll.GetCollectionName(),
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
				
				// If alias was used, show the full alias/backup name format
				if backupName != actualBackupName {
					PrintInfo(fmt.Sprintf("Backup created with alias, accessible via: %s", colorValue(backupName)))
				}
			}
		}
	},
}

func init() {
	createBackupCmd.Flags().StringVarP(&backupName, "name", "n", "", "backup name, if unset will generate a name automatically")
	createBackupCmd.Flags().StringVarP(&collectionNames, "colls", "c", "", "collectionNames to backup, use ',' to connect multiple collections")
	createBackupCmd.Flags().StringVarP(&databases, "databases", "d", "", "databases to backup")
	createBackupCmd.Flags().StringVarP(&dbCollections, "database_collections", "a", "", "databases and collections to backup, json format: {\"db1\":[\"c1\", \"c2\"],\"db2\":[]}")
	createBackupCmd.Flags().BoolVarP(&force, "force", "f", false, "force backup, will skip flush, should make sure data has been stored into disk when using it")
	createBackupCmd.Flags().BoolVarP(&metaOnly, "meta_only", "", false, "only backup collection meta instead of data")
	createBackupCmd.Flags().BoolVarP(&rbac, "rbac", "", false, "whether backup RBAC meta")

	createBackupCmd.Flags().SortFlags = false

	rootCmd.AddCommand(createBackupCmd)
}
