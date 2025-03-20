package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/alias"
)

var (
	getBackName string
	getDetail   bool
)

var getBackupCmd = &cobra.Command{
	Use:   "get",
	Short: "Get backup details by name",
	Long:  "Get detailed information about a backup by its name. Use the --detail flag to include collection details.",

	Run: func(cmd *cobra.Command, args []string) {
		// Validate backup name is provided
		if getBackName == "" {
			PrintError("Backup name must be provided (use --name or -n flag)", nil, 0)
			cmd.Help()
			os.Exit(1)
		}

		// Check if backup name contains alias prefix (alias/backup_name format)
		var configFile = config
		var actualBackupName = getBackName
		var displayName = getBackName
		
		if strings.Contains(getBackName, "/") {
			// Parse alias and backup name
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}
			
			aliasName, parsedBackupName, err := aliasManager.ParseBackupName(getBackName)
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
		PrintInfo(fmt.Sprintf("Using config file: %s", configFile))
		params.GlobalInitWithYaml(configFile)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, &params)

		PrintInfo(fmt.Sprintf("Retrieving backup information for '%s'...", colorValue(displayName)))
		resp := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
			BackupName:    actualBackupName,
			WithoutDetail: !getDetail,
		})

		if resp.GetCode() != backuppb.ResponseCode_Success || resp.GetData() == nil {
			PrintError(fmt.Sprintf("Could not find backup '%s'", displayName), fmt.Errorf(resp.GetMsg()), 1)
		}

		// Display backup details
		backupInfo := resp.GetData()
		PrintSectionTitle(fmt.Sprintf("BACKUP DETAILS: %s", colorValue(displayName)))
		PrintKeyValue("Backup ID", backupInfo.GetId())
		
		// Format and display creation time
		creationTime := time.Unix(backupInfo.GetStartTime(), 0)
		PrintKeyValue("Creation Time", creationTime.Format(time.RFC3339))
		
		// Display backup state
		PrintKeyValue("Status", FormatBackupStatus(backupInfo.GetStateCode()))
		
		// Display number of collections included
		collectionCount := len(backupInfo.GetCollectionBackups())
		PrintKeyValue("Collections", fmt.Sprintf("%d", collectionCount))

		// If there are collections and detail flag is set, display the list
		if collectionCount > 0 && getDetail {
			PrintSubsectionTitle("COLLECTIONS IN BACKUP")
			
			// Create a table for collections
			table := CreateTable([]string{"Collection Name", "Database", "Collection ID", "Partition Count"})
			
			for _, coll := range backupInfo.GetCollectionBackups() {
				dbName := coll.GetDbName()
				if dbName == "" {
					dbName = "default"
				}
				
				table.Append([]string{
					coll.GetCollectionName(),
					dbName,
					fmt.Sprintf("%d", coll.GetCollectionId()),
					fmt.Sprintf("%d", len(coll.GetPartitionBackups())),
				})
			}
			
			// Render the table
			table.Render()
			
			// If JSON output is requested, print it (when verbosity >= 2)
			if verbosity >= 2 {
				PrintSubsectionTitle("RAW BACKUP DATA (JSON)")
				output, _ := json.MarshalIndent(resp.GetData(), "", "    ")
				fmt.Println(string(output))
			}
		}
	},
}

func init() {
	getBackupCmd.Flags().StringVarP(&getBackName, "name", "n", "", "get backup with this name")
	getBackupCmd.Flags().BoolVarP(&getDetail, "detail", "d", false, "get complete backup info")

	rootCmd.AddCommand(getBackupCmd)
}
