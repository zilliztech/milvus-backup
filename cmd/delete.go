package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"
	"errors"
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/alias"
)

var (
	deleteBackName string
	noInteractive bool
)

var deleteBackupCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete subcommand delete backup by name.",
	Long:  "Delete a backup by name. Use the --no-interactive flag to skip confirmation prompt.",

	Run: func(cmd *cobra.Command, args []string) {
		// Validate backup name is provided
		if deleteBackName == "" {
			PrintError("Backup name must be provided (use --name or -n flag)", nil, 0)
			cmd.Help()
			os.Exit(1)
		}
		
		// Check if backup name contains alias prefix (alias/backup_name format)
		var configFile = config
		var actualBackupName = deleteBackName
		var displayName = deleteBackName
		// aliasName variable will be used when parsing backup name
		
		if strings.Contains(deleteBackName, "/") {
			// Parse alias and backup name
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}
			
			aliasName, parsedBackupName, err := aliasManager.ParseBackupName(deleteBackName)
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
				displayName = deleteBackName // Keep original name for display
				PrintInfo(fmt.Sprintf("Using config file for alias '%s': %s", aliasName, configFile))
			}
		}
		
		var params paramtable.BackupParams
		fmt.Println("config:" + configFile)
		params.GlobalInitWithYaml(configFile)
		params.Init()

		context := context.Background()
		backupContext := core.CreateBackupContext(context, &params)
		
		// Get backup details to confirm it exists
		getResp := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
			BackupName: actualBackupName,
		})
		
		if getResp.GetCode() != backuppb.ResponseCode_Success || getResp.GetData() == nil {
			PrintError(fmt.Sprintf("Could not find backup '%s'", displayName), errors.New(getResp.GetMsg()), 1)
		}
		
		// Display backup details
		backupInfo := getResp.GetData()
		PrintSectionTitle(fmt.Sprintf("PREPARING TO DELETE BACKUP: %s", colorValue(displayName)))
		PrintKeyValue("Backup ID", backupInfo.GetId())
		
		// Format and display creation time
		creationTime := time.Unix(backupInfo.GetStartTime(), 0)
		PrintKeyValue("Creation Time", creationTime.Format(time.RFC3339))
		
		// Display number of collections included
		collectionCount := len(backupInfo.GetCollectionBackups())
		PrintKeyValue("Collections", fmt.Sprintf("%d", collectionCount))
		
		// If there are collections, display the list
		if collectionCount > 0 {
			PrintSubsectionTitle("COLLECTIONS IN BACKUP")
			
			// Create a table for collections
			table := CreateTable([]string{"Collection Name", "Database", "Collection ID"})
			
			for _, coll := range backupInfo.GetCollectionBackups() {
				dbName := coll.GetDbName()
				if dbName == "" {
					dbName = "default"
				}
				
				table.Append([]string{
					coll.GetCollectionName(),
					dbName,
					fmt.Sprintf("%d", coll.GetCollectionId()),
				})
			}
			
			// Render the table
			table.Render()
		}
		
		// If interactive mode, ask for user confirmation
		if !noInteractive {
			PrintWarning("This operation will permanently delete the backup and cannot be undone!")
			fmt.Print("Confirm deletion? (y/N): ")
			
			reader := bufio.NewReader(os.Stdin)
			response, err := reader.ReadString('\n')
			if err != nil {
				PrintError("Failed to read input", err, 1)
			}
			
			response = strings.TrimSpace(strings.ToLower(response))
			if response != "y" && response != "yes" {
				PrintInfo("Operation cancelled")
				return
			}
		}
		
		// Execute delete operation
		PrintInfo(fmt.Sprintf("Deleting backup '%s'...", colorValue(displayName)))
		resp := backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
			BackupName: actualBackupName,
		})

		// Handle delete result
		if resp.GetCode() == backuppb.ResponseCode_Success {
			PrintSuccess(fmt.Sprintf("Backup '%s' has been deleted", displayName))
		} else {
			PrintError(fmt.Sprintf("Failed to delete backup '%s'", displayName), errors.New(resp.GetMsg()), 1)
		}
	},
}

func init() {
	deleteBackupCmd.Flags().StringVarP(&deleteBackName, "name", "n", "", "backup name to delete")
	deleteBackupCmd.Flags().BoolVar(&noInteractive, "no-interactive", false, "Do not prompt for confirmation when deleting resources. Be careful using this flag!")
	deleteBackupCmd.MarkFlagRequired("name")

	rootCmd.AddCommand(deleteBackupCmd)
}
