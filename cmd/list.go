package cmd

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/alias"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
)

var (
	collectionName string
	showDetails    bool
)

var listBackupCmd = &cobra.Command{
	Use:   "list [alias]",
	Short: "List all backups in the cluster",
	Long:  "List all backups in a cluster. Optionally specify an alias to list backups from a specific cluster. If no alias is provided, lists backups from all configured aliases.",
	Example: `  # List all backups from all configured aliases
  milvus-backup list

  # List all backups from a specific alias
  milvus-backup list my-cluster

  # List backups containing a specific collection
  milvus-backup list my-cluster --collection my_collection`,

	Run: func(cmd *cobra.Command, args []string) {
		// If no alias is provided, list backups from all aliases
		if len(args) == 0 {
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}

			aliases := aliasManager.ListAliases()
			if len(aliases) == 0 {
				// No aliases configured, use default config
				listBackupsWithConfig(config, "", collectionName)
				return
			}

			// List backups for each alias
			for i, a := range aliases {
				if i > 0 {
					fmt.Println() // Add spacing between aliases
				}
				PrintSectionTitle(fmt.Sprintf("BACKUPS FOR ALIAS '%s'", a.Name))
				listBackupsWithConfig(a.ConfigFilePath, a.Name, collectionName)
			}
			return
		}

		// List backups for a specific alias
		aliasName := args[0]
		aliasManager, err := alias.NewAliasManager()
		if err != nil {
			PrintError("Failed to initialize alias manager", err, 1)
		}

		aliasConfigPath, err := aliasManager.GetConfigPath(aliasName)
		if err != nil {
			PrintError(fmt.Sprintf("Failed to get config path for alias '%s'", aliasName), err, 1)
		}

		listBackupsWithConfig(aliasConfigPath, aliasName, collectionName)
	},
}

// Helper function to list backups with a specific config file
func listBackupsWithConfig(configFile, aliasName, collectionName string) {
	var params paramtable.BackupParams
	params.GlobalInitWithYaml(configFile)
	params.Init()

	ctx := context.Background()
	backupContext := core.CreateBackupContext(ctx, &params)

	backups := backupContext.ListBackups(ctx, &backuppb.ListBackupsRequest{
		CollectionName: collectionName,
	})

	log.Info("list backups response",
		zap.String("collectionName", collectionName),
		zap.Any("response", backups))

	// Set title based on filter conditions
	if collectionName != "" {
		PrintInfo(fmt.Sprintf("Filtering backups containing collection '%s'", collectionName))
	}
	
	// Check if backups were found
	if len(backups.GetData()) == 0 {
		PrintInfo("No backups found")
		return
	}
	
	// Create a table for the backup list
	table := CreateTable([]string{"NAME", "STATUS", "SIZE", "CREATED", "COLLECTIONS", "DURATION"})
	
	// Collect detailed backup information
	backupDetails := make([]*backuppb.BackupInfo, 0, len(backups.GetData()))
	
	// Get details for each backup
	for _, backup := range backups.GetData() {
		getResp := backupContext.GetBackup(ctx, &backuppb.GetBackupRequest{
			BackupName: backup.GetName(),
		})
		
		if getResp.GetCode() == backuppb.ResponseCode_Success && getResp.GetData() != nil {
			backupDetails = append(backupDetails, getResp.GetData())
		} else {
			// Create a minimal backup info for backups without details
			backupDetails = append(backupDetails, &backuppb.BackupInfo{
				Name: backup.GetName(),
			})
		}
	}
	
	// Sort backups by start time (newest first)
	sort.Slice(backupDetails, func(i, j int) bool {
		return backupDetails[i].GetStartTime() > backupDetails[j].GetStartTime()
	})
	
	// Add rows to the table
	for _, backupInfo := range backupDetails {
		name := backupInfo.GetName()
		if aliasName != "" {
			name = aliasName + "/" + name
		}
		
		status := FormatBackupStatus(backupInfo.GetStateCode())
		size := FormatSize(backupInfo.GetSize())
		created := FormatTime(backupInfo.GetStartTime())
		
		// Calculate duration
		duration := "-"
		if backupInfo.GetEndTime() > 0 {
			duration = FormatDuration(backupInfo.GetEndTime() - backupInfo.GetStartTime())
		}
		
		// Count collections
		collections := strconv.Itoa(len(backupInfo.GetCollectionBackups()))
		
		table.Append([]string{name, status, size, created, collections, duration})
	}
	
	// Render the table
	table.Render()
	
	// If detailed view is requested, show more information for each backup
	if showDetails {
		for i, backupInfo := range backupDetails {
			if i > 0 {
				fmt.Println() // Add spacing between backups
			}
			
			// Display backup details
			name := backupInfo.GetName()
			if aliasName != "" {
				name = aliasName + "/" + name
			}
			
			PrintSubsectionTitle(fmt.Sprintf("BACKUP DETAILS: %s", name))
			
			if backupInfo.GetId() != "" {
				// Display backup metadata
				PrintKeyValue("ID", backupInfo.GetId())
				PrintKeyValue("Created", FormatTime(backupInfo.GetStartTime()))
				PrintKeyValue("Completed", FormatTime(backupInfo.GetEndTime()))
				PrintKeyValue("Duration", FormatDuration(backupInfo.GetEndTime() - backupInfo.GetStartTime()))
				PrintKeyValue("Status", FormatBackupStatus(backupInfo.GetStateCode()))
				PrintKeyValue("Size", FormatSize(backupInfo.GetSize()))
				
				if backupInfo.GetMilvusVersion() != "" {
					PrintKeyValue("Milvus Version", backupInfo.GetMilvusVersion())
				}
				
				// Display collection information
				collectionCount := len(backupInfo.GetCollectionBackups())
				PrintKeyValue("Collections", collectionCount)
				
				if collectionCount > 0 {
					fmt.Println()
					fmt.Println("  COLLECTION DETAILS:")
					
					// Create a table for collections
					collTable := CreateTable([]string{"NAME", "ID", "SIZE", "HAS INDEX", "DB"})
					
					// Group collections by database
					dbCollMap := make(map[string][]string)
					
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
						
						// Add collection to table
						hasIndex := "No"
						if coll.GetHasIndex() {
							hasIndex = fmt.Sprintf("Yes (%d)", len(coll.GetIndexInfos()))
						}
						
						collTable.Append([]string{
							coll.GetCollectionName(),
							strconv.FormatInt(coll.GetCollectionId(), 10),
							FormatSize(coll.GetSize()),
							hasIndex,
							dbName,
						})
					}
					
					// Render the collections table
					collTable.Render()
				}
			} else {
				PrintWarning("Unable to retrieve detailed information")
			}
		}
	}
	
	// Show summary
	PrintInfo(fmt.Sprintf("Found %d backups", len(backupDetails)))
}

func init() {
	listBackupCmd.Flags().StringVarP(&collectionName, "collection", "c", "", "only list backups contains a certain collection")

	rootCmd.AddCommand(listBackupCmd)
}
