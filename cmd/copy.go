// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/storage"
	"github.com/zilliztech/milvus-backup/internal/alias"
)

// Parameters for copy operation
var (
	sourceAlias  string
	sourceBackup string
	destAlias    string
	destBackup   string
	workerCount  int
	showProgress bool
	// force variable is already defined in create.go
)

var copyBackupCmd = &cobra.Command{
	Use:   "copy",
	Short: "copy a backup from one cluster to another",
	Long:  `copy a backup from one cluster to another using aliases by physically copying all backup data between storage systems

Two formats are supported:
1. Using flags: milvus-backup copy --source-alias <source-alias> --source-backup <backup-name> --dest-alias <dest-alias>
2. Using positional args: milvus-backup copy <source-alias> <dest-alias> <backup-name>`,
	Run: func(cmd *cobra.Command, args []string) {
		// Check if using positional arguments format
		if len(args) > 0 {
			// Initialize alias manager
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				fmt.Printf("Error initializing alias manager: %v\n", err)
				os.Exit(1)
			}

			// Check if using the correct format (copy source_alias target_alias backup_name)
			if len(args) == 3 {
				// Format: milvus-backup copy <source_alias> <dest_alias> <backup_name>
				sourceAlias = args[0]
				destAlias = args[1]
				sourceBackup = args[2]
				destBackup = args[2] // Backup name must be the same

				// Validate aliases
				_, err := aliasManager.GetAlias(sourceAlias)
				if err != nil {
					fmt.Printf("Error: source alias '%s' not found\n", sourceAlias)
					os.Exit(1)
				}

				_, err = aliasManager.GetAlias(destAlias)
				if err != nil {
					fmt.Printf("Error: destination alias '%s' not found\n", destAlias)
					os.Exit(1)
				}
			} else {
				fmt.Println("Error: Invalid number of arguments")
				fmt.Println("Usage:")
				fmt.Println("  milvus-backup copy <source_alias> <dest_alias> <backup_name>")
				fmt.Println("  milvus-backup copy --source-alias <source-alias> --source-backup <backup-name> --dest-alias <dest-alias>")
				os.Exit(1)
			}
		} else {
			// Using flag format - validate required flags
			if sourceAlias == "" || sourceBackup == "" || destAlias == "" {
				fmt.Println("Error: when using flags, --source-alias, --source-backup, and --dest-alias are required")
				fmt.Println("Alternatively, you can use:")
				fmt.Println("  milvus-backup copy <source_alias> <dest_alias> <backup_name>")
				os.Exit(1)
			}

			// Always use the same name for source and destination
			destBackup = sourceBackup
			
			// If destination backup name was explicitly provided and it's different, show a warning
			if cmd.Flags().Changed("dest-backup") && destBackup != sourceBackup {
				fmt.Println("Warning: Destination backup name must be the same as source backup name.")
				fmt.Printf("Using '%s' for both source and destination backup names.\n", sourceBackup)
				destBackup = sourceBackup
			}
		}

		// Get alias manager
		aliasManager, err := alias.NewAliasManager()
		if err != nil {
			fmt.Printf("Error initializing alias manager: %v\n", err)
			os.Exit(1)
		}

		// Get source config file
		sourceConfigFile, err := aliasManager.GetConfigPath(sourceAlias)
		if err != nil {
			fmt.Printf("Error getting source config: %v\n", err)
			os.Exit(1)
		}

		// Get destination config file
		destConfigFile, err := aliasManager.GetConfigPath(destAlias)
		if err != nil {
			fmt.Printf("Error getting destination config: %v\n", err)
			os.Exit(1)
		}

		// Display copy operation details
		PrintSectionTitle("BACKUP COPY OPERATION")
		PrintKeyValue("Source Cluster", sourceAlias)
		PrintKeyValue("Source Backup", sourceBackup)
		PrintKeyValue("Destination Cluster", destAlias)
		PrintKeyValue("Destination Backup Name", destBackup)
		
		if force {
			PrintWarning("Force mode enabled (will overwrite existing backup in destination cluster)")
		}
		
		// Step 1: Get backup from source cluster
		PrintInfo(fmt.Sprintf("Retrieving backup '%s' information from source cluster '%s'...", sourceBackup, sourceAlias))
		sourceParams := paramtable.BackupParams{}
		sourceParams.GlobalInitWithYaml(sourceConfigFile)
		sourceParams.Init()

		sourceCtx := context.Background()
		sourceBackupContext := core.CreateBackupContext(sourceCtx, &sourceParams)

		getResp := sourceBackupContext.GetBackup(sourceCtx, &backuppb.GetBackupRequest{
			BackupName: sourceBackup,
		})

		if getResp.GetCode() != backuppb.ResponseCode_Success {
			PrintError("Failed to get source backup", fmt.Errorf(getResp.GetMsg()), 0)
			os.Exit(1)
		}

		backupInfo := getResp.GetData()
		if backupInfo == nil {
			PrintError(fmt.Sprintf("Backup '%s' not found in source cluster", sourceBackup), nil, 0)
			os.Exit(1)
		}

		// Step 2: Create backup in destination cluster
		// Display source backup details
		PrintSubsectionTitle("SOURCE BACKUP DETAILS")
		PrintKeyValue("Backup ID", backupInfo.GetId())
		PrintKeyValue("Created At", FormatTime(backupInfo.GetStartTime()))
		PrintKeyValue("Backup Size", FormatSize(backupInfo.GetSize()))
		
		if len(backupInfo.GetCollectionBackups()) > 0 {
			PrintSubsectionTitle("INCLUDED COLLECTIONS")
			for _, coll := range backupInfo.GetCollectionBackups() {
				fmt.Printf("  %s %s (ID: %d)\n", colorInfo(symbolBullet), coll.GetCollectionName(), coll.GetCollectionId())
			}
		}
		
		PrintInfo(fmt.Sprintf("Checking if backup '%s' already exists in destination cluster '%s'...", destBackup, destAlias))
		destParams := paramtable.BackupParams{}
		destParams.GlobalInitWithYaml(destConfigFile)
		destParams.Init()

		destCtx := context.Background()
		destBackupContext := core.CreateBackupContext(destCtx, &destParams)

		// Check if destination backup already exists
		checkResp := destBackupContext.GetBackup(destCtx, &backuppb.GetBackupRequest{
			BackupName: destBackup,
		})

		if checkResp.GetCode() == backuppb.ResponseCode_Success && checkResp.GetData() != nil {
			if force {
				PrintWarning(fmt.Sprintf("Destination backup '%s' already exists, will overwrite using force mode", destBackup))
				
				// Delete existing backup in destination cluster
				PrintInfo(fmt.Sprintf("Deleting existing backup '%s' from destination cluster...", destBackup))
				deleteResp := destBackupContext.DeleteBackup(destCtx, &backuppb.DeleteBackupRequest{
					BackupName: destBackup,
				})
				
				if deleteResp.GetCode() != backuppb.ResponseCode_Success {
					PrintError("Failed to delete existing backup in destination cluster", fmt.Errorf(deleteResp.GetMsg()), 0)
					os.Exit(1)
				}
				
				PrintSuccess(fmt.Sprintf("Existing backup '%s' successfully deleted", destBackup))
			} else {
				PrintError(fmt.Sprintf("Backup '%s' already exists in destination cluster", destBackup), nil, 0)
				PrintInfo("Use a different destination backup name or add --force flag to overwrite")
				os.Exit(1)
			}
		}

		// Prepare collection names for backup
		var collectionNameArr []string
		// 获取集合名称列表
		collectionNameArr = []string{}
		for _, colBackup := range backupInfo.GetCollectionBackups() {
			collectionNameArr = append(collectionNameArr, colBackup.GetCollectionName())
		}

		// Step 4: Initialize storage clients for source and destination
		PrintInfo("Initializing storage clients...")
		
		// Source storage configuration
		sourceStorageConfig := &storage.StorageConfig{
			StorageType:       sourceParams.MinioCfg.StorageType,
			Address:           sourceParams.MinioCfg.Address + ":" + sourceParams.MinioCfg.Port,
			BucketName:        sourceParams.MinioCfg.BackupBucketName,
			AccessKeyID:       sourceParams.MinioCfg.AccessKeyID,
			SecretAccessKeyID: sourceParams.MinioCfg.SecretAccessKey,
			UseSSL:            sourceParams.MinioCfg.UseSSL,
			UseIAM:            sourceParams.MinioCfg.UseIAM,
			IAMEndpoint:       sourceParams.MinioCfg.IAMEndpoint,
			RootPath:          sourceParams.MinioCfg.BackupRootPath,
			CreateBucket:      false,
		}

		// Destination storage configuration
		destStorageConfig := &storage.StorageConfig{
			StorageType:       destParams.MinioCfg.StorageType,
			Address:           destParams.MinioCfg.Address + ":" + destParams.MinioCfg.Port,
			BucketName:        destParams.MinioCfg.BackupBucketName,
			AccessKeyID:       destParams.MinioCfg.AccessKeyID,
			SecretAccessKeyID: destParams.MinioCfg.SecretAccessKey,
			UseSSL:            destParams.MinioCfg.UseSSL,
			UseIAM:            destParams.MinioCfg.UseIAM,
			IAMEndpoint:       destParams.MinioCfg.IAMEndpoint,
			RootPath:          destParams.MinioCfg.BackupRootPath,
			CreateBucket:      true, // Ensure the bucket exists
		}

		// Create storage clients
		sourceStorage, err := storage.NewChunkManager(sourceCtx, &sourceParams, sourceStorageConfig)
		if err != nil {
			PrintError("Failed to create source storage client", err, 0)
			os.Exit(1)
		}

		destStorage, err := storage.NewChunkManager(destCtx, &destParams, destStorageConfig)
		if err != nil {
			PrintError("Failed to create destination storage client", err, 0)
			os.Exit(1)
		}

		// Step 5: Create a copier to copy files between storages
		PrintInfo("Setting up data copier...")
		
		// Configure the copier
		copyOpt := storage.CopyOption{
			WorkerNum:    workerCount,
			CopyByServer: false, // Direct copy between storages
		}

		copier := storage.NewCopier(sourceStorage, destStorage, copyOpt)

		// Step 6: Copy the backup data
		PrintSectionTitle("STARTING BACKUP DATA COPY")
		PrintKeyValue("From", sourceAlias)
		PrintKeyValue("To", destAlias)
		PrintKeyValue("Parallel Workers", workerCount)
		PrintInfo("Starting copy operation...")
		start := time.Now()

		// Source and destination paths
		sourcePath := path.Join(sourceParams.MinioCfg.BackupRootPath, sourceBackup)
		destPath := path.Join(destParams.MinioCfg.BackupRootPath, destBackup)

		// Setup copy input
		copyInput := storage.CopyPathInput{
			SrcBucket:  sourceParams.MinioCfg.BackupBucketName,
			SrcPrefix:  sourcePath,
			DestBucket: destParams.MinioCfg.BackupBucketName,
			DestKeyFn: func(attr storage.ObjectAttr) string {
				// Replace source backup name with destination backup name in the path
				relativePath := strings.TrimPrefix(attr.Key, sourcePath)
				return path.Join(destPath, relativePath)
			},
			OnSuccess: func(attr storage.ObjectAttr) {
				if showProgress {
					fmt.Printf("Copied: %s (%d bytes)\n", attr.Key, attr.Length)
				}
			},
		}

		// Perform the copy operation
		err = copier.CopyPrefix(sourceCtx, copyInput)
		if err != nil {
			PrintError("Failed to copy backup data", err, 0)
			os.Exit(1)
		}

		// No need to create backup metadata as we've copied all files including metadata
		PrintSuccess("All backup files have been copied successfully")

		// Calculate and display statistics
		duration := time.Since(start).Round(time.Second)
		process := copier.Process()
		totalSizeMB := float64(process.TotalSize) / (1024 * 1024)

		PrintSectionTitle("BACKUP COPY COMPLETED!")
		PrintKeyValue("Source", fmt.Sprintf("%s/%s", sourceAlias, sourceBackup))
		PrintKeyValue("Destination", fmt.Sprintf("%s/%s", destAlias, destBackup))
		PrintKeyValue("Files Copied", process.TotalCnt)
		PrintKeyValue("Data Copied", fmt.Sprintf("%.2f MB", totalSizeMB))
		PrintKeyValue("Duration", duration)
		PrintKeyValue("Average Speed", fmt.Sprintf("%.2f MB/s", totalSizeMB/duration.Seconds()))
		
		// 获取目标备份的详细信息
		getDestResp := destBackupContext.GetBackup(destCtx, &backuppb.GetBackupRequest{
			BackupName: destBackup,
		})
		
		if getDestResp.GetCode() == backuppb.ResponseCode_Success && getDestResp.GetData() != nil {
			destBackupInfo := getDestResp.GetData()
			PrintKeyValue("Destination Backup ID", destBackupInfo.GetId())
			
			// Collect database and collection information
			dbCollMap := make(map[string][]string)
			
			if len(destBackupInfo.GetCollectionBackups()) > 0 {
				PrintSubsectionTitle("COPIED COLLECTIONS")
				
				// Create a table for collections
				table := CreateTable([]string{"Collection Name", "Database", "Collection ID"})
				
				for _, coll := range destBackupInfo.GetCollectionBackups() {
					// Get database name
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
		}
		
		fmt.Println()
		PrintSuccess("Backup copy operation completed successfully")
		PrintInfo(fmt.Sprintf("You can view the destination backup using: %s", colorValue(fmt.Sprintf("./milvus-backup list %s", destAlias))))
	},
}



func init() {
	copyBackupCmd.Flags().StringVar(&sourceAlias, "source-alias", "", "Alias of the source cluster")
	copyBackupCmd.Flags().StringVar(&sourceBackup, "source-backup", "", "Name of the backup to copy from the source cluster")
	copyBackupCmd.Flags().StringVar(&destAlias, "dest-alias", "", "Alias of the destination cluster")
	copyBackupCmd.Flags().StringVar(&destBackup, "dest-backup", "", "Name to use for the backup in the destination cluster (must be the same as source backup name)")
	copyBackupCmd.Flags().IntVar(&workerCount, "workers", 10, "Number of parallel workers for copying data")
	copyBackupCmd.Flags().BoolVar(&showProgress, "show-progress", false, "Show detailed progress of file copying")
	copyBackupCmd.Flags().BoolVar(&force, "force", false, "Force overwrite of existing backup in destination cluster")

	rootCmd.AddCommand(copyBackupCmd)
}
