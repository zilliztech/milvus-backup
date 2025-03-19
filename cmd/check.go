package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/core"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/internal/alias"
)

var checkCmd = &cobra.Command{
	Use:   "check [alias]",
	Short: "check if the connects is right.",
	Long:  "check if the connection to Milvus and storage is working correctly. Optionally specify an alias to check a specific cluster.",

	Run: func(cmd *cobra.Command, args []string) {
		var configFile = config
		
		// If an alias is provided, use its config file
		if len(args) > 0 {
			aliasName := args[0]
			aliasManager, err := alias.NewAliasManager()
			if err != nil {
				PrintError("Failed to initialize alias manager", err, 1)
			}
			
			aliasConfigPath, err := aliasManager.GetConfigPath(aliasName)
			if err != nil {
				PrintError(fmt.Sprintf("Failed to get config for alias '%s'", aliasName), err, 1)
			}
			
			configFile = aliasConfigPath
			PrintInfo(fmt.Sprintf("Using config from alias '%s': %s", aliasName, configFile))
		}
		
		var params paramtable.BackupParams
		PrintInfo(fmt.Sprintf("Using config file: %s", configFile))
		params.GlobalInitWithYaml(configFile)
		params.Init()

		PrintSectionTitle("CHECKING CONNECTION")
		PrintInfo("Verifying connection to Milvus and storage...")
		
		context := context.Background()
		backupContext := core.CreateBackupContext(context, &params)

		response := backupContext.Check(context)
		
		// Check if the response indicates success
		if strings.Contains(response, "Succeed to connect") {
			PrintSuccess("Connection check successful")
			PrintKeyValue("Milvus Connection", "OK")
			PrintKeyValue("Storage Connection", "OK")
			
			// Extract and display configuration information
			lines := strings.Split(response, "\n")
			for _, line := range lines {
				if strings.Contains(line, ":") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						key := strings.TrimSpace(parts[0])
						value := strings.TrimSpace(parts[1])
						if key != "" && value != "" {
							PrintKeyValue(key, value)
						}
					}
				}
			}
		} else {
			PrintError("Connection check failed", fmt.Errorf(response), 0)
		}
	},
}

func init() {
	rootCmd.AddCommand(checkCmd)
}
