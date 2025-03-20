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
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/internal/alias"
)

var aliasCmd = &cobra.Command{
	Use:   "alias",
	Short: "alias subcommand manages cluster aliases",
	Long:  `alias subcommand manages cluster aliases for cross-cluster backup and restore operations`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var aliasSetCmd = &cobra.Command{
	Use:   "set",
	Short: "set an alias for a cluster",
	Long:  `set an alias for a cluster with a configuration file`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("Error: alias name is required")
			cmd.Help()
			return
		}

		aliasName := args[0]
		configFile, _ := cmd.Flags().GetString("config")

		manager, err := alias.NewAliasManager()
		if err != nil {
			fmt.Printf("Error initializing alias manager: %v\n", err)
			os.Exit(1)
		}

		if err := manager.SetAlias(aliasName, configFile); err != nil {
			fmt.Printf("Error setting alias: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Alias '%s' set to use config file: %s\n", aliasName, configFile)
	},
}

var aliasListCmd = &cobra.Command{
	Use:   "list",
	Short: "list all aliases",
	Long:  `list all configured cluster aliases`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, err := alias.NewAliasManager()
		if err != nil {
			fmt.Printf("Error initializing alias manager: %v\n", err)
			os.Exit(1)
		}

		aliases := manager.ListAliases()
		if len(aliases) == 0 {
			fmt.Println("No aliases configured")
			return
		}

		fmt.Println("Configured aliases:")
		fmt.Println("-------------------")
		for _, a := range aliases {
			fmt.Printf("Name: %s\nConfig: %s\n\n", a.Name, a.ConfigFilePath)
		}
	},
}

var aliasDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete an alias",
	Long:  `delete a configured cluster alias`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("Error: alias name is required")
			cmd.Help()
			return
		}

		aliasName := args[0]
		manager, err := alias.NewAliasManager()
		if err != nil {
			fmt.Printf("Error initializing alias manager: %v\n", err)
			os.Exit(1)
		}

		if err := manager.DeleteAlias(aliasName); err != nil {
			fmt.Printf("Error deleting alias: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Alias '%s' deleted\n", aliasName)
	},
}

func init() {
	aliasSetCmd.Flags().StringP("config", "c", "", "Path to the configuration file for the alias")
	aliasSetCmd.MarkFlagRequired("config")

	aliasCmd.AddCommand(aliasSetCmd)
	aliasCmd.AddCommand(aliasListCmd)
	aliasCmd.AddCommand(aliasDeleteCmd)

	rootCmd.AddCommand(aliasCmd)
}
