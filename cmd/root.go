package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	config string
)

var rootCmd = &cobra.Command{
	Use:   "milvus-backup",
	Short: "milvus-backup is a backup&restore tool for milvus.",
	Long:  `milvus-backup is a backup&restore tool for milvus.`,
	Run: func(cmd *cobra.Command, args []string) {
		Error(cmd, args, errors.New("unrecognized command"))
	},
}

func Execute() {
	rootCmd.PersistentFlags().StringVarP(&config, "config", "", "backup.yaml", "config YAML file of milvus")
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.Execute()
}

func SetVersionInfo(version, commit, date string) {
	rootCmd.Version = fmt.Sprintf("%s (Built on %s from Git SHA %s)", version, date, commit)
}
