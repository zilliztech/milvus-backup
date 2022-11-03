package cmd

import (
	"errors"
	"github.com/spf13/cobra"
)

var (
	config string
)

var rootCmd = &cobra.Command{
	Use:   "milvus-backup",
	Short: "milvus-backup is a backup tool for milvus.",
	Long:  `milvus-backup is a backup tool for milvus.`,
	Run: func(cmd *cobra.Command, args []string) {
		Error(cmd, args, errors.New("unrecognized command"))
	},
}

func Execute() {
	rootCmd.PersistentFlags().StringVarP(&config, "config", "", "backup.yaml", "config YAML file of milvus")

	rootCmd.Execute()
}
