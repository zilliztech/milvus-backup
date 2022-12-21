package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

const Version = "1.0-beta"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "print the version of Milvus backup tool",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(fmt.Sprintf("Version: %s", Version))
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
