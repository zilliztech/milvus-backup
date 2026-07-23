package config

import (
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
)

// NewCmd builds the "config" command group, which inspects and migrates
// milvus-backup configuration files.
func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "inspect and migrate milvus-backup configuration",
	}

	cmd.AddCommand(newMigrateCmd(opt))

	return cmd
}
