package check

import (
	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
)

func newConfigCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Print all configuration parameters with their values and sources",
		Long: `Print all configuration parameters in a table format showing:
- Parameter name (config file key)
- Current value (secrets are masked)
- Source: where the value came from (override, env, config, default)
- Source key: the specific key used to set the value`,

		RunE: func(cmd *cobra.Command, args []string) error {
			params := opt.InitGlobalVars()
			return params.WriteTable(cmd.OutOrStdout())
		},
	}

	return cmd
}
