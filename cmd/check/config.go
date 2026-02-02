package check

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
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

		Run: func(cmd *cobra.Command, args []string) {
			params := opt.InitGlobalVars()
			printConfig(cmd.OutOrStdout(), params)
		},
	}

	return cmd
}

func printConfig(w io.Writer, c *cfg.Config) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARAMETER\tVALUE\tSOURCE\tSOURCE_KEY")
	fmt.Fprintln(tw, "---------\t-----\t------\t----------")

	for _, e := range c.Entries() {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", e.Name, e.Value, e.Source, e.SourceKey)
	}
	tw.Flush()
}
