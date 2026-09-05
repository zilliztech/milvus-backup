package check

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/app"
	"github.com/zilliztech/milvus-backup/cmd/root"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// writeConfig prints a labeled table of the effective configuration to w.
func writeConfig(w io.Writer, c *v2.Config) error {
	if _, err := io.WriteString(w, "Configuration:\n"); err != nil {
		return fmt.Errorf("check: write config header: %w", err)
	}
	if err := c.WriteTable(w); err != nil {
		return fmt.Errorf("check: write config table: %w", err)
	}
	return nil
}

func NewCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check connectivity to Milvus and object storage",

		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			params := opt.InitGlobalVars()

			// Print the effective configuration before constructing any client,
			// so the resolved values and their sources are visible even when
			// connecting to Milvus or the object storage fails.
			cobra.CheckErr(writeConfig(cmd.OutOrStdout(), params))

			uc, err := app.NewCheck(ctx, params)
			cobra.CheckErr(err)

			cobra.CheckErr(uc.Execute(ctx, cmd.OutOrStdout()))

			return nil
		},
	}

	cmd.AddCommand(newConfigCmd())

	return cmd
}
