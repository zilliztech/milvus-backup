package check

import (
	"github.com/spf13/cobra"
)

// newConfigCmd is the deprecated predecessor of "config show". It stays only to
// redirect existing callers to the new command and prints nothing else; cobra
// emits the deprecation notice and hides it from the help listing.
//
// TODO: remove this alias in a release after 0.6.0.
func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:        "config",
		Short:      `Deprecated: use "config show" instead`,
		Deprecated: `use "config show" instead; this alias will be removed in a release after 0.6.0`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	return cmd
}
