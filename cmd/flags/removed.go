// Package flags holds flag helpers shared by the milvus-backup subcommands.
package flags

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Removed is a flag that no longer has an implementation behind it. The 0.5 line
// still honored these and only logged a deprecation warning; 0.6 rejects them.
//
// Keeping them registered is not what makes the rejection safe: cobra already
// fails on an unknown flag and exits non-zero, so all these stubs buy is the
// message naming the replacement. That is worth one release to whoever upgrades,
// and dropping them later degrades the message without letting anything through.
//
// TODO: delete the removed-flag tables and this package in a release after 0.6.
type Removed struct {
	// Name is the flag as it was spelled while it still worked.
	Name string
	// Shorthand is its one-letter form, empty if it never had one.
	Shorthand string
	// NoValue marks a flag that took no argument, so that it is registered as a
	// boolean and an old command line spelling it as a bare "--force" parses.
	NoValue bool
	// Advice completes the sentence "--name was removed in 0.6, ...".
	Advice string
}

// AddRemoved registers removed flags so that an outdated command line still
// parses and CheckRemoved can answer it by naming the replacement, instead of
// cobra reporting a bare "unknown flag". They are hidden from help: they do
// nothing, and listing them beside the working flags would imply otherwise.
func AddRemoved(cmd *cobra.Command, removed []Removed) {
	for _, r := range removed {
		if r.NoValue {
			cmd.Flags().BoolP(r.Name, r.Shorthand, false, r.Advice)
		} else {
			cmd.Flags().StringP(r.Name, r.Shorthand, "", r.Advice)
		}
		// The flag was registered a line ago, so it cannot be missing.
		_ = cmd.Flags().MarkHidden(r.Name)
	}
}

// CheckRemoved fails if the command line uses a removed flag. Callers run it
// before any other work, so that an outdated command is answered by the flag
// that has to change rather than by an unrelated complaint.
func CheckRemoved(cmd *cobra.Command, removed []Removed) error {
	for _, r := range removed {
		if cmd.Flags().Changed(r.Name) {
			return fmt.Errorf("--%s was removed in 0.6, %s", r.Name, r.Advice)
		}
	}

	return nil
}
