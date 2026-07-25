package flags

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRemoved = []Removed{
	{Name: "colls", Shorthand: "c", Advice: "use --filter instead"},
	{Name: "force", Shorthand: "f", NoValue: true, Advice: "use --strategy=skip_flush instead"},
	{Name: "meta_only", NoValue: true, Advice: "use --strategy=meta_only instead"},
}

// newTestCmd returns a command carrying testRemoved, with the given command line
// already parsed into it.
func newTestCmd(t *testing.T, args ...string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "test"}
	AddRemoved(cmd, testRemoved)
	require.NoError(t, cmd.ParseFlags(args))

	return cmd
}

func TestAddRemoved(t *testing.T) {
	cmd := newTestCmd(t)

	t.Run("Registered", func(t *testing.T) {
		for _, r := range testRemoved {
			f := cmd.Flags().Lookup(r.Name)
			require.NotNil(t, f, "flag --%s should still parse", r.Name)
			assert.Equal(t, r.Shorthand, f.Shorthand)
		}
	})

	t.Run("HiddenFromHelp", func(t *testing.T) {
		usage := cmd.UsageString()
		for _, r := range testRemoved {
			assert.True(t, cmd.Flags().Lookup(r.Name).Hidden)
			assert.NotContains(t, usage, "--"+r.Name)
		}
	})
}

func TestCheckRemoved(t *testing.T) {
	t.Run("NotUsed", func(t *testing.T) {
		assert.NoError(t, CheckRemoved(newTestCmd(t), testRemoved))
	})

	t.Run("LongForm", func(t *testing.T) {
		err := CheckRemoved(newTestCmd(t, "--colls", "coll1"), testRemoved)
		require.Error(t, err)
		assert.Equal(t, "--colls was removed in 0.6, use --filter instead", err.Error())
	})

	t.Run("Shorthand", func(t *testing.T) {
		err := CheckRemoved(newTestCmd(t, "-c", "coll1"), testRemoved)
		require.Error(t, err)
		assert.Equal(t, "--colls was removed in 0.6, use --filter instead", err.Error())
	})

	// A flag that took no value has to keep parsing bare, the way it was written
	// on an old command line.
	t.Run("NoValueBare", func(t *testing.T) {
		err := CheckRemoved(newTestCmd(t, "--force"), testRemoved)
		require.Error(t, err)
		assert.Equal(t, "--force was removed in 0.6, use --strategy=skip_flush instead", err.Error())
	})

	// Setting a removed flag to its zero value is still a command line that has
	// to be rewritten, so it is rejected rather than quietly accepted.
	t.Run("NoValueExplicitFalse", func(t *testing.T) {
		err := CheckRemoved(newTestCmd(t, "--meta_only=false"), testRemoved)
		require.Error(t, err)
		assert.Equal(t, "--meta_only was removed in 0.6, use --strategy=meta_only instead", err.Error())
	})
}
