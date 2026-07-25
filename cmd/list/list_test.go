package list

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/cmd/root"
)

// A removed flag has to fail before the command reaches any other work, so that
// an outdated command line is answered by the flag that has to change.
func TestNewCmd_RemovedFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "Collection", args: []string{"--collection", "coll1"}},
		{name: "CollectionShorthand", args: []string{"-c", "coll1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCmd(&root.Options{})
			cmd.SetArgs(tt.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			err := cmd.Execute()
			require.Error(t, err)
			assert.Equal(t, "--collection was removed in 0.6, listing backups by collection is no longer supported", err.Error())
		})
	}
}
