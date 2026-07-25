package create

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/filter"
)

// A removed flag has to fail before the command reaches any other work, so that
// an outdated command line is answered by the flag that has to change.
func TestNewCmd_RemovedFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "Colls", args: []string{"--colls", "coll1"}, want: "--colls was removed in 0.6, use --filter instead"},
		{name: "CollsShorthand", args: []string{"-c", "coll1"}, want: "--colls was removed in 0.6, use --filter instead"},
		{name: "Databases", args: []string{"--databases", "db1"}, want: "--databases was removed in 0.6, use --filter instead"},
		{name: "DatabaseCollections", args: []string{"--database_collections", `{"db1":[]}`}, want: "--database_collections was removed in 0.6, use --filter instead"},
		{name: "Force", args: []string{"--force"}, want: "--force was removed in 0.6, use --strategy=skip_flush instead"},
		{name: "MetaOnly", args: []string{"--meta_only"}, want: "--meta_only was removed in 0.6, use --strategy=meta_only instead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCmd(&root.Options{})
			cmd.SetArgs(tt.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			err := cmd.Execute()
			require.Error(t, err)
			assert.Equal(t, tt.want, err.Error())
		})
	}
}

func TestOptions_toFilter(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		var o options
		f, err := o.toFilter()
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})

	t.Run("Normal", func(t *testing.T) {
		o := options{filter: "coll1,db1.coll2,db2.*"}
		f, err := o.toFilter()
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"default": {CollName: map[string]struct{}{"coll1": {}}},
			"db1":     {CollName: map[string]struct{}{"coll2": {}}},
			"db2":     {AllowAll: true},
		}, f.DBCollFilter)
	})

	t.Run("Invalid", func(t *testing.T) {
		o := options{filter: "a.b.c"}
		_, err := o.toFilter()
		assert.Error(t, err)
	})
}
