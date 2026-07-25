package restore

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
		{name: "Collections", args: []string{"--collections", "coll1"}, want: "--collections was removed in 0.6, use --filter instead"},
		{name: "CollectionsShorthand", args: []string{"-c", "coll1"}, want: "--collections was removed in 0.6, use --filter instead"},
		{name: "Databases", args: []string{"--databases", "db1"}, want: "--databases was removed in 0.6, use --filter instead"},
		{name: "DatabaseCollections", args: []string{"--database_collections", `{"db1":[]}`}, want: "--database_collections was removed in 0.6, use --filter instead"},
		{name: "RestoreIndex", args: []string{"--restore_index"}, want: "--restore_index was removed in 0.6, use --rebuild_index instead"},
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

func TestOptions_validate(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		var o options
		o.backupName = "backup"
		err := o.validate()
		assert.NoError(t, err)
	})

	t.Run("BackupNameEmpty", func(t *testing.T) {
		var o options
		err := o.validate()
		assert.Error(t, err)
	})

	t.Run("DropAndNotCreate", func(t *testing.T) {
		var o options
		o.backupName = "backup"
		o.dropExistCollection = true
		o.skipCreateCollection = true
		err := o.validate()
		assert.Error(t, err)
	})

	t.Run("ConflictingRenameFlags", func(t *testing.T) {
		var o options
		o.backupName = "backup"
		o.renameSuffix = "suffix"
		o.renameCollectionNames = "rename"
		err := o.validate()
		assert.Error(t, err)
	})
}

func TestOptions_toTaskFilter(t *testing.T) {
	t.Run("NoFilter", func(t *testing.T) {
		var o options
		f, err := o.toTaskFilter()
		assert.NoError(t, err)
		assert.Empty(t, f.DBCollFilter)
	})

	t.Run("Normal", func(t *testing.T) {
		var o options
		o.filter = "db1.*,db2.coll1,coll3,db3."
		f, err := o.toTaskFilter()
		assert.NoError(t, err)
		assert.Equal(t, map[string]filter.CollFilter{
			"db1":     {AllowAll: true},
			"db2":     {CollName: map[string]struct{}{"coll1": {}}},
			"default": {CollName: map[string]struct{}{"coll3": {}}},
			"db3":     {}, // db3. rule produces empty CollFilter (database only, no collections)
		}, f.DBCollFilter)
	})
}
