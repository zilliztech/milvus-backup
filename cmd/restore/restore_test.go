package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/internal/filter"
)

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
