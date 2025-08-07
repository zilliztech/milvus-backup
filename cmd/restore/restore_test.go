package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
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

func TestParseFilter(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		filterMap, err := parseFilter("")
		assert.NoError(t, err)
		assert.Empty(t, filterMap)
	})

	t.Run("Valid", func(t *testing.T) {
		filterMap, err := parseFilter("db1.coll1,db2.coll1,db1.coll2,db3.*")
		assert.NoError(t, err)
		assert.Equal(t, 3, len(filterMap))
		assert.Equal(t, []string{"coll1", "coll2"}, filterMap["db1"].Colls)
		assert.ElementsMatch(t, []string{"coll1"}, filterMap["db2"].Colls)
		assert.ElementsMatch(t, []string{"*"}, filterMap["db3"].Colls)
	})
}
