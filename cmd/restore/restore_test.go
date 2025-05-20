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
