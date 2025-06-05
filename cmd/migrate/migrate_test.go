package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions_Validate(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		var o options
		o.backupName = "backup"
		o.clusterID = "cluster"
		err := o.validate()
		assert.NoError(t, err)
	})

	t.Run("BackupNameEmpty", func(t *testing.T) {
		var o options
		o.clusterID = "cluster"
		err := o.validate()
		assert.Error(t, err)
	})

	t.Run("ClusterIDEmpty", func(t *testing.T) {
		var o options
		o.backupName = "backup"
		err := o.validate()
		assert.Error(t, err)
	})
}
