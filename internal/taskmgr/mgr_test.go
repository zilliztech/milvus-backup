package taskmgr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMgr_AddBackupTask_DuplicateName(t *testing.T) {
	mgr := NewMgr()

	// first create should succeed
	assert.NoError(t, mgr.AddBackupTask("task1", "backupA"))

	// second create with same backup name should fail by default (existing task is still running)
	err := mgr.AddBackupTask("task2", "backupA")
	assert.Error(t, err)
}
