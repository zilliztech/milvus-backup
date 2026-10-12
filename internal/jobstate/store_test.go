package jobstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMgr_AddBackupTask_DuplicateName(t *testing.T) {
	mgr := NewStore()

	// first create should succeed
	_, err := mgr.AddBackupTask("task1", "backupA")
	require.NoError(t, err)

	// second create with same backup name should fail by default (existing task is still running)
	_, err = mgr.AddBackupTask("task2", "backupA")
	assert.Error(t, err)
}
