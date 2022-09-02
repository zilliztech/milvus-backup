package core

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/util/paramtable"
	"testing"
)

func TestCreateBackup(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	req := &backuppb.CreateBackupRequest{
		BackupName: "test_backup7",
	}
	backup.CreateBackup(context, req)
}

func TestListBackups(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backupLists, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, backupLists.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)

	backupListsWithCollection, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
		CollectionName: "hello_milvus",
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(backupListsWithCollection.BackupInfos))

	backupListsWithCollection2, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
		CollectionName: "hello_milvus2",
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(backupListsWithCollection2.BackupInfos))
}

func TestGetBackup(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: "test_backup",
	})
	assert.NoError(t, err)
	assert.Equal(t, backup.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)
}

func TestDeleteBackup(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backup, err := backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: "test_backup6",
	})
	assert.NoError(t, err)
	assert.Equal(t, backup.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)

	backupLists, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, backupLists.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)

	assert.Equal(t, 0, len(backupLists.GetBackupInfos()))

}
