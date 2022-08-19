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
		BackupName: "test_backup",
	}
	backup.CreateBackup(context, req)
}

func TestListBackups(t *testing.T) {
	var params paramtable.ComponentParam
	params.InitOnce()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	backupLists, err := backup.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, backupLists.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)
}
