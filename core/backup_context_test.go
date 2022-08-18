package core

import (
	"context"
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
