package core

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	req := &backuppb.CreateBackupRequest{
		BackupName: "test_21",
	}
	backup.CreateBackup(context, req)
}

func TestListBackups(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backupLists, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, backupLists.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)

	backupListsWithCollection, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
		CollectionName: "hello_milvus",
	})

	for _, backup := range backupListsWithCollection.BackupInfos {
		fmt.Println(backup.GetName())
	}

	//assert.NoError(t, err)
	//assert.Equal(t, 1, len(backupListsWithCollection.BackupInfos))
	//
	//backupListsWithCollection2, err := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
	//	CollectionName: "hello_milvus2",
	//})
	//assert.NoError(t, err)
	//assert.Equal(t, 0, len(backupListsWithCollection2.BackupInfos))
}

func TestGetBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: "test_backup",
	})
	assert.NoError(t, err)
	assert.Equal(t, backup.GetStatus().GetStatusCode(), backuppb.StatusCode_Success)
}

func TestDeleteBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
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

func TestCreateBackupWithUnexistCollection(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	randBackupName := fmt.Sprintf("test_%d", rand.Int())

	req := &backuppb.CreateBackupRequest{
		BackupName:      randBackupName,
		CollectionNames: []string{"not_exist"},
	}
	resp, err := backup.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Fail, resp.GetStatus().GetStatusCode())
	assert.Equal(t, "request backup collection does not exist: not_exist", resp.GetStatus().GetReason())

	// clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}

func TestCreateBackupWithDuplicateName(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	randBackupName := fmt.Sprintf("test_%d", rand.Int())

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp, err := backup.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Success, resp.GetStatus().GetStatusCode())

	req2 := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp2, err := backup.CreateBackup(context, req2)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Fail, resp2.GetStatus().GetStatusCode())
	assert.Equal(t, fmt.Sprintf("backup already exist with the name: %s", req2.GetBackupName()), resp2.GetStatus().GetReason())

	// clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}

func TestCreateBackupWithIllegalName(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	randBackupName := "dahgg$%123"

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp, err := backup.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Fail, resp.GetStatus().GetStatusCode())

	// clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}

func TestGetBackupAfterCreate(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	randBackupName := fmt.Sprintf("test_%d", rand.Int())

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp, err := backupContext.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Success, resp.GetStatus().GetStatusCode())

	backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: randBackupName,
	})
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Success, backup.GetStatus().GetStatusCode())

	// clean
	backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}

func TestGetBackupFaultBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)
	backupContext.Start()

	randBackupName := fmt.Sprintf("test_%d", rand.Int())

	backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp, err := backupContext.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Success, resp.GetStatus().GetStatusCode())

	backupContext.storageClient.RemoveWithPrefix(context, params.MinioCfg.BackupBucketName, BackupMetaPath(params.MinioCfg.BackupRootPath, resp.GetBackupInfo().GetName()))

	backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: randBackupName,
	})
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Fail, backup.GetStatus().GetStatusCode())

	// clean
	backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}

func TestGetBackupUnexistBackupName(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)
	backupContext.Start()

	backup, err := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: "un_exist",
	})
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Fail, backup.GetStatus().GetStatusCode())
}

func TestRestoreBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)
	backup.Start()
	randBackupName := fmt.Sprintf("test_%d", rand.Int())

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp, err := backup.CreateBackup(context, req)
	assert.NoError(t, err)
	assert.Equal(t, backuppb.StatusCode_Success, resp.GetStatus().GetStatusCode())

	restoreResp, err := backup.RestoreBackup(context, &backuppb.RestoreBackupRequest{
		BackupName:       randBackupName,
		CollectionSuffix: "_recover",
	})
	assert.NoError(t, err)
	log.Info("restore backup", zap.Any("resp", restoreResp))

	//clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}
