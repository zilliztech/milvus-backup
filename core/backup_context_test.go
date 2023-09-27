package core

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/utils"
	"github.com/zilliztech/milvus-backup/internal/log"
	"go.uber.org/zap"
)

func TestCreateBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	req := &backuppb.CreateBackupRequest{
		BackupName: "test_21",
		//CollectionNames: []string{"hello_milvus", "hello_milvus2"},
		DbCollections: utils.WrapDBCollections(""),
	}
	backup.CreateBackup(context, req)
}

func TestListBackups(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backupLists := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.Equal(t, backupLists.GetCode(), backuppb.ResponseCode_Success)

	backupListsWithCollection := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{
		//CollectionName: "hello_milvus",
	})

	for _, backup := range backupListsWithCollection.GetData() {
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

	backup := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: "mybackup",
	})
	assert.Equal(t, backup.GetCode(), backuppb.ResponseCode_Success)
}

func TestDeleteBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backupContext := CreateBackupContext(context, params)

	backup := backupContext.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: "test_backup6",
	})
	assert.Equal(t, backup.GetCode(), backuppb.ResponseCode_Success)

	backupLists := backupContext.ListBackups(context, &backuppb.ListBackupsRequest{})
	assert.Equal(t, backupLists.GetCode(), backuppb.ResponseCode_Success)

	assert.Equal(t, 0, len(backupLists.GetData()))

}

func TestCreateBackupWithNoName(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)

	randBackupName := ""

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp := backup.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())

	// clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
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
	resp := backup.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())
	assert.Equal(t, "request backup collection does not exist: not_exist", resp.GetMsg())

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
	resp := backup.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())

	req2 := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp2 := backup.CreateBackup(context, req2)
	assert.Equal(t, backuppb.ResponseCode_Fail, resp2.GetCode())
	assert.Equal(t, fmt.Sprintf("backup already exist with the name: %s", req2.GetBackupName()), resp2.GetMsg())

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
	resp := backup.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Fail, resp.GetCode())

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
	resp := backupContext.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())

	backup := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: randBackupName,
	})
	assert.Equal(t, backuppb.ResponseCode_Success, backup.GetCode())

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
	resp := backupContext.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())

	backupContext.getStorageClient().RemoveWithPrefix(context, params.MinioCfg.BackupBucketName, BackupMetaPath(params.MinioCfg.BackupRootPath, resp.GetData().GetName()))

	backup := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: randBackupName,
	})
	assert.Equal(t, backuppb.ResponseCode_Fail, backup.GetCode())

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

	backup := backupContext.GetBackup(context, &backuppb.GetBackupRequest{
		BackupName: "un_exist",
	})
	assert.Equal(t, backuppb.ResponseCode_Fail, backup.GetCode())
}

func TestRestoreBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)
	backup.Start()
	backupName := "demo"
	//fmt.Sprintf("test_%d", rand.Int())

	restoreResp := backup.RestoreBackup(context, &backuppb.RestoreBackupRequest{
		BackupName:    backupName,
		DbCollections: utils.WrapDBCollections("{\"default\":[]}"),
	})
	log.Info("restore backup", zap.Any("resp", restoreResp))
}

func TestCreateAndRestoreBackup(t *testing.T) {
	var params paramtable.BackupParams
	params.Init()
	context := context.Background()
	backup := CreateBackupContext(context, params)
	backup.Start()
	randBackupName := "test"
	//fmt.Sprintf("test_%d", rand.Int())

	req := &backuppb.CreateBackupRequest{
		BackupName: randBackupName,
	}
	resp := backup.CreateBackup(context, req)
	assert.Equal(t, backuppb.ResponseCode_Success, resp.GetCode())

	getReq := &backuppb.GetBackupRequest{
		BackupName: randBackupName,
	}
	getResp := backup.GetBackup(context, getReq)
	assert.Equal(t, backuppb.ResponseCode_Success, getResp.GetCode())

	restoreResp := backup.RestoreBackup(context, &backuppb.RestoreBackupRequest{
		BackupName:       randBackupName,
		CollectionSuffix: "_recover",
	})
	log.Info("restore backup", zap.Any("resp", restoreResp))

	//clean
	backup.DeleteBackup(context, &backuppb.DeleteBackupRequest{
		BackupName: randBackupName,
	})
}
