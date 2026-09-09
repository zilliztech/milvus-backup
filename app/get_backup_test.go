package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// expectMetaExist teaches the mock client whether the backup dir has a meta
// at all: the exist check lists the backup_meta.json key.
func expectMetaExist(t *testing.T, cli *storage.MockClient, backupDir string, exist bool) {
	t.Helper()

	var iter *storage.MockObjectIterator
	if exist {
		iter = storage.NewMockObjectIterator([]storage.ObjectAttr{
			{Key: backupDir + "/meta/backup_meta.json"},
		})
	} else {
		iter = storage.NewMockObjectIterator(nil)
	}
	cli.EXPECT().ListPrefix(mock.Anything, backupDir+"/meta/backup_meta.json", false).Return(iter, nil)
}

// expectMetaSize teaches the mock client to answer the meta dir size probe.
func expectMetaSize(t *testing.T, cli *storage.MockClient, backupDir string, size int64) {
	t.Helper()

	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: backupDir + "/meta/full_meta.json", Length: size},
	})
	cli.EXPECT().ListPrefix(mock.Anything, backupDir+"/meta/", true).Return(iter, nil)
}

// expectReadableBackup teaches the mock client the whole read of one backup:
// meta exists, meta is readable, meta dir has a size.
func expectReadableBackup(t *testing.T, cli *storage.MockClient, rootPath, name string, size int64) {
	t.Helper()

	backupDir := rootPath + "/" + name
	expectMetaExist(t, cli, backupDir, true)
	expectFullMeta(t, cli, backupDir, &backuppb.BackupInfo{Name: name, Size: size})
	expectMetaSize(t, cli, backupDir, size)
}

func TestGetBackupExecute(t *testing.T) {
	t.Run("ReadsMeta", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectReadableBackup(t, cli, "root", "backup1", 100)

		uc := &GetBackup{cli: cli, rootPath: "root"}
		info, metaSize, err := uc.Execute(context.Background(), "backup1")

		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, "backup1", info.GetName())
		assert.Equal(t, int64(100), metaSize)
	})

	t.Run("NameWithNothingBehindItIsNotFound", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		expectMetaExist(t, cli, "root/backup1", false)

		uc := &GetBackup{cli: cli, rootPath: "root"}
		info, metaSize, err := uc.Execute(context.Background(), "backup1")

		assert.Nil(t, info)
		assert.Zero(t, metaSize)
		assert.ErrorIs(t, err, ErrBackupNotFound)
		assert.Contains(t, err.Error(), "backup1")
	})

	t.Run("RejectsEmptyName", func(t *testing.T) {
		uc := &GetBackup{cli: storage.NewMockClient(t), rootPath: "root"}
		info, _, err := uc.Execute(context.Background(), "")

		assert.Nil(t, info)
		assert.Error(t, err)
	})

	t.Run("FailsWhenExistCheckFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup1/meta/backup_meta.json", false).
			Return(nil, errors.New("connection closed"))

		uc := &GetBackup{cli: cli, rootPath: "root"}
		info, _, err := uc.Execute(context.Background(), "backup1")

		assert.Nil(t, info)
		assert.Error(t, err)
	})

	t.Run("FailsWhenSizeProbeFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)
		backupDir := "root/backup1"
		expectMetaExist(t, cli, backupDir, true)
		expectFullMeta(t, cli, backupDir, &backuppb.BackupInfo{Name: "backup1"})
		cli.EXPECT().
			ListPrefix(mock.Anything, backupDir+"/meta/", true).
			Return(nil, errors.New("connection closed"))

		uc := &GetBackup{cli: cli, rootPath: "root"}
		info, _, err := uc.Execute(context.Background(), "backup1")

		assert.Nil(t, info)
		assert.Error(t, err)
	})
}
