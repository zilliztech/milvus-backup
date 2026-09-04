package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

// expectFullMeta teaches the mock client to serve the full meta of one backup:
// the exist check lists the key, then the read fetches it.
func expectFullMeta(t *testing.T, cli *storage.MockClient, backupDir string, info *backuppb.BackupInfo) {
	t.Helper()

	byts, err := json.Marshal(info)
	require.NoError(t, err)

	key := backupDir + "/meta/full_meta.json"
	iter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: key, Length: int64(len(byts))},
	})
	cli.EXPECT().ListPrefix(mock.Anything, key, false).Return(iter, nil)
	cli.EXPECT().GetObject(mock.Anything, key).
		Return(&storage.Object{Length: int64(len(byts)), Body: io.NopCloser(bytes.NewReader(byts))}, nil)
}

func TestListBackupsExecute(t *testing.T) {
	t.Run("ListsOneSummaryPerBackup", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		backupRootIter := storage.NewMockObjectIterator([]storage.ObjectAttr{
			{Key: "root/backup1"},
			{Key: "root/backup2"},
		})
		cli.EXPECT().ListPrefix(mock.Anything, "root/", false).Return(backupRootIter, nil)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"})
		expectFullMeta(t, cli, "root/backup2",
			&backuppb.BackupInfo{Id: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"})

		uc := &ListBackups{cli: cli, rootPath: "root"}
		summaries, err := uc.Execute(context.Background())

		require.NoError(t, err)
		expected := []BackupSummary{
			{ID: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"},
			{ID: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"},
		}
		assert.ElementsMatch(t, expected, summaries)
	})

	t.Run("SkipsBackupWithUnreadableMeta", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		backupRootIter := storage.NewMockObjectIterator([]storage.ObjectAttr{
			{Key: "root/backup1"},
			{Key: "root/backup2"},
		})
		cli.EXPECT().ListPrefix(mock.Anything, "root/", false).Return(backupRootIter, nil)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"})

		// The second backup's meta cannot even be checked for existence.
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup2/meta/full_meta.json", false).
			Return(nil, errors.New("stat denied"))

		uc := &ListBackups{cli: cli, rootPath: "root"}
		summaries, err := uc.Execute(context.Background())

		require.NoError(t, err)
		expected := []BackupSummary{
			{ID: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"},
		}
		assert.Equal(t, expected, summaries)
	})

	t.Run("FailsWhenRootListingFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		cli.EXPECT().
			ListPrefix(mock.Anything, "root/", false).
			Return(nil, errors.New("connection closed"))

		uc := &ListBackups{cli: cli, rootPath: "root"}
		_, err := uc.Execute(context.Background())

		assert.Error(t, err)
	})
}
