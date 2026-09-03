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

func TestDeleteBackupExecute(t *testing.T) {
	t.Run("DeletesEveryObjectUnderTheBackupDir", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})

		// The delete itself lists the backup dir recursively, then removes
		// every key it yields.
		iter := storage.NewMockObjectIterator([]storage.ObjectAttr{
			{Key: "root/backup1/meta/full_meta.json", Length: 10},
			{Key: "root/backup1/data/1.parquet", Length: 100},
			{Key: "root/backup1/data/2.parquet", Length: 200},
		})
		cli.EXPECT().ListPrefix(mock.Anything, "root/backup1/", true).Return(iter, nil)
		for _, key := range []string{
			"root/backup1/meta/full_meta.json",
			"root/backup1/data/1.parquet",
			"root/backup1/data/2.parquet",
		} {
			cli.EXPECT().DeleteObject(mock.Anything, key).Return(nil)
		}

		uc := &DeleteBackup{cli: cli, rootPath: "root"}
		err := uc.Execute(context.Background(), "backup1")

		require.NoError(t, err)
	})

	t.Run("RefusesWhenMetaUnreadable", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		// The meta exist check fails, so the delete never starts; an
		// unexpected DeleteObject call would fail the mock.
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup1/meta/full_meta.json", false).
			Return(nil, errors.New("stat denied"))

		uc := &DeleteBackup{cli: cli, rootPath: "root"}
		err := uc.Execute(context.Background(), "backup1")

		assert.ErrorContains(t, err, "stat denied")
	})

	t.Run("FailsWhenDeleteListingFails", func(t *testing.T) {
		cli := storage.NewMockClient(t)

		expectFullMeta(t, cli, "root/backup1",
			&backuppb.BackupInfo{Id: "a", Name: "backup1"})
		cli.EXPECT().
			ListPrefix(mock.Anything, "root/backup1/", true).
			Return(nil, errors.New("connection closed"))

		uc := &DeleteBackup{cli: cli, rootPath: "root"}
		err := uc.Execute(context.Background(), "backup1")

		assert.ErrorContains(t, err, "connection closed")
	})
}
