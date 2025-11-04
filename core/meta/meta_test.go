package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func newTestLevelBackupInfo() *levelBackupInfo {
	return &levelBackupInfo{
		backupInfo: &backuppb.BackupInfo{Id: "backup1"},
		collectionInfo: &backuppb.CollectionLevelBackupInfo{Infos: []*backuppb.CollectionBackupInfo{
			{CollectionId: 1},
			{CollectionId: 2},
		}},
		partitionInfo: &backuppb.PartitionLevelBackupInfo{Infos: []*backuppb.PartitionBackupInfo{
			{CollectionId: 1, PartitionId: 1},
			{CollectionId: 1, PartitionId: 2},
			{CollectionId: 2, PartitionId: 1},
			{CollectionId: 2, PartitionId: 2},
		}},
		segmentInfo: &backuppb.SegmentLevelBackupInfo{Infos: []*backuppb.SegmentBackupInfo{
			{CollectionId: 1, PartitionId: 1, SegmentId: 1},
			{CollectionId: 1, PartitionId: 1, SegmentId: 2},
			{CollectionId: 1, PartitionId: 2, SegmentId: 3},
			{CollectionId: 1, PartitionId: 2, SegmentId: 4},

			{CollectionId: 2, PartitionId: 1, SegmentId: 5},
			{CollectionId: 2, PartitionId: 1, SegmentId: 6},
			{CollectionId: 2, PartitionId: 2, SegmentId: 7},
			{CollectionId: 2, PartitionId: 2, SegmentId: 8},
		}},
	}
}

func TestReadFromFull(t *testing.T) {
	cli := storage.NewMockClient(t)

	result := &backuppb.BackupInfo{Name: "backup1"}
	byts, err := json.Marshal(result)
	assert.NoError(t, err)

	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/full_meta.json").
		Return(&storage.Object{Length: int64(len(byts)), Body: io.NopCloser(bytes.NewReader(byts))}, nil)

	backupInfo, err := readFromFull(context.Background(), "backup/backup1", cli)
	assert.NoError(t, err)
	assert.Equal(t, "backup1", backupInfo.Name)
}

func TestLevelToTree(t *testing.T) {
	level := newTestLevelBackupInfo()

	backupInfo := levelToTree(level)
	assert.Equal(t, "backup1", backupInfo.Id)
	assert.Len(t, backupInfo.CollectionBackups, 2)
	for _, collection := range backupInfo.CollectionBackups {
		assert.Len(t, collection.PartitionBackups, 2)
		for _, partition := range collection.PartitionBackups {
			assert.Len(t, partition.SegmentBackups, 2)
		}
	}
}

func TestReadFromLevel(t *testing.T) {
	cli := storage.NewMockClient(t)
	level := newTestLevelBackupInfo()

	backupMetaBytes, err := json.Marshal(level.backupInfo)
	assert.NoError(t, err)
	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/backup_meta.json").
		Return(&storage.Object{Length: int64(len(backupMetaBytes)), Body: io.NopCloser(bytes.NewReader(backupMetaBytes))}, nil)
	collectionMetaBytes, err := json.Marshal(level.collectionInfo)
	assert.NoError(t, err)
	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/collection_meta.json").
		Return(&storage.Object{Length: int64(len(collectionMetaBytes)), Body: io.NopCloser(bytes.NewReader(collectionMetaBytes))}, nil)
	partitionMetaBytes, err := json.Marshal(level.partitionInfo)
	assert.NoError(t, err)
	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/partition_meta.json").
		Return(&storage.Object{Length: int64(len(partitionMetaBytes)), Body: io.NopCloser(bytes.NewReader(partitionMetaBytes))}, nil)
	segmentMetaBytes, err := json.Marshal(level.segmentInfo)
	assert.NoError(t, err)
	cli.EXPECT().
		GetObject(mock.Anything, "backup/backup1/meta/segment_meta.json").
		Return(&storage.Object{Length: int64(len(segmentMetaBytes)), Body: io.NopCloser(bytes.NewReader(segmentMetaBytes))}, nil)

	backupInfo, err := readFromLevel(context.Background(), "backup/backup1", cli)
	assert.NoError(t, err)
	assert.Equal(t, "backup1", backupInfo.Id)
	assert.Len(t, backupInfo.CollectionBackups, 2)
	for _, collection := range backupInfo.CollectionBackups {
		assert.Len(t, collection.PartitionBackups, 2)
		for _, partition := range collection.PartitionBackups {
			assert.Len(t, partition.SegmentBackups, 2)
		}
	}
}

func TestExist(t *testing.T) {
	t.Run("Exist", func(t *testing.T) {
		objs := []storage.ObjectAttr{
			{Key: "backup/backup1/meta/backup_meta.json", Length: 1},
		}
		iter := storage.NewMockObjectIterator(objs)
		cli := storage.NewMockClient(t)
		cli.EXPECT().
			ListPrefix(mock.Anything, "backup/backup1/meta/backup_meta.json", false).
			Return(iter, nil)

		exist, err := Exist(context.Background(), cli, "backup/backup1")
		assert.NoError(t, err)
		assert.True(t, exist)
	})

	t.Run("NotExist", func(t *testing.T) {
		iter := storage.NewMockObjectIterator([]storage.ObjectAttr{})
		cli := storage.NewMockClient(t)
		cli.EXPECT().
			ListPrefix(mock.Anything, "backup/backup1/meta/backup_meta.json", false).
			Return(iter, nil)

		exist, err := Exist(context.Background(), cli, "backup/backup1")
		assert.NoError(t, err)
		assert.False(t, exist)
	})
}

func TestList(t *testing.T) {
	cli := storage.NewMockClient(t)

	keys := []storage.ObjectAttr{{Key: "root/backup1"}, {Key: "root/backup2"}}
	backupRootIter := storage.NewMockObjectIterator(keys)
	cli.EXPECT().
		ListPrefix(mock.Anything, "root/", false).
		Return(backupRootIter, nil)

	backup1 := &backuppb.BackupInfo{Id: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"}
	backup1Bytes, err := json.Marshal(backup1)
	assert.NoError(t, err)

	backup1MetaIter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: "root/backup1/meta/full_meta.json", Length: int64(len(backup1Bytes))},
	})
	cli.EXPECT().
		ListPrefix(mock.Anything, "root/backup1/meta/full_meta.json", false).
		Return(backup1MetaIter, nil)
	cli.EXPECT().
		GetObject(mock.Anything, "root/backup1/meta/full_meta.json").
		Return(&storage.Object{Length: int64(len(backup1Bytes)), Body: io.NopCloser(bytes.NewReader(backup1Bytes))}, nil)

	backup2 := &backuppb.BackupInfo{Id: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"}
	backup2Bytes, err := json.Marshal(backup2)
	assert.NoError(t, err)

	backup2MetaIter := storage.NewMockObjectIterator([]storage.ObjectAttr{
		{Key: "root/backup2/meta/full_meta.json", Length: int64(len(backup2Bytes))},
	})
	cli.EXPECT().
		ListPrefix(mock.Anything, "root/backup2/meta/full_meta.json", false).
		Return(backup2MetaIter, nil)
	cli.EXPECT().
		GetObject(mock.Anything, "root/backup2/meta/full_meta.json").
		Return(&storage.Object{Length: int64(len(backup2Bytes)), Body: io.NopCloser(bytes.NewReader(backup2Bytes))}, nil)

	summaries, err := List(context.Background(), cli, "root")
	assert.NoError(t, err)
	expected := []*backuppb.BackupSummary{
		{Id: "a", Name: "backup1", Size: 100, MilvusVersion: "1.0.0"},
		{Id: "b", Name: "backup2", Size: 200, MilvusVersion: "2.0.0"},
	}
	assert.ElementsMatch(t, expected, summaries)
}
