package core

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/funcutil"
)

func TestBackupSerialize(t *testing.T) {

	constructCollectionSchema := func() *backuppb.CollectionSchema {
		int64Field := "int64"
		floatVecField := "fVec"
		dim := 128
		prefix := "test_backup_"
		collectionName := prefix + funcutil.GenRandomStr()
		pk := &backuppb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     backuppb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &backuppb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     backuppb.DataType_FloatVector,
			TypeParams: []*backuppb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &backuppb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*backuppb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()

	var collection_id int64 = 1

	segement1 := &backuppb.SegmentBackupInfo{
		SegmentId:    1001,
		CollectionId: collection_id,
		PartitionId:  101,
		NumOfRows:    3000,
		Binlogs:      []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement2 := &backuppb.SegmentBackupInfo{
		SegmentId:    1002,
		CollectionId: collection_id,
		PartitionId:  101,
		NumOfRows:    3000,
		Binlogs:      []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement3 := &backuppb.SegmentBackupInfo{
		SegmentId:    1003,
		CollectionId: collection_id,
		PartitionId:  102,
		NumOfRows:    3000,
		Binlogs:      []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement4 := &backuppb.SegmentBackupInfo{
		SegmentId:    1004,
		CollectionId: collection_id,
		PartitionId:  102,
		NumOfRows:    3000,
		Binlogs:      []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*backuppb.FieldBinlog{{Binlogs: []*backuppb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	partition1 := &backuppb.PartitionBackupInfo{
		PartitionId:    101,
		PartitionName:  "20220101",
		CollectionId:   collection_id,
		SegmentBackups: []*backuppb.SegmentBackupInfo{segement1, segement2},
	}

	partition2 := &backuppb.PartitionBackupInfo{
		PartitionId:    102,
		PartitionName:  "20220102",
		CollectionId:   collection_id,
		SegmentBackups: []*backuppb.SegmentBackupInfo{segement3, segement4},
	}

	collection := &backuppb.CollectionBackupInfo{
		CollectionId:     collection_id,
		DbName:           "default",
		CollectionName:   "hello_milvus",
		Schema:           schema,
		ShardsNum:        2,
		ConsistencyLevel: backuppb.ConsistencyLevel_Strong,
		BackupTimestamp:  0,
		PartitionBackups: []*backuppb.PartitionBackupInfo{partition1, partition2},
	}

	backup := &backuppb.BackupInfo{
		Name:              "backup",
		BackupTimestamp:   0,
		CollectionBackups: []*backuppb.CollectionBackupInfo{collection},
	}

	serData, err := serialize(backup)
	assert.NoError(t, err)
	log.Info(string(serData.BackupMetaBytes))
	log.Info(string(serData.CollectionMetaBytes))
	log.Info(string(serData.PartitionMetaBytes))
	log.Info(string(serData.SegmentMetaBytes))

	deserBackup, err := deserialize(serData)
	log.Info(deserBackup.String())
}
