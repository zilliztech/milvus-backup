package core

import (
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/proto/commonpb"
	"github.com/zilliztech/milvus-backup/internal/proto/datapb"
	"github.com/zilliztech/milvus-backup/internal/proto/schemapb"
	"github.com/zilliztech/milvus-backup/internal/util/funcutil"
	"strconv"
	"testing"
)

func TestBackupSerialize(t *testing.T) {

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		int64Field := "int64"
		floatVecField := "fVec"
		dim := 128
		prefix := "test_backup_"
		collectionName := prefix + funcutil.GenRandomStr()
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
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
		Binlogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement2 := &backuppb.SegmentBackupInfo{
		SegmentId:    1002,
		CollectionId: collection_id,
		PartitionId:  101,
		NumOfRows:    3000,
		Binlogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement3 := &backuppb.SegmentBackupInfo{
		SegmentId:    1003,
		CollectionId: collection_id,
		PartitionId:  102,
		NumOfRows:    3000,
		Binlogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
	}

	segement4 := &backuppb.SegmentBackupInfo{
		SegmentId:    1004,
		CollectionId: collection_id,
		PartitionId:  102,
		NumOfRows:    3000,
		Binlogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 3000}}}},
		Statslogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 6000}}}},
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 3000, TimestampFrom: 100, TimestampTo: 200, LogSize: 9000}}}},
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
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		BackupTimestamp:  0,
		BackupStatus:     backuppb.StatusCode_Success,
		BackupError:      "",
		Health:           "",
		PartitionBackups: []*backuppb.PartitionBackupInfo{partition1, partition2},
	}

	backup := &backuppb.BackupInfo{
		Id:                1,
		Name:              "backup",
		BackupTimestamp:   0,
		CollectionBackups: []*backuppb.CollectionBackupInfo{collection},
		BackupStatus:      backuppb.StatusCode_Success,
		BackupError:       "",
		Health:            "",
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
