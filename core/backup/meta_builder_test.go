package backup

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/meta"
)

func newTestMetaBuilder(t *testing.T) *metaBuilder {
	builder := newMetaBuilder("task1", "backup1")

	collRef := collref.New("db1", "coll1")
	coll := &backuppb.CollectionBackupInfo{
		CollectionId:   1,
		DbName:         "db1",
		CollectionName: "coll1",
		PartitionBackups: []*backuppb.PartitionBackupInfo{
			{PartitionId: 10, PartitionName: "part1", CollectionId: 1},
			{PartitionId: 11, PartitionName: "part2", CollectionId: 1},
		},
	}
	builder.addCollection(collRef, coll)

	segments := []*backuppb.SegmentBackupInfo{
		{
			SegmentId:    100,
			CollectionId: 1,
			PartitionId:  10,
			NumOfRows:    1000,
			Size:         4096,
			Binlogs: []*backuppb.FieldBinlog{
				{FieldID: 1, Binlogs: []*backuppb.Binlog{{LogId: 1001, LogPath: "data/1/10/100/1/1001"}}},
			},
		},
		{
			SegmentId:    101,
			CollectionId: 1,
			PartitionId:  11,
			NumOfRows:    2000,
			Size:         8192,
			Binlogs: []*backuppb.FieldBinlog{
				{FieldID: 1, Binlogs: []*backuppb.Binlog{{LogId: 1002, LogPath: "data/1/11/101/1/1002"}}},
			},
		},
	}
	l0Segments := []*backuppb.SegmentBackupInfo{
		{
			SegmentId:    200,
			CollectionId: 1,
			PartitionId:  _allPartitionID,
			IsL0:         true,
			NumOfRows:    500,
			Size:         2048,
			Deltalogs: []*backuppb.FieldBinlog{
				{FieldID: 0, Binlogs: []*backuppb.Binlog{{LogId: 2001, LogPath: "delta/1/200/2001"}}},
			},
		},
	}
	builder.addSegments(append(segments, l0Segments...))

	return builder
}

func TestAddSegments(t *testing.T) {
	t.Run("BeforeCollectionAreDrained", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		// The data side of a collection can land before its schema side:
		// the segments wait in the pendings, and addCollection claims them
		// as it registers the collection.
		builder.addSegments([]*backuppb.SegmentBackupInfo{
			{SegmentId: 100, CollectionId: 1, PartitionId: 10, Size: 4096},
		})

		collRef := collref.New("db1", "coll1")
		builder.addCollection(collRef, &backuppb.CollectionBackupInfo{
			CollectionId: 1, DbName: "db1", CollectionName: "coll1",
			PartitionBackups: []*backuppb.PartitionBackupInfo{{PartitionId: 10, CollectionId: 1}},
		})

		coll := builder.data.GetCollectionBackups()[0]
		assert.EqualValues(t, 4096, coll.GetSize())
		assert.EqualValues(t, 4096, builder.data.GetSize())
		require.Len(t, coll.GetPartitionBackups(), 1)
		require.Len(t, coll.GetPartitionBackups()[0].GetSegmentBackups(), 1)
		assert.Equal(t, int64(100), coll.GetPartitionBackups()[0].GetSegmentBackups()[0].GetSegmentId())

		_, err := builder.buildSegmentMeta()
		assert.NoError(t, err)
	})

	t.Run("UnclaimedFailsTheBuild", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		builder.addSegments([]*backuppb.SegmentBackupInfo{
			{SegmentId: 100, CollectionId: 999, PartitionId: 10, Size: 4096},
		})

		// Segments no collection ever claims mean a DDL failed while the job
		// went on; the build refuses instead of silently dropping them.
		_, err := builder.buildSegmentMeta()
		assert.ErrorContains(t, err, "never claimed")
	})
}

func TestAddPOS(t *testing.T) {
	t.Run("BeforeCollectionIsDrained", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		// The serial plan's flush can record the POS before the collection's
		// DDL lands; addCollection applies it on arrival.
		collRef := collref.New("db1", "coll1")
		builder.addPOS(collRef, map[string]string{"ch1": "cp1"}, 100, 200)

		builder.addCollection(collRef, &backuppb.CollectionBackupInfo{
			CollectionId: 1, DbName: "db1", CollectionName: "coll1",
		})

		coll := builder.data.GetCollectionBackups()[0]
		assert.Equal(t, map[string]string{"ch1": "cp1"}, coll.GetChannelCheckpoints())
		assert.EqualValues(t, 100, coll.GetBackupTimestamp())
		assert.EqualValues(t, 200, coll.GetBackupPhysicalTimestamp())
	})
}

func TestBuildCollectionMetaStripsSegments(t *testing.T) {
	builder := newTestMetaBuilder(t)

	data, err := builder.buildCollectionMeta()
	assert.NoError(t, err)

	var result backuppb.CollectionLevelBackupInfo
	assert.NoError(t, json.Unmarshal(data, &result))

	assert.Len(t, result.Infos, 1)
	coll := result.Infos[0]
	assert.Equal(t, int64(1), coll.GetCollectionId())
	assert.Equal(t, "coll1", coll.GetCollectionName())
	// Partitions are kept but without nested segments
	assert.Len(t, coll.GetPartitionBackups(), 2)
	for _, part := range coll.GetPartitionBackups() {
		assert.Empty(t, part.GetSegmentBackups())
	}
	// L0 segments must be stripped
	assert.Empty(t, coll.GetL0Segments())
}

func TestBuildPartitionMetaStripsSegments(t *testing.T) {
	builder := newTestMetaBuilder(t)

	data, err := builder.buildPartitionMeta()
	assert.NoError(t, err)

	var result backuppb.PartitionLevelBackupInfo
	assert.NoError(t, json.Unmarshal(data, &result))

	assert.Len(t, result.Infos, 2)
	for _, part := range result.Infos {
		assert.Equal(t, int64(1), part.GetCollectionId())
		// Must not contain nested segments
		assert.Empty(t, part.GetSegmentBackups())
	}
}

func TestBuildSegmentMetaContainsAllSegments(t *testing.T) {
	builder := newTestMetaBuilder(t)

	data, err := builder.buildSegmentMeta()
	assert.NoError(t, err)

	var result backuppb.SegmentLevelBackupInfo
	assert.NoError(t, json.Unmarshal(data, &result))

	// 2 regular segments (L0 segments are stored at collection level, not partition level)
	assert.Len(t, result.Infos, 2)
	ids := make([]int64, 0, len(result.Infos))
	for _, seg := range result.Infos {
		ids = append(ids, seg.GetSegmentId())
		assert.NotEmpty(t, seg.GetBinlogs())
	}
	assert.ElementsMatch(t, []int64{100, 101}, ids)
}

func TestBuildFullMetaContainsEverything(t *testing.T) {
	builder := newTestMetaBuilder(t)

	data, err := builder.buildFullMeta()
	assert.NoError(t, err)

	var result backuppb.BackupInfo
	assert.NoError(t, json.Unmarshal(data, &result))

	assert.Len(t, result.GetCollectionBackups(), 1)
	coll := result.GetCollectionBackups()[0]
	assert.Len(t, coll.GetPartitionBackups(), 2)
	assert.Len(t, coll.GetL0Segments(), 1)
	assert.Equal(t, int64(200), coll.GetL0Segments()[0].GetSegmentId())

	var totalSegs int
	for _, part := range coll.GetPartitionBackups() {
		totalSegs += len(part.GetSegmentBackups())
	}
	assert.Equal(t, 2, totalSegs)
}

func TestAddDynamicFields(t *testing.T) {
	t.Run("InjectsDynamicFieldFromEtcd", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		collRef := collref.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			DbName:         "db1",
			CollectionName: "coll1",
			Schema: &backuppb.CollectionSchema{
				Name:               "coll1",
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
					{FieldID: 101, Name: "vec", DataType: backuppb.DataType_FloatVector},
				},
			},
		}
		builder.addCollection(collRef, coll)

		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BytesData{BytesData: []byte("{}")}}
		dynField := &schemapb.FieldSchema{
			FieldID:      102,
			Name:         "$meta",
			DataType:     schemapb.DataType_JSON,
			IsDynamic:    true,
			Nullable:     true,
			DefaultValue: defaultValue,
			Description:  "dynamic schema",
		}
		assert.NoError(t, builder.addDynamicFields(map[int64]*schemapb.FieldSchema{1: dynField}))

		fields := builder.collectionBackups[1].GetSchema().GetFields()
		assert.Len(t, fields, 3)
		dyn := fields[2]
		assert.Equal(t, int64(102), dyn.GetFieldID())
		assert.Equal(t, "$meta", dyn.GetName())
		assert.True(t, dyn.GetIsDynamic())
		assert.True(t, dyn.GetNullable())
		assert.Equal(t, backuppb.DataType_JSON, dyn.GetDataType())

		// Default value round-trips through base64+proto.
		bytes, err := base64.StdEncoding.DecodeString(dyn.GetDefaultValueBase64())
		assert.NoError(t, err)
		var got schemapb.ValueField
		assert.NoError(t, proto.Unmarshal(bytes, &got))
		assert.Equal(t, []byte("{}"), got.GetBytesData())
	})

	t.Run("SkipsCollectionWithoutDynamicField", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		collRef := collref.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId: 1,
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: false,
				Fields:             []*backuppb.FieldSchema{{FieldID: 100, Name: "pk"}},
			},
		}
		builder.addCollection(collRef, coll)

		dynField := &schemapb.FieldSchema{FieldID: 101, Name: "$meta", IsDynamic: true}
		assert.NoError(t, builder.addDynamicFields(map[int64]*schemapb.FieldSchema{1: dynField}))

		// Schema must be untouched when EnableDynamicField is false.
		assert.Len(t, builder.collectionBackups[1].GetSchema().GetFields(), 1)
	})

	// The snapshot task records $meta from the bundle before the etcd scan runs; a
	// second copy from etcd would corrupt the schema.
	t.Run("SkipsCollectionThatAlreadyCarriesTheField", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		collRef := collref.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId: 1,
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{FieldID: 100, Name: "pk"},
					{FieldID: 101, Name: "$meta", IsDynamic: true, Nullable: true},
				},
			},
		}
		builder.addCollection(collRef, coll)

		dynField := &schemapb.FieldSchema{FieldID: 101, Name: "$meta", IsDynamic: true}
		assert.NoError(t, builder.addDynamicFields(map[int64]*schemapb.FieldSchema{1: dynField}))

		fields := builder.collectionBackups[1].GetSchema().GetFields()
		require.Len(t, fields, 2)
		assert.True(t, fields[1].GetNullable())
	})

	t.Run("ErrorsOnMissingEntryForCollection", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		collRef := collref.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			CollectionName: "coll1",
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields:             []*backuppb.FieldSchema{{FieldID: 100, Name: "pk"}},
			},
		}
		builder.addCollection(collRef, coll)

		// etcd has no record for collection 1.
		err := builder.addDynamicFields(map[int64]*schemapb.FieldSchema{2: {FieldID: 101, IsDynamic: true}})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"coll1"`)
		// Schema must remain untouched on error.
		assert.Len(t, builder.collectionBackups[1].GetSchema().GetFields(), 1)
	})
}

func TestSequentialBuildDoesNotCorruptData(t *testing.T) {
	builder := newTestMetaBuilder(t)

	// Simulate production call order: collection → partition → segment → full
	_, err := builder.buildCollectionMeta()
	assert.NoError(t, err)
	_, err = builder.buildPartitionMeta()
	assert.NoError(t, err)
	_, err = builder.buildSegmentMeta()
	assert.NoError(t, err)

	data, err := builder.buildFullMeta()
	assert.NoError(t, err)

	var result backuppb.BackupInfo
	assert.NoError(t, json.Unmarshal(data, &result))

	coll := result.GetCollectionBackups()[0]
	assert.Len(t, coll.GetPartitionBackups(), 2)
	assert.Len(t, coll.GetL0Segments(), 1)

	var totalSegs int
	for _, part := range coll.GetPartitionBackups() {
		totalSegs += len(part.GetSegmentBackups())
	}
	assert.Equal(t, 2, totalSegs)
}

func newIndexExtraTestBuilder() *metaBuilder {
	builder := newMetaBuilder("task1", "backup1")
	collRef := collref.New("db1", "coll1")
	builder.addCollection(collRef, &backuppb.CollectionBackupInfo{
		CollectionId:   1,
		DbName:         "db1",
		CollectionName: "coll1",
		IndexInfos:     []*backuppb.IndexInfo{{IndexId: 100, FieldName: "vec", IndexName: "vec_idx"}},
	})

	return builder
}

func TestAddIndexExtraInfo(t *testing.T) {
	t.Run("MergesExtraAttributes", func(t *testing.T) {
		builder := newIndexExtraTestBuilder()

		indexes := []*indexpb.FieldIndex{
			{
				IndexInfo:  &indexpb.IndexInfo{CollectionID: 1, IndexID: 100, FieldID: 7, IsAutoIndex: true, MinIndexVersion: 1, MaxIndexVersion: 2},
				CreateTime: 42,
			},
		}
		assert.NoError(t, builder.addIndexExtraInfo(indexes))

		got := builder.collectionBackups[1].GetIndexInfos()[0]
		assert.Equal(t, int64(7), got.GetFieldId())
		assert.Equal(t, uint64(42), got.GetCreateTime())
		assert.True(t, got.GetIsAutoIndex())
		assert.Equal(t, int32(1), got.GetMinIndexVersion())
		assert.Equal(t, int32(2), got.GetMaxIndexVersion())
	})

	t.Run("UnknownCollectionID", func(t *testing.T) {
		builder := newIndexExtraTestBuilder()

		indexes := []*indexpb.FieldIndex{
			{IndexInfo: &indexpb.IndexInfo{CollectionID: 999, IndexID: 100}},
		}
		err := builder.addIndexExtraInfo(indexes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "collection 999 not in backup")
	})

	t.Run("IndexNotInBackupInfos", func(t *testing.T) {
		builder := newIndexExtraTestBuilder()

		indexes := []*indexpb.FieldIndex{
			{IndexInfo: &indexpb.IndexInfo{CollectionID: 1, IndexID: 300}},
		}
		err := builder.addIndexExtraInfo(indexes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not in backup index infos")
	})
}

func TestBackupCollectionIDs(t *testing.T) {
	builder := newIndexExtraTestBuilder()
	builder.addCollection(collref.New("db1", "coll2"), &backuppb.CollectionBackupInfo{CollectionId: 2})

	assert.ElementsMatch(t, []int64{1, 2}, builder.backupCollectionIDs())
}

func TestAddSnapshot(t *testing.T) {
	builder := newTestMetaBuilder(t)
	collRef := collref.New("db1", "coll1")

	snapshot := &backuppb.SnapshotBackupInfo{
		MetadataPath: "snapshots/1/metadata/2.json",
		TotalFiles:   3,
		TotalBytes:   4096,
	}
	require.NoError(t, builder.addSnapshot(collRef, snapshot, 456, nil))

	coll := builder.data.GetCollectionBackups()[0]
	assert.Equal(t, "snapshots/1/metadata/2.json", coll.GetSnapshotBackup().GetMetadataPath())
	// In this format the size is what the export job reported, and backup_timestamp is
	// the snapshot's own boundary.
	assert.EqualValues(t, 4096, coll.GetSize())
	assert.EqualValues(t, 456, coll.GetBackupTimestamp())

	t.Run("BeforeCollectionIsDrained", func(t *testing.T) {
		other := newMetaBuilder("task1", "backup1")
		otherRef := collref.New("db1", "late")

		// The export can finish before the collection's DDL lands; the bundle
		// waits in the pendings and addCollection applies it.
		require.NoError(t, other.addSnapshot(otherRef, snapshot, 789, nil))
		other.addCollection(otherRef, &backuppb.CollectionBackupInfo{CollectionId: 2, CollectionName: "late"})

		coll := other.data.GetCollectionBackups()[0]
		assert.Equal(t, "snapshots/1/metadata/2.json", coll.GetSnapshotBackup().GetMetadataPath())
		assert.EqualValues(t, 4096, coll.GetSize())
		assert.EqualValues(t, 789, coll.GetBackupTimestamp())
	})

	// The DDL half reads the schema through DescribeCollection, which hides the
	// dynamic field; the bundle's schema is the internal one and carries it.
	bundleSchema := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true, Nullable: true},
		},
	}
	newDynColl := func() *backuppb.CollectionBackupInfo {
		return &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			DbName:         "db1",
			CollectionName: "coll1",
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
					{FieldID: 101, Name: "vec", DataType: backuppb.DataType_FloatVector},
				},
			},
		}
	}

	t.Run("RecordsTheBundlesDynamicField", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		builder.addCollection(collRef, newDynColl())

		require.NoError(t, builder.addSnapshot(collRef, snapshot, 456, bundleSchema))

		// Only the dynamic field is taken: the system fields are Milvus's own and
		// the user fields are already there from DescribeCollection.
		fields := builder.data.GetCollectionBackups()[0].GetSchema().GetFields()
		require.Len(t, fields, 3)
		dyn := fields[2]
		assert.Equal(t, int64(102), dyn.GetFieldID())
		assert.Equal(t, "$meta", dyn.GetName())
		assert.Equal(t, backuppb.DataType_JSON, dyn.GetDataType())
		assert.True(t, dyn.GetIsDynamic())
		assert.True(t, dyn.GetNullable())
	})

	t.Run("DynamicFieldSurvivesThePendingOrder", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		require.NoError(t, builder.addSnapshot(collRef, snapshot, 456, bundleSchema))
		builder.addCollection(collRef, newDynColl())

		fields := builder.data.GetCollectionBackups()[0].GetSchema().GetFields()
		require.Len(t, fields, 3)
		assert.True(t, fields[2].GetIsDynamic())
	})

	t.Run("LeavesARecordedDynamicFieldAlone", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		coll := newDynColl()
		coll.Schema.Fields = append(coll.Schema.Fields, &backuppb.FieldSchema{FieldID: 102, Name: "$meta", IsDynamic: true})
		builder.addCollection(collRef, coll)

		require.NoError(t, builder.addSnapshot(collRef, snapshot, 456, bundleSchema))

		assert.Len(t, builder.data.GetCollectionBackups()[0].GetSchema().GetFields(), 3)
	})

	t.Run("BundleWithoutADynamicFieldChangesNothing", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		builder.addCollection(collRef, newDynColl())

		plain := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk"}}}
		require.NoError(t, builder.addSnapshot(collRef, snapshot, 456, plain))

		assert.Len(t, builder.data.GetCollectionBackups()[0].GetSchema().GetFields(), 2)
	})
}

// backup_meta.json is the top-level view, and a reader that fetches only it still has
// to be able to tell which format the backup is in.
func TestBuildBackupMetaCarriesFormat(t *testing.T) {
	builder := newTestMetaBuilder(t)
	builder.setFormat(meta.FormatSnapshot)

	byts, err := builder.buildBackupMeta()
	require.NoError(t, err)

	var got backuppb.BackupInfo
	require.NoError(t, json.Unmarshal(byts, &got))
	assert.Equal(t, meta.FormatSnapshot, got.GetFormat())
}

func TestMetaBuilder_CollectionsMissingDynField(t *testing.T) {
	t.Run("NamesOnlyTheDynamicOnesStillLackingTheField", func(t *testing.T) {
		b := newMetaBuilder("task", "backup")
		b.data.CollectionBackups = []*backuppb.CollectionBackupInfo{
			{DbName: "db2", CollectionName: "z", Schema: &backuppb.CollectionSchema{EnableDynamicField: true}},
			{DbName: "db1", CollectionName: "plain", Schema: &backuppb.CollectionSchema{}},
			{DbName: "db1", CollectionName: "a", Schema: &backuppb.CollectionSchema{EnableDynamicField: true}},
			// Recorded from its snapshot bundle: complete without etcd.
			{DbName: "db1", CollectionName: "snap", Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields:             []*backuppb.FieldSchema{{FieldID: 101, Name: "$meta", IsDynamic: true}},
			}},
		}

		assert.Equal(t, []string{"db1.a", "db2.z"}, b.collectionsMissingDynField())
	})

	t.Run("EmptyWhenNoneHasDynamicSchema", func(t *testing.T) {
		b := newMetaBuilder("task", "backup")
		b.data.CollectionBackups = []*backuppb.CollectionBackupInfo{
			{DbName: "db1", CollectionName: "plain", Schema: &backuppb.CollectionSchema{}},
		}

		assert.Empty(t, b.collectionsMissingDynField())
	})
}
