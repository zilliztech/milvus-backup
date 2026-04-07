package backup

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

func newTestMetaBuilder(t *testing.T) *metaBuilder {
	builder := newMetaBuilder("task1", "backup1")

	ns := namespace.New("db1", "coll1")
	coll := &backuppb.CollectionBackupInfo{
		CollectionId:   1,
		DbName:         "db1",
		CollectionName: "coll1",
		PartitionBackups: []*backuppb.PartitionBackupInfo{
			{PartitionId: 10, PartitionName: "part1", CollectionId: 1},
			{PartitionId: 11, PartitionName: "part2", CollectionId: 1},
		},
	}
	builder.addCollection(ns, coll)

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
	assert.NoError(t, builder.addSegments(append(segments, l0Segments...)))

	return builder
}

func TestAddSegments(t *testing.T) {
	t.Run("UnknownCollectionID", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		segments := []*backuppb.SegmentBackupInfo{
			{
				SegmentId:    100,
				CollectionId: 999,
				PartitionId:  10,
				Size:         4096,
			},
		}

		err := builder.addSegments(segments)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup: collection backup not found")
		assert.Contains(t, err.Error(), "999")
	})
}

func TestAddPOS(t *testing.T) {
	t.Run("UnknownNamespace", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")

		ns := namespace.New("db_unknown", "coll_unknown")
		err := builder.addPOS(ns, map[string]string{"ch1": "cp1"}, 100, 200)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup: collection backup not found")
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

		ns := namespace.New("db1", "coll1")
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
		builder.addCollection(ns, coll)

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
		ns := namespace.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId: 1,
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: false,
				Fields:             []*backuppb.FieldSchema{{FieldID: 100, Name: "pk"}},
			},
		}
		builder.addCollection(ns, coll)

		dynField := &schemapb.FieldSchema{FieldID: 101, Name: "$meta", IsDynamic: true}
		assert.NoError(t, builder.addDynamicFields(map[int64]*schemapb.FieldSchema{1: dynField}))

		// Schema must be untouched when EnableDynamicField is false.
		assert.Len(t, builder.collectionBackups[1].GetSchema().GetFields(), 1)
	})

	t.Run("ErrorsOnMissingEntryForCollection", func(t *testing.T) {
		builder := newMetaBuilder("task1", "backup1")
		ns := namespace.New("db1", "coll1")
		coll := &backuppb.CollectionBackupInfo{
			CollectionId:   1,
			CollectionName: "coll1",
			Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields:             []*backuppb.FieldSchema{{FieldID: 100, Name: "pk"}},
			},
		}
		builder.addCollection(ns, coll)

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
