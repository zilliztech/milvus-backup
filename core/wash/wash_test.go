package wash

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestPkTypeName(t *testing.T) {
	int64Coll := &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
		{Name: "vec", DataType: backuppb.DataType_FloatVector},
		{Name: "id", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
	}}}
	pt, err := pkTypeName(int64Coll)
	assert.NoError(t, err)
	assert.Equal(t, "Int64", pt)

	varcharColl := &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
		{Name: "id", IsPrimaryKey: true, DataType: backuppb.DataType_VarChar},
	}}}
	pt, err = pkTypeName(varcharColl)
	assert.NoError(t, err)
	assert.Equal(t, "VarChar", pt)

	noPk := &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
		{Name: "vec", DataType: backuppb.DataType_FloatVector},
	}}}
	_, err = pkTypeName(noPk)
	assert.Error(t, err)
}

func TestDropL0(t *testing.T) {
	info := &backuppb.BackupInfo{CollectionBackups: []*backuppb.CollectionBackupInfo{{
		CollectionId: 1,
		// collection-level (all-partitions) L0
		L0Segments: []*backuppb.SegmentBackupInfo{{SegmentId: 100, IsL0: true, PartitionId: allPartitionID}},
		PartitionBackups: []*backuppb.PartitionBackupInfo{{
			PartitionId: 10,
			SegmentBackups: []*backuppb.SegmentBackupInfo{
				{SegmentId: 200, IsL0: false},
				{SegmentId: 201, IsL0: true}, // partition-level L0
				{SegmentId: 202, IsL0: false},
			},
		}},
	}}}

	dropL0(info)

	coll := info.GetCollectionBackups()[0]
	assert.Nil(t, coll.GetL0Segments(), "collection-level L0 must be dropped")

	segs := coll.GetPartitionBackups()[0].GetSegmentBackups()
	assert.Len(t, segs, 2, "partition-level L0 must be dropped, data segments kept")
	for _, seg := range segs {
		assert.False(t, seg.GetIsL0())
	}
	assert.Equal(t, int64(200), segs[0].GetSegmentId())
	assert.Equal(t, int64(202), segs[1].GetSegmentId())
}
