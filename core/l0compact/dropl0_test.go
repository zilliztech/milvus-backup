package l0compact

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestDropL0(t *testing.T) {
	info := &backuppb.BackupInfo{CollectionBackups: []*backuppb.CollectionBackupInfo{{
		CollectionId: 1,
		L0Segments:   []*backuppb.SegmentBackupInfo{{SegmentId: 100, IsL0: true, PartitionId: allPartitionID}},
		PartitionBackups: []*backuppb.PartitionBackupInfo{{
			PartitionId: 10,
			SegmentBackups: []*backuppb.SegmentBackupInfo{
				{SegmentId: 200, IsL0: false},
				{SegmentId: 201, IsL0: true},
			},
		}},
	}}}
	dropL0(info)
	coll := info.GetCollectionBackups()[0]
	assert.Nil(t, coll.GetL0Segments())
	segs := coll.GetPartitionBackups()[0].GetSegmentBackups()
	assert.Len(t, segs, 1)
	assert.Equal(t, int64(200), segs[0].GetSegmentId())
}
