package l0compact

import (
	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const allPartitionID int64 = -1

// dropL0 removes all L0 segments from the meta: collection-level L0Segments and
// any partition segment with IsL0=true.
func dropL0(info *backuppb.BackupInfo) {
	for _, coll := range info.GetCollectionBackups() {
		coll.L0Segments = nil
		for _, part := range coll.GetPartitionBackups() {
			part.SegmentBackups = lo.Filter(part.GetSegmentBackups(),
				func(seg *backuppb.SegmentBackupInfo, _ int) bool { return !seg.GetIsL0() })
		}
	}
}
