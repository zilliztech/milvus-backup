package l0compact

import (
	"github.com/samber/lo"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

const allPartitionID int64 = -1

// HasL0 checks the same collection- and partition-level segments removed by dropL0.
func HasL0(info *backuppb.BackupInfo) bool {
	for _, coll := range info.GetCollectionBackups() {
		if len(coll.GetL0Segments()) > 0 {
			return true
		}
		for _, part := range coll.GetPartitionBackups() {
			for _, seg := range part.GetSegmentBackups() {
				if seg.GetIsL0() {
					return true
				}
			}
		}
	}
	return false
}

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
