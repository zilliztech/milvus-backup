package restoreplan

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// Secondary holds the inputs fixed before the first replay message is sent.
type Secondary struct {
	SourceWAL          message.WALName
	SourceID, TargetID string
	Backup             *backuppb.BackupInfo
	TargetChannels     map[string]string
	Layouts            map[int64]datapb.SnapshotLayout
	Collections        map[int64]*SecondaryCollection
}

type SecondaryCollection struct {
	StageBinlogs bool
	// Non-L0, partition L0, then collection-global L0.
	Phases [3][]SecondaryImport
}

type SecondaryImport struct {
	PartitionID int64
	Batch       BinlogImportBatch
}
