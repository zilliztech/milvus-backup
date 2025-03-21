package restore

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestCollectionTask_BackupTS(t *testing.T) {
	t.Run("Truncate", func(t *testing.T) {
		task := &backuppb.RestoreCollectionTask{
			TruncateBinlogByTs: true,
			CollBackup:         &backuppb.CollectionBackupInfo{BackupTimestamp: 100},
		}

		ct := &CollectionTask{task: task}
		assert.Equal(t, uint64(100), ct.backupTS())
	})

	t.Run("NotTruncate", func(t *testing.T) {
		task := &backuppb.RestoreCollectionTask{
			TruncateBinlogByTs: false,
			CollBackup:         &backuppb.CollectionBackupInfo{BackupTimestamp: 100},
		}

		ct := &CollectionTask{task: task}
		assert.Equal(t, uint64(0), ct.backupTS())
	})
}

func TestGetFailedReason(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "failed_reason", Value: "hello"}})
		assert.Equal(t, "hello", r)
	})

	t.Run("WithoutFailedReason", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, "", r)
	})
}

func TestGetProcess(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "progress_percent", Value: "100"}})
		assert.Equal(t, 100, r)
	})

	t.Run("WithoutProgress", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, 0, r)
	})
}
