package backup

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestCollectionTask_GroupID(t *testing.T) {
	t.Run("NormalSegment", func(t *testing.T) {
		task := &CollectionTask{}
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: 200})
		assert.Equal(t, int64(100), groupID)
	})

	t.Run("AllPartitionSegment", func(t *testing.T) {
		task := &CollectionTask{}
		groupID := task.groupID(&milvuspb.PersistentSegmentInfo{SegmentID: 100, PartitionID: -1})
		assert.Equal(t, int64(0), groupID)
	})
}
