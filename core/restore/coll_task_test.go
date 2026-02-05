package restore

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

func newTestCollTask() *collTask {
	return &collTask{logger: zap.NewNop(), option: &Option{}}
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

func TestCollectionTask_ezk(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "hello"}
		assert.Equal(t, "hello", ct.ezk())
	})

	t.Run("WithoutEZK", func(t *testing.T) {
		ct := newTestCollTask()
		assert.Equal(t, "", ct.ezk())
	})
}

func TestToPaths(t *testing.T) {
	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	paths := toPaths(dir)
	assert.Equal(t, []string{"insert", "delta"}, paths)

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	paths = toPaths(dir)
	assert.Equal(t, []string{"insert", ""}, paths)

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	paths = toPaths(dir)
	assert.Equal(t, []string{"delta"}, paths)
}

func TestL0SegmentBatches(t *testing.T) {
	segs := make([]*backuppb.SegmentBackupInfo, 0, 10)
	for i := 0; i < 10; i++ {
		vch := fmt.Sprintf("vch%d", i%2)
		sv := int64(i % 2)
		seg := &backuppb.SegmentBackupInfo{
			SegmentId:      int64(i),
			PartitionId:    1,
			VChannel:       vch,
			Size:           1,
			StorageVersion: sv,
		}
		segs = append(segs, seg)
	}

	t.Run("SingleL0InOneJob", func(t *testing.T) {
		ct := newTestCollTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(false).Once()
		ct.grpcCli = grpcCli

		batches, err := ct.l0SegmentBatches(segs)
		assert.NoError(t, err)
		assert.Len(t, batches, 10)

		for _, b := range batches {
			require.Len(t, b.partitionDirs, 1)
			for _, dir := range b.partitionDirs {
				require.Empty(t, dir.insertLogDir)
				require.NotEmpty(t, dir.deltaLogDir)
			}
		}
	})

	t.Run("MultiL0InOneJob", func(t *testing.T) {
		ct := newTestCollTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(true).Once()
		ct.grpcCli = grpcCli

		batches, err := ct.l0SegmentBatches(segs)
		assert.NoError(t, err)
		assert.Len(t, batches, 2)

		for _, b := range batches {
			require.Len(t, b.partitionDirs, 5)
			for _, dir := range b.partitionDirs {
				require.Empty(t, dir.insertLogDir)
				require.NotEmpty(t, dir.deltaLogDir)
			}
		}
	})
}
