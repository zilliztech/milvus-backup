package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func newTestCollTask() *collTask {
	return &collTask{logger: zap.NewNop(), option: &Option{}, maxSegsPerImportJob: 256}
}

// newTestStorageClient builds a storage client of the given provider without
// touching any backend: constructing a client never connects.
func newTestStorageClient(t *testing.T, provider string) storage.Client {
	cli, err := storage.NewClient(context.Background(), storage.Config{
		Provider:   provider,
		Endpoint:   "localhost:9000",
		Credential: storage.Credential{Type: storage.Static, AK: "a", SK: "b"},
	})
	require.NoError(t, err)
	return cli
}

func TestCollTask_ezk(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		ct := newTestCollTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "hello"}
		assert.Equal(t, "hello", ct.ezk())
	})

	t.Run("WithoutEZK", func(t *testing.T) {
		ct := newTestCollTask()
		assert.Equal(t, "", ct.ezk())
	})

	t.Run("WithMapping", func(t *testing.T) {
		ct := newTestCollTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "old_key"}
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "new_key", ct.ezk())
	})

	t.Run("WithMappingNoMatch", func(t *testing.T) {
		ct := newTestCollTask()
		ct.dbBackup = &backuppb.DatabaseBackupInfo{Ezk: "other_key"}
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "other_key", ct.ezk())
	})

	t.Run("WithMappingEmptyEZK", func(t *testing.T) {
		ct := newTestCollTask()
		ct.option.EZKMapping = map[string]string{"old_key": "new_key"}
		assert.Equal(t, "", ct.ezk())
	})
}

func TestL0SegmentBatches(t *testing.T) {
	segs := make([]*backuppb.SegmentBackupInfo, 0, 10)
	for i := range 10 {
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

	// Each vchannel holds 5 segments, so a limit of 2 splits it into 2+2+1.
	t.Run("MaxSegsPerImportJob", func(t *testing.T) {
		ct := newTestCollTask()
		ct.collBackup = &backuppb.CollectionBackupInfo{CollectionId: 1}
		ct.maxSegsPerImportJob = 2
		grpcCli := milvus.NewMockGrpc(t)
		grpcCli.EXPECT().HasFeature(milvus.MultiL0InOneJob).Return(true).Once()
		ct.grpcCli = grpcCli

		batches, err := ct.l0SegmentBatches(segs)
		assert.NoError(t, err)
		assert.Len(t, batches, 6)

		for _, b := range batches {
			require.LessOrEqual(t, len(b.partitionDirs), 2)
		}
	})
}
