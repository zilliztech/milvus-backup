package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
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

func TestToPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	ct := &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, ct.toPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert"}, ct.toPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, ct.toPaths(dir))

	// empty
	dir = partitionDir{}
	assert.Empty(t, ct.toPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	ct = &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderLocal), milvusLocalPath: "/var/lib/milvus/data"}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, ct.toPaths(dir))

	// paths already absolute are left alone (a same-directory local backup)
	dir = partitionDir{insertLogDir: "/data/insert"}
	assert.Equal(t, []string{"/data/insert"}, ct.toPaths(dir))

	// a trailing slash is kept: the LocalChunkManager globs the prefix
	dir = partitionDir{insertLogDir: "insert/"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert/"}, ct.toPaths(dir))
}

func TestToGrpcPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	ct := &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, ct.toGrpcPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert", ""}, ct.toGrpcPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, ct.toGrpcPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	ct = &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderLocal), milvusLocalPath: "/var/lib/milvus/data"}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, ct.toGrpcPaths(dir))
}

func TestCollTask_destKey(t *testing.T) {
	// a non-local target keeps the key as-is
	ct := &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderMinio)}
	assert.Equal(t, "restore-temp-1/", ct.destKey("restore-temp-1/"))

	// a local target resolves against the directory milvus-backup writes to,
	// keeping the key's trailing slash for the prefix replacement on copy
	ct = &collTask{milvusStorage: newTestStorageClient(t, v2.ProviderLocal), milvusRootPath: "/data"}
	assert.Equal(t, "/data/restore-temp-1/", ct.destKey("restore-temp-1/"))
	assert.Equal(t, "/data/insert", ct.destKey("insert"))

	assert.Equal(t, "", ct.destKey(""))
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
