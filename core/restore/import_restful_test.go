package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func TestRESTfulImportPlannerPlanTasks(t *testing.T) {
	// planning only assembles tasks, so no client is touched
	newPlanner := func(maxSegs int, multiL0 bool) *restfulImportPlanner {
		return &restfulImportPlanner{
			maxSegsPerImportJob: maxSegs,
			multiL0InOneJob:     multiL0,
			newStaging:          func() *copyTask { return nil },
			logger:              zap.NewNop(),
		}
	}

	dirs := func(n int) []partitionDir {
		ds := make([]partitionDir, 0, n)
		for range n {
			ds = append(ds, partitionDir{insertLogDir: "insert", size: 1})
		}
		return ds
	}

	// 10 dirs chunk into 5 tasks of 2 dirs each
	t.Run("ChunkByMaxSegsPerImportJob", func(t *testing.T) {
		tasks := newPlanner(2, true).planTasks("p1", []dirGroup{{dirs: dirs(10)}})
		require.Len(t, tasks, 5)
		for _, tk := range tasks {
			assert.Len(t, tk.(*importViaRESTFulTask).dirs, 2)
		}
	})

	// the group's request parameters ride along on every task
	t.Run("GroupParamsRideAlong", func(t *testing.T) {
		tasks := newPlanner(256, true).planTasks("p1",
			[]dirGroup{{timestamp: 7, storageVersion: 2, isL0: true, dirs: dirs(3)}})
		require.Len(t, tasks, 1)
		rt := tasks[0].(*importViaRESTFulTask)
		assert.Equal(t, "p1", rt.partitionName)
		assert.Equal(t, uint64(7), rt.timestamp)
		assert.Equal(t, int64(2), rt.storageVersion)
		assert.True(t, rt.isL0)
	})

	// a target below 2.6.5 takes one L0 segment per job, so each L0 dir
	// becomes a task of its own
	t.Run("L0SingleDirPerTask", func(t *testing.T) {
		tasks := newPlanner(2, false).planTasks("p1", []dirGroup{{isL0: true, dirs: dirs(10)}})
		require.Len(t, tasks, 10)
		for _, tk := range tasks {
			assert.Len(t, tk.(*importViaRESTFulTask).dirs, 1)
		}
	})
}

func TestToPaths(t *testing.T) {
	// a non-local target keeps the bucket-relative paths as-is
	task := &importViaRESTFulTask{milvusStorage: newTestStorageClient(t, cfg.ProviderMinio)}

	// normal
	dir := partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"insert", "delta"}, task.toPaths(dir))

	// without delta
	dir = partitionDir{insertLogDir: "insert"}
	assert.Equal(t, []string{"insert"}, task.toPaths(dir))

	// without insert
	dir = partitionDir{deltaLogDir: "delta"}
	assert.Equal(t, []string{"delta"}, task.toPaths(dir))

	// empty
	dir = partitionDir{}
	assert.Empty(t, task.toPaths(dir))

	// a local target resolves import paths against the path Milvus sees
	task = &importViaRESTFulTask{
		milvusStorage:   newTestStorageClient(t, cfg.ProviderLocal),
		milvusLocalPath: "/var/lib/milvus/data",
	}
	dir = partitionDir{insertLogDir: "insert", deltaLogDir: "delta"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert", "/var/lib/milvus/data/delta"}, task.toPaths(dir))

	// paths already absolute are left alone (a same-directory local backup)
	dir = partitionDir{insertLogDir: "/data/insert"}
	assert.Equal(t, []string{"/data/insert"}, task.toPaths(dir))

	// a trailing slash is kept: the LocalChunkManager globs the prefix
	dir = partitionDir{insertLogDir: "insert/"}
	assert.Equal(t, []string{"/var/lib/milvus/data/insert/"}, task.toPaths(dir))
}
