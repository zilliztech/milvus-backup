package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/zilliztech/milvus-backup/core/tasklet"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// fakeDataTask stands in for a plan's data task. The real data task flips its
// half past flushing when it starts and settles it by its own outcome; settle
// mirrors that. A task with settle off leaves the half in flushing, the state
// a task that never launched leaves behind.
type fakeDataTask struct {
	tracker *jobstate.BackupCollDMLTracker

	settle bool
	err    error

	ran atomic.Bool
}

func (t *fakeDataTask) Execute(context.Context) error {
	t.ran.Store(true)
	if !t.settle {
		return t.err
	}

	t.tracker.SetPrepare()
	if t.err != nil {
		t.tracker.SetFail(t.err)
		return t.err
	}

	t.tracker.SetSuccess()
	return nil
}

func TestBulkFlushPlan_Execute(t *testing.T) {
	newTracker := func(t *testing.T) *jobstate.BackupTracker {
		t.Helper()

		tracker, err := jobstate.NewStore().AddBackupTask("task-1", "mybackup")
		require.NoError(t, err)

		return tracker
	}

	newArgs := func(tracker *jobstate.BackupTracker, cli milvus.Grpc) collTaskArgs {
		return collTaskArgs{
			TaskID:      "task-1",
			tracker:     tracker,
			MetaBuilder: newMetaBuilder("task-1", "mybackup"),
			Grpc:        cli,
			Throttling: concurrencyThrottling{
				DDLSem: semaphore.NewWeighted(4),
				DMLSem: semaphore.NewWeighted(4),
			},
		}
	}

	// The DDL branch races the data branch under test, and a failed data
	// branch can cancel the context before every DDL task has launched, so
	// each DDL call is allowed but not required. The assertions below read
	// only DML halves.
	succeedDDL := func(cli *milvus.MockGrpc, refs []collref.Name) {
		for _, ref := range refs {
			cli.EXPECT().DescribeCollection(mock.Anything, ref.DBName(), ref.CollName()).
				Return(&milvuspb.DescribeCollectionResponse{
					CollectionID:   1,
					DbName:         ref.DBName(),
					CollectionName: ref.CollName(),
					Schema:         &schemapb.CollectionSchema{Name: ref.CollName()},
					ShardsNum:      1,
				}, nil).Maybe()
			cli.EXPECT().ListIndex(mock.Anything, ref.DBName(), ref.CollName()).Return(nil, nil).Maybe()
			cli.EXPECT().GetLoadingProgress(mock.Anything, ref.DBName(), ref.CollName()).Return(int64(100), nil).Maybe()
			cli.EXPECT().ShowPartitions(mock.Anything, ref.DBName(), ref.CollName()).
				Return(&milvuspb.ShowPartitionsResponse{PartitionNames: []string{"_default"}, PartitionIDs: []int64{10}}, nil).Maybe()
		}
	}

	t.Run("FlushAllFailureFailsAllFlushingHalves", func(t *testing.T) {
		refs := []collref.Name{collref.New("db1", "coll_a"), collref.New("db1", "coll_b")}
		errFlush := errors.New("flush all boom")

		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().FlushAll(mock.Anything).Return(nil, errFlush)
		succeedDDL(cli, refs)

		tracker := newTracker(t)
		dataTasks := make(map[collref.Name]*fakeDataTask, len(refs))
		for _, ref := range refs {
			dataTasks[ref] = &fakeDataTask{} // must never run: the flush failed
		}
		newData := func(ref collref.Name, _ collTaskArgs) tasklet.Tasklet { return dataTasks[ref] }
		plan := newBulkFlushPlan(refs, newArgs(tracker, cli), newData)

		require.Error(t, plan.Execute(context.Background()))

		snap := tracker.Snapshot()
		for _, ref := range refs {
			assert.False(t, dataTasks[ref].ran.Load())

			dml := snap.Colls[ref].DML
			require.NotNil(t, dml)
			assert.Equal(t, jobstate.BackupCollDMLStateFail, dml.State)
			assert.Contains(t, dml.ErrorMessage, errFlush.Error())
			assert.False(t, dml.EndTime.IsZero())
		}
	})

	t.Run("DataBranchFailureFailsOnlyStuckFlushingHalves", func(t *testing.T) {
		refs := []collref.Name{collref.New("db1", "coll_a"), collref.New("db1", "coll_b"), collref.New("db1", "coll_c")}
		errData := errors.New("data boom")

		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().FlushAll(mock.Anything).Return(&milvuspb.FlushAllResponse{}, nil)
		succeedDDL(cli, refs)

		tracker := newTracker(t)
		tasks := make(map[collref.Name]*fakeDataTask, len(refs))
		for i, ref := range refs {
			task := &fakeDataTask{tracker: tracker.NewCollDMLTracker(ref)}
			switch i {
			case 0:
				// leaves the half in flushing, the state a never-launched task leaves
			case 1:
				// settles itself as success
				task.settle = true
			default:
				// settles itself as failed; runs last so its failure cannot
				// preempt a task that has yet to settle
				task.settle, task.err = true, errData
			}
			tasks[ref] = task
		}
		newData := func(ref collref.Name, _ collTaskArgs) tasklet.Tasklet { return tasks[ref] }
		args := newArgs(tracker, cli)
		args.Throttling.DMLSem = semaphore.NewWeighted(1) // run the three tasks in refs order
		plan := newBulkFlushPlan(refs, args, newData)

		require.Error(t, plan.Execute(context.Background()))

		assert.True(t, tasks[refs[1]].ran.Load())
		assert.True(t, tasks[refs[2]].ran.Load())

		snap := tracker.Snapshot()

		stuck := snap.Colls[refs[0]].DML
		require.NotNil(t, stuck)
		assert.Equal(t, jobstate.BackupCollDMLStateFail, stuck.State)
		assert.Equal(t, errData.Error(), stuck.ErrorMessage)
		assert.False(t, stuck.EndTime.IsZero())

		succeeded := snap.Colls[refs[1]].DML
		require.NotNil(t, succeeded)
		assert.Equal(t, jobstate.BackupCollDMLStateSuccess, succeeded.State)

		failed := snap.Colls[refs[2]].DML
		require.NotNil(t, failed)
		assert.Equal(t, jobstate.BackupCollDMLStateFail, failed.State)
		assert.Equal(t, errData.Error(), failed.ErrorMessage)
	})
}
