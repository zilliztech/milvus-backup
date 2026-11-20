package restore

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func newSnapshot2PCTask(t *testing.T, local *twoPCCluster, secondaries ...*twoPCCluster) *snapshotImport2PCTask {
	importSecondaries := make([]importSecondary, 0, len(secondaries))
	for _, secondary := range secondaries {
		importSecondaries = append(importSecondaries, importSecondary{addr: secondary.name, cli: secondary})
	}
	return &snapshotImport2PCTask{importer: newSnapshotImporter(t, local), secondaries: importSecondaries}
}

func TestSnapshotImport2PCTaskExecute(t *testing.T) {
	t.Run("CommitsPrimaryAfterSecondaryMirrorIsReady", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var events []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &events, &mu,
				string(milvus.ImportStateImporting),
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &events, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCommitting),
				string(milvus.ImportStateCompleted))
			local.recordStates = true
			secondary.recordStates = true

			task := newSnapshot2PCTask(t, local, secondary)
			require.NoError(t, task.Execute(t.Context()))

			in := local.gotBulkInsert
			assert.Equal(t, "db", in.DB)
			assert.Equal(t, "coll", in.CollectionName)
			assert.Equal(t, [][]string{{"s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json"}}, in.Paths)
			assert.Equal(t, milvus.SourceTypeSnapshot, in.SourceType)
			assert.Equal(t, `{"extfs":{"access_key_id":"ak"}}`, in.ExternalSpec)
			assert.JSONEq(t, `{"_default":"_default"}`, in.PartitionMapping)
			assert.Equal(t, "key", in.EZK)
			assert.True(t, in.ManualCommit)
			assert.Empty(t, in.PartitionName)
			assert.Zero(t, in.BackupTS)
			assert.False(t, in.IsL0)
			assert.Zero(t, in.StorageVersion)

			assert.Equal(t, []string{
				"local:create",
				"local:state:Importing",
				"local:state:Uncommitted",
				"secondary:state:Uncommitted",
				"local:commit",
				"secondary:state:Committing",
				"secondary:state:Completed",
				"local:state:Completed",
			}, events)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, secondary.count("create"))
			assert.Zero(t, secondary.count("commit"))
			assert.Zero(t, secondary.count("abort"))
		})
	})

	t.Run("SecondaryFailureBeforeCommitAbortsPrimary", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var events []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &events, &mu, string(milvus.ImportStateUncommitted))
			secondary := newTwoPCCluster("secondary", &events, &mu, string(milvus.ImportStateFailed))

			task := newSnapshot2PCTask(t, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "disk quota exceeded")
			assert.Equal(t, []string{"local:create", "local:abort"}, events)
			assert.Zero(t, local.count("commit"))
			assert.Zero(t, secondary.count("commit"))
			assert.Zero(t, secondary.count("abort"))
		})
	})

	t.Run("SecondaryFailureAfterCommitReportsDivergence", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var events []string
			var mu sync.Mutex
			local := newTwoPCCluster("local", &events, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateCompleted))
			secondary := newTwoPCCluster("secondary", &events, &mu,
				string(milvus.ImportStateUncommitted),
				string(milvus.ImportStateFailed))

			task := newSnapshot2PCTask(t, local, secondary)
			err := task.Execute(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "already committed")
			assert.Contains(t, err.Error(), "diverge")
			assert.Equal(t, []string{"local:create", "local:commit"}, events)
			assert.Zero(t, local.count("abort"))
			assert.Zero(t, secondary.count("abort"))
		})
	})
}

func TestSnapshotTwoPhaseResumeKeepsMissingPrimaryID(t *testing.T) {
	var events []string
	var mu sync.Mutex
	primary := newTwoPCCluster("primary", &events, &mu, "missing")
	mirror := newTwoPCCluster("mirror", &events, &mu, string(milvus.ImportStateCompleted))
	task := newSnapshot2PCTask(t, primary, mirror)
	tr, err := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, err)
	si := task.importer
	si.jobs = tr.NewCollTracker(si.target, 1)
	si.job = si.jobs.NewJob(1)
	si.job.Bind("job1")
	si.job.MarkCommitRequested()
	require.ErrorIs(t, task.Execute(t.Context()), milvus.ErrImportJobNotFound)
	id, done := si.job.Current()
	assert.Equal(t, "job1", id)
	assert.False(t, done)
	assert.True(t, si.job.CommitRequested())
	assert.Empty(t, events)
}
