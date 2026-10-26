package restore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

// The mapping keeps the backup's partition layout: without one the import routes
// every row of an ordinary collection into _default.
func TestSnapshotPartitionMapping(t *testing.T) {
	t.Run("IdentityMapping", func(t *testing.T) {
		coll := &backuppb.CollectionBackupInfo{
			PartitionBackups: []*backuppb.PartitionBackupInfo{
				{PartitionName: "_default"}, {PartitionName: "p1"},
			},
		}

		mapping, err := snapshotPartitionMapping(coll)
		require.NoError(t, err)
		assert.JSONEq(t, `{"_default": "_default", "p1": "p1"}`, mapping)
	})

	// A partition-key target hashes its rows across the partitions it has; the
	// server rejects a mapping on one.
	t.Run("PartitionKeyCollectionTakesNoMapping", func(t *testing.T) {
		coll := &backuppb.CollectionBackupInfo{
			Schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{{Name: "key", IsPartitionKey: true}},
			},
			PartitionBackups: []*backuppb.PartitionBackupInfo{{PartitionName: "p1"}},
		}

		mapping, err := snapshotPartitionMapping(coll)
		require.NoError(t, err)
		assert.Empty(t, mapping)
	})

	// The server bounds the option to 64 KiB; an oversized mapping fails where
	// the input can be named rather than at job submission.
	t.Run("Over64KiBFails", func(t *testing.T) {
		parts := make([]*backuppb.PartitionBackupInfo, 0, 2000)
		for i := range 2000 {
			parts = append(parts, &backuppb.PartitionBackupInfo{
				PartitionName: fmt.Sprintf("partition-%s-%d", strings.Repeat("x", 32), i),
			})
		}
		coll := &backuppb.CollectionBackupInfo{PartitionBackups: parts}

		_, err := snapshotPartitionMapping(coll)
		assert.ErrorContains(t, err, "64 KiB")
	})
}

// snapshotImportEndpoint is a scripted Restful endpoint for the snapshot import
// tests: it captures the create request and answers the state poll from a
// sequence, repeating the last entry.
type snapshotImportEndpoint struct {
	states []string
	calls  int

	gotBulkInsert milvus.BulkInsertV2Input
}

func (e *snapshotImportEndpoint) BulkInsert(_ context.Context, in milvus.BulkInsertV2Input) (string, error) {
	e.gotBulkInsert = in
	return "job1", nil
}

func (e *snapshotImportEndpoint) GetBulkInsertState(context.Context, string, string) (*milvus.GetProcessResp, error) {
	i := min(e.calls, len(e.states)-1)
	e.calls++

	resp := &milvus.GetProcessResp{}
	resp.Data.State = e.states[i]
	if e.states[i] == string(milvus.ImportStateFailed) {
		resp.Data.Reason = "source bucket not readable"
	}
	return resp, nil
}

func (e *snapshotImportEndpoint) GetSegmentInfo(context.Context, string, int64, int64) (*milvus.SegmentInfo, error) {
	return nil, nil
}

func (e *snapshotImportEndpoint) CommitImport(context.Context, string, string) error { return nil }

func (e *snapshotImportEndpoint) AbortImport(context.Context, string, string) error { return nil }

func newSnapshotImportTask(endpoint *snapshotImportEndpoint) *snapshotImportTask {
	target := collref.New("db", "coll")
	collTracker := jobstate.NewStore().AddRestoreTask("task1").NewCollTracker(target, 1)

	return &snapshotImportTask{
		taskID:           "task1",
		target:           target,
		metadataURI:      "s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json",
		externalSpec:     `{"extfs":{"access_key_id":"ak"}}`,
		partitionMapping: `{"_default":"_default"}`,
		ezk:              "key",

		restfulCli: endpoint,
		job:        collTracker.NewJob(1),

		logger: zap.NewNop(),
	}
}

func TestSnapshotImportTaskExecute(t *testing.T) {
	// One job carries the whole bundle: the create request is the snapshot
	// contract — the metadata URI as the single file, source_type=snapshot, the
	// source's spec and the partition mapping — with the binlog-only knobs unset.
	t.Run("SubmitsTheSnapshotContract", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			endpoint := &snapshotImportEndpoint{states: []string{
				string(milvus.ImportStateImporting),
				string(milvus.ImportStateCompleted),
			}}

			task := newSnapshotImportTask(endpoint)
			require.NoError(t, task.Execute(t.Context()))

			in := endpoint.gotBulkInsert
			assert.Equal(t, "db", in.DB)
			assert.Equal(t, "coll", in.CollectionName)
			assert.Equal(t, [][]string{{"s3://backup-bucket/backup/mybackup/bundle/snapshots/1/metadata/2.json"}}, in.Paths)
			assert.Equal(t, milvus.SourceTypeSnapshot, in.SourceType)
			assert.Equal(t, `{"extfs":{"access_key_id":"ak"}}`, in.ExternalSpec)
			assert.JSONEq(t, `{"_default":"_default"}`, in.PartitionMapping)
			assert.Equal(t, "key", in.EZK)
			assert.Empty(t, in.PartitionName)
			assert.False(t, in.ManualCommit)
			assert.Zero(t, in.BackupTS)
			assert.False(t, in.IsL0)
			assert.Zero(t, in.StorageVersion)
		})
	})

	// A failed job settles the task's half with its reason.
	t.Run("FailedJobSettlesTheHalf", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			endpoint := &snapshotImportEndpoint{states: []string{string(milvus.ImportStateFailed)}}

			task := newSnapshotImportTask(endpoint)
			err := task.Execute(t.Context())
			assert.ErrorContains(t, err, "source bucket not readable")
		})
	})
}
