// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package secondary

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func taskWithBackup(cli milvus.Grpc, colls ...*backuppb.CollectionBackupInfo) *Task {
	return &Task{
		args:   TaskArgs{Backup: &backuppb.BackupInfo{CollectionBackups: colls}},
		grpc:   cli,
		logger: zap.NewNop(),
	}
}

func coll(db, name string) *backuppb.CollectionBackupInfo {
	return &backuppb.CollectionBackupInfo{DbName: db, CollectionName: name}
}

func TestCheckTargetIsUnused(t *testing.T) {
	orders, invoices := coll("default", "orders"), coll("default", "invoices")

	t.Run("a target that holds none of them is accepted", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(false, nil)
		cli.EXPECT().HasCollection(mock.Anything, "default", "invoices").Return(false, nil)
		assert.NoError(t, taskWithBackup(cli, orders, invoices).checkTargetIsUnused(context.Background()))
	})

	t.Run("a target that already holds one is refused, and it is named", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(true, nil)
		cli.EXPECT().HasCollection(mock.Anything, "default", "invoices").Return(false, nil)
		err := taskWithBackup(cli, orders, invoices).checkTargetIsUnused(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "default.orders")
		assert.NotContains(t, err.Error(), "default.invoices")
		assert.Contains(t, err.Error(), "new secondary")
	})

	t.Run("a database that does not exist yet is not a reason to refuse", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "other", "orders").
			Return(false, errors.New("database not found"))
		assert.NoError(t, taskWithBackup(cli, coll("other", "orders")).checkTargetIsUnused(context.Background()))
	})
}

func TestVerifyRestored(t *testing.T) {
	prevTimeout, prevInterval := _restoreVerifyTimeout, _restoreVerifyInterval
	_restoreVerifyTimeout, _restoreVerifyInterval = 30*time.Millisecond, time.Millisecond
	defer func() {
		_restoreVerifyTimeout, _restoreVerifyInterval = prevTimeout, prevInterval
	}()

	t.Run("collections that are present pass", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(true, nil)
		assert.NoError(t, taskWithBackup(cli, coll("default", "orders")).verifyRestored(context.Background()))
	})

	// The whole point: every message was accepted and nothing was created.
	t.Run("a collection that never appears fails the restore", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(false, nil)
		err := taskWithBackup(cli, coll("default", "orders")).verifyRestored(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "default.orders")
		assert.Contains(t, err.Error(), "reported no errors")
		assert.Contains(t, err.Error(), "new secondary")
	})

	t.Run("a collection that appears a moment later still passes", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(false, nil).Once()
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").Return(true, nil)
		assert.NoError(t, taskWithBackup(cli, coll("default", "orders")).verifyRestored(context.Background()))
	})

	t.Run("a failing lookup is reported rather than treated as missing", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").
			Return(false, errors.New("connection refused"))
		err := taskWithBackup(cli, coll("default", "orders")).verifyRestored(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})
}

func cfgWith(clusterID string, pchannels ...string) *commonpb.ReplicateConfiguration {
	return &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "other", Pchannels: []string{"other-rootcoord-dml_0"}},
			{ClusterId: clusterID, Pchannels: pchannels},
		},
	}
}

func infoAt(tick uint64) *milvuspb.GetReplicateInfoResponse {
	return &milvuspb.GetReplicateInfoResponse{
		Checkpoint: &commonpb.ReplicateCheckpoint{TimeTick: tick},
	}
}

func taskForTarget(cli milvus.Grpc) *Task {
	return &Task{
		args: TaskArgs{
			SourceClusterID: "src",
			TargetClusterID: "tgt",
			Backup:          &backuppb.BackupInfo{},
		},
		grpc:   cli,
		logger: zap.NewNop(),
	}
}

func TestCheckTargetNotRestored(t *testing.T) {
	t.Run("a newly deployed secondary reports zero on every pchannel", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(cfgWith("tgt", "tgt-rootcoord-dml_0", "tgt-rootcoord-dml_1"), nil)
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").Return(infoAt(0), nil)
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_1").Return(infoAt(0), nil)
		assert.NoError(t, taskForTarget(cli).checkTargetNotRestored(context.Background()))
	})

	// The case that is otherwise invisible: the collections were dropped, so
	// nothing is listed, but the checkpoint of the restore that created them
	// is still there.
	t.Run("a target that was restored before is refused, naming the pchannel", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(cfgWith("tgt", "tgt-rootcoord-dml_0", "tgt-rootcoord-dml_1"), nil)
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").Return(infoAt(0), nil)
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_1").
			Return(infoAt(468522223459106838), nil)
		err := taskForTarget(cli).checkTargetNotRestored(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tgt-rootcoord-dml_1")
		assert.Contains(t, err.Error(), "restored before")
		assert.Contains(t, err.Error(), "new secondary")
	})

	t.Run("a cluster with no replication configuration is not refused", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(nil, errors.New("deadline exceeded"))
		assert.NoError(t, taskForTarget(cli).checkTargetNotRestored(context.Background()))
	})

	t.Run("a configuration that does not list the target is not refused", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(cfgWith("someone-else"), nil)
		assert.NoError(t, taskForTarget(cli).checkTargetNotRestored(context.Background()))
	})

	t.Run("a checkpoint that cannot be read is reported", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(cfgWith("tgt", "tgt-rootcoord-dml_0"), nil)
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").
			Return(nil, errors.New("connection refused"))
		err := taskForTarget(cli).checkTargetNotRestored(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})
}

func TestCheckBackupHasFullMeta(t *testing.T) {
	full := func() *backuppb.BackupInfo {
		return &backuppb.BackupInfo{
			Name:                 "bk",
			ControlChannelName:   "by-dev-rootcoord-dml_0",
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"},
			FlushAllMsgsBase64:   map[string]string{"by-dev-rootcoord-dml_0": "AA==", "by-dev-rootcoord-dml_1": "AA=="},
		}
	}
	task := func(b *backuppb.BackupInfo) *Task {
		return &Task{args: TaskArgs{Backup: b}, logger: zap.NewNop()}
	}

	t.Run("a backup read from full_meta.json is accepted", func(t *testing.T) {
		assert.NoError(t, task(full()).checkBackupHasFullMeta())
	})

	t.Run("a backup without a control channel is refused and full_meta.json is named", func(t *testing.T) {
		b := full()
		b.ControlChannelName = ""
		err := task(b).checkBackupHasFullMeta()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "control channel")
		assert.Contains(t, err.Error(), "meta/full_meta.json")
		assert.Contains(t, err.Error(), "skip_flush")
		assert.Contains(t, err.Error(), `"bk"`)
	})

	t.Run("a backup read from the per-level files is refused, naming everything absent", func(t *testing.T) {
		err := task(&backuppb.BackupInfo{Name: "bk"}).checkBackupHasFullMeta()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "control channel, pchannel list, flush-all messages")
	})

	t.Run("a backup without flush-all messages is refused", func(t *testing.T) {
		b := full()
		b.FlushAllMsgsBase64 = nil
		err := task(b).checkBackupHasFullMeta()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "flush-all messages")
		assert.NotContains(t, err.Error(), "control channel,")
	})
}

func TestCheckTargetIsUnusedOtherErrors(t *testing.T) {
	t.Run("an error other than database-not-found is not a reason to refuse either", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollection(mock.Anything, "default", "orders").
			Return(false, errors.New("rpc error: connection reset"))
		assert.NoError(t, taskWithBackup(cli, coll("default", "orders")).checkTargetIsUnused(context.Background()))
	})

	t.Run("database-not-found is recognized in wrapped client errors", func(t *testing.T) {
		assert.True(t, isDatabaseNotFound(errors.New(`client: has collection failed: client: operation failed: error_code:UnexpectedError reason:"database not found[database=aml_endor_db]" code:800`)))
		assert.False(t, isDatabaseNotFound(errors.New("collection not found")))
		assert.False(t, isDatabaseNotFound(nil))
	})
}

func TestWaitCollCreated(t *testing.T) {
	prevTimeout, prevInterval := _collCreateTimeout, _collCreateInterval
	_collCreateTimeout, _collCreateInterval = 30*time.Millisecond, time.Millisecond
	defer func() {
		_collCreateTimeout, _collCreateInterval = prevTimeout, prevInterval
	}()

	collRef := collref.New("default", "orders")

	t.Run("a collection that is already there returns at once", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollectionByID(mock.Anything, int64(7)).Return(true, nil).Once()
		assert.NoError(t, taskWithBackup(cli).waitCollCreated(context.Background(), collRef, 7))
	})

	t.Run("a collection that appears late is waited for", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		var calls int
		cli.EXPECT().HasCollectionByID(mock.Anything, int64(7)).
			RunAndReturn(func(context.Context, int64) (bool, error) {
				calls++
				return calls > 2, nil
			})
		assert.NoError(t, taskWithBackup(cli).waitCollCreated(context.Background(), collRef, 7))
		assert.Equal(t, 3, calls)
	})

	// The whole point: the create was sent, the target reported no error, and the
	// collection is not there. Importing into it would be accepted and then killed
	// partway through, blaming the collection for having been dropped.
	t.Run("a collection that never appears fails before the import", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollectionByID(mock.Anything, int64(7)).Return(false, nil)
		err := taskWithBackup(cli).waitCollCreated(context.Background(), collRef, 7)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "default.orders")
		assert.Contains(t, err.Error(), "7")
		assert.Contains(t, err.Error(), "reserved")
	})

	t.Run("an error reaching the target is reported as such", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().HasCollectionByID(mock.Anything, int64(7)).
			Return(false, errors.New("connection refused")).Once()
		err := taskWithBackup(cli).waitCollCreated(context.Background(), collRef, 7)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})
}
