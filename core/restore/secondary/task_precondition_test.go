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
	"encoding/base64"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
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
		Checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "src", TimeTick: tick},
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

func TestCheckTargetReady(t *testing.T) {
	t.Run("FreshSecondary", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).
			Return(cfgWith("tgt", "tgt-rootcoord-dml_0", "tgt-rootcoord-dml_1"), nil).Once()
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").Return(infoAt(0), nil).Once()
		cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_1").Return(infoAt(0), nil).Once()
		assert.NoError(t, taskForTarget(cli).checkTargetReady(context.Background()))
	})

	// The second channel must be checked even when the first has not advanced.
	for _, tick := range []uint64{1, 468522223459106838} {
		name := "PartialRestore"
		if tick > 1 {
			name = "CDCAlreadyAdvanced"
		}
		t.Run(name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().GetReplicateConfiguration(mock.Anything).
				Return(cfgWith("tgt", "tgt-rootcoord-dml_0", "tgt-rootcoord-dml_1"), nil).Once()
			cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").Return(infoAt(0), nil).Once()
			cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_1").Return(infoAt(tick), nil).Once()
			err := taskForTarget(cli).checkTargetReady(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "tgt-rootcoord-dml_1")
			assert.Contains(t, err.Error(), "CDC")
			assert.Contains(t, err.Error(), "new secondary")
		})
	}

	t.Run("ConfigurationReadFailure", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, context.DeadlineExceeded).Once()
		err := taskForTarget(cli).checkTargetReady(context.Background())
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	for _, tc := range []struct {
		name   string
		config *commonpb.ReplicateConfiguration
	}{
		{"NoConfiguration", nil},
		{"TargetMissing", cfgWith("someone-else", "other-rootcoord-dml_0")},
		{"NoTargetChannels", cfgWith("tgt")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().GetReplicateConfiguration(mock.Anything).Return(tc.config, nil).Once()
			err := taskForTarget(cli).checkTargetReady(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "tgt")
		})
	}

	for _, tc := range []struct {
		name     string
		response *milvuspb.GetReplicateInfoResponse
		err      error
	}{
		{"CheckpointReadFailure", nil, context.DeadlineExceeded},
		{"NilResponse", nil, nil},
		{"MissingCheckpoint", &milvuspb.GetReplicateInfoResponse{}, nil},
		{"WrongSource", &milvuspb.GetReplicateInfoResponse{Checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "other"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().GetReplicateConfiguration(mock.Anything).
				Return(cfgWith("tgt", "tgt-rootcoord-dml_0"), nil).Once()
			cli.EXPECT().GetReplicateInfo(mock.Anything, "src", "tgt-rootcoord-dml_0").Return(tc.response, tc.err).Once()
			err := taskForTarget(cli).checkTargetReady(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "tgt-rootcoord-dml_0")
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
			}
		})
	}
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

// Exercise Execute through the real gRPC client so preflight failures must
// reach the caller and task status before any restore stream is created.
func TestExecuteSecondaryPreflight(t *testing.T) {
	flush := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithBroadcast([]string{"src-rootcoord-dml_0_vcchan"}).MustBuildBroadcast().WithBroadcastID(1).SplitIntoMutableMessage()[0].
		WithTimeTick(100).WithLastConfirmed(milvus.NewFakeMessageID(100)).
		IntoImmutableMessage(milvus.NewFakeMessageID(100)).IntoImmutableMessageProto()
	flushBytes, err := proto.Marshal(flush)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		configErr  error
		checkpoint *commonpb.ReplicateCheckpoint
		want       string
	}{
		{name: "FreshSecondary", checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "src"}},
		{name: "CDCAlreadyAdvanced", checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "src", TimeTick: 468522223459106838}, want: "CDC"},
		{name: "ConfigurationReadFailure", configErr: status.Error(codes.PermissionDenied, "cannot inspect replication"), want: "cannot inspect replication"},
		{name: "MissingCheckpoint", want: "no replicate checkpoint"},
		{name: "WrongSource", checkpoint: &commonpb.ReplicateCheckpoint{ClusterId: "other"}, want: "source cluster"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &preflightServer{configErr: tc.configErr, checkpoint: tc.checkpoint}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			grpcServer := grpc.NewServer()
			milvuspb.RegisterMilvusServiceServer(grpcServer, srv)
			t.Cleanup(grpcServer.Stop)
			go grpcServer.Serve(listener)

			params := cfg.New()
			params.Milvus.Grpc.Address.Val = "127.0.0.1"
			params.Milvus.Grpc.Port.Val = listener.Addr().(*net.TCPAddr).Port
			params.Milvus.Grpc.TLSMode.Val = cfg.TLSDisabled
			store := jobstate.NewStore()
			task, err := NewTask(TaskArgs{
				TaskID: "restore", SourceClusterID: "src", TargetClusterID: "tgt", Params: params, Store: store,
				Backup: &backuppb.BackupInfo{
					ControlChannelName:   "src-rootcoord-dml_0_vcchan",
					PhysicalChannelNames: []string{"src-rootcoord-dml_0"},
					FlushAllMsgsBase64:   map[string]string{"src-rootcoord-dml_0": base64.StdEncoding.EncodeToString(flushBytes)},
					DatabaseBackups:      []*backuppb.DatabaseBackupInfo{{DbId: 1, DbName: "default"}},
				},
			})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = task.Execute(ctx)
			if tc.want == "" {
				require.NoError(t, err)
				view, err := store.GetRestoreTask("restore")
				require.NoError(t, err)
				assert.Equal(t, jobstate.RestoreStateSuccess, view.State)
				srv.mu.Lock()
				defer srv.mu.Unlock()
				assert.Equal(t, []uint64{1, 2, 100}, srv.ticks, "DDL and RBAC must precede the backed-up FlushAll")
				assert.True(t, proto.Equal(flush, srv.lastMessage), "FlushAll must retain the backup's time tick and message ID")
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Nil(t, task.streamCli, "preflight must finish before opening restore streams")
			assert.Zero(t, srv.streams.Load())
			view, err := store.GetRestoreTask("restore")
			require.NoError(t, err)
			assert.Equal(t, jobstate.RestoreStateFail, view.State)
			assert.Contains(t, view.ErrorMessage, tc.want)
		})
	}
}

type preflightServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	configErr   error
	checkpoint  *commonpb.ReplicateCheckpoint
	streams     atomic.Int32
	mu          sync.Mutex
	ticks       []uint64
	lastMessage *commonpb.ImmutableMessage
}

func (s *preflightServer) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	return &milvuspb.ConnectResponse{Status: &commonpb.Status{}, ServerInfo: &commonpb.ServerInfo{BuildTags: "2.6.17"}}, nil
}

func (s *preflightServer) GetReplicateConfiguration(context.Context, *milvuspb.GetReplicateConfigurationRequest) (*milvuspb.GetReplicateConfigurationResponse, error) {
	return &milvuspb.GetReplicateConfigurationResponse{Status: &commonpb.Status{}, Configuration: cfgWith("tgt", "tgt-rootcoord-dml_0")}, s.configErr
}

func (s *preflightServer) GetReplicateInfo(context.Context, *milvuspb.GetReplicateInfoRequest) (*milvuspb.GetReplicateInfoResponse, error) {
	return &milvuspb.GetReplicateInfoResponse{Checkpoint: s.checkpoint}, nil
}

func (s *preflightServer) CreateReplicateStream(stream milvuspb.MilvusService_CreateReplicateStreamServer) error {
	s.streams.Add(1)
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		msg := req.GetReplicateMessage().GetMessage()
		tick, err := milvus.GetTT(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.ticks = append(s.ticks, tick)
		s.lastMessage = msg
		s.mu.Unlock()
		if err := stream.Send(&milvuspb.ReplicateResponse{Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{ConfirmedTimeTick: tick},
		}}); err != nil {
			return err
		}
	}
}

func (s *preflightServer) BackupRBAC(context.Context, *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	return &milvuspb.BackupRBACMetaResponse{Status: &commonpb.Status{}}, nil
}
