package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/namespace"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/taskmgr"
)

func TestTask_ExecuteMarksInitClientFailure(t *testing.T) {
	mgr := taskmgr.NewMgr()
	require.NoError(t, mgr.AddBackupTask("task1", "backup1"))
	params := v2.New()
	params.Milvus.Grpc.TLSMode.Val = v2.TLSMutual

	task := &Task{
		taskID:  "task1",
		logger:  zap.NewNop(),
		params:  params,
		taskMgr: mgr,
	}

	err := task.Execute(context.Background())
	assert.ErrorContains(t, err, "client: load client cert")

	view, getErr := mgr.GetBackupTask("task1")
	require.NoError(t, getErr)
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_FAIL, view.StateCode())
	assert.Equal(t, int32(0), view.Progress())
	assert.Contains(t, view.ErrorMessage(), "client: load client cert")
	assert.False(t, view.EndTime().IsZero())

	brief := pbconv.NewBackupInfoBrief(view, nil, 0)
	assert.Equal(t, backuppb.BackupTaskStateCode_BACKUP_FAIL, brief.GetStateCode())
	assert.Contains(t, brief.GetErrorMessage(), "client: load client cert")
	assert.Greater(t, brief.GetEndTime(), int64(0))
}

func TestTask_runRBACTask(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().BackupRBAC(mock.Anything).Return(&milvuspb.BackupRBACMetaResponse{}, nil).Once()

		task := &Task{
			option:      Option{BackupRBAC: true},
			logger:      zap.NewNop(),
			taskID:      "backup1",
			metaBuilder: newMetaBuilder("task1", "backup1"),
			grpc:        cli,
		}
		err := task.backupRBAC(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Skip", func(t *testing.T) {
		task := &Task{logger: zap.NewNop()}
		err := task.backupRBAC(context.Background())
		assert.NoError(t, err)
	})
}

func TestTask_listDBAndNSS(t *testing.T) {
	ctx := context.Background()

	t.Run("NoFilterSupportMultiDB", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasFeature(milvus.MultiDatabase).Return(true)
		mockGrpc.EXPECT().ListDatabases(ctx).Return([]string{"db1", "db2"}, nil)
		mockGrpc.EXPECT().ListCollections(ctx, "db1").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)
		mockGrpc.EXPECT().ListCollections(ctx, "db2").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll3"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1", "db2"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll3"),
		}, nss)
	})

	t.Run("NoFilterNotSupportMultiDB", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasFeature(milvus.MultiDatabase).Return(false)
		mockGrpc.EXPECT().ListCollections(ctx, "default").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("default", "coll1"),
			namespace.New("default", "coll2"),
		}, nss)
	})

	t.Run("DBPattern", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().ListCollections(ctx, "db1").Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1", "coll2"},
		}, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {AllowAll: true},
				},
			}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
		}, nss)
	})

	t.Run("TargetedPattern", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasCollection(ctx, "db1", "coll1").Return(true, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {CollName: map[string]struct{}{"coll1": {}}},
				},
			}},
		}

		dbNames, nss, err := task.listDBAndNSS(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []namespace.NS{
			namespace.New("db1", "coll1"),
		}, nss)
	})

	t.Run("CollectionNotFound", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().HasCollection(ctx, "db1", "not_exist").Return(false, nil)

		task := &Task{
			logger: zap.NewNop(),
			grpc:   mockGrpc,
			option: Option{Filter: filter.Filter{
				DBCollFilter: map[string]filter.CollFilter{
					"db1": {CollName: map[string]struct{}{"not_exist": {}}},
				},
			}},
		}

		_, _, err := task.listDBAndNSS(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "filter collection db1.not_exist not found")
	})
}

func TestTask_excludeExternalColl(t *testing.T) {
	ctx := context.Background()

	externalResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "id"},
			{Name: "vec", ExternalField: "vec"},
		}},
	}
	normalResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "vec"},
		}},
	}

	t.Run("Normal", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().DescribeCollection(ctx, "db1", "coll1").Return(normalResp, nil).Once()
		mockGrpc.EXPECT().DescribeCollection(ctx, "db1", "coll2").Return(externalResp, nil).Once()
		mockGrpc.EXPECT().DescribeCollection(ctx, "db2", "coll3").Return(normalResp, nil).Once()

		task := &Task{logger: zap.NewNop(), grpc: mockGrpc}

		nss, err := task.excludeExternalColl(ctx, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db1", "coll2"),
			namespace.New("db2", "coll3"),
		})
		assert.NoError(t, err)
		assert.Equal(t, []namespace.NS{
			namespace.New("db1", "coll1"),
			namespace.New("db2", "coll3"),
		}, nss)
	})

	t.Run("DescribeCollectionFailed", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().DescribeCollection(ctx, "db1", "coll1").
			Return(nil, errors.New("mock error")).Once()

		task := &Task{logger: zap.NewNop(), grpc: mockGrpc}

		_, err := task.excludeExternalColl(ctx, []namespace.NS{namespace.New("db1", "coll1")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "describe collection db1.coll1")
	})
}

func TestTask_resolveFormat(t *testing.T) {
	tests := []struct {
		name      string
		format    Format
		snapshot  bool
		want      string
		wantError string
	}{
		{
			name:     "AutoPicksSnapshotOn3_0",
			format:   FormatAuto,
			snapshot: true,
			want:     meta.FormatSnapshot,
		},
		{
			name:   "AutoPicksBinlogOnOldServer",
			format: FormatAuto,
			want:   meta.FormatBinlog,
		},
		{
			name:     "SnapshotPinnedOn3_0",
			format:   FormatSnapshot,
			snapshot: true,
			want:     meta.FormatSnapshot,
		},
		{
			name:      "SnapshotRefusedOnOldServer",
			format:    FormatSnapshot,
			wantError: "snapshot format needs a milvus 3.0 or newer server",
		},
		{
			name:     "BinlogAcceptedOn3_0",
			format:   FormatBinlog,
			snapshot: true,
			want:     meta.FormatBinlog,
		},
		{
			name:   "BinlogPinnedOnOldServer",
			format: FormatBinlog,
			want:   meta.FormatBinlog,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockGrpc := milvus.NewMockGrpc(t)
			if tt.format != FormatBinlog {
				mockGrpc.EXPECT().HasFeature(milvus.Snapshot).Return(tt.snapshot).Once()
			}

			task := &Task{
				logger: zap.NewNop(),
				grpc:   mockGrpc,
				option: Option{Format: tt.format},
			}

			got, err := task.resolveFormat()
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
