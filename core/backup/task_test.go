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

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/filter"
	"github.com/zilliztech/milvus-backup/internal/meta"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

func TestTask_ExecuteInitClientFailure(t *testing.T) {
	params := cfg.New()
	params.Milvus.Grpc.TLSMode.Val = cfg.TLSMutual

	// The task only answers; it does not record. Terminal states are the
	// job's business, so a failed Execute writes nothing — the zero tracker
	// is safe precisely because of that split.
	task := &Task{
		logger: zap.NewNop(),
		params: params,
	}

	err := task.Execute(context.Background())
	assert.ErrorContains(t, err, "client: load client cert")
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

func TestTask_listDBAndNames(t *testing.T) {
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

		dbNames, collRefs, err := task.listDBAndNames(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1", "db2"}, dbNames)
		assert.ElementsMatch(t, []collref.Name{
			collref.New("db1", "coll1"),
			collref.New("db1", "coll2"),
			collref.New("db2", "coll3"),
		}, collRefs)
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

		dbNames, collRefs, err := task.listDBAndNames(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"default"}, dbNames)
		assert.ElementsMatch(t, []collref.Name{
			collref.New("default", "coll1"),
			collref.New("default", "coll2"),
		}, collRefs)
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

		dbNames, collRefs, err := task.listDBAndNames(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []collref.Name{
			collref.New("db1", "coll1"),
			collref.New("db1", "coll2"),
		}, collRefs)
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

		dbNames, collRefs, err := task.listDBAndNames(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"db1"}, dbNames)
		assert.ElementsMatch(t, []collref.Name{
			collref.New("db1", "coll1"),
		}, collRefs)
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

		_, _, err := task.listDBAndNames(ctx)
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

		collRefs, err := task.excludeExternalColl(ctx, []collref.Name{
			collref.New("db1", "coll1"),
			collref.New("db1", "coll2"),
			collref.New("db2", "coll3"),
		})
		assert.NoError(t, err)
		assert.Equal(t, []collref.Name{
			collref.New("db1", "coll1"),
			collref.New("db2", "coll3"),
		}, collRefs)
	})

	t.Run("DescribeCollectionFailed", func(t *testing.T) {
		mockGrpc := milvus.NewMockGrpc(t)
		mockGrpc.EXPECT().DescribeCollection(ctx, "db1", "coll1").
			Return(nil, errors.New("mock error")).Once()

		task := &Task{logger: zap.NewNop(), grpc: mockGrpc}

		_, err := task.excludeExternalColl(ctx, []collref.Name{collref.New("db1", "coll1")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "describe collection db1.coll1")
	})
}

func TestTask_resolveFormat(t *testing.T) {
	sameService := storage.Config{Provider: cfg.ProviderMinio, Endpoint: "minio:9000", Bucket: "a"}
	crossService := storage.Config{Provider: cfg.ProviderAWS, Region: "us-west-2", Bucket: "b"}

	tests := []struct {
		name      string
		format    Format
		snapshot  bool
		backupCfg storage.Config
		want      string
		wantError string
	}{
		{
			name:      "AutoPicksSnapshotOn3_0",
			format:    FormatAuto,
			snapshot:  true,
			backupCfg: sameService,
			want:      meta.FormatSnapshot,
		},
		{
			name:   "AutoPicksBinlogOnOldServer",
			format: FormatAuto,
			want:   meta.FormatBinlog,
		},
		{
			// Backup streams the snapshot files through its own clients, so a
			// backup storage on another service is no longer a reason to refuse.
			name:      "AutoPicksSnapshotAcrossServices",
			format:    FormatAuto,
			snapshot:  true,
			backupCfg: crossService,
			want:      meta.FormatSnapshot,
		},
		{
			name:      "SnapshotPinnedOn3_0",
			format:    FormatSnapshot,
			snapshot:  true,
			backupCfg: sameService,
			want:      meta.FormatSnapshot,
		},
		{
			name:      "SnapshotRefusedOnOldServer",
			format:    FormatSnapshot,
			wantError: "snapshot format needs a milvus 3.0 or newer server",
		},
		{
			name:      "SnapshotAcceptedAcrossServices",
			format:    FormatSnapshot,
			snapshot:  true,
			backupCfg: crossService,
			want:      meta.FormatSnapshot,
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

			// resolveFormat itself no longer reads the storage configs: the
			// snapshot target is built later, when the backup executes.
			task := &Task{
				logger:        zap.NewNop(),
				grpc:          mockGrpc,
				option:        Option{Format: tt.format},
				milvusStorage: storage.NewMockClient(t),
				backupStorage: storage.NewMockClient(t),
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
