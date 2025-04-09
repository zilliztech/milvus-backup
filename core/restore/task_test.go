package restore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/mocks"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestTask_PrepareDB(t *testing.T) {
	cli := mocks.NewMockGrpc(t)

	task := &backuppb.RestoreBackupTask{
		// database may be duplicated
		CollectionRestoreTasks: []*backuppb.RestoreCollectionTask{
			{TargetDbName: "db1"}, {TargetDbName: "db2"}, {TargetDbName: "db2"}, // db in target
			{TargetDbName: "db3"}, {TargetDbName: "db3"}, // db not in target
		},
	}
	cli.EXPECT().ListDatabases(mock.Anything).Return([]string{"db1", "db2"}, nil).Once()
	cli.EXPECT().CreateDatabase(mock.Anything, "db3").Return(nil).Once()

	rt := &Task{task: task, grpcCli: cli, logger: zap.NewNop()}
	err := rt.prepareDB(context.Background())
	assert.NoError(t, err)
}

func TestTask_CheckCollExist(t *testing.T) {
	testCases := []struct {
		has        bool
		skipCreate bool
		dropExist  bool

		ok bool
	}{
		{has: true, skipCreate: false, dropExist: false, ok: false},
		{has: true, skipCreate: true, dropExist: false, ok: true},
		{has: true, skipCreate: false, dropExist: true, ok: true},
		{has: true, skipCreate: true, dropExist: true, ok: false},
		{has: false, skipCreate: false, dropExist: false, ok: true},
		{has: false, skipCreate: true, dropExist: false, ok: false},
		{has: false, skipCreate: false, dropExist: true, ok: true},
		{has: false, skipCreate: true, dropExist: true, ok: false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("has:%v, skipCreate:%v, dropExist:%v", tc.has, tc.skipCreate, tc.dropExist), func(t *testing.T) {
			cli := mocks.NewMockGrpc(t)

			task := &backuppb.RestoreCollectionTask{
				TargetDbName:         "db1",
				TargetCollectionName: "coll1",
				SkipCreateCollection: tc.skipCreate,
				DropExistCollection:  tc.dropExist,
			}

			cli.EXPECT().HasCollection(mock.Anything, "db1", "coll1").Return(tc.has, nil).Once()

			rt := &Task{grpcCli: cli, logger: zap.NewNop()}
			err := rt.checkCollExist(context.Background(), task)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
