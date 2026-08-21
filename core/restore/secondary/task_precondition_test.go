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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
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
