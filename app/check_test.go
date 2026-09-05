package app

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/storage"
)

const (
	milvusRoot = "milvus_root"
	backupRoot = "backup_root"
)

// newCheckUnderTest wires a Check against mocks, mirroring what NewCheck does
// against real clients. The constructor itself needs live endpoints, so tests
// build the struct directly.
func newCheckUnderTest(grpc milvus.Grpc, milvusCli, backupCli storage.Client) *Check {
	return &Check{
		grpc:           grpc,
		milvusStorage:  milvusCli,
		backupStorage:  backupCli,
		milvusRootPath: milvusRoot,
		backupRootPath: backupRoot,
		transferMode:   "direct",
	}
}

// expectStoragesReadable teaches the mocks for the two connectivity checks
// that run before the write-and-copy step: listing the Milvus root
// recursively and listing the backup root non-recursively.
func expectStoragesReadable(t *testing.T, milvusCli, backupCli *storage.MockClient, milvusFiles []storage.ObjectAttr) {
	t.Helper()

	milvusCli.EXPECT().
		ListPrefix(mock.Anything, milvusRoot+"/", true).
		Return(storage.NewMockObjectIterator(milvusFiles), nil)
	backupCli.EXPECT().
		ListPrefix(mock.Anything, backupRoot+"/", false).
		Return(storage.NewMockObjectIterator(nil), nil)
}

// expectWriteAndCopy teaches the mocks for the write-and-copy step: one byte
// is written into the Milvus root, copied into the backup root, verified
// there, and both check objects are deleted again. The copy lists its source
// prefix twice (once to copy, once to build the verify expectation) and the
// verify lists the destination once; those prefixes carry a generated uuid,
// so both listings answer with one fixed object. The fixed key passes through
// ExpectedDestObjects unchanged (it never contains the src prefix), so the
// verify step finds exactly what it expects.
func expectWriteAndCopy(t *testing.T, milvusCli, backupCli *storage.MockClient) {
	t.Helper()

	milvusCli.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil)
	milvusCli.EXPECT().Config().Return(storage.Config{}).Times(2)
	backupCli.EXPECT().Config().Return(storage.Config{}).Times(2)

	// One expectation per listing: the raw mock Call overwrites (not queues)
	// chained Return values, so a Times(2) expectation would hand both calls
	// the same iterator instance and the second listing would look empty.
	oneObject := func() storage.ObjectIterator {
		return storage.NewMockObjectIterator([]storage.ObjectAttr{{Key: "copied-object", Length: 1}})
	}
	milvusCli.EXPECT().
		ListPrefix(mock.Anything, mock.Anything, true).
		Return(oneObject(), nil).
		Once()
	milvusCli.EXPECT().
		ListPrefix(mock.Anything, mock.Anything, true).
		Return(oneObject(), nil).
		Once()
	backupCli.EXPECT().
		ListPrefix(mock.Anything, mock.Anything, true).
		Return(oneObject(), nil).
		Once()

	backupCli.EXPECT().CopyObject(mock.Anything, mock.Anything).Return(nil)

	milvusCli.EXPECT().DeleteObject(mock.Anything, mock.Anything).Return(nil).Once()
	backupCli.EXPECT().DeleteObject(mock.Anything, mock.Anything).Return(nil).Once()
}

func TestCheckExecute(t *testing.T) {
	t.Run("ReportsVersionAndSuccess", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		backupCli := storage.NewMockClient(t)
		expectStoragesReadable(t, milvusCli, backupCli, []storage.ObjectAttr{
			{Key: milvusRoot + "/db1/1.parquet", Length: 10},
		})
		expectWriteAndCopy(t, milvusCli, backupCli)

		var out bytes.Buffer
		uc := newCheckUnderTest(grpc, milvusCli, backupCli)
		err := uc.Execute(context.Background(), &out)

		require.NoError(t, err)
		assert.Contains(t, out.String(), "Milvus version: 2.6.0")
		assert.Contains(t, out.String(), "Success!")
		assert.NotContains(t, out.String(), "!!!")
	})

	t.Run("WarnsWhenMilvusRootIsEmpty", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		backupCli := storage.NewMockClient(t)
		expectStoragesReadable(t, milvusCli, backupCli, nil)
		expectWriteAndCopy(t, milvusCli, backupCli)

		var out bytes.Buffer
		uc := newCheckUnderTest(grpc, milvusCli, backupCli)
		err := uc.Execute(context.Background(), &out)

		require.NoError(t, err)
		assert.Contains(t, out.String(), "!!! Milvus root path is empty !!!")
		assert.NotContains(t, out.String(), "Success!")
	})

	t.Run("FailsWhenMilvusUnreachable", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("", errors.New("connection refused"))

		// No storage expectations: an unexpected call would fail the mocks,
		// so nothing past the first check may run.
		uc := newCheckUnderTest(grpc, storage.NewMockClient(t), storage.NewMockClient(t))
		err := uc.Execute(context.Background(), &bytes.Buffer{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "check milvus connect")
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("FailsWhenMilvusRootUnreadable", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		milvusCli.EXPECT().
			ListPrefix(mock.Anything, milvusRoot+"/", true).
			Return(nil, errors.New("stat denied"))

		uc := newCheckUnderTest(grpc, milvusCli, storage.NewMockClient(t))
		err := uc.Execute(context.Background(), &bytes.Buffer{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "check milvus storage")
		assert.Contains(t, err.Error(), "stat denied")
	})

	t.Run("FailsWhenBackupRootUnreadable", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		milvusCli.EXPECT().
			ListPrefix(mock.Anything, milvusRoot+"/", true).
			Return(storage.NewMockObjectIterator(nil), nil)
		backupCli := storage.NewMockClient(t)
		backupCli.EXPECT().
			ListPrefix(mock.Anything, backupRoot+"/", false).
			Return(nil, errors.New("bucket denied")).
			Once()

		uc := newCheckUnderTest(grpc, milvusCli, backupCli)
		err := uc.Execute(context.Background(), &bytes.Buffer{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "check backup storage")
		assert.Contains(t, err.Error(), "bucket denied")
	})

	t.Run("FailsWhenWriteToMilvusStorageFails", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		backupCli := storage.NewMockClient(t)
		expectStoragesReadable(t, milvusCli, backupCli, nil)
		milvusCli.EXPECT().
			UploadObject(mock.Anything, mock.Anything).
			Return(errors.New("quota exceeded"))

		uc := newCheckUnderTest(grpc, milvusCli, backupCli)
		err := uc.Execute(context.Background(), &bytes.Buffer{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "write to milvus storage")
		assert.Contains(t, err.Error(), "quota exceeded")
	})

	t.Run("FailsWhenCopyListingFails", func(t *testing.T) {
		grpc := milvus.NewMockGrpc(t)
		grpc.EXPECT().GetVersion(mock.Anything).Return("2.6.0", nil)

		milvusCli := storage.NewMockClient(t)
		backupCli := storage.NewMockClient(t)
		expectStoragesReadable(t, milvusCli, backupCli, nil)
		milvusCli.EXPECT().UploadObject(mock.Anything, mock.Anything).Return(nil)
		milvusCli.EXPECT().Config().Return(storage.Config{}).Times(2)
		backupCli.EXPECT().Config().Return(storage.Config{}).Times(2)
		milvusCli.EXPECT().
			ListPrefix(mock.Anything, mock.Anything, true).
			Return(nil, errors.New("src list denied")).
			Once()
		// The written check object is still cleaned up when the copy fails.
		milvusCli.EXPECT().DeleteObject(mock.Anything, mock.Anything).Return(nil).Once()

		uc := newCheckUnderTest(grpc, milvusCli, backupCli)
		err := uc.Execute(context.Background(), &bytes.Buffer{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "copy from milvus storage to backup storage")
		assert.Contains(t, err.Error(), "src list denied")
		assert.False(t, strings.Contains(err.Error(), "verify copy"), "verify must not run after a failed copy")
	})
}
