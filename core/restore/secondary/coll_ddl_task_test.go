package secondary

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func newDDLTestCollBackup(version int32) *backuppb.CollectionBackupInfo {
	return &backuppb.CollectionBackupInfo{
		CollectionId:   123,
		DbName:         "default",
		CollectionName: "test_coll",
		Schema: &backuppb.CollectionSchema{
			Name:    "test_coll",
			Version: version,
			Fields: []*backuppb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
			},
		},
		VirtualChannelNames: []string{"vch-1"},
	}
}

func newTestDDLTask(collBackup *backuppb.CollectionBackupInfo, stream *recordingStream, logger *zap.Logger) *collDDLTask {
	return &collDDLTask{
		taskID:     "task-1",
		backupInfo: &backuppb.BackupInfo{ControlChannelName: "control-ch"},
		dbBackup:   &backuppb.DatabaseBackupInfo{DbId: 1},
		collBackup: collBackup,
		streamCli:  stream,
		logger:     logger,
	}
}

func TestCreateColl_CarriesSchemaVersionInMessageBody(t *testing.T) {
	collBackup := newDDLTestCollBackup(7)
	stream := &recordingStream{}
	task := newTestDDLTask(collBackup, stream, zap.NewNop())

	err := task.createColl(context.Background())
	require.NoError(t, err)

	createMessages := stream.messagesByType(strconv.Itoa(int(message.MessageTypeCreateCollection)))
	require.NotEmpty(t, createMessages)
	for _, msg := range createMessages {
		req := &msgpb.CreateCollectionRequest{}
		require.NoError(t, proto.Unmarshal(msg.GetPayload(), req))
		assert.Equal(t, int32(7), req.GetCollectionSchema().GetVersion())
	}
}

func TestCreateColl_WarnsWhenSchemaVersionAboveZero(t *testing.T) {
	observedCore, observedLogs := observer.New(zapcore.WarnLevel)
	collBackup := newDDLTestCollBackup(7)
	stream := &recordingStream{}
	task := newTestDDLTask(collBackup, stream, zap.New(observedCore))

	err := task.createColl(context.Background())
	require.NoError(t, err)

	warns := observedLogs.FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0].Message, "SCHEMA_VERSION_MISMATCH")
	assert.Equal(t, int32(7), warns[0].ContextMap()["source_schema_version"])
}

func TestCreateColl_NoWarnWhenSchemaVersionZero(t *testing.T) {
	observedCore, observedLogs := observer.New(zapcore.WarnLevel)
	collBackup := newDDLTestCollBackup(0)
	stream := &recordingStream{}
	task := newTestDDLTask(collBackup, stream, zap.New(observedCore))

	err := task.createColl(context.Background())
	require.NoError(t, err)

	assert.Empty(t, observedLogs.FilterLevelExact(zapcore.WarnLevel).All())
}
