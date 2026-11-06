package secondary

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func secondaryEvolvedSchema(t *testing.T) *backuppb.CollectionSchema {
	t.Helper()
	typeSchema, err := proto.Marshal(&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}}})
	require.NoError(t, err)
	return &backuppb.CollectionSchema{Name: "test_coll", Version: 3, EnableDynamicField: true, EnableNamespace: true,
		Fields: []*backuppb.FieldSchema{
			{FieldID: 100, Name: "id"},
			{FieldID: 102, Name: "$meta", IsDynamic: true, DataType: backuppb.DataType_JSON},
			{FieldID: 103, Name: "$namespace_id", DataType: backuppb.DataType_VarChar},
		},
		Properties: []*backuppb.KeyValuePair{{Key: "max_field_id", Value: "111"}},
		Functions:  []*backuppb.FunctionSchema{{Id: 100, Name: "function", InputFieldIds: []int64{100}, InputFieldNames: []string{"id"}}},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{FieldID: 104, Name: "items", Fields: []*backuppb.FieldSchema{
			{FieldID: 109, Name: "value", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array, ElementNullable: true, TypeSchema: typeSchema},
		}}}}
}

func assertSecondarySchema(t *testing.T, schema *schemapb.CollectionSchema) {
	t.Helper()
	assert.True(t, schema.GetEnableNamespace())
	assert.True(t, schema.GetEnableDynamicField())
	assert.Equal(t, int32(3), schema.GetVersion())
	assert.Equal(t, int64(102), schema.GetFields()[1].GetFieldID())
	assert.Equal(t, int64(103), schema.GetFields()[2].GetFieldID())
	assert.Equal(t, int64(100), schema.GetFunctions()[0].GetId())
	structure := schema.GetStructArrayFields()[0]
	assert.Equal(t, int64(104), structure.GetFieldID())
	assert.Equal(t, int64(109), structure.GetFields()[0].GetFieldID())
	assert.Equal(t, "items[value]", structure.GetFields()[0].GetName())
	assert.True(t, structure.GetFields()[0].GetElementNullable())
	assert.NotNil(t, structure.GetFields()[0].GetTypeSchema())
	assert.Equal(t, "111", pbconv.MilvusKVToMap(schema.GetProperties())["max_field_id"])
	assert.NotContains(t, pbconv.MilvusKVToMap(schema.GetProperties()), "preserve_field_ids", "WAL already carries final IDs")
}

func TestSecondaryWALPreservesNewSchema(t *testing.T) {
	stream := &recordingStream{}
	coll := newTestCollBackup(newTestVChannels(), nil, nil)
	coll.Schema = secondaryEvolvedSchema(t)
	ddlt := &collDDLTask{collBackup: coll, dbBackup: &backuppb.DatabaseBackupInfo{DbId: 1},
		backupInfo: &backuppb.BackupInfo{ControlChannelName: "control"}, streamCli: stream, logger: zap.NewNop()}
	require.NoError(t, ddlt.createColl(context.Background()))
	msgs := stream.messagesByType(strconv.Itoa(int(message.MessageTypeCreateCollection)))
	require.NotEmpty(t, msgs)
	for _, msg := range msgs {
		var body message.CreateCollectionRequest
		require.NoError(t, proto.Unmarshal(msg.GetPayload(), &body))
		assertSecondarySchema(t, body.GetCollectionSchema())
	}
}

func TestSecondarySnapshotImportPreservesNewSchema(t *testing.T) {
	stream := &recordingStream{}
	coll := newTestCollBackup(newTestVChannels(), []*backuppb.PartitionBackupInfo{{PartitionId: 10, PartitionName: "_default"}}, nil)
	coll.Schema = secondaryEvolvedSchema(t)
	task := newTestSnapDMLTask(t, coll, stream)
	_, err := task.sendImportMsg(context.Background())
	require.NoError(t, err)
	bodies := importBodies(t, stream)
	require.NotEmpty(t, bodies)
	for _, body := range bodies {
		assertSecondarySchema(t, body.GetSchema())
	}
}

func TestSecondaryRejectsNewShapesOnOldTarget(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "OldTarget"
		if supported {
			name = "SupportingTarget"
		}
		t.Run(name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			cli.EXPECT().HasFeature(milvus.PreserveFieldIDs).Return(supported)
			coll := &backuppb.CollectionBackupInfo{CollectionName: "test_coll", Schema: secondaryEvolvedSchema(t)}
			err := taskWithBackup(cli, coll).checkSchemaSupport()
			if supported {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, "element_nullable")
			}
		})
	}
}

func TestSecondaryRequiresNamespaceMetadata(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "namespaced", EnableNamespace: true}
	assert.ErrorContains(t, checkDynamicField(schema), "$namespace_id")
	schema.Properties = []*commonpb.KeyValuePair{{Key: "namespace.mode", Value: "partition"}}
	assert.NoError(t, checkDynamicField(schema))
}

func TestSecondaryMessagesUseResponseProperties(t *testing.T) {
	for _, name := range []string{"CollectionDDL", "BinlogImport", "SnapshotImport"} {
		t.Run(name, func(t *testing.T) {
			stream := &recordingStream{}
			coll := newTestCollBackup(newTestVChannels(), []*backuppb.PartitionBackupInfo{{PartitionId: 10, PartitionName: "_default"}}, nil)
			coll.Schema = secondaryEvolvedSchema(t)
			coll.Properties = []*backuppb.KeyValuePair{{Key: "max_field_id", Value: "200"}, {Key: "response.only", Value: "carry"}}
			switch name {
			case "CollectionDDL":
				ddlt := &collDDLTask{collBackup: coll, dbBackup: &backuppb.DatabaseBackupInfo{DbId: 1},
					backupInfo: &backuppb.BackupInfo{ControlChannelName: "control"}, streamCli: stream, logger: zap.NewNop()}
				require.NoError(t, ddlt.createColl(context.Background()))
			case "SnapshotImport":
				_, err := newTestSnapDMLTask(t, coll, stream).sendImportMsg(context.Background())
				require.NoError(t, err)
			case "BinlogImport":
				_, err := newTestBinlogDMLTask(t, coll, stream).sendImportMsg(context.Background(), 10, batch{
					timestamp: 100, storageVersion: 2, partitionDirs: []partitionDir{{insertLogDir: "/insert"}},
				})
				require.NoError(t, err)
			}
			var schemas []*schemapb.CollectionSchema
			if name == "CollectionDDL" {
				for _, msg := range stream.messagesByType(strconv.Itoa(int(message.MessageTypeCreateCollection))) {
					var body message.CreateCollectionRequest
					require.NoError(t, proto.Unmarshal(msg.GetPayload(), &body))
					schemas = append(schemas, body.GetCollectionSchema())
				}
			} else {
				for _, body := range importBodies(t, stream) {
					schemas = append(schemas, body.GetSchema())
				}
			}
			require.NotEmpty(t, schemas)
			for _, schema := range schemas {
				assert.Equal(t, "200", pbconv.MilvusKVToMap(schema.GetProperties())["max_field_id"])
				assert.Equal(t, "carry", pbconv.MilvusKVToMap(schema.GetProperties())["response.only"])
			}
			assert.Equal(t, "111", pbconv.MilvusKVToMap(pbconv.BakKVToMilvusKV(coll.GetSchema().GetProperties()))["max_field_id"], "conversion must not mutate metadata")
		})
	}
}

func TestSecondaryPreflightUsesResponseNamespaceMode(t *testing.T) {
	coll := &backuppb.CollectionBackupInfo{CollectionName: "namespaced", Schema: &backuppb.CollectionSchema{Name: "namespaced", EnableNamespace: true,
		Fields: []*backuppb.FieldSchema{{FieldID: 100, Name: "id"}}},
		Properties: []*backuppb.KeyValuePair{{Key: "namespace.mode", Value: "partition"}}}
	assert.NoError(t, taskWithBackup(milvus.NewMockGrpc(t), coll).checkSchemaSupport())
}
