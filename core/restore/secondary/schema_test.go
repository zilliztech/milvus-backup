package secondary

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func structEvolvedSchema(t *testing.T) *backuppb.CollectionSchema {
	t.Helper()
	nested, err := proto.Marshal(&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
		Kind:     &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}},
		Nullable: true,
	}}, Nullable: true})
	require.NoError(t, err)
	return &backuppb.CollectionSchema{
		Name: "test_coll",
		Fields: []*backuppb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: backuppb.DataType_Int64},
			{FieldID: 101, Name: "labels", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
				Nullable: true, ElementNullable: true, TypeSchema: nested},
		},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Fields: []*backuppb.FieldSchema{
			{FieldID: 105, Name: "value", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
				ElementNullable: true, Nullable: true, TypeSchema: nested},
		}}},
	}
}

// A secondary restore bypasses the proxy, so its WAL messages must already
// carry the struct attributes the primary path gets from the server-side
// qualification: element_nullable and the recursive type_schema.
func TestSecondaryMessagesCarryStructSchema(t *testing.T) {
	nested, err := proto.Marshal(&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
		Kind:     &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}},
		Nullable: true,
	}}, Nullable: true})
	require.NoError(t, err)

	for _, name := range []string{"CollectionDDL", "BinlogImport", "SnapshotImport"} {
		t.Run(name, func(t *testing.T) {
			stream := &recordingStream{}
			coll := newTestCollBackup(newTestVChannels(), []*backuppb.PartitionBackupInfo{{PartitionId: 10, PartitionName: "_default"}}, nil)
			coll.Schema = structEvolvedSchema(t)

			switch name {
			case "CollectionDDL":
				ddlt := newTestDDLTask(coll, stream, zap.NewNop())
				require.NoError(t, ddlt.createColl(context.Background()))
			case "BinlogImport":
				_, err := newTestBinlogDMLTask(t, coll, stream).sendImportMsg(context.Background(), 10, batch{
					timestamp: 100, storageVersion: 2, partitionDirs: []partitionDir{{insertLogDir: "/insert"}},
				})
				require.NoError(t, err)
			case "SnapshotImport":
				_, err := newTestSnapDMLTask(t, coll, stream).sendImportMsg(context.Background())
				require.NoError(t, err)
			}

			var schemas []*schemapb.CollectionSchema
			if name == "CollectionDDL" {
				for _, msg := range stream.messagesByType(strconv.Itoa(int(message.MessageTypeCreateCollection))) {
					req := &msgpb.CreateCollectionRequest{}
					require.NoError(t, proto.Unmarshal(msg.GetPayload(), req))
					schemas = append(schemas, req.GetCollectionSchema())
				}
			} else {
				for _, body := range importBodies(t, stream) {
					schemas = append(schemas, body.GetSchema())
				}
			}
			require.NotEmpty(t, schemas)

			for _, schema := range schemas {
				var labels *schemapb.FieldSchema
				for _, field := range schema.GetFields() {
					if field.GetName() == "labels" {
						labels = field
						break
					}
				}
				require.NotNil(t, labels, name)
				assert.True(t, labels.GetElementNullable(), name)
				require.NotNil(t, labels.GetTypeSchema(), name)
				got, err := proto.Marshal(labels.GetTypeSchema())
				require.NoError(t, err)
				assert.Equal(t, nested, got, name)

				structure := schema.GetStructArrayFields()[0]
				require.Len(t, structure.GetFields(), 1)
				sub := structure.GetFields()[0]
				assert.Equal(t, "items[value]", sub.GetName(), "secondary messages pre-qualify sub-field names")
				assert.True(t, sub.GetElementNullable(), name)
				require.NotNil(t, sub.GetTypeSchema(), name)
			}
		})
	}
}
