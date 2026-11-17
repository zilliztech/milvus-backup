package restore

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func TestCreateCollectionInputCarriesStructSchema(t *testing.T) {
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
	inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "skip_type", Value: "yes"}}}
	nested, err := proto.Marshal(&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}, Nullable: true})
	require.NoError(t, err)

	ct := newTestCollDDLTask()
	ct.target = collref.New("db1", "coll1")
	ct.collBackup = &backuppb.CollectionBackupInfo{
		Schema: &backuppb.CollectionSchema{
			Name: "source",
			Fields: []*backuppb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: backuppb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "labels", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
					Nullable: true, ElementNullable: true, TypeSchema: nested,
					TypeParams: []*backuppb.KeyValuePair{{Key: "skip_type", Value: "yes"}}},
			},
			StructArrayFields: []*backuppb.StructArrayFieldSchema{{FieldID: 103, Name: "items", Fields: []*backuppb.FieldSchema{
				{FieldID: 105, Name: "value", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
					ElementNullable: true, Nullable: true, TypeSchema: nested},
			}}},
		},
	}
	ct.option.SkipParams = SkipParams{FieldTypeParams: []string{"skip_type"}, FieldIndexParams: []string{"skip_index"}}

	input, addFields, err := ct.createCollectionInput()
	require.NoError(t, err)
	assert.Empty(t, addFields)

	require.Len(t, input.Schema.GetFields(), 2)
	labels := input.Schema.GetFields()[1]
	assert.True(t, labels.GetElementNullable())
	assert.Empty(t, labels.GetTypeParams(), "the skip list applies to the field's own type params")
	// The skip list also reaches the type params nested inside the recursive type schema.
	outer := labels.GetTypeSchema()
	require.NotNil(t, outer)
	assert.True(t, outer.GetNullable())
	nestedLevel := outer.GetArrayElement()
	require.NotNil(t, nestedLevel)
	assert.Empty(t, nestedLevel.GetTypeParams())
	assert.Equal(t, schemapb.DataType_Int64, nestedLevel.GetArrayElement().GetLeafType())

	structure := input.Schema.GetStructArrayFields()[0]
	require.Len(t, structure.GetFields(), 1)
	sub := structure.GetFields()[0]
	// The proxy qualifies bare sub-field names on the create path, so the
	// primary request keeps them bare.
	assert.Equal(t, "value", sub.GetName())
	assert.True(t, sub.GetElementNullable())
	require.NotNil(t, sub.GetTypeSchema())
	want, err := proto.Marshal(&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
		Kind:     &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}},
		Nullable: true,
	}}, Nullable: true})
	require.NoError(t, err)
	got, err := proto.Marshal(sub.GetTypeSchema())
	require.NoError(t, err)
	assert.Equal(t, want, got, "the sub-field's nested type schema must arrive intact after skip filtering")
}

// A requested drop must not run while backup metadata is still undecoded: the
// target has to survive a restore that cannot even read its own input.
func TestCreateInputFailsBeforeDrop(t *testing.T) {
	ct := newTestCollDDLTask()
	ct.option.DropExistCollection = true
	ct.target = collref.New("db1", "coll1")
	ct.collBackup = &backuppb.CollectionBackupInfo{
		Schema: &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: backuppb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "labels", DataType: backuppb.DataType_Array, TypeSchema: []byte{0xff}},
			},
		},
	}

	cli := milvus.NewMockGrpc(t) // no expectations: any gRPC call fails the test
	ct.grpcCli = cli

	err := ct.execute(context.Background())
	assert.ErrorContains(t, err, "type_schema")
}
