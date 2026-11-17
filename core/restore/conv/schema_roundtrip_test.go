package conv

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestTypeSchemaMetadataRoundTrip(t *testing.T) {
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
	inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}}
	nested := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}, Nullable: true}
	data, err := proto.Marshal(nested)
	require.NoError(t, err)
	field := &backuppb.FieldSchema{FieldID: 109, Name: "nested", DataType: backuppb.DataType_Array, ElementType: backuppb.DataType_Array,
		Nullable: true, ElementNullable: true, TypeSchema: data}

	// The metadata JSON encodes the bytes as base64; a stored backup must come
	// back with the same recursive type.
	encoded, err := json.Marshal(field)
	require.NoError(t, err)
	var stored backuppb.FieldSchema
	require.NoError(t, json.Unmarshal(encoded, &stored))

	fields, err := Fields([]*backuppb.FieldSchema{&stored})
	require.NoError(t, err)
	assert.True(t, proto.Equal(nested, fields[0].GetTypeSchema()))
	assert.True(t, fields[0].GetNullable())
	assert.True(t, fields[0].GetElementNullable())
	assert.Equal(t, data, stored.GetTypeSchema(), "conversion must not consume the metadata bytes")
}

func TestTypeSchemaErrors(t *testing.T) {
	_, err := Fields([]*backuppb.FieldSchema{{Name: "labels", FieldID: 105, TypeSchema: []byte{0xff}}})
	assert.ErrorContains(t, err, "type_schema")
	assert.ErrorContains(t, err, "labels")
	assert.ErrorContains(t, err, "105")
}
