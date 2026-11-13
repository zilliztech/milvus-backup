package fieldid

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

func targetField(id int64, name string) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{FieldID: id, Name: name, DataType: schemapb.DataType_Int64}
}

func targetStructField(id int64, name string, subs ...*schemapb.FieldSchema) *schemapb.StructArrayFieldSchema {
	return &schemapb.StructArrayFieldSchema{FieldID: id, Name: name, Fields: subs}
}

func expectDescribe(t *testing.T, schema *schemapb.CollectionSchema) *milvus.MockGrpc {
	t.Helper()

	cli := milvus.NewMockGrpc(t)
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{Schema: schema}, nil).Once()
	return cli
}

func TestVerify(t *testing.T) {
	t.Run("MatchingIDs", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec"), targetField(102, "a")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a")},
		})
		assert.NoError(t, err)
	})

	t.Run("MismatchedID", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec"), targetField(102, "a"), targetField(103, "c")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), userField(104, "c")},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "c" is id 103 on the target but id 104 in the backup`)
	})

	t.Run("MissingVisibleField", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a")},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "a" is missing from the target`)
	})

	t.Run("HiddenMetaOmittedByDescribe", func(t *testing.T) {
		// Public DescribeCollection filters the dynamic field out; its absence
		// is not an id mismatch.
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), dynamicField(102)},
		})
		assert.NoError(t, err)
	})

	t.Run("HiddenNamespaceOmittedByDescribe", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, common.NamespaceFieldName)},
		})
		assert.NoError(t, err)
	})

	t.Run("StructSubFieldMismatch", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{
			Fields:            []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "vec")},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{targetStructField(102, "s", targetField(103, "a"), targetField(104, "c"))},
		})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields:            []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec")},
			StructArrayFields: []*backuppb.StructArrayFieldSchema{structField(102, "s", userField(103, "a"), userField(105, "c"))},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "s[c]" is id 104 on the target but id 105 in the backup`)
	})

	t.Run("QualifiedStructSubFieldName", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{
			Fields:            []*schemapb.FieldSchema{targetField(100, "id")},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{targetStructField(101, "s", targetField(102, "s[a]"))},
		})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields:            []*backuppb.FieldSchema{userField(100, "id")},
			StructArrayFields: []*backuppb.StructArrayFieldSchema{structField(101, "s", userField(102, "a"))},
		})
		assert.NoError(t, err)
	})

	t.Run("SystemFieldsIgnored", func(t *testing.T) {
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			targetField(100, "id"),
			{FieldID: common.RowIDField, Name: common.RowIDFieldName},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName},
		}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id")},
		})
		assert.NoError(t, err)
	})

	t.Run("ExtraTargetFieldAccepted", func(t *testing.T) {
		// A collection created outside restore may carry an extra field; no
		// source column is stored under another field's id, so it is not a
		// mismatch.
		cli := expectDescribe(t, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{targetField(100, "id"), targetField(101, "extra")}})
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id")},
		})
		assert.NoError(t, err)
	})

	t.Run("DescribeError", func(t *testing.T) {
		cli := milvus.NewMockGrpc(t)
		cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(nil, assert.AnError).Once()
		err := Verify(context.Background(), cli, collref.New("db", "coll"), &backuppb.CollectionSchema{
			Fields: []*backuppb.FieldSchema{userField(100, "id")},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "describe target collection")
	})
}
