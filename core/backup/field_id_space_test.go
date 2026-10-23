package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func bakField(id int64, dynamic bool) *backuppb.FieldSchema {
	return &backuppb.FieldSchema{FieldID: id, IsDynamic: dynamic}
}

func bakFields(ids ...int64) []*backuppb.FieldSchema {
	fields := make([]*backuppb.FieldSchema, 0, len(ids))
	for _, id := range ids {
		fields = append(fields, bakField(id, false))
	}
	return fields
}

func TestCheckFieldIDSpaceFromMeta(t *testing.T) {
	t.Run("ContiguousBinlogSchema", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: bakFields(100, 101, 102)}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	t.Run("DroppedMiddleField", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: bakFields(100, 102)}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "101")
	})

	t.Run("IDSpaceStartsAboveFloor", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: bakFields(102, 103)}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "100-101")
	})

	// A field dropped last leaves no trace in the schema and no hole below
	// the max ID, so the replay still lands on every surviving ID.
	t.Run("TrailingDropIsInvisible", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: bakFields(100, 101)}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	t.Run("StructFieldIDsShareTheSpace", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields: bakFields(100),
			StructArrayFields: []*backuppb.StructArrayFieldSchema{{
				FieldID: 101,
				Fields:  bakFields(102),
			}},
		}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	t.Run("StructFieldHole", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields: bakFields(100),
			StructArrayFields: []*backuppb.StructArrayFieldSchema{{
				FieldID: 103,
				Fields:  bakFields(104),
			}},
		}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "101-102")
	})

	// Without the real $meta field, one narrow non-leading hole may be the
	// hidden $meta slot, which restore's guess relies on, so it stays allowed.
	t.Run("DynamicHiddenMetaContiguous", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: bakFields(100, 101, 102)}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	t.Run("DynamicHiddenMetaSingleNarrowHole", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: bakFields(100, 102)}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	// $meta is always appended after the fields that exist when it is created,
	// so it can never be the first user ID: a leading hole proves a drop.
	t.Run("DynamicHiddenMetaLeadingHole", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: bakFields(101)}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "100")
	})

	t.Run("DynamicHiddenMetaTwoHoles", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: bakFields(100, 102, 104)}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "101")
		assert.Contains(t, err.Error(), "103")
	})

	t.Run("DynamicHiddenMetaWideHole", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: bakFields(100, 103)}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "101-102")
	})

	// With the real $meta field recorded, the whole space is known and even a
	// single narrow hole is fatal — the drop the hidden-slot ambiguity hid.
	t.Run("DynamicRealMetaContiguous", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{bakField(100, false), bakField(101, true), bakField(102, false)},
		}
		assert.NoError(t, checkFieldIDSpaceFromMeta("db.coll", schema))
	})

	t.Run("DynamicRealMetaHoleBelowMeta", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{bakField(100, false), bakField(102, false), bakField(103, true)},
		}
		err := checkFieldIDSpaceFromMeta("db.coll", schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "101")
	})
}
