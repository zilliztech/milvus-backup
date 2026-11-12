package fieldid

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func userField(id int64, name string) *backuppb.FieldSchema {
	return &backuppb.FieldSchema{FieldID: id, Name: name, DataType: backuppb.DataType_Int64}
}

func dynamicField(id int64) *backuppb.FieldSchema {
	return &backuppb.FieldSchema{FieldID: id, Name: common.MetaFieldName, IsDynamic: true, DataType: backuppb.DataType_JSON}
}

func structField(id int64, name string, subs ...*backuppb.FieldSchema) *backuppb.StructArrayFieldSchema {
	return &backuppb.StructArrayFieldSchema{FieldID: id, Name: name, Fields: subs}
}

func maxFieldIDProperty(id int64) *backuppb.KeyValuePair {
	return &backuppb.KeyValuePair{Key: common.MaxFieldIDKey, Value: strconv.FormatInt(id, 10)}
}

func TestCheckLegacy(t *testing.T) {
	t.Run("UnchangedSchema", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a")}}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("AppendedField", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), userField(103, "b")}}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DroppedTrailingField", func(t *testing.T) {
		// c was 103 and is gone; the remaining ids are still contiguous.
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a")}}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DroppedMiddleField", func(t *testing.T) {
		// b was 103 and is gone: every field after it would slide down one id.
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), userField(104, "c")}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "c" would get id 103 instead of the source id 104`)
	})

	t.Run("DroppedTrailingThenAdded", func(t *testing.T) {
		// c(103) dropped, d added at 104: the target would give d id 103.
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), userField(104, "d")}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "d" would get id 103`)
	})

	t.Run("DynamicAppendedField", func(t *testing.T) {
		// $meta at 103, b added after it at 104: the split guesses 103 and the
		// target reproduces both.
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), dynamicField(103), userField(104, "b")},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DynamicAppendedFieldWithHighWater", func(t *testing.T) {
		// Where AddField continues from depends on the target's generation (a
		// 3.0 target raises its high-water mark with max_field_id, 2.6 does
		// not), so the pre-create model leaves added fields to Verify.
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(102, "a"), dynamicField(103), userField(104, "b")},
			Properties:         []*backuppb.KeyValuePair{maxFieldIDProperty(104)},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DynamicWithoutRecordedMeta", func(t *testing.T) {
		// No $meta and no high-water mark: the guest id 102 is the only hole,
		// and the target reproduces it.
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(103, "a"), userField(104, "b")},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DynamicWithoutRecordedMetaWithHighWater", func(t *testing.T) {
		// The $meta id is reconstructed from the high-water mark; where AddField
		// continues from is left to Verify.
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(103, "a"), userField(104, "b")},
			Properties:         []*backuppb.KeyValuePair{maxFieldIDProperty(104)},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("StructUnchanged", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields:            []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec")},
			StructArrayFields: []*backuppb.StructArrayFieldSchema{structField(102, "s", userField(103, "a"), userField(104, "b"))},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("StructDroppedSubField", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields:            []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec")},
			StructArrayFields: []*backuppb.StructArrayFieldSchema{structField(102, "s", userField(103, "a"), userField(105, "c"))},
		}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "s[c]" would get id 104 instead of the source id 105`)
	})

	t.Run("DynamicAndStruct", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), dynamicField(102)},
			StructArrayFields:  []*backuppb.StructArrayFieldSchema{structField(103, "s", userField(104, "a"))},
		}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("DynamicFieldWithoutDynamicEnabled", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), dynamicField(101)}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, `field "$meta" (source id 101) is absent from the create plan`)
	})

	t.Run("HiddenMetaAmbiguous", func(t *testing.T) {
		// Two holes below the high-water mark: the $meta id cannot be placed,
		// so neither the source nor the target layout is known.
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), userField(104, "a")},
			Properties:         []*backuppb.KeyValuePair{maxFieldIDProperty(104)},
		}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "cannot place the hidden $meta id")
	})

	t.Run("DuplicateSourceID", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "a"), userField(101, "b")}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "share source field id 101")
	})

	t.Run("FieldOutsideUserRange", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(50, "a")}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "outside the user field id range")
	})

	t.Run("SystemFieldsIgnored", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
			userField(100, "id"),
			{FieldID: common.RowIDField, Name: common.RowIDFieldName},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName},
		}}
		assert.NoError(t, CheckLegacy(schema))
	})

	t.Run("UnparsableMaxFieldID", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields:     []*backuppb.FieldSchema{userField(100, "id")},
			Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "abc"}},
		}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "parse max_field_id value")
	})

	t.Run("MaxFieldIDBelowUserRange", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			Fields:     []*backuppb.FieldSchema{userField(100, "id")},
			Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "99"}},
		}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "below the first user field id")
	})

	t.Run("DynamicWithoutNonDynamicFields", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{EnableDynamicField: true, Fields: []*backuppb.FieldSchema{dynamicField(100)}}
		err := CheckLegacy(schema)
		require.Error(t, err)
		assert.ErrorContains(t, err, "dynamic collection has no non-dynamic fields")
	})
}

func TestBuildPlan(t *testing.T) {
	t.Run("DynamicSplitsAtGuessedID", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{
			EnableDynamicField: true,
			Fields:             []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec"), dynamicField(102), userField(103, "a")},
		}
		plan, err := BuildPlan(schema)
		require.NoError(t, err)
		assert.Equal(t, int64(102), plan.DynamicFieldID)
		assert.Equal(t, []string{"id", "vec"}, names(plan.Create))
		assert.Equal(t, []string{"a"}, names(plan.Add))
	})

	t.Run("StaticKeepsEveryField", func(t *testing.T) {
		schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{userField(100, "id"), userField(101, "vec")}}
		plan, err := BuildPlan(schema)
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "vec"}, names(plan.Create))
		assert.Empty(t, plan.Add)
	})
}

func names(fields []*backuppb.FieldSchema) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.GetName())
	}
	return names
}
