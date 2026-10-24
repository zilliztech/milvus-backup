package replay_test

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/replay"
)

func TestBuildPlanSplitsFieldsAtGuessedDynamicID(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "field1", FieldID: common.StartOfUserFieldID},
			{Name: "field2", FieldID: common.StartOfUserFieldID + 1},
			{Name: "field3", FieldID: common.StartOfUserFieldID + 2},
			// $meta occupies ID 103.
			{Name: "field4", FieldID: common.StartOfUserFieldID + 4},
			{Name: "field5", FieldID: common.StartOfUserFieldID + 5},
		},
	}

	plan, err := replay.BuildPlan(schema, schema.GetFields())
	require.NoError(t, err)

	assert.Equal(t, []string{"field1", "field2", "field3"}, fieldNames(plan.CreateFields))
	assert.Equal(t, []string{"field4", "field5"}, fieldNames(plan.AddFields))
	assert.True(t, plan.EnableDynamicField)
}

func TestBuildPlanWithoutDynamicField(t *testing.T) {
	schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
		{Name: "field1", FieldID: common.StartOfUserFieldID},
		{Name: "field2", FieldID: common.StartOfUserFieldID + 1},
	}}

	plan, err := replay.BuildPlan(schema, schema.GetFields())
	require.NoError(t, err)

	assert.Equal(t, []string{"field1", "field2"}, fieldNames(plan.CreateFields))
	assert.Empty(t, plan.AddFields)
	assert.False(t, plan.EnableDynamicField)
}

func TestCheckAcceptsContiguousNonDynamicSchema(t *testing.T) {
	schema := &backuppb.CollectionSchema{Fields: []*backuppb.FieldSchema{
		{Name: "pk", FieldID: common.StartOfUserFieldID},
		{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
	}}

	assert.NoError(t, replay.Check(schema))
}

func TestCheckAcceptsDynamicSchemaWithStruct(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: common.StartOfUserFieldID},
			{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
			{Name: "$meta", FieldID: common.StartOfUserFieldID + 2, IsDynamic: true},
		},
		StructArrayFields: []*backuppb.StructArrayFieldSchema{{
			Name:    "struct",
			FieldID: common.StartOfUserFieldID + 3,
			Fields: []*backuppb.FieldSchema{{
				Name:    "sub",
				FieldID: common.StartOfUserFieldID + 4,
			}},
		}},
	}

	assert.NoError(t, replay.Check(schema))
}

func TestCheckAcceptsAddFieldWithoutHighWaterProperty(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: common.StartOfUserFieldID},
			{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
			{Name: "$meta", FieldID: common.StartOfUserFieldID + 2, IsDynamic: true},
			{Name: "added", FieldID: common.StartOfUserFieldID + 3},
		},
	}

	assert.NoError(t, replay.Check(schema))
}

func TestCheckRejectsAddFieldShiftedByHighWaterProperty(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: common.StartOfUserFieldID},
			{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
			{Name: "$meta", FieldID: common.StartOfUserFieldID + 2, IsDynamic: true},
			{Name: "added", FieldID: common.StartOfUserFieldID + 3},
		},
		Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "103"}},
	}

	err := replay.Check(schema)
	require.Error(t, err)
	assert.ErrorContains(t, err, `field "added" changes id from 103 to 104`)
}

func TestCheckInfersSingleHiddenDynamicFieldFromHighWater(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: common.StartOfUserFieldID},
			{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
		},
		Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "102"}},
	}

	assert.NoError(t, replay.Check(schema))
}

func TestCheckInfersLegacyHiddenDynamicField(t *testing.T) {
	schema := &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*backuppb.FieldSchema{
			{Name: "pk", FieldID: common.StartOfUserFieldID},
			{Name: "vec", FieldID: common.StartOfUserFieldID + 1},
			{Name: "added", FieldID: common.StartOfUserFieldID + 3},
		},
	}

	assert.NoError(t, replay.Check(schema))
}

func TestCheckDropAndAddHistories(t *testing.T) {
	tests := []struct {
		name      string
		schema    *backuppb.CollectionSchema
		wantError string
	}{
		{
			name: "DroppedMiddleField",
			schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "survivor", FieldID: 102},
				},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "102"}},
			},
			wantError: `field "survivor" changes id from 102 to 101`,
		},
		{
			name: "NonDynamicTrailingDrop",
			schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
				},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "102"}},
			},
		},
		{
			name: "DynamicTrailingDropWithVisibleMeta",
			schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
					{Name: "$meta", FieldID: 102, IsDynamic: true},
				},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "103"}},
			},
		},
		{
			name: "DynamicDropBeforeVisibleMeta",
			schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
					{Name: "$meta", FieldID: 103, IsDynamic: true},
				},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "103"}},
			},
			wantError: `field "$meta" changes id from 103 to 102`,
		},
		{
			name: "AmbiguousHiddenMetaAfterDrop",
			schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
				},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "103"}},
			},
			wantError: "cannot determine hidden $meta id",
		},
		{
			name: "PlainFieldAddedAfterStruct",
			schema: &backuppb.CollectionSchema{
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
					{Name: "added", FieldID: 104},
				},
				StructArrayFields: []*backuppb.StructArrayFieldSchema{{
					Name: "struct", FieldID: 102,
					Fields: []*backuppb.FieldSchema{{Name: "sub", FieldID: 103}},
				}},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "104"}},
			},
			wantError: `field "added" changes id from 104 to 102`,
		},
		{
			name: "DynamicEnabledAfterStruct",
			schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*backuppb.FieldSchema{
					{Name: "pk", FieldID: 100},
					{Name: "vec", FieldID: 101},
					{Name: "$meta", FieldID: 104, IsDynamic: true},
				},
				StructArrayFields: []*backuppb.StructArrayFieldSchema{{
					Name: "struct", FieldID: 102,
					Fields: []*backuppb.FieldSchema{{Name: "sub", FieldID: 103}},
				}},
				Properties: []*backuppb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "104"}},
			},
			wantError: `field "$meta" changes id from 104 to 102`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := replay.Check(tt.schema)
			if tt.wantError == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantError)
		})
	}
}

func fieldNames(fields []*backuppb.FieldSchema) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.GetName())
	}
	return names
}
