// Package replay builds restore's schema plan without executing it and checks
// whether Milvus's field ID assignment reproduces a backup's physical schema.
package replay

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/funcs"
)

// Plan is the field-ordering part of a collection restore. Restore converts and
// executes it; backup simulates Milvus's ID assignment over the same plan.
type Plan[T funcs.FieldSchema] struct {
	CreateFields       []T
	AddFields          []T
	StructArrayFields  []*backuppb.StructArrayFieldSchema
	EnableDynamicField bool
	DynamicFieldID     int64
	MaxFieldID         *backuppb.KeyValuePair
}

// BuildPlan splits fields exactly as restore does. In particular, a recorded
// dynamic field belongs to the source schema, not to the create plan:
// CreateCollection owns the target's dynamic field and AddField rejects it.
func BuildPlan[T funcs.FieldSchema](schema *backuppb.CollectionSchema, allFields []T) (*Plan[T], error) {
	fields := make([]T, 0, len(allFields))
	for _, field := range allFields {
		if field.GetIsDynamic() {
			continue
		}
		fields = append(fields, field)
	}

	plan := &Plan[T]{
		StructArrayFields:  schema.GetStructArrayFields(),
		EnableDynamicField: schema.GetEnableDynamicField(),
		MaxFieldID:         maxFieldIDProperty(schema.GetProperties()),
	}
	if !schema.GetEnableDynamicField() {
		plan.CreateFields = fields
		return plan, nil
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("replay: dynamic collection has no non-dynamic fields")
	}

	dynFieldID := funcs.GuessDynFieldID(fields)
	plan.DynamicFieldID = dynFieldID
	for _, field := range fields {
		if field.GetFieldID() < dynFieldID {
			plan.CreateFields = append(plan.CreateFields, field)
		} else {
			plan.AddFields = append(plan.AddFields, field)
		}
	}

	return plan, nil
}

func maxFieldIDProperty(properties []*backuppb.KeyValuePair) *backuppb.KeyValuePair {
	for _, property := range properties {
		if property.GetKey() == common.MaxFieldIDKey {
			return property
		}
	}
	return nil
}
