// Package fieldid guards the invariant a binlog restore cannot recover from:
// every target field must be the same id its columns are stored under.
//
// The backup stores each column under its source field id. Milvus's
// CreateCollection assigns ids from 100 in schema order, so only a source whose
// ids follow that layout — no dropped field left a hole, no added field
// continued past one — comes back with the same ids. When it does not, import
// reads another column's data and still reports success.
//
// CheckLegacy models the create path before anything touches the target.
// Verify reads the ids the target actually assigned and is the exact check.
package fieldid

import (
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/funcs"
)

// Plan is restore's split of a schema: the fields CreateCollection creates and
// the fields AddField adds afterwards.
type Plan struct {
	Create []*backuppb.FieldSchema
	Add    []*backuppb.FieldSchema

	// DynamicFieldID is the source id guessed for the implicit $meta field. It
	// decides the split; it is not a target id.
	DynamicFieldID int64
}

// BuildPlan splits a schema exactly as restore's create path does. A recorded
// dynamic field belongs to the source schema, not to the create plan:
// CreateCollection owns the target's $meta and AddField rejects it.
func BuildPlan(schema *backuppb.CollectionSchema) (*Plan, error) {
	fields := make([]*backuppb.FieldSchema, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if field.GetIsDynamic() {
			continue
		}
		fields = append(fields, field)
	}

	plan := &Plan{}
	if !schema.GetEnableDynamicField() {
		plan.Create = fields
		return plan, nil
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("dynamic collection has no non-dynamic fields")
	}

	plan.DynamicFieldID = funcs.GuessDynFieldID(fields)
	for _, field := range fields {
		if field.GetFieldID() < plan.DynamicFieldID {
			plan.Create = append(plan.Create, field)
		} else {
			plan.Add = append(plan.Add, field)
		}
	}

	return plan, nil
}

// structSubFieldName gives a struct sub-field one name on both sides of the
// comparison: a backup schema may carry the qualified name Describe returns,
// while a target Describe may return the bare one.
func structSubFieldName(structName, fieldName string) string {
	if strings.HasPrefix(fieldName, structName+"[") && strings.HasSuffix(fieldName, "]") {
		return fieldName
	}
	return fmt.Sprintf("%s[%s]", structName, fieldName)
}
