package replay

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

type sourceField struct {
	name string
	id   int64
}

// Check reports whether restore's plan reproduces every source field ID. A
// schema whose hidden fields cannot be reconstructed is rejected as unknown
// rather than treated as replayable.
func Check(schema *backuppb.CollectionSchema) error {
	plan, err := BuildPlan(schema, schema.GetFields())
	if err != nil {
		return err
	}
	maxFieldID, err := parseMaxFieldID(plan.MaxFieldID)
	if err != nil {
		return err
	}

	targetIDs := simulateFieldIDs(plan, maxFieldID)
	source, err := sourceFields(schema, plan, maxFieldID)
	if err != nil {
		return err
	}
	for _, field := range source {
		targetID, ok := targetIDs[field.name]
		if !ok {
			return fmt.Errorf("replay: field %q with source id %d is absent from the restore plan", field.name, field.id)
		}
		if targetID != field.id {
			return fmt.Errorf("replay: field %q changes id from %d to %d", field.name, field.id, targetID)
		}
	}
	if len(targetIDs) != len(source) {
		return fmt.Errorf("replay: restore produces %d fields, source has %d", len(targetIDs), len(source))
	}

	return nil
}

func parseMaxFieldID(property *backuppb.KeyValuePair) (*int64, error) {
	if property == nil {
		return nil, nil
	}
	value, err := strconv.ParseInt(property.GetValue(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("replay: parse %s value %q: %w", common.MaxFieldIDKey, property.GetValue(), err)
	}
	if value < common.StartOfUserFieldID {
		return nil, fmt.Errorf("replay: %s value %d is below the first user field id", common.MaxFieldIDKey, value)
	}
	return &value, nil
}

func sourceFields(schema *backuppb.CollectionSchema, plan *Plan[*backuppb.FieldSchema], maxFieldID *int64) ([]sourceField, error) {
	var fields []sourceField
	metaVisible := false
	for _, field := range schema.GetFields() {
		if common.IsSystemField(field.GetFieldID()) {
			continue
		}
		fields = append(fields, sourceField{name: field.GetName(), id: field.GetFieldID()})
		metaVisible = metaVisible || field.GetIsDynamic()
	}
	for _, structField := range schema.GetStructArrayFields() {
		fields = append(fields, sourceField{name: structField.GetName(), id: structField.GetFieldID()})
		for _, field := range structField.GetFields() {
			fields = append(fields, sourceField{name: structSubFieldName(structField.GetName(), field.GetName()), id: field.GetFieldID()})
		}
	}

	if !schema.GetEnableDynamicField() || metaVisible {
		return fields, nil
	}
	if maxFieldID == nil {
		// Upstream versions without the high-water property predate DropField,
		// so restore's legacy guess identifies the only missing user field.
		return append(fields, sourceField{name: common.MetaFieldName, id: plan.DynamicFieldID}), nil
	}
	metaID, err := inferSingleHiddenFieldID(fields, *maxFieldID)
	if err != nil {
		return nil, err
	}
	return append(fields, sourceField{name: common.MetaFieldName, id: metaID}), nil
}

func inferSingleHiddenFieldID(fields []sourceField, maxFieldID int64) (int64, error) {
	ids := make([]int64, 0, len(fields))
	seen := make(map[int64]struct{}, len(fields))
	for _, field := range fields {
		if field.id < common.StartOfUserFieldID || field.id > maxFieldID {
			return 0, fmt.Errorf("replay: field %q id %d lies outside the allocated range ending at %d", field.name, field.id, maxFieldID)
		}
		if _, ok := seen[field.id]; ok {
			return 0, fmt.Errorf("replay: duplicate field id %d", field.id)
		}
		seen[field.id] = struct{}{}
		ids = append(ids, field.id)
	}

	missing := maxFieldID - common.StartOfUserFieldID + 1 - int64(len(ids))
	if missing != 1 {
		return 0, fmt.Errorf("replay: cannot determine hidden %s id: %d ids are missing below high water %d", common.MetaFieldName, missing, maxFieldID)
	}

	slices.Sort(ids)
	next := int64(common.StartOfUserFieldID)
	for _, id := range ids {
		if id != next {
			return next, nil
		}
		next++
	}
	return next, nil
}

func structSubFieldName(structName, fieldName string) string {
	if strings.HasPrefix(fieldName, structName+"[") && strings.HasSuffix(fieldName, "]") {
		return fieldName
	}
	return fmt.Sprintf("%s[%s]", structName, fieldName)
}

func simulateFieldIDs(plan *Plan[*backuppb.FieldSchema], maxFieldID *int64) map[string]int64 {
	ids := make(map[string]int64)
	next := int64(common.StartOfUserFieldID)
	for _, field := range plan.CreateFields {
		ids[field.GetName()] = next
		next++
	}
	if plan.EnableDynamicField {
		ids[common.MetaFieldName] = next
		next++
	}
	for _, structField := range plan.StructArrayFields {
		ids[structField.GetName()] = next
		next++
		for _, field := range structField.GetFields() {
			ids[structSubFieldName(structField.GetName(), field.GetName())] = next
			next++
		}
	}

	highWater := next - 1
	if maxFieldID != nil {
		highWater = max(highWater, *maxFieldID)
	}
	for _, field := range plan.AddFields {
		highWater++
		ids[field.GetName()] = highWater
	}
	return ids
}
