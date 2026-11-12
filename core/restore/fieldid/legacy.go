package fieldid

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// CheckLegacy reports whether the target's legacy create path reproduces every
// source field id. It models what the server does before anything touches the
// target, so a backup whose columns cannot be reproduced is refused before a
// target collection is dropped or created.
//
// The model covers the create side: CreateCollection numbers the client's
// fields from 100 in order, appends the $meta field, then numbers the struct
// fields. That sequence is version-independent. Where AddField continues from
// is not: a 3.0 target raises its high-water mark with the backup's
// max_field_id property, while 2.6 allocates from the field ids alone, so the
// pre-create model leaves added fields to Verify.
//
// A refusal here is safe to act on before any side effect. A pass is not the
// final word: Verify reads the ids the target actually assigned, which is what
// covers a server that allocates differently from the model.
func CheckLegacy(schema *backuppb.CollectionSchema) error {
	plan, err := BuildPlan(schema)
	if err != nil {
		return fmt.Errorf("build the field plan: %w", err)
	}

	highWater, err := highWaterMark(schema.GetProperties())
	if err != nil {
		return err
	}

	source, err := legacySourceFields(schema, plan, highWater)
	if err != nil {
		return err
	}
	target := legacyFieldIDs(schema, plan)

	sourceByName := make(map[string]field, len(source))
	for _, f := range source {
		sourceByName[f.name] = f
	}

	var mismatches []string
	for _, f := range source {
		id, ok := target[f.name]
		if !ok {
			mismatches = append(mismatches, fmt.Sprintf("field %q (source id %d) is absent from the create plan", f.name, f.id))
			continue
		}
		if id != f.id {
			mismatches = append(mismatches, fmt.Sprintf("field %q would get id %d instead of the source id %d: the backup stores its data under %d, so the import would read another column", f.name, id, f.id, f.id))
		}
	}

	extra := make([]string, 0)
	for name := range target {
		if _, ok := sourceByName[name]; ok {
			continue
		}
		if name == common.MetaFieldName {
			// Older metadata does not record the hidden $meta id; the model
			// supplies one and there is nothing to compare it with.
			continue
		}
		extra = append(extra, name)
	}
	slices.Sort(extra)
	for _, name := range extra {
		mismatches = append(mismatches, fmt.Sprintf("the create plan has field %q, which the source schema does not record", name))
	}

	if len(mismatches) != 0 {
		return fmt.Errorf("%s", strings.Join(mismatches, "; "))
	}

	return nil
}

// legacySourceFields lists the source fields with their recorded ids. When the
// backup did not record the hidden $meta field, its id is reconstructed from
// the high-water mark instead of being ignored.
func legacySourceFields(schema *backuppb.CollectionSchema, plan *Plan, highWater *int64) ([]field, error) {
	fields, err := recordedFields(schema)
	if err != nil {
		return nil, err
	}
	if !schema.GetEnableDynamicField() || recordedDynamicField(schema) {
		return fields, nil
	}
	if highWater == nil {
		// Metadata without the high-water property predates dropped fields, so
		// the guessed $meta id is the only missing user field.
		return append(fields, field{name: common.MetaFieldName, id: plan.DynamicFieldID}), nil
	}
	id, err := inferHiddenFieldID(fields, *highWater, common.MetaFieldName)
	if err != nil {
		return nil, err
	}
	return append(fields, field{name: common.MetaFieldName, id: id}), nil
}

func recordedDynamicField(schema *backuppb.CollectionSchema) bool {
	for _, f := range schema.GetFields() {
		if f.GetIsDynamic() {
			return true
		}
	}
	return false
}

// inferHiddenFieldID finds the only id missing below the high-water mark. The
// public DescribeCollection filters hidden fields out, so a backup may record
// the high-water mark but not the id itself; with exactly one hole the field
// can still be placed.
func inferHiddenFieldID(fields []field, highWater int64, name string) (int64, error) {
	ids := make([]int64, 0, len(fields))
	seen := make(map[int64]struct{}, len(fields))
	for _, f := range fields {
		if f.id > highWater {
			return 0, fmt.Errorf("field %q id %d lies above the high-water mark %d", f.name, f.id, highWater)
		}
		if _, ok := seen[f.id]; ok {
			return 0, fmt.Errorf("duplicate source field id %d", f.id)
		}
		seen[f.id] = struct{}{}
		ids = append(ids, f.id)
	}

	missing := highWater - common.StartOfUserFieldID + 1 - int64(len(ids))
	if missing != 1 {
		return 0, fmt.Errorf("cannot place the hidden %s id: %d ids are missing below the high-water mark %d", name, missing, highWater)
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

// legacyFieldIDs models the ids the legacy create path assigns.
func legacyFieldIDs(schema *backuppb.CollectionSchema, plan *Plan) map[string]int64 {
	ids := make(map[string]int64, len(plan.Create)+len(plan.Add)+4)

	next := int64(common.StartOfUserFieldID)
	for _, f := range plan.Create {
		if isSystemFieldID(f.GetFieldID()) {
			continue
		}
		ids[f.GetName()] = next
		next++
	}
	// CreateCollection appends the dynamic field after the client's fields.
	if schema.GetEnableDynamicField() {
		ids[common.MetaFieldName] = next
		next++
	}
	for _, structure := range schema.GetStructArrayFields() {
		ids[structure.GetName()] = next
		next++
		for _, f := range structure.GetFields() {
			ids[structSubFieldName(structure.GetName(), f.GetName())] = next
			next++
		}
	}

	highest := next - 1
	for _, f := range plan.Add {
		if isSystemFieldID(f.GetFieldID()) {
			continue
		}
		highest++
		ids[f.GetName()] = highest
	}

	return ids
}

// highWaterMark reads the collection's max_field_id property, the id above the
// last one ever allocated. Records it only when the backup carries it.
func highWaterMark(properties []*backuppb.KeyValuePair) (*int64, error) {
	for _, kv := range properties {
		if kv.GetKey() != common.MaxFieldIDKey {
			continue
		}
		value, err := strconv.ParseInt(kv.GetValue(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s value %q: %w", common.MaxFieldIDKey, kv.GetValue(), err)
		}
		if value < common.StartOfUserFieldID {
			return nil, fmt.Errorf("%s value %d is below the first user field id", common.MaxFieldIDKey, value)
		}
		return &value, nil
	}
	return nil, nil
}
