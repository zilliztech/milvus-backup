package conv

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

var ErrSchemaEvolutionUnsupported = errors.New("restore: target must support struct sub-field evolution and preserve_field_ids")

// ValidateLegacySchema checks whether a target preceding struct sub-field
// evolution can restore this schema without losing flags or remapping columns.
func ValidateLegacySchema(schema *schemapb.CollectionSchema) error {
	unsupported := func(reason string) error {
		return fmt.Errorf("%w: %s; enable milvus.preserveFieldIDs only on a supporting server", ErrSchemaEvolutionUnsupported, reason)
	}
	checkField := func(field *schemapb.FieldSchema) error {
		if field.GetElementNullable() {
			return unsupported(fmt.Sprintf("field %q with element_nullable", field.GetName()))
		}
		if field.GetElementType() == schemapb.DataType_Array || field.GetTypeSchema().GetArrayElement().GetArrayElement() != nil {
			return unsupported(fmt.Sprintf("field %q with nested arrays", field.GetName()))
		}
		return nil
	}
	for _, field := range schema.GetFields() {
		if err := checkField(field); err != nil {
			return err
		}
	}
	for _, structure := range schema.GetStructArrayFields() {
		for _, field := range structure.GetFields() {
			if err := checkField(field); err != nil {
				return err
			}
		}
	}
	if len(schema.GetStructArrayFields()) == 0 {
		return nil
	}
	// Appending to the last struct can leave IDs completely contiguous. Public
	// Describe strips the per-sub-field origin marker, but a changed schema's
	// version still tells us we cannot prove all its columns were original.
	if schema.GetVersion() > 0 {
		return unsupported(fmt.Sprintf("struct schema version %d may contain evolved sub-fields even with contiguous IDs", schema.GetVersion()))
	}
	// Legacy CreateCollection assigns IDs to top-level fields (including $meta),
	// then each struct and its children. Any deviation would remap binlog columns.
	expected := int64(common.StartOfUserFieldID)
	var dynamic bool
	for _, field := range schema.GetFields() {
		if field.GetIsDynamic() {
			dynamic = true
		}
		if field.GetFieldID() != expected {
			return unsupported("struct schema with evolved field IDs")
		}
		expected++
	}
	if schema.GetEnableDynamicField() && !dynamic {
		expected++ // Old backup metadata omitted the implicit dynamic field.
	}
	for _, structure := range schema.GetStructArrayFields() {
		if structure.GetFieldID() != expected {
			return unsupported("struct schema with evolved field IDs")
		}
		expected++
		for _, field := range structure.GetFields() {
			if field.GetFieldID() != expected {
				return unsupported("struct schema with evolved sub-field IDs")
			}
			expected++
		}
	}
	for _, property := range schema.GetProperties() {
		if property.GetKey() != "max_field_id" {
			continue
		}
		maxID, err := strconv.ParseInt(property.GetValue(), 10, 64)
		if err != nil {
			return fmt.Errorf("restore: invalid max_field_id %q: %w", property.GetValue(), err)
		}
		if maxID >= expected {
			return unsupported("struct schema with deleted field IDs (max_field_id exceeds active fields)")
		}
	}
	return nil
}

func NeedsNamespaceField(schema *schemapb.CollectionSchema) bool {
	return schema.GetEnableNamespace() && !common.IsNamespaceModePartition(schema.GetProperties()...)
}
