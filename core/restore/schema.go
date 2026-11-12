package restore

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/core/restore/conv"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
)

const (
	preserveFieldIDsProperty = "preserve_field_ids"
	maxFieldIDProperty       = "max_field_id"
)

func filterParams(params []*commonpb.KeyValuePair, skip []string) []*commonpb.KeyValuePair {
	return slices.DeleteFunc(slices.Clone(params), func(kv *commonpb.KeyValuePair) bool {
		return slices.Contains(skip, kv.GetKey())
	})
}

func (ddlt *collDDLTask) createCollectionInput() (milvus.CreateCollectionInput, []*schemapb.FieldSchema, error) {
	schema, err := ddlt.sourceSchema()
	if err != nil {
		return milvus.CreateCollectionInput{}, nil, err
	}
	normalizeProxySchema(schema)
	capable := ddlt.grpcCli.HasFeature(milvus.PreserveFieldIDs)
	preserve := capable
	if missing := missingHiddenFields(schema); preserve && len(missing) != 0 {
		if collectionHasSegments(ddlt.collBackup) {
			return milvus.CreateCollectionInput{}, nil, fmt.Errorf("restore: preserving field IDs requires source IDs for %s; take a new backup with --set milvus.preserveFieldIDs=true or --backup_index_extra (snapshot backups capture hidden fields automatically); unknown binlog IDs may belong to dropped fields", strings.Join(missing, ", "))
		}
		// An empty collection has no source columns to align. Ordinary creation
		// safely supplies hidden IDs even if old metadata did not record them.
		preserve = false
		ddlt.logger.Info("empty collection has no hidden field IDs; create with ordinary field ID allocation")
	}
	if !capable {
		if err := validateLegacySchema(schema); err != nil {
			return milvus.CreateCollectionInput{}, nil, err
		}
	} else if preserve {
		if err := validatePreservedIDs(schema); err != nil {
			return milvus.CreateCollectionInput{}, nil, err
		}
	}

	if capable && schema.GetEnableNamespace() {
		for _, kv := range schema.GetProperties() {
			if kv.GetKey() == common.NamespaceModeKey && slices.Contains(ddlt.option.SkipParams.CollectionProperties, common.NamespaceModeKey) {
				return milvus.CreateCollectionInput{}, nil, fmt.Errorf("restore: cannot skip namespace.mode when preserving a namespace schema")
			}
		}
	}
	properties := ddlt.propertiesForSchema(schema)
	var addFields []*schemapb.FieldSchema
	if preserve {
		// A dropped trailing field is absent from schema; retaining this high-water
		// mark is what prevents its ID from being allocated again after restore.
		for _, kv := range schema.GetProperties() {
			if kv.GetKey() == maxFieldIDProperty && slices.Contains(ddlt.option.SkipParams.CollectionProperties, maxFieldIDProperty) {
				return milvus.CreateCollectionInput{}, nil, fmt.Errorf("restore: cannot skip max_field_id when preserving source field IDs")
			}
		}
		properties = append(properties, &commonpb.KeyValuePair{Key: preserveFieldIDsProperty, Value: "true"})
	} else {
		if capable {
			// No rows are imported into this empty fallback; create all visible
			// fields together so functions can resolve their outputs immediately.
			schema.Fields = slices.DeleteFunc(schema.Fields, func(field *schemapb.FieldSchema) bool {
				return field.GetIsDynamic() || field.GetName() == common.NamespaceFieldName
			})
		} else {
			schema.Fields, addFields = legacyFields(schema)
		}
	}

	for _, field := range schema.GetFields() {
		ddlt.filterFieldParams(field)
	}
	for _, structure := range schema.GetStructArrayFields() {
		for _, field := range structure.GetFields() {
			ddlt.filterFieldParams(field)
		}
	}
	for _, field := range addFields {
		ddlt.filterFieldParams(field)
	}
	schema.Name = ddlt.target.CollName()
	schema.DbName = ddlt.target.DBName()
	if ddlt.collOverride.Description != "" {
		schema.Description = ddlt.collOverride.Description
	}
	// Never carry a source request switch into an ordinary create fallback.
	schema.Properties = filterParams(schema.GetProperties(), []string{preserveFieldIDsProperty})
	return milvus.CreateCollectionInput{
		DB:           ddlt.target.DBName(),
		Schema:       schema,
		ConsLevel:    commonpb.ConsistencyLevel(ddlt.collBackup.GetConsistencyLevel()),
		ShardNum:     ddlt.shardNum(),
		PartitionNum: partitionNum(schema, ddlt.collBackup.GetPartitionBackups()),
		Properties:   properties,
	}, addFields, nil
}

func (ddlt *collDDLTask) filterFieldParams(field *schemapb.FieldSchema) {
	field.TypeParams = filterParams(field.GetTypeParams(), ddlt.option.SkipParams.FieldTypeParams)
	field.IndexParams = filterParams(field.GetIndexParams(), ddlt.option.SkipParams.FieldIndexParams)
	for typ := field.GetTypeSchema(); typ != nil; typ = typ.GetArrayElement() {
		typ.TypeParams = filterParams(typ.GetTypeParams(), ddlt.option.SkipParams.FieldTypeParams)
	}
}

// validatePreservedIDs refuses incomplete metadata instead of letting a server
// assign fresh IDs to a field whose binlogs still use its original ID.
func validatePreservedIDs(schema *schemapb.CollectionSchema) error {
	ids := make(map[int64]struct{})
	add := func(id int64, name string) error {
		if id < common.StartOfUserFieldID {
			return fmt.Errorf("restore: field %q has invalid source ID %d", name, id)
		}
		if _, found := ids[id]; found {
			return fmt.Errorf("restore: duplicate source field ID %d (%q)", id, name)
		}
		ids[id] = struct{}{}
		return nil
	}
	names := make(map[string]int64)
	var dynamic, namespace bool
	for _, field := range schema.GetFields() {
		if err := add(field.GetFieldID(), field.GetName()); err != nil {
			return err
		}
		names[field.GetName()] = field.GetFieldID()
		if field.GetIsDynamic() && field.GetName() != common.MetaFieldName {
			return fmt.Errorf("restore: unexpected dynamic field %q", field.GetName())
		}
		if field.GetName() == common.MetaFieldName {
			if !field.GetIsDynamic() || field.GetDataType() != schemapb.DataType_JSON {
				return fmt.Errorf("restore: $meta must be a dynamic JSON field")
			}
			dynamic = true
		}
		if field.GetName() == common.NamespaceFieldName {
			if field.GetDataType() != schemapb.DataType_VarChar {
				return fmt.Errorf("restore: $namespace_id must be a VarChar field")
			}
			namespace = true
		}
	}
	for _, structure := range schema.GetStructArrayFields() {
		if err := add(structure.GetFieldID(), structure.GetName()); err != nil {
			return err
		}
		for _, field := range structure.GetFields() {
			if err := add(field.GetFieldID(), field.GetName()); err != nil {
				return err
			}
			if field.GetFieldID() <= structure.GetFieldID() {
				return fmt.Errorf("restore: sub-field %q ID %d must exceed struct %q ID %d", field.GetName(), field.GetFieldID(), structure.GetName(), structure.GetFieldID())
			}
		}
	}
	if dynamic != schema.GetEnableDynamicField() {
		return fmt.Errorf("restore: source $meta presence must match enable_dynamic_field; take a new backup with --backup_index_extra")
	}
	if namespace != conv.NeedsNamespaceField(schema) {
		return fmt.Errorf("restore: source $namespace_id presence must match enable_namespace and namespace.mode; take a new backup with --backup_index_extra")
	}
	functionIDs := make(map[int64]struct{})
	for _, function := range schema.GetFunctions() {
		if function.GetId() < common.StartOfUserFieldID {
			return fmt.Errorf("restore: function %q has invalid source ID %d", function.GetName(), function.GetId())
		}
		if _, exists := functionIDs[function.GetId()]; exists {
			return fmt.Errorf("restore: duplicate function ID %d", function.GetId())
		}
		functionIDs[function.GetId()] = struct{}{}
		if err := validateFunctionFields(function.GetName(), function.GetInputFieldNames(), function.GetInputFieldIds(), names); err != nil {
			return err
		}
		if err := validateFunctionFields(function.GetName(), function.GetOutputFieldNames(), function.GetOutputFieldIds(), names); err != nil {
			return err
		}
	}
	for _, kv := range schema.GetProperties() {
		if kv.GetKey() == maxFieldIDProperty {
			if _, err := strconv.ParseInt(kv.GetValue(), 10, 64); err != nil {
				return fmt.Errorf("restore: invalid max_field_id %q: %w", kv.GetValue(), err)
			}
		}
	}
	return nil
}

func validateFunctionFields(function string, fieldNames []string, fieldIDs []int64, fields map[string]int64) error {
	if len(fieldIDs) != 0 && len(fieldNames) != len(fieldIDs) {
		return fmt.Errorf("restore: function %q field names and IDs have different lengths", function)
	}
	for i, name := range fieldNames {
		id, exists := fields[name]
		if !exists {
			return fmt.Errorf("restore: function %q references unknown field %q", function, name)
		}
		if len(fieldIDs) != 0 && fieldIDs[i] != id {
			return fmt.Errorf("restore: function %q field %q ID %d does not match source ID %d", function, name, fieldIDs[i], id)
		}
	}
	return nil
}

func normalizeProxySchema(schema *schemapb.CollectionSchema) {
	schema.Fields = slices.DeleteFunc(schema.Fields, func(field *schemapb.FieldSchema) bool {
		return field.GetName() == common.RowIDFieldName || field.GetName() == common.TimeStampFieldName
	})
	for _, structure := range schema.GetStructArrayFields() {
		for _, field := range structure.GetFields() {
			prefix := structure.GetName() + "["
			if strings.HasPrefix(field.GetName(), prefix) && strings.HasSuffix(field.GetName(), "]") {
				field.Name = strings.TrimSuffix(strings.TrimPrefix(field.GetName(), prefix), "]")
			}
			field.TypeParams = filterParams(field.GetTypeParams(), []string{"__struct_sub_field_added__"})
		}
	}
}

func missingHiddenFields(schema *schemapb.CollectionSchema) []string {
	names := make(map[string]bool)
	for _, field := range schema.GetFields() {
		names[field.GetName()] = true
	}
	var missing []string
	if schema.GetEnableDynamicField() && !names[common.MetaFieldName] {
		missing = append(missing, common.MetaFieldName)
	}
	if conv.NeedsNamespaceField(schema) && !names[common.NamespaceFieldName] {
		missing = append(missing, common.NamespaceFieldName)
	}
	return missing
}

func collectionHasSegments(coll *backuppb.CollectionBackupInfo) bool {
	// Older snapshot metadata does not enumerate its segments here, so only
	// binlog metadata can prove emptiness. Snapshot bundles capture hidden IDs.
	if coll.GetSnapshotBackup() != nil || len(coll.GetL0Segments()) != 0 || coll.GetSize() > 0 {
		return true
	}
	for _, partition := range coll.GetPartitionBackups() {
		if len(partition.GetSegmentBackups()) != 0 {
			return true
		}
	}
	return false
}

func validateLegacySchema(schema *schemapb.CollectionSchema) error {
	return conv.ValidateLegacySchema(schema)
}

func partitionNum(schema *schemapb.CollectionSchema, partitions []*backuppb.PartitionBackupInfo) int {
	if conv.NeedsNamespaceField(schema) {
		return len(partitions)
	}
	for _, field := range schema.GetFields() {
		if field.GetIsPartitionKey() {
			return len(partitions)
		}
	}
	return 0
}
