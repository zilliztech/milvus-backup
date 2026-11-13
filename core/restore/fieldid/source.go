package fieldid

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// field is one source column: the name the comparison keys on and the id its
// data is stored under.
type field struct {
	name string
	id   int64
}

// recordedFields lists the fields a backup schema records. A field carrying a
// system id is dropped: the backup never stores a column for it.
func recordedFields(schema *backuppb.CollectionSchema) ([]field, error) {
	fields := make([]field, 0, len(schema.GetFields()))
	seen := make(map[int64]string, len(schema.GetFields()))

	for _, f := range schema.GetFields() {
		if err := addField(&fields, seen, f.GetName(), f.GetFieldID()); err != nil {
			return nil, err
		}
	}
	for _, structure := range schema.GetStructArrayFields() {
		if err := addField(&fields, seen, structure.GetName(), structure.GetFieldID()); err != nil {
			return nil, err
		}
		for _, f := range structure.GetFields() {
			if err := addField(&fields, seen, structSubFieldName(structure.GetName(), f.GetName()), f.GetFieldID()); err != nil {
				return nil, err
			}
		}
	}

	return fields, nil
}

// describedFields lists the fields a target DescribeCollection returns. It
// mirrors recordedFields, including the struct sub-field name normalization.
func describedFields(schema *schemapb.CollectionSchema) ([]field, error) {
	fields := make([]field, 0, len(schema.GetFields()))
	seen := make(map[int64]string, len(schema.GetFields()))

	for _, f := range schema.GetFields() {
		if err := addField(&fields, seen, f.GetName(), f.GetFieldID()); err != nil {
			return nil, err
		}
	}
	for _, structure := range schema.GetStructArrayFields() {
		if err := addField(&fields, seen, structure.GetName(), structure.GetFieldID()); err != nil {
			return nil, err
		}
		for _, f := range structure.GetFields() {
			if err := addField(&fields, seen, structSubFieldName(structure.GetName(), f.GetName()), f.GetFieldID()); err != nil {
				return nil, err
			}
		}
	}

	return fields, nil
}

func addField(fields *[]field, seen map[int64]string, name string, id int64) error {
	if isSystemFieldID(id) {
		return nil
	}
	if id < common.StartOfUserFieldID {
		return fmt.Errorf("field %q has id %d, outside the user field id range; the metadata does not record source field ids", name, id)
	}
	if other, ok := seen[id]; ok {
		return fmt.Errorf("fields %q and %q share source field id %d", other, name, id)
	}
	seen[id] = name
	*fields = append(*fields, field{name: name, id: id})
	return nil
}

// isSystemFieldID reports whether the id belongs to a field the server owns:
// RowID or Timestamp. They never address a user column in the backup.
func isSystemFieldID(id int64) bool {
	return id == common.RowIDField || id == common.TimeStampField
}
