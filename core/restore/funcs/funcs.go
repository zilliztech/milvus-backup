package funcs

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// GuessDynFieldID guess the dynamic field id.
// The describeCollection API does not return the $meta field, so we need to guess it.
func GuessDynFieldID(fields []*schemapb.FieldSchema) int64 {
	fieldIDs := make([]int64, 0, len(fields))

	// if $meta field exists, return its field id
	for _, field := range fields {
		if field.GetIsDynamic() {
			return field.GetFieldID()
		}
		if common.IsSystemField(field.GetFieldID()) {
			continue
		}

		fieldIDs = append(fieldIDs, field.GetFieldID())
	}

	sort.Slice(fieldIDs, func(i, j int) bool { return fieldIDs[i] < fieldIDs[j] })
	// if collection has gap in field id, means the collection added field after created.
	// so the first gap is the dynamic field id.
	// for example, if field ids are [1, 2, 4, 5], return 3
	for i := range len(fieldIDs) - 1 {
		if fieldIDs[i+1] != fieldIDs[i]+1 {
			return fieldIDs[i] + 1
		}
	}

	// if no gap, means the collection did not add field after created.
	// so the max field id + 1 is the dynamic field id.
	return fieldIDs[len(fieldIDs)-1] + 1
}
