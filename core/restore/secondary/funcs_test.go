package secondary

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestCheckDynamicField(t *testing.T) {
	t.Run("DynamicDisabled", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: false,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
			},
		}
		assert.NoError(t, checkDynamicField(schema))
	})

	t.Run("DynamicEnabledAndPresent", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "$meta", IsDynamic: true},
			},
		}
		assert.NoError(t, checkDynamicField(schema))
	})

	t.Run("DynamicEnabledButMissing", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "coll",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
			},
		}
		err := checkDynamicField(schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"coll"`)
	})
}
