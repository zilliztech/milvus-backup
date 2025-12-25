package funcs

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/stretchr/testify/assert"
)

func TestGuessDynFieldID(t *testing.T) {
	t.Run("HaveDynField", func(t *testing.T) {
		fields := []*schemapb.FieldSchema{
			{FieldID: common.RowIDField},
			{FieldID: common.TimeStampField},
			{FieldID: common.StartOfUserFieldID},
			{FieldID: common.StartOfUserFieldID + 1},
			{FieldID: common.StartOfUserFieldID + 2, IsDynamic: true},
			{FieldID: common.StartOfUserFieldID + 3},
		}
		id := GuessDynFieldID(fields)
		assert.Equal(t, int64(common.StartOfUserFieldID+2), id)
	})

	t.Run("NoDynFieldButHaveGap", func(t *testing.T) {
		fields := []*schemapb.FieldSchema{
			{FieldID: common.RowIDField},
			{FieldID: common.TimeStampField},
			{FieldID: common.StartOfUserFieldID},
			{FieldID: common.StartOfUserFieldID + 1},
			// $meta field id is common.StartOfUserFieldID + 2
			{FieldID: common.StartOfUserFieldID + 3},
		}
		id := GuessDynFieldID(fields)
		assert.Equal(t, int64(common.StartOfUserFieldID+2), id)
	})

	t.Run("NoDynFieldAndNoGap", func(t *testing.T) {
		fields := []*schemapb.FieldSchema{
			{FieldID: common.RowIDField},
			{FieldID: common.TimeStampField},
			{FieldID: common.StartOfUserFieldID},
			{FieldID: common.StartOfUserFieldID + 1},
			{FieldID: common.StartOfUserFieldID + 2},
		}
		id := GuessDynFieldID(fields)
		assert.Equal(t, int64(common.StartOfUserFieldID+3), id)
	})
}
