package backup

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func newTestCollDDLTask() *collDDLTask { return &collDDLTask{logger: zap.NewNop()} }

func TestCollDDLTask_convSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:               "name",
		Description:        "description",
		AutoID:             true,
		EnableDynamicField: true,
		Properties:         []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}},
	}

	ct := newTestCollDDLTask()
	bakSchema, err := ct.convSchema(schema)
	assert.NoError(t, err)
	assert.Equal(t, schema.GetName(), bakSchema.GetName())
	assert.Equal(t, schema.GetDescription(), bakSchema.GetDescription())
	assert.Equal(t, schema.GetAutoID(), bakSchema.GetAutoID())
	assert.Equal(t, schema.GetEnableDynamicField(), bakSchema.GetEnableDynamicField())
	assert.ElementsMatch(t, schema.GetProperties(), pbconv.BakKVToMilvusKV(bakSchema.GetProperties()))
}
