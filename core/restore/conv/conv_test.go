package conv

import (
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestDefaultValue(t *testing.T) {
	t.Run("HasBase64", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueBase64: base64.StdEncoding.EncodeToString(bytes)}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("HasProto", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueProto: string(bytes)}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("WithoutDefault", func(t *testing.T) {
		field := &backuppb.FieldSchema{}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Nil(t, val)
	})
}
