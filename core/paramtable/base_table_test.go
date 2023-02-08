package paramtable

import (
	"github.com/stretchr/testify/assert"
	memkv "github.com/zilliztech/milvus-backup/internal/kv/mem"
	"testing"
)

func TestParseDataSizeWithDefault(t *testing.T) {
	base := &BaseTable{
		params: memkv.NewMemoryKV(),
	}
	base.Init()
	sizeKey := "sizeKey"

	size2g, err := base.ParseDataSizeWithDefault(sizeKey, "2g")
	assert.NoError(t, err)
	assert.Equal(t, size2g, int64(2*1024*1024*1024))

	size2G, err := base.ParseDataSizeWithDefault(sizeKey, "2g")
	assert.NoError(t, err)
	assert.Equal(t, size2G, int64(2*1024*1024*1024))

	size3M, err := base.ParseDataSizeWithDefault(sizeKey, "3Mb")
	assert.NoError(t, err)
	assert.Equal(t, size3M, int64(3*1024*1024))

	size3m, err := base.ParseDataSizeWithDefault(sizeKey, "3M")
	assert.NoError(t, err)
	assert.Equal(t, size3m, int64(3*1024*1024))

	size1024k, err := base.ParseDataSizeWithDefault(sizeKey, "1024k")
	assert.NoError(t, err)
	assert.Equal(t, size1024k, int64(1024*1024))

	size1024000, err := base.ParseDataSizeWithDefault(sizeKey, "1024000")
	assert.NoError(t, err)
	assert.Equal(t, size1024000, int64(1024000))
}
