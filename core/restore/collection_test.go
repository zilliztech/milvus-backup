package restore

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestGetFailedReason(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "failed_reason", Value: "hello"}})
		assert.Equal(t, "hello", r)
	})

	t.Run("WithoutFailedReason", func(t *testing.T) {
		r := getFailedReason([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, "", r)
	})
}

func TestGetProcess(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "progress_percent", Value: "100"}})
		assert.Equal(t, 100, r)
	})

	t.Run("WithoutProgress", func(t *testing.T) {
		r := getProcess([]*commonpb.KeyValuePair{{Key: "hello", Value: "world"}})
		assert.Equal(t, 0, r)
	})
}
