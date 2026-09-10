package backup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollDynFieldTaskExecute(t *testing.T) {
	t.Run("UnreachableEtcdNamesEndpoint", func(t *testing.T) {
		etcd := newEtcdMeta(&blockingEtcd{}, []string{"127.0.0.1:2379"})
		etcd.timeout = 100 * time.Millisecond

		task := newCollDynFieldTask("task1", etcd, "by-dev", newMetaBuilder("task1", "backup1"))
		err := task.Execute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "127.0.0.1:2379")
	})
}

func TestParseCollIDFromFieldKey(t *testing.T) {
	prefix := "by-dev/meta/root-coord/fields/"

	t.Run("ValidKey", func(t *testing.T) {
		collID, ok := parseCollIDFromFieldKey("by-dev/meta/root-coord/fields/12345/678", prefix)
		assert.True(t, ok)
		assert.Equal(t, int64(12345), collID)
	})

	t.Run("PrefixMismatch", func(t *testing.T) {
		_, ok := parseCollIDFromFieldKey("by-dev/meta/other/12345/678", prefix)
		assert.False(t, ok)
	})

	t.Run("MissingFieldIDSegment", func(t *testing.T) {
		_, ok := parseCollIDFromFieldKey("by-dev/meta/root-coord/fields/12345", prefix)
		assert.False(t, ok)
	})

	t.Run("EmptyCollectionID", func(t *testing.T) {
		_, ok := parseCollIDFromFieldKey("by-dev/meta/root-coord/fields//678", prefix)
		assert.False(t, ok)
	})

	t.Run("NonNumericCollectionID", func(t *testing.T) {
		_, ok := parseCollIDFromFieldKey("by-dev/meta/root-coord/fields/abc/678", prefix)
		assert.False(t, ok)
	})
}
