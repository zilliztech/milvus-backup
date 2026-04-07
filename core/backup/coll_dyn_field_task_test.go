package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
