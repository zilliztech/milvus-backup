package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStrategy(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s, err := ParseStrategy("")
		assert.NoError(t, err)
		assert.Equal(t, StrategyAuto, s)
	})

	t.Run("AutoLiteral", func(t *testing.T) {
		s, err := ParseStrategy("auto")
		assert.NoError(t, err)
		assert.Equal(t, StrategyAuto, s)
	})

	t.Run("SnapshotIsAFormat", func(t *testing.T) {
		_, err := ParseStrategy("snapshot")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup format")
	})

	t.Run("AllValuesParse", func(t *testing.T) {
		for word, want := range _strategyMap {
			got, err := ParseStrategy(word)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		_, err := ParseStrategy("invalid")
		assert.Error(t, err)
	})
}
