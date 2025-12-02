package backup

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultName(t *testing.T) {
	ti := time.Unix(100, 100)
	name := DefaultName(ti)
	assert.Equal(t, "backup_1970_01_01_00_01_40_100", name)
}

func TestSupportStrategy(t *testing.T) {
	strategies := SupportStrategy()
	assert.Len(t, strategies, len(_strategyMap))
}

func TestParseStrategy(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s, err := ParseStrategy("")
		assert.NoError(t, err)
		assert.Equal(t, StrategyAuto, s)
	})

	t.Run("Valid", func(t *testing.T) {
		for _, strategy := range SupportStrategy() {
			s, err := ParseStrategy(strategy)
			assert.NoError(t, err)
			assert.Equal(t, _strategyMap[strategy], s)
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		_, err := ParseStrategy("invalid")
		assert.Error(t, err)
	})
}

func TestValidateName(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		err := ValidateName("backup")
		assert.NoError(t, err)
	})

	t.Run("Whitespace", func(t *testing.T) {
		err := ValidateName("backup ")
		assert.Error(t, err)
	})

	t.Run("Length", func(t *testing.T) {
		err := ValidateName(strings.Repeat("a", 129))
		assert.Error(t, err)
	})

	t.Run("StartWithNumber", func(t *testing.T) {
		err := ValidateName("1backup")
		assert.Error(t, err)
	})

	t.Run("SpecialChar", func(t *testing.T) {
		err := ValidateName("backup!")
		assert.Error(t, err)
	})
}
