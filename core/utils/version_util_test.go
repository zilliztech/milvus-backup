package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSupportVersion(t *testing.T) {
	support, err := IsSupportVersion("2.2.0-pre+dev")
	assert.NoError(t, err)
	assert.Equal(t, true, support)
}
