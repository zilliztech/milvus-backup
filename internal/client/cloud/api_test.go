package cloud

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMask(t *testing.T) {
	// less than 6 characters
	res := mask("123")
	assert.Equal(t, "****", res)

	// 6 characters
	res = mask("123456")
	assert.Equal(t, "****", res)

	// long than 6 characters
	res = mask("123456789")
	assert.Equal(t, "12****89", res)
}
