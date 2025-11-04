package del

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption_validate(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		var o options
		o.name = "backup"
		err := o.validate()
		assert.NoError(t, err)
	})

	t.Run("NameEmpty", func(t *testing.T) {
		var o options
		err := o.validate()
		assert.Error(t, err)
	})
}
