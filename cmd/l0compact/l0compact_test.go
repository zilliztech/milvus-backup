package l0compact

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateRejectsBadNames(t *testing.T) {
	// --output that would escape or nest outside a normal backup dir.
	for _, bad := range []string{"../victim", "a/b", "..", "/abs", ".hidden"} {
		o := &options{name: "src", output: bad}
		assert.Error(t, o.validate(), "validate() accepted invalid --output %q", bad)
	}
	// --name is validated the same way.
	assert.Error(t, (&options{name: "../x", output: "out"}).validate(), "validate() accepted invalid --name ../x")
	// A valid name/output pair passes.
	assert.NoError(t, (&options{name: "src", output: "src_l0c"}).validate())
	// Default output is also validated (derived from a valid name).
	o := &options{name: "src"}
	assert.NoError(t, o.validate())
	assert.Equal(t, "src_l0compacted", o.output)
}
