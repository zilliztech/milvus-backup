package l0compact

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyVersion(t *testing.T) {
	cases := map[int64]StorageKind{0: KindV1, 1: KindV1, 2: KindV2, 3: KindV2}
	for v, want := range cases {
		got, err := ClassifyVersion(v)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	}

	_, err := ClassifyVersion(4)
	assert.Error(t, err, "want fail-fast on version 4")
}
