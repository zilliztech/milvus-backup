package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasSpecialChar(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"", false},
		{"_", false},
		{"a", false},
		{"1", false},
		{"a1", false},
		{"a_1", false},
		{"_a1", false},
		{"a-1", true},
		{"a 1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasSpecialChar(tt.name))
		})
	}
}

func TestHasWhitespace(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"", false},
		{"a", false},
		{"a b", true},
		{"a\tb", true},
		{"a\nb", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasWhitespace(tt.name))
		})
	}
}
