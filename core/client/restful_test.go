package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRestfulAuth(t *testing.T) {
	got := restfulAuth("username", "password")
	assert.Equal(t, "username:password", got)

	got = restfulAuth("", "")
	assert.Equal(t, "", got)
}
