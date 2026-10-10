package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGcpClientMethodSet pins the wrapper's whole reason to exist: GCS has
// no multi-object delete, so gcpClient must not satisfy batchDeleter, while
// still exposing minioBacked for gcp to gcp server-side copies.
func TestGcpClientMethodSet(t *testing.T) {
	cli, err := newGCPClient(Config{
		Provider: "gcp",
		Endpoint: "storage.googleapis.com",
		Bucket:   "test-bucket",
		Credential: Credential{
			Type: Static,
			AK:   "ak",
			SK:   "sk",
		},
	})
	require.NoError(t, err)

	_, ok := interface{}(cli).(batchDeleter)
	assert.False(t, ok, "gcpClient must not promote DeleteObjects: GCS has no multi-object delete")

	mb, ok := interface{}(cli).(minioBacked)
	require.True(t, ok, "gcpClient must keep minioBacked for server-side copy")
	assert.Equal(t, "test-bucket", mb.minioCfg().Bucket)
	assert.Equal(t, "gcp", mb.minioCfg().Provider)
}
