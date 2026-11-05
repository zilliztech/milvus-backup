package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGcpClientMethodSet pins the wrapper's whole reason to exist: GCS has
// no multi-object delete, so gcpClient must not satisfy batchDeleter, while
// still exposing s3Backed for gcp to gcp server-side copies.
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

	mb, ok := interface{}(cli).(s3Backed)
	require.True(t, ok, "gcpClient must keep s3Backed for server-side copy")
	assert.Equal(t, "test-bucket", mb.s3Cfg().Bucket)
	assert.Equal(t, "gcp", mb.s3Cfg().Provider)
}
