package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsStandardGCSEndpoint locks the behavior that drives whether
// newGCPNativeClient sets option.WithEndpoint: standard Google Cloud Storage
// hosts must be reported as standard (so WithEndpoint is skipped), while
// custom / emulator hosts must not be (so WithEndpoint is set). See the doc
// comment on isStandardGCSEndpoint for why a wrong answer here breaks client
// init with "multiple credential options provided".
func TestIsStandardGCSEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		want     bool
	}{
		// Standard GCS — WithEndpoint must be skipped.
		{"empty endpoint", "", true},
		{"gcs host with port", "storage.googleapis.com:443", true},
		{"gcs host without port", "storage.googleapis.com", true},
		{"gcs virtual-hosted bucket host", "mybucket.storage.googleapis.com:443", true},
		{"www googleapis host", "www.googleapis.com:443", true},
		{"bare googleapis host", "googleapis.com:443", true},
		{"port only, no host", ":443", true},

		// Custom / emulator hosts — WithEndpoint must be set.
		{"localhost emulator", "localhost:4443", false},
		{"fake-gcs-server emulator", "fake-gcs-server:4443", false},
		{"loopback ip with port", "127.0.0.1:9000", false},
		{"custom host with port", "minio.example.com:9000", false},
		{"custom host without port", "localhost", false},
		{"ipv6 emulator host", "[::1]:4443", false},
		{"lookalike domain is not GCS", "storage.googleapis.com.evil.example:443", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isStandardGCSEndpoint(tc.endpoint),
				"endpoint %q", tc.endpoint)
		})
	}
}
