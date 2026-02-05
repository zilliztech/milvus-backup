package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAzureMultipartCopyThreshold(t *testing.T) {
	t.Run("DefaultThreshold", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredBelowLimit", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 100}}
		assert.Equal(t, int64(100*_MiB), cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredAtLimit", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 256}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})

	t.Run("ConfiguredAboveLimitCapped", func(t *testing.T) {
		cli := &AzureClient{cfg: Config{MultipartCopyThresholdMiB: 500}}
		assert.Equal(t, _azureMaxSyncCopySize, cli.multipartCopyThreshold())
	})
}
