package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// azureSharedKey builds the key pair a shared-key SAS is signed with: the SDK
// requires a base64-decodable 256-bit key, which no printable string is.
func azureSharedKey() string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("k"), 32))
}

func TestCrossAccountAzure(t *testing.T) {
	azure := func(account string) Config {
		return Config{
			Provider:   v2.ProviderAzure,
			Endpoint:   "core.windows.net:443",
			Bucket:     "bucket",
			Credential: Credential{Type: Static, AK: account, SK: azureSharedKey(), AzureAccountName: account},
		}
	}

	tests := []struct {
		name string
		src  Config
		dest Config
		want bool
	}{
		{"TwoAccounts", azure("milvus-account"), azure("backup-account"), true},
		{"OneAccount", azure("account"), azure("account"), false},
		// The same account behind two endpoints is still one account: its
		// reads need no SAS, and Milvus rejects one there as an input error.
		{
			"OneAccountTwoEndpoints", azure("account"), func() Config {
				c := azure("account")
				c.Endpoint = "privatelink.blob.core.windows.net:443"
				return c
			}(), false,
		},
		{"AzureToS3", azure("account"), Config{Provider: v2.ProviderS3}, false},
		{"S3ToAzure", Config{Provider: v2.ProviderS3}, azure("account"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CrossAccountAzure(tt.src, tt.dest))
		})
	}
}

func TestResolveSourceSAS(t *testing.T) {
	// An operator-provided token is passed through as the escape hatch for
	// accounts whose key milvus-backup cannot sign with — trimmed to the bare
	// query, which is the shape the extfs field and Milvus both expect.
	t.Run("UsesTheExplicitToken", func(t *testing.T) {
		cfg := Config{Provider: v2.ProviderAzure, SourceSAS: "?sv=2024-08-04&sig=abc"}

		got, err := ResolveSourceSAS(t.Context(), cfg)
		require.NoError(t, err)
		assert.Equal(t, "sv=2024-08-04&sig=abc", got.SourceSAS)
	})

	// A shared key signs a container-scoped service SAS locally: the token is a
	// query naming the container, the read permission, and a signature.
	t.Run("MintsAServiceSASFromTheSharedKey", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderAzure,
			Endpoint:   "core.windows.net:443",
			Bucket:     "milvus-bucket",
			Credential: Credential{Type: Static, AK: "milvusaccount", SK: azureSharedKey(), AzureAccountName: "milvusaccount"},
		}

		got, err := ResolveSourceSAS(t.Context(), cfg)
		require.NoError(t, err)

		query, err := url.ParseQuery(got.SourceSAS)
		require.NoError(t, err)
		assert.Equal(t, "c", query.Get("sr"))
		assert.Contains(t, query.Get("sp"), "r")
		assert.NotEmpty(t, query.Get("sig"))
		expiry, err := time.Parse(sas.TimeFormat, query.Get("se"))
		require.NoError(t, err)
		assert.True(t, expiry.After(time.Now()))
	})

	// A credential that can neither sign nor delegate leaves nothing to mint
	// with, which is a configuration problem rather than a fallback case.
	t.Run("MintNeedsSharedKeyOrIAM", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderAzure,
			Credential: Credential{Type: MinioCredProvider, AzureAccountName: "milvusaccount"},
		}

		_, err := ResolveSourceSAS(t.Context(), cfg)
		assert.ErrorContains(t, err, "shared key or iam")
	})

	// The user-delegation path needs the service, so a canceled context fails
	// it rather than minting forever.
	t.Run("UserDelegationHonorsTheContext", func(t *testing.T) {
		cfg := Config{
			Provider:   v2.ProviderAzure,
			Endpoint:   "core.windows.net:443",
			Bucket:     "milvus-bucket",
			Credential: Credential{Type: IAM, AzureAccountName: "milvusaccount"},
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := ResolveSourceSAS(ctx, cfg)
		assert.Error(t, err)
	})
}
