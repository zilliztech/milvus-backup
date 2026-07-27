package loader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// write puts content in a temp file and returns its path, so a test starts from
// a real file the loader has to identify on its own.
func write(t *testing.T, content string) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "backup.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))

	return p
}

func TestLoad_V2File(t *testing.T) {
	out, err := Load(write(t, `
configVersion: v2
milvus:
  grpc:
    address: milvus-proxy
    port: 19531
`), nil)
	require.NoError(t, err)

	assert.Equal(t, "milvus-proxy", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19531, out.Milvus.Grpc.Port.Val)
}

// A file without the discriminator predates it, so it is v1 and gets
// translated. The v1 spellings have to land in their v2 homes.
func TestLoad_V1FileIsTranslated(t *testing.T) {
	out, err := Load(write(t, `
milvus:
  address: milvus-proxy
  port: 19531
  rpcChannelName: my-replicate
backup:
  gcPause:
    address: http://datacoord:9091
  parallelism:
    copydata: 32
`), nil)
	require.NoError(t, err)

	assert.Equal(t, "milvus-proxy", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, 19531, out.Milvus.Grpc.Port.Val)
	assert.Equal(t, "my-replicate", out.Milvus.Replicate.RPCChannelName.Val)
	assert.Equal(t, "http://datacoord:9091", out.Milvus.Management.Endpoint.Val)
	assert.Equal(t, 32, out.Transfer.Concurrency.Val)
}

// Naming the v1 schema explicitly picks the same path as omitting the key.
func TestLoad_V1FileWithExplicitVersion(t *testing.T) {
	out, err := Load(write(t, `
configVersion: v1
milvus:
  address: milvus-proxy
`), nil)
	require.NoError(t, err)

	assert.Equal(t, "milvus-proxy", out.Milvus.Grpc.Address.Val)
}

// A v1 deployment keeps its v1 environment variables, because the v1 file is
// resolved with the v1 schema before anything is translated.
func TestLoad_V1EnvNamesStillResolve(t *testing.T) {
	t.Setenv("MILVUS_ADDRESS", "from-v1-env")
	t.Setenv("MINIO_BUCKET_NAME", "v1-bucket")

	out, err := Load(write(t, "milvus:\n  port: 19531\n"), nil)
	require.NoError(t, err)

	assert.Equal(t, "from-v1-env", out.Milvus.Grpc.Address.Val)
	assert.Equal(t, "v1-bucket", out.Milvus.Storage.BucketName.Val)
}

// A v2 file is resolved with v2 names only, so a v1 variable left set in the
// environment is inert rather than quietly winning. Both names below spell the
// same credential.
func TestLoad_V2FileIgnoresV1EnvNames(t *testing.T) {
	t.Setenv("MINIO_SECRET_KEY", "from-v1-env")
	t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "from-v2-env")

	out, err := Load(write(t, "configVersion: v2\n"), nil)
	require.NoError(t, err)

	assert.Equal(t, "from-v2-env", out.Milvus.Storage.Auth.SecretAccessKey.Val)
}

// With no file there is no discriminator to read, and nothing to be backward
// compatible with either: overrides and env name v2 parameters.
func TestLoad_NoConfigFileUsesV2Names(t *testing.T) {
	t.Setenv("MILVUS_PASSWORD", "from-v2-env")

	out, err := Load("", map[string]string{"milvus.grpc.port": "19531"})
	require.NoError(t, err)

	assert.Equal(t, "from-v2-env", out.Milvus.Password.Val)
	assert.Equal(t, 19531, out.Milvus.Grpc.Port.Val)
}

func TestLoad_UnknownVersion(t *testing.T) {
	_, err := Load(write(t, "configVersion: v3\n"), nil)

	require.Error(t, err)
	assert.ErrorContains(t, err, "v3")
	assert.ErrorContains(t, err, "not a schema version this build knows")
}

// The v1 path validates what it produced, so a provider v1 never checked fails
// while the config is being loaded instead of when a client is built.
func TestLoad_V1ValidationError(t *testing.T) {
	_, err := Load(write(t, "minio:\n  storageType: bogus\n"), nil)

	require.Error(t, err)
	assert.ErrorContains(t, err, "bogus")
}
