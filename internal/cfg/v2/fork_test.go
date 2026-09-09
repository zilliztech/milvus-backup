package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// Fork(nil) re-resolves the very source the receiver was loaded from, so the
// copy comes out identical — values and provenance alike.
func TestFork_NilOverrideIsAnIndependentCopy(t *testing.T) {
	t.Setenv("MILVUS_STORAGE_AUTH_ACCESS_KEY_ID", "env-ak")

	c, err := Load(writeYAML(t, "milvus:\n  user: fromfile\n"), map[string]string{"milvus.grpc.port": "19531"})
	require.NoError(t, err)

	forked, err := c.Fork(nil)
	require.NoError(t, err)

	assert.Equal(t, c.Entries(), forked.Entries())
	assert.NotSame(t, c, forked)
}

func TestFork_AppliesOverride(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	forked, err := c.Fork(map[string]string{"backup.storage.rootPath": "forked-root"})
	require.NoError(t, err)

	assert.Equal(t, "forked-root", forked.Backup.Storage.RootPath.Val)
	assert.Equal(t, param.SourceOverride, forked.Backup.Storage.RootPath.Used.Kind)
	assert.Equal(t, "backup.storage.rootpath", forked.Backup.Storage.RootPath.Used.Key)

	// The receiver is never mutated.
	assert.Equal(t, "backup", c.Backup.Storage.RootPath.Val)
	assert.Equal(t, param.SourceDefault, c.Backup.Storage.RootPath.Used.Kind)
}

// A fork is reload-equivalent: re-resolution re-runs backup.storage
// inheritance, so overriding a milvus.storage leaf cascades into the
// backup.storage leaves that were never set explicitly.
func TestFork_CascadesIntoUnsetBackupLeaves(t *testing.T) {
	c, err := Load(writeYAML(t, "milvus:\n  storage:\n    bucketName: milvus-bucket\n"), nil)
	require.NoError(t, err)
	require.Equal(t, "milvus-bucket", c.Backup.Storage.BucketName.Val)

	forked, err := c.Fork(map[string]string{"milvus.storage.bucketName": "fork-bucket"})
	require.NoError(t, err)

	assert.Equal(t, "fork-bucket", forked.Milvus.Storage.BucketName.Val)
	assert.Equal(t, "fork-bucket", forked.Backup.Storage.BucketName.Val)

	// A leaf that was set explicitly does not follow the cascade.
	assert.Equal(t, "backup", forked.Backup.Storage.RootPath.Val)
}

// The environment a fork sees is the snapshot the source was built with:
// re-resolution consults it again, and an override may still name a credential
// by its environment variable, as --set does.
func TestFork_EnvAndCredentialSpelling(t *testing.T) {
	t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "env-sk")

	c, err := Load("", nil)
	require.NoError(t, err)

	t.Run("EnvValueSurvivesIntoTheFork", func(t *testing.T) {
		forked, err := c.Fork(nil)
		require.NoError(t, err)

		assert.Equal(t, "env-sk", forked.Milvus.Storage.Auth.SecretAccessKey.Val)
		assert.Equal(t, param.SourceEnv, forked.Milvus.Storage.Auth.SecretAccessKey.Used.Kind)
	})

	t.Run("OverrideByEnvName", func(t *testing.T) {
		forked, err := c.Fork(map[string]string{"MILVUS_STORAGE_AUTH_ACCESS_KEY_ID": "fork-ak"})
		require.NoError(t, err)

		assert.Equal(t, "fork-ak", forked.Milvus.Storage.Auth.AccessKeyID.Val)
		assert.Equal(t, param.SourceOverride, forked.Milvus.Storage.Auth.AccessKeyID.Used.Kind)
	})
}

// Fork-of-fork stacks override layers on the same source, so overrides
// accumulate; each fork along the way keeps its own view.
func TestFork_ForkOfForkAccumulates(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	fork1, err := c.Fork(map[string]string{"backup.storage.rootPath": "fork-1"})
	require.NoError(t, err)

	fork2, err := fork1.Fork(map[string]string{"transfer.concurrency": "42"})
	require.NoError(t, err)

	assert.Equal(t, "fork-1", fork2.Backup.Storage.RootPath.Val)
	assert.Equal(t, 42, fork2.Transfer.Concurrency.Val)

	assert.Equal(t, "fork-1", fork1.Backup.Storage.RootPath.Val)
	assert.Equal(t, 128, fork1.Transfer.Concurrency.Val)
}

func TestFork_NeverLoadedConfig(t *testing.T) {
	_, err := New().Fork(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not loaded from a source")
}

// An unknown key is an error, not the warning a load would log: a key handed
// to Fork was written against this build, so a misspelling is a caller bug.
func TestFork_UnknownKeyIsAnError(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	t.Run("Misspelled", func(t *testing.T) {
		_, err := c.Fork(map[string]string{"milvus.grpc.adress": "localhost"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unknown v2 key "milvus.grpc.adress"`)
	})

	t.Run("V1Key", func(t *testing.T) {
		_, err := c.Fork(map[string]string{"milvus.address": "localhost"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"milvus.address"`)
		assert.Contains(t, err.Error(), "milvus.grpc.address")
	})

	t.Run("V1EnvName", func(t *testing.T) {
		_, err := c.Fork(map[string]string{"MINIO_SECRET_KEY": "sk"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY")
	})

	t.Run("RemovedV1Key", func(t *testing.T) {
		_, err := c.Fork(map[string]string{"http.enabled": "true"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"http.enabled" was removed in v2`)
	})
}

// A fork is validated like a first load: an override that resolves but breaks
// a rule fails the fork instead of producing a config nothing checked.
func TestFork_ValidationFailure(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	_, err = c.Fork(map[string]string{"transfer.mode": "bogus"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid value "bogus"`)
}

// An override that cannot even be parsed for the parameter it names fails the
// same way a bad --set fails a load.
func TestFork_ParseFailure(t *testing.T) {
	c, err := Load("", nil)
	require.NoError(t, err)

	_, err = c.Fork(map[string]string{"milvus.grpc.port": "not-a-number"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}
