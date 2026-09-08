package v2

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

// leafValues flattens a configuration into Go field path -> resolved value, so
// two configurations can be compared leaf by leaf.
func leafValues(c *Config) map[string]any {
	out := map[string]any{}
	param.Walk(c, func(name string, f param.Field) { out[name] = f.YAMLValue() })

	return out
}

// leafDiff returns the names of the leaves whose resolved values differ
// between a and b.
func leafDiff(a, b *Config) []string {
	av, bv := leafValues(a), leafValues(b)

	var diff []string
	for name, v := range av {
		if !assert.ObjectsAreEqual(v, bv[name]) {
			diff = append(diff, name)
		}
	}

	return diff
}

func TestFork_FrozenCopy(t *testing.T) {
	parent, err := Load(filepath.Join("testdata", "complete.yaml"), map[string]string{"milvus.password": "supersecret123"})
	require.NoError(t, err)

	forked, err := parent.Fork(nil)
	require.NoError(t, err)

	assert.Empty(t, leafDiff(parent, forked))

	// Provenance is preserved as-is: a --set value stays an override, a file
	// value stays file-sourced, an untouched leaf stays defaulted.
	assert.Equal(t, param.Used{Kind: param.SourceOverride, Key: "milvus.password"}, forked.Milvus.Password.Used)
	assert.Equal(t, param.Used{Kind: param.SourceConfigFile, Key: "milvus.grpc.address"}, forked.Milvus.Grpc.Address.Used)
	// complete.yaml leaves milvusAddress unset; its empty value survives the
	// render round trip, and the fork still reports it as defaulted.
	assert.Equal(t, param.SourceDefault, forked.Milvus.Storage.MilvusAddress.Used.Kind)
}

func TestFork_OverrideApplies(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	forked, err := parent.Fork(map[string]string{"backup.storage.rootPath": "req-path"})
	require.NoError(t, err)

	assert.Equal(t, "req-path", forked.Backup.Storage.RootPath.Val)
	assert.Equal(t, param.Used{Kind: param.SourceOverride, Key: "backup.storage.rootpath"}, forked.Backup.Storage.RootPath.Used)

	// The fork differs from its parent at exactly the overridden leaf.
	assert.Equal(t, []string{"Backup.Storage.RootPath"}, leafDiff(parent, forked))
}

// An override may name a credential by its environment name, the way --set
// accepts both spellings.
func TestFork_OverrideByEnvName(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	forked, err := parent.Fork(map[string]string{"MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY": "forked-sk"})
	require.NoError(t, err)

	assert.Equal(t, "forked-sk", forked.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.Equal(t, param.Used{Kind: param.SourceOverride, Key: "milvus_storage_auth_secret_access_key"},
		forked.Milvus.Storage.Auth.SecretAccessKey.Used)
	assert.Equal(t, []string{"Milvus.Storage.Auth.SecretAccessKey"}, leafDiff(parent, forked))
}

// Overriding milvus.storage must not cascade into backup.storage: a fork
// overrides leaves, it does not re-run inheritance.
func TestFork_NoCascade(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)
	require.Equal(t, "a-bucket", parent.Backup.Storage.BucketName.Val) // inherited from milvus.storage

	forked, err := parent.Fork(map[string]string{"milvus.storage.bucketName": "other-bucket"})
	require.NoError(t, err)

	assert.Equal(t, "other-bucket", forked.Milvus.Storage.BucketName.Val)
	assert.Equal(t, "a-bucket", forked.Backup.Storage.BucketName.Val)
	assert.Equal(t, []string{"Milvus.Storage.BucketName"}, leafDiff(parent, forked))
}

func TestFork_UnknownKey(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	t.Run("Misspelled", func(t *testing.T) {
		_, err := parent.Fork(map[string]string{"backup.storage.nope": "x"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"backup.storage.nope" matches no v2 config key`)
	})

	t.Run("V1Key", func(t *testing.T) {
		_, err := parent.Fork(map[string]string{"minio.backuprootpath": "x"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"minio.backuprootpath" is a v1 key`)
		assert.Contains(t, err.Error(), "backup.storage.rootPath")
	})

	t.Run("RemovedV1Key", func(t *testing.T) {
		_, err := parent.Fork(map[string]string{"http.enabled": "true"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"http.enabled" is a v1 key that was removed in v2`)
	})

	t.Run("EveryUnknownKeyIsReported", func(t *testing.T) {
		_, err := parent.Fork(map[string]string{"foo.bar": "x", "milvus.address": "localhost"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "foo.bar")
		assert.Contains(t, err.Error(), "milvus.address")
	})
}

func TestFork_InvalidValue(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	_, err = parent.Fork(map[string]string{"milvus.grpc.port": "not-a-port"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not-a-port")
}

// An override that leaves the configuration in an invalid state fails the fork
// rather than producing a config no load would have accepted.
func TestFork_ValidationFails(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	_, err = parent.Fork(map[string]string{"milvus.storage.provider": "bogus"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid value "bogus"`)
}

// A fork resolves from the receiver's rendered values with the environment
// disabled: a variable set after the parent loaded must not leak in.
func TestFork_EnvNotPickedUp(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)
	require.Equal(t, "minioadmin", parent.Milvus.Storage.Auth.SecretAccessKey.Val)

	t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "changed")

	forked, err := parent.Fork(nil)
	require.NoError(t, err)
	assert.Equal(t, "minioadmin", forked.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.Equal(t, param.SourceDefault, forked.Milvus.Storage.Auth.SecretAccessKey.Used.Kind)
}

// A value the parent took from the environment keeps that provenance in the
// fork, env stamp included.
func TestFork_ProvenancePreserved(t *testing.T) {
	t.Setenv("MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY", "from-env")

	parent, err := Load("", nil)
	require.NoError(t, err)
	require.Equal(t, param.SourceEnv, parent.Milvus.Storage.Auth.SecretAccessKey.Used.Kind)

	forked, err := parent.Fork(map[string]string{"restore.keepTempFiles": "true"})
	require.NoError(t, err)

	assert.Equal(t, "from-env", forked.Milvus.Storage.Auth.SecretAccessKey.Val)
	assert.Equal(t, param.Used{Kind: param.SourceEnv, Key: "milvus_storage_auth_secret_access_key"},
		forked.Milvus.Storage.Auth.SecretAccessKey.Used)
}

func TestFork_ListOverride(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	forked, err := parent.Fork(map[string]string{"milvus.etcd.endpoints": "a:2379, b:2379"})
	require.NoError(t, err)

	assert.Equal(t, []string{"a:2379", "b:2379"}, forked.Milvus.Etcd.Endpoints.Val)
	assert.Equal(t, param.Used{Kind: param.SourceOverride, Key: "milvus.etcd.endpoints"}, forked.Milvus.Etcd.Endpoints.Used)
	assert.Equal(t, []string{"localhost:2379"}, parent.Milvus.Etcd.Endpoints.Val)
}

func TestFork_ParentUnchanged(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	_, err = parent.Fork(map[string]string{"backup.storage.rootPath": "req-path"})
	require.NoError(t, err)

	assert.Equal(t, "backup", parent.Backup.Storage.RootPath.Val)
	assert.Equal(t, param.SourceDefault, parent.Backup.Storage.RootPath.Used.Kind)
}

// A fork shares no mutable state with its parent: writing into either one
// leaves the other alone.
func TestFork_DeepCopyIndependence(t *testing.T) {
	parent, err := Load("", nil)
	require.NoError(t, err)

	forked, err := parent.Fork(nil)
	require.NoError(t, err)

	forked.Backup.Storage.RootPath.Val = "changed"
	forked.Milvus.Etcd.Endpoints.Val[0] = "changed:2379"

	assert.Equal(t, "backup", parent.Backup.Storage.RootPath.Val)
	assert.Equal(t, []string{"localhost:2379"}, parent.Milvus.Etcd.Endpoints.Val)
}
