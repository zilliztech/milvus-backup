package jobstate

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func encryptionConfig(t *testing.T) cfg.PersistenceConfig {
	t.Helper()
	c := cfg.New().Restore.Persistence
	c.Enabled.Val = true
	c.Type.Val = "local"
	c.Local.Directory.Val = t.TempDir()
	c.Encryption.Enabled.Val = true
	c.Encryption.Provider.Val = "config"
	c.Encryption.Config.KeyID.Val = "test-key"
	c.Encryption.Config.Key.Val = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32))
	return c
}

func TestEncryptedPersistence(t *testing.T) {
	ctx := t.Context()
	c := encryptionConfig(t)
	b, err := openPersistenceBackend(c)
	require.NoError(t, err)
	require.NoError(t, b.Check(ctx))
	disk := localPersistenceBackend{Directory: c.Local.Directory.Val}
	value := []byte("secret task payload")
	save := func() encryptedRecord {
		require.NoError(t, b.Save(ctx, "job", value))
		raw, err := disk.Load(ctx, "job")
		require.NoError(t, err)
		assert.NotContains(t, string(raw), string(value))
		var r encryptedRecord
		require.NoError(t, json.Unmarshal(raw, &r))
		return r
	}
	first, second := save(), save()
	assert.NotEqual(t, first.Data, second.Data)
	keys, err := newKeyProvider(c.Encryption)
	require.NoError(t, err)
	a, err := keys.Unwrap(ctx, first.Key)
	require.NoError(t, err)
	next, err := keys.Unwrap(ctx, second.Key)
	require.NoError(t, err)
	assert.NotEqual(t, a, next)
	put := func(key string, r encryptedRecord) {
		raw, err := json.Marshal(r)
		require.NoError(t, err)
		require.NoError(t, disk.Save(ctx, key, raw))
	}
	got, err := b.Load(ctx, "job")
	require.NoError(t, err)
	assert.Equal(t, value, got)
	put("other", second)
	_, err = b.Load(ctx, "other")
	assert.Error(t, err)
	for _, tc := range []struct {
		name string
		edit func(*encryptedRecord)
	}{
		{"Version", func(r *encryptedRecord) { r.Version++ }},
		{"KeyID", func(r *encryptedRecord) { r.Key.KeyID = "other" }},
		{"WrappedKey", func(r *encryptedRecord) { r.Key.Ciphertext = bytes.Clone(r.Key.Ciphertext); r.Key.Ciphertext[0] ^= 1 }},
		{"Payload", func(r *encryptedRecord) { r.Data = bytes.Clone(r.Data); r.Data[0] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := second
			tc.edit(&r)
			put("job", r)
			_, err := b.Load(ctx, "job")
			assert.Error(t, err)
		})
	}
	put("job", second)
	c.Encryption.Config.Key.Val = base64.StdEncoding.EncodeToString(make([]byte, 32))
	wrong, err := openPersistenceBackend(c)
	require.NoError(t, err)
	_, err = wrong.Load(ctx, "job")
	assert.Error(t, err)
	require.NoError(t, disk.Save(ctx, "job", []byte(`{"Version":1}`)))
	_, err = b.Load(ctx, "job")
	assert.Error(t, err)
	require.NoError(t, b.Delete(ctx, "job"))
	_, err = b.Load(ctx, "job")
	assert.ErrorIs(t, err, ErrPersistenceKeyNotFound)
}

func TestColdResumeEncryptedState(t *testing.T) {
	_, tr, _ := persistenceFixture(t)
	config := encryptionConfig(t)
	s := NewStore()
	s.ConfigurePersistence(config)
	tr.task.persistence = s
	tr.task.plan.Restore.Persistence = config
	tr.Finish(assert.AnError)
	key := restoreStateKey("job")
	data, err := localPersistenceBackend{Directory: config.Local.Directory.Val}.Load(t.Context(), key)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "remote-job")
	for _, enabled := range []bool{true, false} {
		tr.task.plan.Restore.Persistence.Encryption.Enabled.Val = enabled
		encoded, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), config.Encryption.Config.Key.Val)
	}
	cold := NewStore()
	cold.ConfigurePersistence(config)
	owner, _, err := cold.StartResume("job")
	require.NoError(t, err)
	restart := NewStore()
	restart.ConfigurePersistence(config)
	_, err = restart.GetRestoreTask("job")
	assert.ErrorIs(t, err, ErrRestoreConflict)
	owner.Finish(nil)
	_, err = restart.GetRestoreTask("job")
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestInvalidEncryptionConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*cfg.PersistenceEncryptionConfig)
	}{
		{"UnsupportedProvider", func(c *cfg.PersistenceEncryptionConfig) { c.Provider.Val = "unknown" }},
		{"MissingID", func(c *cfg.PersistenceEncryptionConfig) { c.Config.KeyID.Val = "" }},
		{"InvalidBase64", func(c *cfg.PersistenceEncryptionConfig) { c.Config.Key.Val = "!!!!" }},
		{"ShortKey", func(c *cfg.PersistenceEncryptionConfig) { c.Config.Key.Val = "YQ==" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := encryptionConfig(t)
			tc.edit(&config.Encryption)
			b, err := openPersistenceBackend(config)
			assert.Error(t, err)
			assert.Nil(t, b)
		})
	}
}

func TestEnvEncryptionStartupCheck(t *testing.T) {
	config := encryptionConfig(t) // Valid YAML keys must not satisfy the env provider.
	config.Encryption.Provider.Val = "env"
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{17}, 32))
	for _, tc := range []struct {
		name, id, key string
		want          []string
	}{
		{"MissingID", "", key, []string{envMasterKeyID + " is required"}},
		{"MissingKey", "from-env", "", []string{envMasterKey + " is required"}},
		{"MissingBoth", "", "", []string{envMasterKeyID + " is required", envMasterKey + " is required"}},
		{"MalformedKey", "from-env", "invalid-secret", []string{"base64"}},
		{"ShortKey", "from-env", "YQ==", []string{"32-byte"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envMasterKeyID, tc.id)
			t.Setenv(envMasterKey, tc.key)
			store := NewStore()
			store.ConfigurePersistence(config)
			err := store.CheckPersistence(t.Context())
			require.Error(t, err)
			for _, want := range tc.want {
				assert.ErrorContains(t, err, want)
			}
			if tc.key != "" {
				assert.NotContains(t, err.Error(), tc.key)
			}
		})
	}
	t.Setenv(envMasterKeyID, "from-env")
	t.Setenv(envMasterKey, key)
	store := NewStore()
	store.ConfigurePersistence(config)
	require.NoError(t, store.CheckPersistence(t.Context()))
	// A fresh config provider using the env credentials can read the same record.
	backend, _, err := store.persistenceStore()
	require.NoError(t, err)
	value := []byte("saved progress")
	require.NoError(t, backend.Save(t.Context(), "job", value))
	config.Encryption.Provider.Val = "config"
	reader, err := openPersistenceBackend(config)
	require.NoError(t, err)
	_, err = reader.Load(t.Context(), "job")
	assert.Error(t, err)
	config.Encryption.Config.KeyID.Val = "from-env"
	config.Encryption.Config.Key.Val = key
	reader, err = openPersistenceBackend(config)
	require.NoError(t, err)
	got, err := reader.Load(t.Context(), "job")
	require.NoError(t, err)
	assert.Equal(t, value, got)
}
