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
	c.Storage.Provider.Val = "local"
	c.Storage.RootPath.Val = t.TempDir()
	c.Encryption.Enabled.Val = true
	c.Encryption.KeyID.Val = "test-key"
	c.Encryption.Key.Val = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32))
	return c
}

func TestEncryptedPersistence(t *testing.T) {
	ctx := t.Context()
	c := encryptionConfig(t)
	b, err := openPersistenceBackend(c)
	require.NoError(t, err)
	require.NoError(t, b.Check(ctx))
	disk := localPersistenceBackend{Directory: c.Storage.RootPath.Val}
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
	c.Encryption.Key.Val = base64.StdEncoding.EncodeToString(make([]byte, 32))
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
	data, err := localPersistenceBackend{Directory: config.Storage.RootPath.Val}.Load(t.Context(), key)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "remote-job")
	for _, enabled := range []bool{true, false} {
		tr.task.plan.Restore.Persistence.Encryption.Enabled.Val = enabled
		encoded, err := tr.task.encodeLocked(markerAvailable, persistenceLocationFromConfig(config))
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), config.Encryption.Key.Val)
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

func TestEncryptionStartupCheck(t *testing.T) {
	validKey := encryptionConfig(t).Encryption.Key.Val
	for _, tc := range []struct {
		name, id, key string
		want          []string
	}{
		{"MissingID", "", validKey, []string{"keyID is required"}},
		{"MissingKey", "id", "", []string{"key is required"}},
		{"MissingBoth", "", "", []string{"keyID is required", "key is required"}},
		{"MalformedKey", "id", "invalid-secret", []string{"base64"}},
		{"ShortKey", "id", "YQ==", []string{"32-byte"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := encryptionConfig(t)
			config.Encryption.KeyID.Val = tc.id
			config.Encryption.Key.Val = tc.key
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
}

func TestEncryptedRecordAcrossKeySources(t *testing.T) {
	c := encryptionConfig(t)
	writer, err := openPersistenceBackend(c)
	require.NoError(t, err)
	require.NoError(t, writer.Save(t.Context(), "job", []byte("saved progress")))

	t.Setenv("RESTORE_PERSISTENCE_ENCRYPTION_KEY_ID", c.Encryption.KeyID.Val)
	t.Setenv("RESTORE_PERSISTENCE_ENCRYPTION_KEY", c.Encryption.Key.Val)
	loaded, err := cfg.Load("", map[string]string{
		"restore.persistence.enabled":            "true",
		"restore.persistence.storage.rootPath":   c.Storage.RootPath.Val,
		"restore.persistence.encryption.enabled": "true",
	})
	require.NoError(t, err)
	reader, err := openPersistenceBackend(loaded.Restore.Persistence)
	require.NoError(t, err)
	require.NoError(t, reader.Check(t.Context()))
	got, err := reader.Load(t.Context(), "job")
	require.NoError(t, err)
	assert.Equal(t, []byte("saved progress"), got)
}
