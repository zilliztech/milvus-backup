package cfg

import (
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

type PersistenceEncryptionConfig struct {
	Enabled param.Value[bool]
	KeyID   param.Value[string]
	Key     param.Value[string]
}

type PersistenceConfig struct {
	Enabled    param.Value[bool]
	Storage    StorageConfig
	Encryption PersistenceEncryptionConfig
}

func newPersistenceConfig() PersistenceConfig {
	var c PersistenceConfig
	c.Enabled = param.Value[bool]{Default: false, Keys: []string{"restore.persistence.enabled"}}
	c.Storage = newStorageConfig("restore.persistence.storage", "RESTORE_PERSISTENCE_STORAGE")
	c.Storage.Provider.Default = ProviderLocal
	c.Storage.RootPath.Default = "restore-state"
	e := &c.Encryption
	e.Enabled.Keys = []string{"restore.persistence.encryption.enabled"}
	e.KeyID = param.Value[string]{
		Keys:    []string{"restore.persistence.encryption.keyID"},
		EnvKeys: []string{"RESTORE_PERSISTENCE_ENCRYPTION_KEY_ID"},
	}
	e.Key = param.Value[string]{
		Keys:    []string{"restore.persistence.encryption.key"},
		EnvKeys: []string{"RESTORE_PERSISTENCE_ENCRYPTION_KEY"},
		Opts:    param.SecretValue,
	}
	return c
}
func (c *PersistenceConfig) Resolve(s *param.Source) error {
	return param.Resolve(
		s,
		&c.Enabled,
		&c.Storage,
		&c.Encryption.Enabled,
		&c.Encryption.KeyID,
		&c.Encryption.Key,
	)
}
