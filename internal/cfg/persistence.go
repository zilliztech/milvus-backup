package cfg

import (
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

type PersistenceEncryptionConfig struct {
	Enabled  param.Value[bool]
	Provider param.Value[string]
	Config   struct{ KeyID, Key param.Value[string] }
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
	e.Provider = param.Value[string]{Default: "config", Keys: []string{"restore.persistence.encryption.provider"}}
	e.Config.KeyID.Keys = []string{"restore.persistence.encryption.config.keyID"}
	e.Config.Key = param.Value[string]{Keys: []string{"restore.persistence.encryption.config.key"}, Opts: param.SecretValue}
	return c
}
func (c *PersistenceConfig) Resolve(s *param.Source) error {
	return param.Resolve(
		s,
		&c.Enabled,
		&c.Storage,
		&c.Encryption.Enabled,
		&c.Encryption.Provider,
		&c.Encryption.Config.KeyID,
		&c.Encryption.Config.Key,
	)
}
