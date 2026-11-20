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
	Enabled param.Value[bool]
	Type    param.Value[string]
	Local   struct{ Directory param.Value[string] }
	S3      struct {
		Endpoint, Region, Bucket, Prefix           param.Value[string]
		UseSSL                                     param.Value[bool]
		AccessKeyID, SecretAccessKey, SessionToken param.Value[string]
	}
	Encryption PersistenceEncryptionConfig
}

func newPersistenceConfig() PersistenceConfig {
	var c PersistenceConfig
	c.Enabled = param.Value[bool]{Default: true, Keys: []string{"restore.persistence.enabled"}}
	c.Type = param.Value[string]{Default: "local", Keys: []string{"restore.persistence.type"}}
	c.Local.Directory = param.Value[string]{Default: "restore-state", Keys: []string{"restore.persistence.local.directory"}}
	c.S3.Endpoint = param.Value[string]{Default: "localhost:9000", Keys: []string{"restore.persistence.s3.endpoint"}}
	c.S3.Region = param.Value[string]{Keys: []string{"restore.persistence.s3.region"}}
	c.S3.Bucket = param.Value[string]{Default: "backup-state", Keys: []string{"restore.persistence.s3.bucket"}}
	c.S3.Prefix = param.Value[string]{Default: "restore/", Keys: []string{"restore.persistence.s3.prefix"}}
	c.S3.UseSSL = param.Value[bool]{Default: true, Keys: []string{"restore.persistence.s3.useSSL"}}
	c.S3.AccessKeyID = param.Value[string]{Keys: []string{"restore.persistence.s3.accessKeyID"}, EnvKeys: []string{"RESTORE_PERSISTENCE_S3_ACCESS_KEY_ID"}, Opts: param.SecretValue}
	c.S3.SecretAccessKey = param.Value[string]{Keys: []string{"restore.persistence.s3.secretAccessKey"}, EnvKeys: []string{"RESTORE_PERSISTENCE_S3_SECRET_ACCESS_KEY"}, Opts: param.SecretValue}
	c.S3.SessionToken = param.Value[string]{Keys: []string{"restore.persistence.s3.sessionToken"}, EnvKeys: []string{"RESTORE_PERSISTENCE_S3_SESSION_TOKEN"}, Opts: param.SecretValue}
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
		&c.Type,
		&c.Local.Directory,
		&c.S3.Endpoint,
		&c.S3.Region,
		&c.S3.Bucket,
		&c.S3.Prefix,
		&c.S3.UseSSL,
		&c.S3.AccessKeyID,
		&c.S3.SecretAccessKey,
		&c.S3.SessionToken,
		&c.Encryption.Enabled,
		&c.Encryption.Provider,
		&c.Encryption.Config.KeyID,
		&c.Encryption.Config.Key,
	)
}
