package v2

import "github.com/zilliztech/milvus-backup/internal/cfg/param"

// StorageAuthConfig models authentication explicitly instead of forcing every
// provider into one access-key shaped field set. Type selects which of the
// remaining fields apply; validate.go rejects the rest.
type StorageAuthConfig struct {
	Type param.Value[string]

	// AccessKeyID and SecretAccessKey hold S3-compatible static credentials,
	// including the GCP interoperability access ID and secret.
	AccessKeyID     param.Value[string]
	SecretAccessKey param.Value[string]
	SessionToken    param.Value[string]

	// AccountKey is the Azure storage account key. The account name lives on
	// the storage config, since every Azure auth type needs it to build the
	// blob service URL.
	AccountKey param.Value[string]

	// CredentialsFile is the GCP service account credentials JSON file.
	CredentialsFile param.Value[string]

	// Endpoint is the IAM endpoint credentials are fetched from.
	Endpoint param.Value[string]
}

// StorageConfig describes one storage backend: the Milvus deployment storage,
// or the destination backup data is kept in.
type StorageConfig struct {
	Provider param.Value[string]

	Address param.Value[string]
	Port    param.Value[int]
	Region  param.Value[string]
	UseSSL  param.Value[bool]

	// AccountName is the Azure storage account, and is not used by any other
	// provider.
	AccountName param.Value[string]

	// BucketName is the bucket data lives in, or the container for Azure.
	BucketName param.Value[string]
	RootPath   param.Value[string]

	Auth StorageAuthConfig
}

// newStorageConfig builds a storage section rooted at keyPrefix, with
// environment variables rooted at envPrefix.
func newStorageConfig(keyPrefix, envPrefix string) StorageConfig {
	key := func(name string) []string { return []string{keyPrefix + "." + name} }
	env := func(name string) []string { return []string{envPrefix + "_" + name} }

	return StorageConfig{
		Provider: param.Value[string]{Default: ProviderMinio, Keys: key("provider"), EnvKeys: env("PROVIDER")},

		Address: param.Value[string]{Default: "localhost", Keys: key("address"), EnvKeys: env("ADDRESS")},
		Port:    param.Value[int]{Default: 9000, Keys: key("port"), EnvKeys: env("PORT")},
		Region:  param.Value[string]{Default: "", Keys: key("region"), EnvKeys: env("REGION")},
		UseSSL:  param.Value[bool]{Default: false, Keys: key("useSSL"), EnvKeys: env("USE_SSL")},

		AccountName: param.Value[string]{Default: "", Keys: key("accountName"), EnvKeys: env("ACCOUNT_NAME")},

		BucketName: param.Value[string]{Default: "a-bucket", Keys: key("bucketName"), EnvKeys: env("BUCKET_NAME")},
		RootPath:   param.Value[string]{Default: "files", Keys: key("rootPath"), EnvKeys: env("ROOT_PATH")},

		Auth: StorageAuthConfig{
			Type: param.Value[string]{Default: AuthStatic, Keys: key("auth.type"), EnvKeys: env("AUTH_TYPE")},

			AccessKeyID:     param.Value[string]{Default: "minioadmin", Keys: key("auth.accessKeyID"), EnvKeys: env("AUTH_ACCESS_KEY_ID")},
			SecretAccessKey: param.Value[string]{Default: "minioadmin", Keys: key("auth.secretAccessKey"), EnvKeys: env("AUTH_SECRET_ACCESS_KEY"), Opts: param.SecretValue},
			SessionToken:    param.Value[string]{Default: "", Keys: key("auth.sessionToken"), EnvKeys: env("AUTH_SESSION_TOKEN"), Opts: param.SecretValue},

			AccountKey: param.Value[string]{Default: "", Keys: key("auth.accountKey"), EnvKeys: env("AUTH_ACCOUNT_KEY"), Opts: param.SecretValue},

			CredentialsFile: param.Value[string]{Default: "", Keys: key("auth.credentialsFile"), EnvKeys: env("AUTH_CREDENTIALS_FILE")},

			Endpoint: param.Value[string]{Default: "", Keys: key("auth.endpoint"), EnvKeys: env("AUTH_ENDPOINT")},
		},
	}
}

// inherit makes the resolved values of other the defaults of c, so a config
// that only names what differs still describes a complete backend. RootPath is
// deliberately left alone: backup data does not belong under the Milvus root
// path.
func (c *StorageConfig) inherit(other *StorageConfig) {
	c.Provider.Default = other.Provider.Val

	c.Address.Default = other.Address.Val
	c.Port.Default = other.Port.Val
	c.Region.Default = other.Region.Val
	c.UseSSL.Default = other.UseSSL.Val

	c.AccountName.Default = other.AccountName.Val
	c.BucketName.Default = other.BucketName.Val

	c.Auth.Type.Default = other.Auth.Type.Val
	c.Auth.AccessKeyID.Default = other.Auth.AccessKeyID.Val
	c.Auth.SecretAccessKey.Default = other.Auth.SecretAccessKey.Val
	c.Auth.SessionToken.Default = other.Auth.SessionToken.Val
	c.Auth.AccountKey.Default = other.Auth.AccountKey.Val
	c.Auth.CredentialsFile.Default = other.Auth.CredentialsFile.Val
	c.Auth.Endpoint.Default = other.Auth.Endpoint.Val
}

func (c *StorageConfig) Resolve(s *param.Source) error {
	return param.Resolve(s,
		&c.Provider,
		&c.Address, &c.Port, &c.Region, &c.UseSSL,
		&c.AccountName, &c.BucketName, &c.RootPath,
		&c.Auth.Type,
		&c.Auth.AccessKeyID, &c.Auth.SecretAccessKey, &c.Auth.SessionToken,
		&c.Auth.AccountKey,
		&c.Auth.CredentialsFile,
		&c.Auth.Endpoint,
	)
}
