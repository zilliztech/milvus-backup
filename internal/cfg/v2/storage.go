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

	// SourceSASToken is a read-scoped SAS for this account's container, handed
	// to Milvus as extfs.source_sas_token when a snapshot-format copy reads
	// across Azure storage accounts: no single credential can authorize reading
	// another account's blobs, so the source read needs one. It applies to the
	// side the copy reads from — the milvus storage for a backup, the backup
	// storage for a restore — and is independent of the auth type. Empty means
	// milvus-backup mints one from this account's own credential for the
	// duration of the task.
	SourceSASToken param.Value[string]

	// BucketName is the bucket data lives in, or the container for Azure.
	BucketName param.Value[string]
	RootPath   param.Value[string]

	// LocalPath is the path the Milvus process itself sees as its
	// localStorage.path, for the local provider. milvus-backup writes to
	// rootPath (the host view of the same directory); when the two differ, such
	// as a container bind-mount, the import paths handed to Milvus must use the
	// path Milvus resolves. Empty means the same as rootPath.
	LocalPath param.Value[string]

	// MilvusAddress and MilvusPort name the endpoint the Milvus server itself
	// uses to reach this storage, when it differs from the address and port
	// milvus-backup connects to: a container port mapping, a private link, or
	// an internal DNS name gives one store two endpoints. Snapshot URIs handed
	// to Milvus must name the endpoint Milvus resolves, since Milvus connects
	// to it. An empty milvusAddress means no override; a zero milvusPort means
	// the same as port.
	MilvusAddress param.Value[string]
	MilvusPort    param.Value[int]

	Auth StorageAuthConfig
}

// newStorageConfig builds a storage section rooted at keyPrefix, with the
// credential environment variables rooted at envPrefix. Only the credentials
// carry one: see the package documentation for why.
func newStorageConfig(keyPrefix, envPrefix string) StorageConfig {
	key := func(name string) []string { return []string{keyPrefix + "." + name} }
	env := func(name string) []string { return []string{envPrefix + "_" + name} }

	return StorageConfig{
		Provider: param.Value[string]{Default: ProviderMinio, Keys: key("provider")},

		Address: param.Value[string]{Default: "localhost", Keys: key("address")},
		Port:    param.Value[int]{Default: 9000, Keys: key("port")},
		Region:  param.Value[string]{Default: "", Keys: key("region")},
		UseSSL:  param.Value[bool]{Default: false, Keys: key("useSSL")},

		// The account name is half of the Azure credential: it is what the
		// account key belongs to, and deployments hand out the two together.
		AccountName: param.Value[string]{Default: "", Keys: key("accountName"), EnvKeys: env("ACCOUNT_NAME")},

		SourceSASToken: param.Value[string]{Default: "", Keys: key("sourceSASToken"), EnvKeys: env("SOURCE_SAS_TOKEN"), Opts: param.SecretValue},

		BucketName: param.Value[string]{Default: "a-bucket", Keys: key("bucketName")},
		RootPath:   param.Value[string]{Default: "files", Keys: key("rootPath")},
		LocalPath:  param.Value[string]{Default: "", Keys: key("localPath")},

		MilvusAddress: param.Value[string]{Default: "", Keys: key("milvusAddress")},
		MilvusPort:    param.Value[int]{Default: 0, Keys: key("milvusPort")},

		Auth: StorageAuthConfig{
			Type: param.Value[string]{Default: AuthStatic, Keys: key("auth.type")},

			AccessKeyID:     param.Value[string]{Default: "minioadmin", Keys: key("auth.accessKeyID"), EnvKeys: env("AUTH_ACCESS_KEY_ID")},
			SecretAccessKey: param.Value[string]{Default: "minioadmin", Keys: key("auth.secretAccessKey"), EnvKeys: env("AUTH_SECRET_ACCESS_KEY"), Opts: param.SecretValue},
			SessionToken:    param.Value[string]{Default: "", Keys: key("auth.sessionToken"), EnvKeys: env("AUTH_SESSION_TOKEN"), Opts: param.SecretValue},

			AccountKey: param.Value[string]{Default: "", Keys: key("auth.accountKey"), EnvKeys: env("AUTH_ACCOUNT_KEY"), Opts: param.SecretValue},

			CredentialsFile: param.Value[string]{Default: "", Keys: key("auth.credentialsFile"), EnvKeys: env("AUTH_CREDENTIALS_FILE")},

			Endpoint: param.Value[string]{Default: "", Keys: key("auth.endpoint")},
		},
	}
}

// inherit makes the resolved values of other the defaults of c, so a config
// that only names what differs still describes a complete backend. RootPath is
// deliberately left alone: backup data does not belong under the Milvus root
// path. The Milvus-view endpoint override is left alone too: it describes how
// one specific Milvus reaches the store, which says nothing about another.
// SourceSASToken is left alone as well: it grants reads of one account's
// container, so inheriting it into a section that names another account would
// hand Milvus a token for the wrong one.
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
		&c.AccountName, &c.SourceSASToken, &c.BucketName, &c.RootPath, &c.LocalPath,
		&c.MilvusAddress, &c.MilvusPort,
		&c.Auth.Type,
		&c.Auth.AccessKeyID, &c.Auth.SecretAccessKey, &c.Auth.SessionToken,
		&c.Auth.AccountKey,
		&c.Auth.CredentialsFile,
		&c.Auth.Endpoint,
	)
}
