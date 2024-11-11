package storage

// Option for setting params used by chunk manager client.
type StorageConfig struct {
	StorageType       string
	Address           string
	BucketName        string
	AccessKeyID       string
	SecretAccessKeyID string
	GcpCredentialJSON string
	UseSSL            bool
	CreateBucket      bool
	RootPath          string
	UseIAM            bool
	IAMEndpoint       string

	backupAccessKeyID       string
	backupSecretAccessKeyID string
	BackupGcpCredentialJSON string
	backupBucketName        string
	backupRootPath          string
}

func newDefaultConfig() *StorageConfig {
	return &StorageConfig{}
}

// Option is used to StorageConfig the retry function.
type Option func(*StorageConfig)

func Address(addr string) Option {
	return func(c *StorageConfig) {
		c.Address = addr
	}
}

func BucketName(bucketName string) Option {
	return func(c *StorageConfig) {
		c.BucketName = bucketName
	}
}

func AccessKeyID(accessKeyID string) Option {
	return func(c *StorageConfig) {
		c.AccessKeyID = accessKeyID
	}
}
func SecretAccessKeyID(secretAccessKeyID string) Option {
	return func(c *StorageConfig) {
		c.SecretAccessKeyID = secretAccessKeyID
	}
}

func GcpCredentialJSON(gcpCredentialJSON string) Option {
	return func(c *StorageConfig) {
		c.GcpCredentialJSON = gcpCredentialJSON
	}
}

func UseSSL(useSSL bool) Option {
	return func(c *StorageConfig) {
		c.UseSSL = useSSL
	}
}

func CreateBucket(createBucket bool) Option {
	return func(c *StorageConfig) {
		c.CreateBucket = createBucket
	}
}

func UseIAM(useIAM bool) Option {
	return func(c *StorageConfig) {
		c.UseIAM = useIAM
	}
}

func IAMEndpoint(iamEndpoint string) Option {
	return func(c *StorageConfig) {
		c.IAMEndpoint = iamEndpoint
	}
}
