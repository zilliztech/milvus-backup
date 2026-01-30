package cfg

type Config struct {
	Log    LogConfig
	HTTP   HTTPConfig
	Cloud  CloudConfig
	Milvus MilvusConfig
	Minio  MinioConfig
	Backup BackupConfig
}

func New() *Config {
	return &Config{
		Log:    newLogConfig(),
		HTTP:   newHTTPConfig(),
		Cloud:  newCloudConfig(),
		Milvus: newMilvusConfig(),
		Minio:  newMinioConfig(),
		Backup: newBackupConfig(),
	}
}

func (c *Config) Resolve(s *source) error {
	if err := c.Log.Resolve(s); err != nil {
		return err
	}
	if err := c.HTTP.Resolve(s); err != nil {
		return err
	}
	if err := c.Cloud.Resolve(s); err != nil {
		return err
	}
	if err := c.Milvus.Resolve(s); err != nil {
		return err
	}
	if err := c.Minio.Resolve(s); err != nil {
		return err
	}
	if err := c.Backup.Resolve(s); err != nil {
		return err
	}
	return nil
}

type LogFileConfig struct {
	Filename   Value[string]
	MaxSize    Value[int]
	MaxDays    Value[int]
	MaxBackups Value[int]
}

type LogConfig struct {
	Level   Value[string]
	Console Value[bool]
	File    LogFileConfig
}

func newLogConfig() LogConfig {
	return LogConfig{
		Level:   Value[string]{Default: "info", Keys: []string{"log.level"}, EnvKeys: []string{"LOG_LEVEL"}},
		Console: Value[bool]{Default: true, Keys: []string{"log.console"}, EnvKeys: []string{"LOG_CONSOLE"}},
		File: LogFileConfig{
			Filename:   Value[string]{Default: "logs/backup.log", Keys: []string{"log.file.filename"}, EnvKeys: []string{"LOG_FILE_PATH"}},
			MaxSize:    Value[int]{Default: 300, Keys: []string{"log.file.maxSize"}},
			MaxDays:    Value[int]{Default: 0, Keys: []string{"log.file.maxDays"}},
			MaxBackups: Value[int]{Default: 0, Keys: []string{"log.file.maxBackups"}},
		},
	}
}

func (c *LogConfig) Resolve(s *source) error {
	if err := c.Level.Resolve(s); err != nil {
		return err
	}
	if err := c.Console.Resolve(s); err != nil {
		return err
	}
	if err := c.File.Filename.Resolve(s); err != nil {
		return err
	}
	if err := c.File.MaxSize.Resolve(s); err != nil {
		return err
	}
	if err := c.File.MaxDays.Resolve(s); err != nil {
		return err
	}
	if err := c.File.MaxBackups.Resolve(s); err != nil {
		return err
	}
	return nil
}

type HTTPConfig struct {
	Enabled   Value[bool]
	DebugMode Value[bool]
}

func newHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Enabled:   Value[bool]{Default: true, Keys: []string{"http.enabled"}},
		DebugMode: Value[bool]{Default: false, Keys: []string{"http.debug_mode"}},
	}
}

func (c *HTTPConfig) Resolve(s *source) error {
	if err := c.Enabled.Resolve(s); err != nil {
		return err
	}
	if err := c.DebugMode.Resolve(s); err != nil {
		return err
	}
	return nil
}

type CloudConfig struct {
	Address Value[string]
	APIKey  Value[string]
}

func newCloudConfig() CloudConfig {
	return CloudConfig{
		Address: Value[string]{Default: "https://api.cloud.zilliz.com", Keys: []string{"cloud.address"}},
		APIKey:  (Value[string]{Default: "", Keys: []string{"cloud.apikey"}}).WithOptions(SecretValue),
	}
}

func (c *CloudConfig) Resolve(s *source) error {
	if err := c.Address.Resolve(s); err != nil {
		return err
	}
	if err := c.APIKey.Resolve(s); err != nil {
		return err
	}
	return nil
}

type EtcdConfig struct {
	Endpoints Value[string]
	RootPath  Value[string]
}

type MilvusConfig struct {
	Address Value[string]
	Port    Value[int]

	User     Value[string]
	Password Value[string]

	TLSMode Value[int]

	CACertPath Value[string]
	ServerName Value[string]

	MTLSCertPath Value[string]
	MTLSKeyPath  Value[string]

	RPCChannelName Value[string]

	Etcd EtcdConfig
}

func newMilvusConfig() MilvusConfig {
	return MilvusConfig{
		Address: Value[string]{Default: "localhost", Keys: []string{"milvus.address"}, EnvKeys: []string{"MILVUS_ADDRESS"}},
		Port:    Value[int]{Default: 19530, Keys: []string{"milvus.port"}, EnvKeys: []string{"MILVUS_PORT"}},

		User:     Value[string]{Default: "", Keys: []string{"milvus.user"}, EnvKeys: []string{"MILVUS_USER"}},
		Password: (Value[string]{Default: "", Keys: []string{"milvus.password"}, EnvKeys: []string{"MILVUS_PASSWORD"}}).WithOptions(SecretValue),

		TLSMode: Value[int]{Default: 0, Keys: []string{"milvus.tlsMode"}, EnvKeys: []string{"MILVUS_TLS_MODE"}},

		CACertPath: Value[string]{Default: "", Keys: []string{"milvus.caCertPath"}, EnvKeys: []string{"MILVUS_CA_CERT_PATH"}},
		ServerName: Value[string]{Default: "", Keys: []string{"milvus.serverName"}, EnvKeys: []string{"MILVUS_SERVER_NAME"}},

		MTLSCertPath: Value[string]{Default: "", Keys: []string{"milvus.mtlsCertPath"}, EnvKeys: []string{"MILVUS_MTLS_CERT_PATH"}},
		MTLSKeyPath:  Value[string]{Default: "", Keys: []string{"milvus.mtlsKeyPath"}, EnvKeys: []string{"MILVUS_MTLS_KEY_PATH"}},

		RPCChannelName: Value[string]{Default: "by-dev-replicate-msg", Keys: []string{"milvus.rpcChannelName"}, EnvKeys: []string{"MILVUS_RPC_CHANNEL_NAME"}},

		Etcd: EtcdConfig{
			Endpoints: Value[string]{Default: "localhost:2379", Keys: []string{"milvus.etcd.endpoints"}},
			RootPath:  Value[string]{Default: "by-dev", Keys: []string{"milvus.etcd.rootPath"}},
		},
	}
}

func (c *MilvusConfig) Resolve(s *source) error {
	if err := c.Address.Resolve(s); err != nil {
		return err
	}
	if err := c.Port.Resolve(s); err != nil {
		return err
	}
	if err := c.User.Resolve(s); err != nil {
		return err
	}
	if err := c.Password.Resolve(s); err != nil {
		return err
	}
	if err := c.TLSMode.Resolve(s); err != nil {
		return err
	}
	if err := c.CACertPath.Resolve(s); err != nil {
		return err
	}
	if err := c.ServerName.Resolve(s); err != nil {
		return err
	}
	if err := c.MTLSCertPath.Resolve(s); err != nil {
		return err
	}
	if err := c.MTLSKeyPath.Resolve(s); err != nil {
		return err
	}
	if err := c.RPCChannelName.Resolve(s); err != nil {
		return err
	}
	if err := c.Etcd.Endpoints.Resolve(s); err != nil {
		return err
	}
	if err := c.Etcd.RootPath.Resolve(s); err != nil {
		return err
	}
	return nil
}

type MinioConfig struct {
	StorageType Value[string]

	Address Value[string]
	Port    Value[int]
	Region  Value[string]

	AccessKeyID       Value[string]
	SecretAccessKey   Value[string]
	Token             Value[string]
	GcpCredentialJSON Value[string]

	UseSSL      Value[bool]
	BucketName  Value[string]
	RootPath    Value[string]
	UseIAM      Value[bool]
	IAMEndpoint Value[string]

	BackupStorageType Value[string]
	BackupAddress     Value[string]
	BackupPort        Value[int]
	BackupRegion      Value[string]

	BackupAccessKeyID       Value[string]
	BackupSecretAccessKey   Value[string]
	BackupToken             Value[string]
	BackupGcpCredentialJSON Value[string]
	BackupUseSSL            Value[bool]
	BackupBucketName        Value[string]
	BackupRootPath          Value[string]
	BackupUseIAM            Value[bool]
	BackupIAMEndpoint       Value[string]

	CrossStorage Value[bool]
}

func newMinioConfig() MinioConfig {
	// Defaults align with configs/backup.yaml and old paramtable constants.
	return MinioConfig{
		StorageType: Value[string]{Default: "minio", Keys: []string{"minio.storageType", "minio.cloudProvider"}},

		Address: Value[string]{Default: "localhost", Keys: []string{"minio.address"}, EnvKeys: []string{"MINIO_ADDRESS"}},
		Port:    Value[int]{Default: 9000, Keys: []string{"minio.port"}, EnvKeys: []string{"MINIO_PORT"}},
		Region:  Value[string]{Default: "", Keys: []string{"minio.region"}, EnvKeys: []string{"MINIO_REGION"}},

		AccessKeyID:       Value[string]{Default: "minioadmin", Keys: []string{"minio.accessKeyID"}, EnvKeys: []string{"MINIO_ACCESS_KEY"}},
		SecretAccessKey:   (Value[string]{Default: "minioadmin", Keys: []string{"minio.secretAccessKey"}, EnvKeys: []string{"MINIO_SECRET_KEY"}}).WithOptions(SecretValue),
		Token:             (Value[string]{Default: "", Keys: []string{"minio.token"}, EnvKeys: []string{"MINIO_TOKEN"}}).WithOptions(SecretValue),
		GcpCredentialJSON: (Value[string]{Default: "", Keys: []string{"minio.gcpCredentialJSON"}, EnvKeys: []string{"GCP_KEY_JSON"}}).WithOptions(SecretValue),

		UseSSL:      Value[bool]{Default: false, Keys: []string{"minio.useSSL"}, EnvKeys: []string{"MINIO_USE_SSL"}},
		BucketName:  Value[string]{Default: "a-bucket", Keys: []string{"minio.bucketName"}, EnvKeys: []string{"MINIO_BUCKET_NAME"}},
		RootPath:    Value[string]{Default: "files", Keys: []string{"minio.rootPath"}, EnvKeys: []string{"MINIO_ROOT_PATH"}},
		UseIAM:      Value[bool]{Default: false, Keys: []string{"minio.useIAM"}, EnvKeys: []string{"MINIO_USE_IAM"}},
		IAMEndpoint: Value[string]{Default: "", Keys: []string{"minio.iamEndpoint"}, EnvKeys: []string{"MINIO_IAM_ENDPOINT"}},

		// Backup fields default to corresponding "milvus storage" values at resolve time.
		BackupStorageType: Value[string]{Default: "", Keys: []string{"storage.backupStorageType", "minio.backupStorageType"}},
		BackupAddress:     Value[string]{Default: "", Keys: []string{"minio.backupAddress"}, EnvKeys: []string{"MINIO_BACKUP_ADDRESS"}},
		BackupPort:        Value[int]{Default: 0, Keys: []string{"minio.backupPort"}, EnvKeys: []string{"MINIO_BACKUP_PORT"}},
		BackupRegion:      Value[string]{Default: "", Keys: []string{"minio.backupRegion"}, EnvKeys: []string{"MINIO_BACKUP_REGION"}},

		BackupAccessKeyID:       Value[string]{Default: "", Keys: []string{"minio.backupAccessKeyID"}, EnvKeys: []string{"MINIO_BACKUP_ACCESS_KEY"}},
		BackupSecretAccessKey:   (Value[string]{Default: "", Keys: []string{"minio.backupSecretAccessKey"}, EnvKeys: []string{"MINIO_BACKUP_SECRET_KEY"}}).WithOptions(SecretValue),
		BackupToken:             (Value[string]{Default: "", Keys: []string{"minio.backupToken"}, EnvKeys: []string{"MINIO_BACKUP_TOKEN"}}).WithOptions(SecretValue),
		BackupGcpCredentialJSON: (Value[string]{Default: "", Keys: []string{"minio.backupGcpCredentialJSON"}, EnvKeys: []string{"BACKUP_GCP_KEY_JSON"}}).WithOptions(SecretValue),
		BackupUseSSL:            Value[bool]{Default: false, Keys: []string{"minio.backupUseSSL"}, EnvKeys: []string{"MINIO_BACKUP_USE_SSL"}},
		BackupBucketName:        Value[string]{Default: "", Keys: []string{"minio.backupBucketName"}, EnvKeys: []string{"MINIO_BACKUP_BUCKET_NAME"}},
		BackupRootPath:          Value[string]{Default: "", Keys: []string{"minio.backupRootPath"}, EnvKeys: []string{"MINIO_BACKUP_ROOT_PATH"}},
		BackupUseIAM:            Value[bool]{Default: false, Keys: []string{"minio.backupUseIAM"}, EnvKeys: []string{"MINIO_BACKUP_USE_IAM"}},
		BackupIAMEndpoint:       Value[string]{Default: "", Keys: []string{"minio.backupIamEndpoint"}, EnvKeys: []string{"MINIO_BACKUP_IAM_ENDPOINT"}},

		CrossStorage: Value[bool]{Default: false, Keys: []string{"minio.crossStorage"}},
	}
}

func (c *MinioConfig) Resolve(s *source) error {
	// Resolve "milvus storage" fields first.
	primary := []interface {
		Resolve(*source) error
	}{
		&c.StorageType,
		&c.Address,
		&c.Port,
		&c.Region,
		&c.AccessKeyID,
		&c.SecretAccessKey,
		&c.Token,
		&c.GcpCredentialJSON,
		&c.UseSSL,
		&c.BucketName,
		&c.RootPath,
		&c.UseIAM,
		&c.IAMEndpoint,
	}
	for _, f := range primary {
		if err := f.Resolve(s); err != nil {
			return err
		}
	}

	// Backward-compatible defaults: if backup configs are not provided, inherit from primary configs.
	if c.BackupStorageType.Default == "" {
		c.BackupStorageType.Default = c.StorageType.Value()
	}
	if c.BackupAddress.Default == "" {
		c.BackupAddress.Default = c.Address.Value()
	}
	if c.BackupPort.Default == 0 {
		c.BackupPort.Default = c.Port.Value()
	}
	if c.BackupRegion.Default == "" {
		c.BackupRegion.Default = c.Region.Value()
	}
	if c.BackupAccessKeyID.Default == "" {
		c.BackupAccessKeyID.Default = c.AccessKeyID.Value()
	}
	if c.BackupSecretAccessKey.Default == "" {
		c.BackupSecretAccessKey.Default = c.SecretAccessKey.Value()
	}
	if c.BackupToken.Default == "" {
		c.BackupToken.Default = c.Token.Value()
	}
	if !c.BackupUseSSL.Default {
		c.BackupUseSSL.Default = c.UseSSL.Value()
	}
	if c.BackupBucketName.Default == "" {
		c.BackupBucketName.Default = c.BucketName.Value()
	}
	if c.BackupRootPath.Default == "" {
		// old default: minio.backupRootPath -> minio.rootPath -> "backup"
		c.BackupRootPath.Default = c.RootPath.Value()
		if c.BackupRootPath.Default == "" {
			c.BackupRootPath.Default = "backup"
		}
	}
	if !c.BackupUseIAM.Default {
		c.BackupUseIAM.Default = c.UseIAM.Value()
	}
	if c.BackupIAMEndpoint.Default == "" {
		c.BackupIAMEndpoint.Default = c.IAMEndpoint.Value()
	}

	backup := []interface {
		Resolve(*source) error
	}{
		&c.BackupStorageType,
		&c.BackupAddress,
		&c.BackupPort,
		&c.BackupRegion,
		&c.BackupAccessKeyID,
		&c.BackupSecretAccessKey,
		&c.BackupToken,
		&c.BackupGcpCredentialJSON,
		&c.BackupUseSSL,
		&c.BackupBucketName,
		&c.BackupRootPath,
		&c.BackupUseIAM,
		&c.BackupIAMEndpoint,
		&c.CrossStorage,
	}
	for _, f := range backup {
		if err := f.Resolve(s); err != nil {
			return err
		}
	}
	return nil
}

type BackupParallelismConfig struct {
	CopyData          Value[int]
	BackupCollection  Value[int]
	BackupSegment     Value[int]
	RestoreCollection Value[int]
	ImportJob         Value[int]
}

type BackupGCPauseConfig struct {
	Enable  Value[bool]
	Address Value[string]
}

type BackupConfig struct {
	Parallelism   BackupParallelismConfig
	KeepTempFiles Value[bool]
	GCPause       BackupGCPauseConfig
}

func newBackupConfig() BackupConfig {
	return BackupConfig{
		Parallelism: BackupParallelismConfig{
			CopyData:          Value[int]{Default: 128, Keys: []string{"backup.parallelism.copydata"}, EnvKeys: []string{"BACKUP_PARALLELISM_COPYDATA"}},
			BackupCollection:  Value[int]{Default: 4, Keys: []string{"backup.parallelism.backupCollection"}, EnvKeys: []string{"BACKUP_PARALLELISM_BACKUP_COLLECTION"}},
			BackupSegment:     Value[int]{Default: 1024, Keys: []string{"backup.parallelism.backupSegment"}},
			RestoreCollection: Value[int]{Default: 2, Keys: []string{"backup.parallelism.restoreCollection"}, EnvKeys: []string{"BACKUP_PARALLELISM_RESTORE_COLLECTION"}},
			ImportJob:         Value[int]{Default: 768, Keys: []string{"backup.parallelism.importJob"}},
		},
		KeepTempFiles: Value[bool]{Default: false, Keys: []string{"backup.keepTempFiles"}, EnvKeys: []string{"BACKUP_KEEP_TEMP_FILES"}},
		GCPause: BackupGCPauseConfig{
			Enable:  Value[bool]{Default: true, Keys: []string{"backup.gcPause.enable"}, EnvKeys: []string{"BACKUP_GC_PAUSE_ENABLE"}},
			Address: Value[string]{Default: "http://localhost:9091", Keys: []string{"backup.gcPause.address"}, EnvKeys: []string{"BACKUP_GC_PAUSE_ADDRESS"}},
		},
	}
}

func (c *BackupConfig) Resolve(s *source) error {
	fields := []interface {
		Resolve(*source) error
	}{
		&c.Parallelism.CopyData,
		&c.Parallelism.BackupCollection,
		&c.Parallelism.BackupSegment,
		&c.Parallelism.RestoreCollection,
		&c.Parallelism.ImportJob,
		&c.KeepTempFiles,
		&c.GCPause.Enable,
		&c.GCPause.Address,
	}
	for _, f := range fields {
		if err := f.Resolve(s); err != nil {
			return err
		}
	}
	return nil
}
