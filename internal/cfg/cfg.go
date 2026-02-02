package cfg

import (
	"cmp"
	"reflect"
)

type Resolver interface {
	Resolve(*source) error
}

func resolve(s *source, rs ...Resolver) error {
	for _, r := range rs {
		if err := r.Resolve(s); err != nil {
			return err
		}
	}
	return nil
}

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
	return resolve(s, &c.Log, &c.HTTP, &c.Cloud, &c.Milvus, &c.Minio, &c.Backup)
}

func (c *Config) Entries() []Entry {
	var (
		entries       []Entry
		displayerType = reflect.TypeOf((*Displayer)(nil)).Elem()
		collect       func(v reflect.Value, prefix string)
	)

	collect = func(v reflect.Value, prefix string) {
		t := v.Type()
		for i := range v.NumField() {
			field := v.Field(i)
			name := t.Field(i).Name
			if prefix != "" {
				name = prefix + "." + name
			}

			if field.Addr().Type().Implements(displayerType) {
				entries = append(entries, field.Addr().Interface().(Displayer).Display(name))
				continue
			}

			if field.Kind() == reflect.Struct {
				collect(field, name)
			}
		}
	}

	collect(reflect.ValueOf(c).Elem(), "")
	return entries
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
		Level:   Value[string]{Default: "info", Keys: []string{"log.level"}},
		Console: Value[bool]{Default: true, Keys: []string{"log.console"}},
		File: LogFileConfig{
			Filename:   Value[string]{Default: "logs/backup.log", Keys: []string{"log.file.filename"}},
			MaxSize:    Value[int]{Default: 300, Keys: []string{"log.file.maxSize"}},
			MaxDays:    Value[int]{Default: 0, Keys: []string{"log.file.maxDays"}},
			MaxBackups: Value[int]{Default: 0, Keys: []string{"log.file.maxBackups"}},
		},
	}
}

func (c *LogConfig) Resolve(s *source) error {
	return resolve(s, &c.Level, &c.Console, &c.File.Filename, &c.File.MaxSize, &c.File.MaxDays, &c.File.MaxBackups)
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
	return resolve(s, &c.Enabled, &c.DebugMode)
}

type CloudConfig struct {
	Address Value[string]
	APIKey  Value[string]
}

func newCloudConfig() CloudConfig {
	return CloudConfig{
		Address: Value[string]{Default: "https://api.cloud.zilliz.com", Keys: []string{"cloud.address"}},
		APIKey:  Value[string]{Default: "", Keys: []string{"cloud.apikey"}, Opts: SecretValue},
	}
}

func (c *CloudConfig) Resolve(s *source) error {
	return resolve(s, &c.Address, &c.APIKey)
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
		Password: Value[string]{Default: "", Keys: []string{"milvus.password"}, EnvKeys: []string{"MILVUS_PASSWORD"}, Opts: SecretValue},

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
	return resolve(s,
		&c.Address, &c.Port,
		&c.User, &c.Password,
		&c.TLSMode, &c.CACertPath, &c.ServerName,
		&c.MTLSCertPath, &c.MTLSKeyPath,
		&c.RPCChannelName,
		&c.Etcd.Endpoints, &c.Etcd.RootPath,
	)
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
		StorageType: Value[string]{Default: "minio", Keys: []string{"storage.storageType", "minio.storageType", "minio.cloudProvider"}},

		Address: Value[string]{Default: "localhost", Keys: []string{"minio.address"}, EnvKeys: []string{"MINIO_ADDRESS"}},
		Port:    Value[int]{Default: 9000, Keys: []string{"minio.port"}, EnvKeys: []string{"MINIO_PORT"}},
		Region:  Value[string]{Default: "", Keys: []string{"minio.region"}, EnvKeys: []string{"MINIO_REGION"}},

		AccessKeyID:       Value[string]{Default: "minioadmin", Keys: []string{"minio.accessKeyID"}, EnvKeys: []string{"MINIO_ACCESS_KEY"}},
		SecretAccessKey:   Value[string]{Default: "minioadmin", Keys: []string{"minio.secretAccessKey"}, EnvKeys: []string{"MINIO_SECRET_KEY"}, Opts: SecretValue},
		Token:             Value[string]{Default: "", Keys: []string{"minio.token"}, EnvKeys: []string{"MINIO_TOKEN"}, Opts: SecretValue},
		GcpCredentialJSON: Value[string]{Default: "", Keys: []string{"minio.gcpCredentialJSON"}, EnvKeys: []string{"GCP_KEY_JSON"}, Opts: SecretValue},

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
		BackupSecretAccessKey:   Value[string]{Default: "", Keys: []string{"minio.backupSecretAccessKey"}, EnvKeys: []string{"MINIO_BACKUP_SECRET_KEY"}, Opts: SecretValue},
		BackupToken:             Value[string]{Default: "", Keys: []string{"minio.backupToken"}, EnvKeys: []string{"MINIO_BACKUP_TOKEN"}, Opts: SecretValue},
		BackupGcpCredentialJSON: Value[string]{Default: "", Keys: []string{"minio.backupGcpCredentialJSON"}, EnvKeys: []string{"BACKUP_GCP_KEY_JSON"}, Opts: SecretValue},
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
	if err := resolve(s,
		&c.StorageType, &c.Address, &c.Port, &c.Region,
		&c.AccessKeyID, &c.SecretAccessKey, &c.Token, &c.GcpCredentialJSON,
		&c.UseSSL, &c.BucketName, &c.RootPath, &c.UseIAM, &c.IAMEndpoint,
	); err != nil {
		return err
	}

	// Backward-compatible defaults: if backup configs are not provided, inherit from primary configs.
	c.BackupStorageType.Default = cmp.Or(c.BackupStorageType.Default, c.StorageType.Val)
	c.BackupAddress.Default = cmp.Or(c.BackupAddress.Default, c.Address.Val)
	c.BackupPort.Default = cmp.Or(c.BackupPort.Default, c.Port.Val)
	c.BackupRegion.Default = cmp.Or(c.BackupRegion.Default, c.Region.Val)
	c.BackupAccessKeyID.Default = cmp.Or(c.BackupAccessKeyID.Default, c.AccessKeyID.Val)
	c.BackupSecretAccessKey.Default = cmp.Or(c.BackupSecretAccessKey.Default, c.SecretAccessKey.Val)
	c.BackupToken.Default = cmp.Or(c.BackupToken.Default, c.Token.Val)
	c.BackupUseSSL.Default = c.BackupUseSSL.Default || c.UseSSL.Val
	c.BackupBucketName.Default = cmp.Or(c.BackupBucketName.Default, c.BucketName.Val)
	c.BackupRootPath.Default = cmp.Or(c.BackupRootPath.Default, c.RootPath.Val, "backup")
	c.BackupUseIAM.Default = c.BackupUseIAM.Default || c.UseIAM.Val
	c.BackupIAMEndpoint.Default = cmp.Or(c.BackupIAMEndpoint.Default, c.IAMEndpoint.Val)

	return resolve(s,
		&c.BackupStorageType, &c.BackupAddress, &c.BackupPort, &c.BackupRegion,
		&c.BackupAccessKeyID, &c.BackupSecretAccessKey, &c.BackupToken, &c.BackupGcpCredentialJSON,
		&c.BackupUseSSL, &c.BackupBucketName, &c.BackupRootPath, &c.BackupUseIAM, &c.BackupIAMEndpoint,
		&c.CrossStorage,
	)
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
	return resolve(s,
		&c.Parallelism.CopyData, &c.Parallelism.BackupCollection, &c.Parallelism.BackupSegment,
		&c.Parallelism.RestoreCollection, &c.Parallelism.ImportJob,
		&c.KeepTempFiles,
		&c.GCPause.Enable, &c.GCPause.Address,
	)
}
