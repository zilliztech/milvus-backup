package cfg

import (
	"cmp"
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
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
	Log      LogConfig
	HTTP     HTTPConfig
	Cloud    CloudConfig
	Milvus   MilvusConfig
	Transfer TransferConfig
	Backup   BackupConfig
	Restore  RestoreConfig
}

func New() *Config {
	return &Config{
		Log:      newLogConfig(),
		HTTP:     newHTTPConfig(),
		Cloud:    newCloudConfig(),
		Milvus:   newMilvusConfig(),
		Transfer: newTransferConfig(),
		Backup:   newBackupConfig(),
		Restore:  newRestoreConfig(),
	}
}

func (c *Config) Resolve(s *source) error {
	if err := resolve(s, &c.Log, &c.HTTP, &c.Cloud, &c.Milvus); err != nil {
		return err
	}
	c.Backup.Storage.inherit(&c.Milvus.Storage)
	return resolve(s, &c.Transfer, &c.Backup, &c.Restore)
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

// WriteTable prints all configuration parameters in a table showing the
// parameter name, current value (secrets masked), the source the value came
// from (override, env, config, default), and the source key.
func (c *Config) WriteTable(w io.Writer) error {
	// Render into a strings.Builder first, whose Write never fails, so the
	// only error-returning I/O is the single write and flush to the underlying
	// writer below.
	var sb strings.Builder
	fmt.Fprintln(&sb, "PARAMETER\tVALUE\tSOURCE\tSOURCE_KEY\tSTATUS")
	fmt.Fprintln(&sb, "---------\t-----\t------\t----------\t------")
	for _, e := range c.Entries() {
		status := ""
		if e.Deprecated {
			status = "deprecated; use " + e.Replacement
		}
		fmt.Fprintf(&sb, "%s\t%s\t%s\t%s\t%s\n", e.Name, e.Value, e.Source, e.SourceKey, status)
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := io.WriteString(tw, sb.String()); err != nil {
		return fmt.Errorf("cfg: write config table: %w", err)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("cfg: flush config table: %w", err)
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
	Enabled         Value[bool]
	DebugMode       Value[bool]
	SwaggerBasePath Value[string]
}

func newHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Enabled:         Value[bool]{Default: true, Keys: []string{"http.enabled"}},
		DebugMode:       Value[bool]{Default: false, Keys: []string{"http.debugMode"}},
		SwaggerBasePath: Value[string]{Default: "", Keys: []string{"http.swaggerBasePath"}},
	}
}

func (c *HTTPConfig) Resolve(s *source) error {
	return resolve(s, &c.Enabled, &c.DebugMode, &c.SwaggerBasePath)
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

	Etcd    EtcdConfig
	Storage StorageConfig
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
		Storage: newMilvusStorageConfig(),
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
		&c.Storage,
	)
}

// StorageConfig describes one storage endpoint. It is shared by the Milvus
// source storage and the backup destination storage.
type StorageConfig struct {
	Provider Value[string]
	Address  Value[string]
	Port     Value[int]
	Region   Value[string]

	AccessKeyID       Value[string]
	SecretAccessKey   Value[string]
	Token             Value[string]
	GcpCredentialJSON Value[string]

	UseSSL      Value[bool]
	BucketName  Value[string]
	RootPath    Value[string]
	UseIAM      Value[bool]
	IAMEndpoint Value[string]
}

func newMilvusStorageConfig() StorageConfig {
	return StorageConfig{
		Provider: Value[string]{Default: "minio", Keys: []string{"milvus.storage.provider", "storage.storageType", "minio.storageType", "minio.cloudProvider"}, EnvKeys: []string{"MILVUS_STORAGE_PROVIDER"}},
		Address:  Value[string]{Default: "localhost", Keys: []string{"milvus.storage.address", "minio.address"}, EnvKeys: []string{"MILVUS_STORAGE_ADDRESS", "MINIO_ADDRESS"}},
		Port:     Value[int]{Default: 9000, Keys: []string{"milvus.storage.port", "minio.port"}, EnvKeys: []string{"MILVUS_STORAGE_PORT", "MINIO_PORT"}},
		Region:   Value[string]{Default: "", Keys: []string{"milvus.storage.region", "minio.region"}, EnvKeys: []string{"MILVUS_STORAGE_REGION", "MINIO_REGION"}},

		AccessKeyID:       Value[string]{Default: "minioadmin", Keys: []string{"milvus.storage.accessKeyID", "minio.accessKeyID"}, EnvKeys: []string{"MILVUS_STORAGE_ACCESS_KEY", "MINIO_ACCESS_KEY"}},
		SecretAccessKey:   Value[string]{Default: "minioadmin", Keys: []string{"milvus.storage.secretAccessKey", "minio.secretAccessKey"}, EnvKeys: []string{"MILVUS_STORAGE_SECRET_KEY", "MINIO_SECRET_KEY"}, Opts: SecretValue},
		Token:             Value[string]{Default: "", Keys: []string{"milvus.storage.token", "minio.token"}, EnvKeys: []string{"MILVUS_STORAGE_TOKEN", "MINIO_TOKEN"}, Opts: SecretValue},
		GcpCredentialJSON: Value[string]{Default: "", Keys: []string{"milvus.storage.gcpCredentialJSON", "minio.gcpCredentialJSON"}, EnvKeys: []string{"MILVUS_STORAGE_GCP_KEY_JSON", "GCP_KEY_JSON"}, Opts: SecretValue},

		UseSSL:      Value[bool]{Default: false, Keys: []string{"milvus.storage.useSSL", "minio.useSSL"}, EnvKeys: []string{"MILVUS_STORAGE_USE_SSL", "MINIO_USE_SSL"}},
		BucketName:  Value[string]{Default: "a-bucket", Keys: []string{"milvus.storage.bucketName", "minio.bucketName"}, EnvKeys: []string{"MILVUS_STORAGE_BUCKET_NAME", "MINIO_BUCKET_NAME"}},
		RootPath:    Value[string]{Default: "files", Keys: []string{"milvus.storage.rootPath", "minio.rootPath"}, EnvKeys: []string{"MILVUS_STORAGE_ROOT_PATH", "MINIO_ROOT_PATH"}},
		UseIAM:      Value[bool]{Default: false, Keys: []string{"milvus.storage.useIAM", "minio.useIAM"}, EnvKeys: []string{"MILVUS_STORAGE_USE_IAM", "MINIO_USE_IAM"}},
		IAMEndpoint: Value[string]{Default: "", Keys: []string{"milvus.storage.iamEndpoint", "minio.iamEndpoint"}, EnvKeys: []string{"MILVUS_STORAGE_IAM_ENDPOINT", "MINIO_IAM_ENDPOINT"}},
	}
}

func newBackupStorageConfig() StorageConfig {
	return StorageConfig{
		Provider: Value[string]{Keys: []string{"backup.storage.provider", "storage.backupStorageType", "minio.backupStorageType"}, EnvKeys: []string{"BACKUP_STORAGE_PROVIDER"}},
		Address:  Value[string]{Keys: []string{"backup.storage.address", "minio.backupAddress"}, EnvKeys: []string{"BACKUP_STORAGE_ADDRESS", "MINIO_BACKUP_ADDRESS"}},
		Port:     Value[int]{Keys: []string{"backup.storage.port", "minio.backupPort"}, EnvKeys: []string{"BACKUP_STORAGE_PORT", "MINIO_BACKUP_PORT"}},
		Region:   Value[string]{Keys: []string{"backup.storage.region", "minio.backupRegion"}, EnvKeys: []string{"BACKUP_STORAGE_REGION", "MINIO_BACKUP_REGION"}},

		AccessKeyID:       Value[string]{Keys: []string{"backup.storage.accessKeyID", "minio.backupAccessKeyID"}, EnvKeys: []string{"BACKUP_STORAGE_ACCESS_KEY", "MINIO_BACKUP_ACCESS_KEY"}},
		SecretAccessKey:   Value[string]{Keys: []string{"backup.storage.secretAccessKey", "minio.backupSecretAccessKey"}, EnvKeys: []string{"BACKUP_STORAGE_SECRET_KEY", "MINIO_BACKUP_SECRET_KEY"}, Opts: SecretValue},
		Token:             Value[string]{Keys: []string{"backup.storage.token", "minio.backupToken"}, EnvKeys: []string{"BACKUP_STORAGE_TOKEN", "MINIO_BACKUP_TOKEN"}, Opts: SecretValue},
		GcpCredentialJSON: Value[string]{Keys: []string{"backup.storage.gcpCredentialJSON", "minio.backupGcpCredentialJSON"}, EnvKeys: []string{"BACKUP_STORAGE_GCP_KEY_JSON", "BACKUP_GCP_KEY_JSON"}, Opts: SecretValue},

		UseSSL:      Value[bool]{Keys: []string{"backup.storage.useSSL", "minio.backupUseSSL"}, EnvKeys: []string{"BACKUP_STORAGE_USE_SSL", "MINIO_BACKUP_USE_SSL"}},
		BucketName:  Value[string]{Keys: []string{"backup.storage.bucketName", "minio.backupBucketName"}, EnvKeys: []string{"BACKUP_STORAGE_BUCKET_NAME", "MINIO_BACKUP_BUCKET_NAME"}},
		RootPath:    Value[string]{Keys: []string{"backup.storage.rootPath", "minio.backupRootPath"}, EnvKeys: []string{"BACKUP_STORAGE_ROOT_PATH", "MINIO_BACKUP_ROOT_PATH"}},
		UseIAM:      Value[bool]{Keys: []string{"backup.storage.useIAM", "minio.backupUseIAM"}, EnvKeys: []string{"BACKUP_STORAGE_USE_IAM", "MINIO_BACKUP_USE_IAM"}},
		IAMEndpoint: Value[string]{Keys: []string{"backup.storage.iamEndpoint", "minio.backupIamEndpoint"}, EnvKeys: []string{"BACKUP_STORAGE_IAM_ENDPOINT", "MINIO_BACKUP_IAM_ENDPOINT"}},
	}
}

func (c *StorageConfig) inherit(source *StorageConfig) {
	c.Provider.Default = source.Provider.Val
	c.Address.Default = source.Address.Val
	c.Port.Default = source.Port.Val
	c.Region.Default = source.Region.Val
	c.AccessKeyID.Default = source.AccessKeyID.Val
	c.SecretAccessKey.Default = source.SecretAccessKey.Val
	c.Token.Default = source.Token.Val
	c.GcpCredentialJSON.Default = source.GcpCredentialJSON.Val
	c.UseSSL.Default = source.UseSSL.Val
	c.BucketName.Default = source.BucketName.Val
	c.RootPath.Default = cmp.Or(source.RootPath.Val, "backup")
	c.UseIAM.Default = source.UseIAM.Val
	c.IAMEndpoint.Default = source.IAMEndpoint.Val
}

func (c *StorageConfig) Resolve(s *source) error {
	return resolve(s,
		&c.Provider, &c.Address, &c.Port, &c.Region,
		&c.AccessKeyID, &c.SecretAccessKey, &c.Token, &c.GcpCredentialJSON,
		&c.UseSSL, &c.BucketName, &c.RootPath, &c.UseIAM, &c.IAMEndpoint,
	)
}

const (
	TransferModeAuto      = "auto"
	TransferModeDirect    = "direct"
	TransferModeStreaming = "streaming"
)

type TransferConfig struct {
	// Mode controls how objects move between the two storage endpoints:
	// auto chooses conservatively, direct uses the storage COPY API, and
	// streaming downloads and uploads through the milvus-backup process.
	Mode Value[string]

	// Concurrency limits the number of objects being copied at the same time.
	Concurrency Value[int]

	// MultipartCopyThresholdMiB is the file size threshold above which multipart copy is used.
	MultipartCopyThresholdMiB Value[int64]
}

func newTransferConfig() TransferConfig {
	return TransferConfig{
		Mode: Value[string]{Default: TransferModeAuto, Keys: []string{"transfer.mode", "backup.transfer.mode"}, EnvKeys: []string{"TRANSFER_MODE", "BACKUP_TRANSFER_MODE"}},
		Concurrency: Value[int]{
			Default: 128,
			Keys:    []string{"transfer.concurrency", "backup.parallelism.copydata"},
			EnvKeys: []string{"TRANSFER_CONCURRENCY", "BACKUP_PARALLELISM_COPYDATA"},
		},
		MultipartCopyThresholdMiB: Value[int64]{
			Default: 500,
			Keys:    []string{"transfer.multipartCopyThresholdMiB", "backup.transfer.multipartCopyThresholdMiB", "minio.multipartCopyThresholdMiB"},
			EnvKeys: []string{"TRANSFER_MULTIPART_COPY_THRESHOLD_MIB", "BACKUP_TRANSFER_MULTIPART_COPY_THRESHOLD_MIB"},
		},
	}
}

func (c *TransferConfig) Resolve(s *source) error {
	// Resolve the canonical mode first. The legacy crossStorage boolean maps to
	// streaming=true and auto=false, matching the historical automatic fallback
	// when providers differ.
	canonical := c.Mode
	canonical.Default = ""
	if err := canonical.Resolve(s); err != nil {
		return err
	}
	if canonical.Used.Kind != SourceDefault {
		c.Mode = canonical
	} else {
		legacy := Value[bool]{Default: false, Keys: []string{"minio.crossStorage"}}
		if err := legacy.Resolve(s); err != nil {
			return err
		}
		c.Mode.Val = TransferModeAuto
		c.Mode.Used = Used{Kind: SourceDefault}
		if legacy.Used.Kind != SourceDefault {
			c.Mode.Used = legacy.Used
			if legacy.Val {
				c.Mode.Val = TransferModeStreaming
			}
		}
	}

	switch c.Mode.Val {
	case TransferModeAuto, TransferModeDirect, TransferModeStreaming:
	default:
		return fmt.Errorf("cfg: unsupported transfer.mode %q (want auto, direct, or streaming)", c.Mode.Val)
	}

	if err := resolve(s, &c.Concurrency, &c.MultipartCopyThresholdMiB); err != nil {
		return err
	}
	if c.Concurrency.Val <= 0 {
		return fmt.Errorf("cfg: transfer.concurrency must be greater than zero")
	}
	return nil
}

type BackupConcurrencyConfig struct {
	Collections Value[int]
	Segments    Value[int]
}

type BackupGCPauseConfig struct {
	Enable  Value[bool]
	Address Value[string]
}

type BackupConfig struct {
	Storage     StorageConfig
	Concurrency BackupConcurrencyConfig
	GCPause     BackupGCPauseConfig
}

func newBackupConfig() BackupConfig {
	return BackupConfig{
		Storage: newBackupStorageConfig(),
		Concurrency: BackupConcurrencyConfig{
			Collections: Value[int]{Default: 4, Keys: []string{"backup.concurrency.collections", "backup.parallelism.backupCollection"}, EnvKeys: []string{"BACKUP_CONCURRENCY_COLLECTIONS", "BACKUP_PARALLELISM_BACKUP_COLLECTION"}},
			Segments:    Value[int]{Default: 1024, Keys: []string{"backup.concurrency.segments", "backup.parallelism.backupSegment"}, EnvKeys: []string{"BACKUP_CONCURRENCY_SEGMENTS"}},
		},
		GCPause: BackupGCPauseConfig{
			Enable:  Value[bool]{Default: true, Keys: []string{"backup.gcPause.enable"}, EnvKeys: []string{"BACKUP_GC_PAUSE_ENABLE"}},
			Address: Value[string]{Default: "http://localhost:9091", Keys: []string{"backup.gcPause.address"}, EnvKeys: []string{"BACKUP_GC_PAUSE_ADDRESS"}},
		},
	}
}

func (c *BackupConfig) Resolve(s *source) error {
	if err := resolve(s,
		&c.Storage,
		&c.Concurrency.Collections, &c.Concurrency.Segments,
		&c.GCPause.Enable, &c.GCPause.Address,
	); err != nil {
		return err
	}
	if c.Concurrency.Collections.Val <= 0 || c.Concurrency.Segments.Val <= 0 {
		return fmt.Errorf("cfg: backup concurrency values must be greater than zero")
	}
	return nil
}

type RestoreConcurrencyConfig struct {
	Collections Value[int]
	ImportJobs  Value[int]
}

type RestoreConfig struct {
	Concurrency   RestoreConcurrencyConfig
	KeepTempFiles Value[bool]
}

func newRestoreConfig() RestoreConfig {
	return RestoreConfig{
		Concurrency: RestoreConcurrencyConfig{
			Collections: Value[int]{Default: 2, Keys: []string{"restore.concurrency.collections", "backup.parallelism.restoreCollection"}, EnvKeys: []string{"RESTORE_CONCURRENCY_COLLECTIONS", "BACKUP_PARALLELISM_RESTORE_COLLECTION"}},
			ImportJobs:  Value[int]{Default: 768, Keys: []string{"restore.concurrency.importJobs", "backup.parallelism.importJob"}, EnvKeys: []string{"RESTORE_CONCURRENCY_IMPORT_JOBS"}},
		},
		KeepTempFiles: Value[bool]{Default: false, Keys: []string{"restore.keepTempFiles", "backup.keepTempFiles"}, EnvKeys: []string{"RESTORE_KEEP_TEMP_FILES", "BACKUP_KEEP_TEMP_FILES"}},
	}
}

func (c *RestoreConfig) Resolve(s *source) error {
	if err := resolve(s,
		&c.Concurrency.Collections, &c.Concurrency.ImportJobs,
		&c.KeepTempFiles,
	); err != nil {
		return err
	}
	if c.Concurrency.Collections.Val <= 0 || c.Concurrency.ImportJobs.Val <= 0 {
		return fmt.Errorf("cfg: restore concurrency values must be greater than zero")
	}
	return nil
}
