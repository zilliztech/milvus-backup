// Package v2 implements the v2 milvus-backup configuration schema.
//
// A configuration file is decoded entirely as one schema version: v2 accepts
// only v2 keys, environment variables and --set paths, and rejects everything
// else instead of carrying per-key aliases. See internal/cfg for the v1 schema.
package v2

import (
	"io"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

const (
	// Version is the value of the top-level discriminator this package decodes.
	Version = "v2"
	// VersionKey is the config file key holding the schema version.
	VersionKey = "configVersion"
)

// Storage providers accepted by v2. The v1 spelling aliases (ali, alibaba,
// alicloud, tc) are dropped: each provider has exactly one name.
const (
	ProviderLocal     = "local"
	ProviderMinio     = "minio"
	ProviderS3        = "s3"
	ProviderAWS       = "aws"
	ProviderGCP       = "gcp"
	ProviderGCPNative = "gcpnative"
	ProviderAzure     = "azure"
	ProviderAliyun    = "aliyun"
	ProviderTencent   = "tencent"
	ProviderHwc       = "hwc"
)

// Authentication types. Which ones a provider accepts is decided in validate.go.
const (
	// AuthStatic is an S3-compatible access key ID and secret access key pair,
	// with an optional session token.
	AuthStatic = "static"
	// AuthSharedKey is the Azure storage account key.
	AuthSharedKey = "sharedKey"
	// AuthServiceAccount is a GCP service account credentials file.
	AuthServiceAccount = "serviceAccount"
	// AuthIAM fetches credentials from an IAM endpoint.
	AuthIAM = "iam"
	// AuthDefault uses the provider SDK credential chain: workload identity,
	// instance role, or DefaultAzureCredential.
	AuthDefault = "default"
)

// TLS modes for the Milvus gRPC connection, named after the certificate each
// one verifies. v1 spelled these as the numbers 0, 1 and 2, copied from the
// Milvus server side parameter common.security.tlsMode.
const (
	// TLSDisabled is a plaintext connection. Milvus: tlsMode 0.
	TLSDisabled = "disabled"
	// TLSServer verifies the server certificate. Milvus: tlsMode 1.
	TLSServer = "server"
	// TLSMutual verifies the server certificate and presents a client
	// certificate. Milvus: tlsMode 2.
	TLSMutual = "mutual"
)

// Transfer modes, deciding how bytes move between two storage backends.
const (
	// TransferAuto uses a storage-side copy when both ends are the same
	// backend, and streams through milvus-backup otherwise.
	TransferAuto = "auto"
	// TransferDirect always asks the storage service to copy.
	TransferDirect = "direct"
	// TransferStreaming always downloads from the source and uploads to the
	// destination through milvus-backup.
	TransferStreaming = "streaming"
)

type Config struct {
	Log      LogConfig
	Server   ServerConfig
	Milvus   MilvusConfig
	Backup   BackupConfig
	Restore  RestoreConfig
	Transfer TransferConfig
	Cloud    CloudConfig
}

func New() *Config {
	return &Config{
		Log:      newLogConfig(),
		Server:   newServerConfig(),
		Milvus:   newMilvusConfig(),
		Backup:   newBackupConfig(),
		Restore:  newRestoreConfig(),
		Transfer: newTransferConfig(),
		Cloud:    newCloudConfig(),
	}
}

func (c *Config) Resolve(s *param.Source) error {
	if err := param.Resolve(s, &c.Log, &c.Server, &c.Milvus, &c.Restore, &c.Transfer, &c.Cloud); err != nil {
		return err
	}

	// backup.storage describes a destination that is usually the very backend
	// Milvus already uses, so anything left out falls back to milvus.storage.
	// rootPath is the exception: backup data does not belong under the Milvus
	// root path, so it keeps its own default.
	c.Backup.Storage.inherit(&c.Milvus.Storage)

	return param.Resolve(s, &c.Backup)
}

func (c *Config) Entries() []param.Entry { return param.Entries(c) }

// WriteTable prints all configuration parameters in a table showing the
// parameter name, current value (secrets masked), the source the value came
// from (override, env, config, default), and the source key.
func (c *Config) WriteTable(w io.Writer) error { return param.WriteTable(w, c.Entries()) }

type LogFileConfig struct {
	Path       param.Value[string]
	MaxSizeMiB param.Value[int]
	MaxDays    param.Value[int]
	MaxBackups param.Value[int]
}

type LogConfig struct {
	Level   param.Value[string]
	Console param.Value[bool]
	File    LogFileConfig
}

func newLogConfig() LogConfig {
	return LogConfig{
		Level:   param.Value[string]{Default: "info", Keys: []string{"log.level"}, EnvKeys: []string{"LOG_LEVEL"}},
		Console: param.Value[bool]{Default: true, Keys: []string{"log.console"}, EnvKeys: []string{"LOG_CONSOLE"}},
		File: LogFileConfig{
			Path:       param.Value[string]{Default: "logs/backup.log", Keys: []string{"log.file.path"}, EnvKeys: []string{"LOG_FILE_PATH"}},
			MaxSizeMiB: param.Value[int]{Default: 300, Keys: []string{"log.file.maxSizeMiB"}, EnvKeys: []string{"LOG_FILE_MAX_SIZE_MIB"}},
			MaxDays:    param.Value[int]{Default: 0, Keys: []string{"log.file.maxDays"}, EnvKeys: []string{"LOG_FILE_MAX_DAYS"}},
			MaxBackups: param.Value[int]{Default: 0, Keys: []string{"log.file.maxBackups"}, EnvKeys: []string{"LOG_FILE_MAX_BACKUPS"}},
		},
	}
}

func (c *LogConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.Level, &c.Console, &c.File.Path, &c.File.MaxSizeMiB, &c.File.MaxDays, &c.File.MaxBackups)
}

// ServerConfig configures the milvus-backup HTTP API server itself.
type ServerConfig struct {
	DebugMode       param.Value[bool]
	SwaggerBasePath param.Value[string]
}

func newServerConfig() ServerConfig {
	return ServerConfig{
		DebugMode:       param.Value[bool]{Default: false, Keys: []string{"server.debugMode"}, EnvKeys: []string{"SERVER_DEBUG_MODE"}},
		SwaggerBasePath: param.Value[string]{Default: "", Keys: []string{"server.swaggerBasePath"}, EnvKeys: []string{"SERVER_SWAGGER_BASE_PATH"}},
	}
}

func (c *ServerConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.DebugMode, &c.SwaggerBasePath)
}

// MilvusGrpcConfig is the public gRPC endpoint, used for metadata, collection
// and database operations, flush, RBAC, and legacy import.
type MilvusGrpcConfig struct {
	Address param.Value[string]
	Port    param.Value[int]

	TLSMode param.Value[string]

	CACertPath param.Value[string]
	ServerName param.Value[string]

	MTLSCertPath param.Value[string]
	MTLSKeyPath  param.Value[string]
}

// MilvusRestConfig is the public REST endpoint, used for segment describe
// during backup and for import create/describe during restore. When empty it
// is derived from the gRPC connection, which covers deployments where one
// proxy serves both protocols.
type MilvusRestConfig struct {
	Endpoint param.Value[string]
}

// MilvusManagementConfig is the Milvus internal management HTTP endpoint. It
// serves DataCoord GC pause/resume and RootCoord EZK backup, so it is not
// owned by GC pause alone.
type MilvusManagementConfig struct {
	Endpoint param.Value[string]
}

// MilvusReplicateConfig configures the replicate message channel used by
// secondary restore.
type MilvusReplicateConfig struct {
	RPCChannelName param.Value[string]
}

// MilvusEtcdConfig is direct access to the Milvus metadata store. It is only
// used by features that read extra metadata, such as index extra information
// and dynamic field schemas.
type MilvusEtcdConfig struct {
	Endpoints param.List
	RootPath  param.Value[string]
}

type MilvusConfig struct {
	User     param.Value[string]
	Password param.Value[string]

	Grpc       MilvusGrpcConfig
	Rest       MilvusRestConfig
	Management MilvusManagementConfig
	Replicate  MilvusReplicateConfig
	Etcd       MilvusEtcdConfig
	Storage    StorageConfig
}

func newMilvusConfig() MilvusConfig {
	return MilvusConfig{
		User:     param.Value[string]{Default: "", Keys: []string{"milvus.user"}, EnvKeys: []string{"MILVUS_USER"}},
		Password: param.Value[string]{Default: "", Keys: []string{"milvus.password"}, EnvKeys: []string{"MILVUS_PASSWORD"}, Opts: param.SecretValue},

		Grpc: MilvusGrpcConfig{
			Address: param.Value[string]{Default: "localhost", Keys: []string{"milvus.grpc.address"}, EnvKeys: []string{"MILVUS_GRPC_ADDRESS"}},
			Port:    param.Value[int]{Default: 19530, Keys: []string{"milvus.grpc.port"}, EnvKeys: []string{"MILVUS_GRPC_PORT"}},

			TLSMode: param.Value[string]{Default: TLSDisabled, Keys: []string{"milvus.grpc.tlsMode"}, EnvKeys: []string{"MILVUS_GRPC_TLS_MODE"}},

			CACertPath: param.Value[string]{Default: "", Keys: []string{"milvus.grpc.caCertPath"}, EnvKeys: []string{"MILVUS_GRPC_CA_CERT_PATH"}},
			ServerName: param.Value[string]{Default: "", Keys: []string{"milvus.grpc.serverName"}, EnvKeys: []string{"MILVUS_GRPC_SERVER_NAME"}},

			MTLSCertPath: param.Value[string]{Default: "", Keys: []string{"milvus.grpc.mtlsCertPath"}, EnvKeys: []string{"MILVUS_GRPC_MTLS_CERT_PATH"}},
			MTLSKeyPath:  param.Value[string]{Default: "", Keys: []string{"milvus.grpc.mtlsKeyPath"}, EnvKeys: []string{"MILVUS_GRPC_MTLS_KEY_PATH"}},
		},

		Rest: MilvusRestConfig{
			Endpoint: param.Value[string]{Default: "", Keys: []string{"milvus.rest.endpoint"}, EnvKeys: []string{"MILVUS_REST_ENDPOINT"}},
		},

		Management: MilvusManagementConfig{
			Endpoint: param.Value[string]{Default: "http://localhost:9091", Keys: []string{"milvus.management.endpoint"}, EnvKeys: []string{"MILVUS_MANAGEMENT_ENDPOINT"}},
		},

		Replicate: MilvusReplicateConfig{
			RPCChannelName: param.Value[string]{Default: "by-dev-replicate-msg", Keys: []string{"milvus.replicate.rpcChannelName"}, EnvKeys: []string{"MILVUS_REPLICATE_RPC_CHANNEL_NAME"}},
		},

		Etcd: MilvusEtcdConfig{
			Endpoints: param.List{Default: []string{"localhost:2379"}, Keys: []string{"milvus.etcd.endpoints"}, EnvKeys: []string{"MILVUS_ETCD_ENDPOINTS"}},
			RootPath:  param.Value[string]{Default: "by-dev", Keys: []string{"milvus.etcd.rootPath"}, EnvKeys: []string{"MILVUS_ETCD_ROOT_PATH"}},
		},

		Storage: newStorageConfig("milvus.storage", "MILVUS_STORAGE"),
	}
}

func (c *MilvusConfig) Resolve(s *param.Source) error {
	return param.Resolve(s,
		&c.User, &c.Password,
		&c.Grpc.Address, &c.Grpc.Port,
		&c.Grpc.TLSMode, &c.Grpc.CACertPath, &c.Grpc.ServerName,
		&c.Grpc.MTLSCertPath, &c.Grpc.MTLSKeyPath,
		&c.Rest.Endpoint,
		&c.Management.Endpoint,
		&c.Replicate.RPCChannelName,
		&c.Etcd.Endpoints, &c.Etcd.RootPath,
		&c.Storage,
	)
}

type BackupConcurrencyConfig struct {
	Collections param.Value[int]
	Segments    param.Value[int]
}

type BackupConfig struct {
	Storage     StorageConfig
	Concurrency BackupConcurrencyConfig
	PauseGC     param.Value[bool]
}

func newBackupConfig() BackupConfig {
	storage := newStorageConfig("backup.storage", "BACKUP_STORAGE")
	storage.RootPath.Default = "backup"

	return BackupConfig{
		Storage: storage,
		Concurrency: BackupConcurrencyConfig{
			Collections: param.Value[int]{Default: 4, Keys: []string{"backup.concurrency.collections"}, EnvKeys: []string{"BACKUP_CONCURRENCY_COLLECTIONS"}},
			Segments:    param.Value[int]{Default: 1024, Keys: []string{"backup.concurrency.segments"}, EnvKeys: []string{"BACKUP_CONCURRENCY_SEGMENTS"}},
		},
		PauseGC: param.Value[bool]{Default: true, Keys: []string{"backup.pauseGC"}, EnvKeys: []string{"BACKUP_PAUSE_GC"}},
	}
}

func (c *BackupConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.Storage, &c.Concurrency.Collections, &c.Concurrency.Segments, &c.PauseGC)
}

type RestoreConcurrencyConfig struct {
	Collections param.Value[int]
	ImportJobs  param.Value[int]
}

type RestoreConfig struct {
	Concurrency   RestoreConcurrencyConfig
	KeepTempFiles param.Value[bool]
}

func newRestoreConfig() RestoreConfig {
	return RestoreConfig{
		Concurrency: RestoreConcurrencyConfig{
			Collections: param.Value[int]{Default: 2, Keys: []string{"restore.concurrency.collections"}, EnvKeys: []string{"RESTORE_CONCURRENCY_COLLECTIONS"}},
			// Should be less than the Milvus dataCoord.import.maxImportJobNum.
			ImportJobs: param.Value[int]{Default: 768, Keys: []string{"restore.concurrency.importJobs"}, EnvKeys: []string{"RESTORE_CONCURRENCY_IMPORT_JOBS"}},
		},
		KeepTempFiles: param.Value[bool]{Default: false, Keys: []string{"restore.keepTempFiles"}, EnvKeys: []string{"RESTORE_KEEP_TEMP_FILES"}},
	}
}

func (c *RestoreConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.Concurrency.Collections, &c.Concurrency.ImportJobs, &c.KeepTempFiles)
}

// TransferConfig is the object transfer policy shared by backup, restore,
// check, and migrate.
type TransferConfig struct {
	Mode        param.Value[string]
	Concurrency param.Value[int]

	// MultipartCopyThresholdMiB is the file size above which multipart copy is
	// used. GCP does not support multipart copy and always uses single copy.
	MultipartCopyThresholdMiB param.Value[int64]
}

func newTransferConfig() TransferConfig {
	return TransferConfig{
		Mode:                      param.Value[string]{Default: TransferAuto, Keys: []string{"transfer.mode"}, EnvKeys: []string{"TRANSFER_MODE"}},
		Concurrency:               param.Value[int]{Default: 128, Keys: []string{"transfer.concurrency"}, EnvKeys: []string{"TRANSFER_CONCURRENCY"}},
		MultipartCopyThresholdMiB: param.Value[int64]{Default: 500, Keys: []string{"transfer.multipartCopyThresholdMiB"}, EnvKeys: []string{"TRANSFER_MULTIPART_COPY_THRESHOLD_MIB"}},
	}
}

func (c *TransferConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.Mode, &c.Concurrency, &c.MultipartCopyThresholdMiB)
}

// CloudConfig is the Zilliz Cloud control plane, used to migrate a backup into
// a Zilliz Cloud cluster.
type CloudConfig struct {
	Endpoint param.Value[string]
	APIKey   param.Value[string]
}

func newCloudConfig() CloudConfig {
	return CloudConfig{
		Endpoint: param.Value[string]{Default: "https://api.cloud.zilliz.com", Keys: []string{"zillizCloud.endpoint"}, EnvKeys: []string{"ZILLIZ_CLOUD_ENDPOINT"}},
		APIKey:   param.Value[string]{Default: "", Keys: []string{"zillizCloud.apiKey"}, EnvKeys: []string{"ZILLIZ_CLOUD_API_KEY"}, Opts: param.SecretValue},
	}
}

func (c *CloudConfig) Resolve(s *param.Source) error {
	return param.Resolve(s, &c.Endpoint, &c.APIKey)
}
