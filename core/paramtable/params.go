package paramtable

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// BackupParams is the root configuration structure
type BackupParams struct {
	LogCfg      LogConfig    `yaml:"log" mapstructure:"log"`
	HTTPCfg     HTTPConfig   `yaml:"http" mapstructure:"http"`
	CloudConfig CloudConfig  `yaml:"cloud" mapstructure:"cloud"`
	MilvusCfg   MilvusConfig `yaml:"milvus" mapstructure:"milvus"`
	MinioCfg    MinioConfig  `yaml:"minio" mapstructure:"minio"`
	BackupCfg   BackupCfg    `yaml:"backup" mapstructure:"backup"`
}

type LogConfig struct {
	Level   string        `yaml:"level" mapstructure:"level"`
	Console bool          `yaml:"console" mapstructure:"console"`
	File    LogFileConfig `yaml:"file" mapstructure:"file"`
}

type LogFileConfig struct {
	RootPath   string `yaml:"rootPath" mapstructure:"rootPath"`
	Filename   string `yaml:"filename" mapstructure:"filename"`
	MaxSize    int    `yaml:"maxSize" mapstructure:"maxSize"`
	MaxDays    int    `yaml:"maxDays" mapstructure:"maxDays"`
	MaxBackups int    `yaml:"maxBackups" mapstructure:"maxBackups"`
}

type HTTPConfig struct {
	Enabled        bool `yaml:"enabled" mapstructure:"enabled"`
	DebugMode      bool `yaml:"debug_mode" mapstructure:"debug_mode"`
	SimpleResponse bool `yaml:"simpleResponse" mapstructure:"simpleResponse"`
}

type CloudConfig struct {
	Address string `yaml:"address" mapstructure:"address"`
	APIKey  string `yaml:"apikey" mapstructure:"apikey"`
}

type EtcdConfig struct {
	Endpoints string `yaml:"endpoints" mapstructure:"endpoints"`
	RootPath  string `yaml:"rootPath" mapstructure:"rootPath"`
}

type MilvusConfig struct {
	Address  string `yaml:"address" mapstructure:"address"`
	Port     string `yaml:"port" mapstructure:"port"`
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`

	TLSMode    int    `yaml:"tlsMode" mapstructure:"tlsMode"`
	CACertPath string `yaml:"caCertPath" mapstructure:"caCertPath"`
	ServerName string `yaml:"serverName" mapstructure:"serverName"`

	MTLSCertPath string `yaml:"mtlsCertPath" mapstructure:"mtlsCertPath"`
	MTLSKeyPath  string `yaml:"mtlsKeyPath" mapstructure:"mtlsKeyPath"`

	EtcdConfig EtcdConfig `yaml:"etcd" mapstructure:"etcd"`

	RPCChannelName string `yaml:"rpcChannelName" mapstructure:"rpcChannelName"`
}

type MinioConfig struct {
	StorageType       string `yaml:"storageType" mapstructure:"storageType"`
	CloudProvider     string `yaml:"cloudProvider" mapstructure:"cloudProvider"`
	Address           string `yaml:"address" mapstructure:"address"`
	Port              string `yaml:"port" mapstructure:"port"`
	Region            string `yaml:"region" mapstructure:"region"`
	AccessKeyID       string `yaml:"accessKeyID" mapstructure:"accessKeyID"`
	SecretAccessKey   string `yaml:"secretAccessKey" mapstructure:"secretAccessKey"`
	Token             string `yaml:"token" mapstructure:"token"`
	GcpCredentialJSON string `yaml:"gcpCredentialJSON" mapstructure:"gcpCredentialJSON"`
	UseSSL            bool   `yaml:"useSSL" mapstructure:"useSSL"`
	BucketName        string `yaml:"bucketName" mapstructure:"bucketName"`
	RootPath          string `yaml:"rootPath" mapstructure:"rootPath"`
	UseIAM            bool   `yaml:"useIAM" mapstructure:"useIAM"`
	IAMEndpoint       string `yaml:"iamEndpoint" mapstructure:"iamEndpoint"`

	BackupStorageType       string `yaml:"backupStorageType" mapstructure:"backupStorageType"`
	BackupAddress           string `yaml:"backupAddress" mapstructure:"backupAddress"`
	BackupPort              string `yaml:"backupPort" mapstructure:"backupPort"`
	BackupRegion            string `yaml:"backupRegion" mapstructure:"backupRegion"`
	BackupAccessKeyID       string `yaml:"backupAccessKeyID" mapstructure:"backupAccessKeyID"`
	BackupSecretAccessKey   string `yaml:"backupSecretAccessKey" mapstructure:"backupSecretAccessKey"`
	BackupToken             string `yaml:"backupToken" mapstructure:"backupToken"`
	BackupGcpCredentialJSON string `yaml:"backupGcpCredentialJSON" mapstructure:"backupGcpCredentialJSON"`
	BackupUseSSL            bool   `yaml:"backupUseSSL" mapstructure:"backupUseSSL"`
	BackupBucketName        string `yaml:"backupBucketName" mapstructure:"backupBucketName"`
	BackupRootPath          string `yaml:"backupRootPath" mapstructure:"backupRootPath"`
	BackupUseIAM            bool   `yaml:"backupUseIAM" mapstructure:"backupUseIAM"`
	BackupIAMEndpoint       string `yaml:"backupIamEndpoint" mapstructure:"backupIamEndpoint"`

	CrossStorage bool `yaml:"crossStorage" mapstructure:"crossStorage"`
}

type BackupCfg struct {
	Parallelism   ParallelismConfig `yaml:"parallelism" mapstructure:"parallelism"`
	KeepTempFiles bool              `yaml:"keepTempFiles" mapstructure:"keepTempFiles"`
	GCPause       GCPauseConfig     `yaml:"gcPause" mapstructure:"gcPause"`
}

type ParallelismConfig struct {
	BackupCollection  int `yaml:"backupCollection" mapstructure:"backupCollection"`
	BackupSegment     int `yaml:"backupSegment" mapstructure:"backupSegment"`
	CopyData          int `yaml:"copydata" mapstructure:"copydata"`
	RestoreCollection int `yaml:"restoreCollection" mapstructure:"restoreCollection"`
	ImportJob         int `yaml:"importJob" mapstructure:"importJob"`
}

type GCPauseConfig struct {
	Enable  bool   `yaml:"enable" mapstructure:"enable"`
	Address string `yaml:"address" mapstructure:"address"`
}

// Storage type constants
const (
	Local                     = "local"
	Minio                     = "minio"
	S3                        = "s3"
	CloudProviderAWS          = "aws"
	CloudProviderGCP          = "gcp"
	CloudProviderGCPNative    = "gcpnative"
	CloudProviderAli          = "ali"
	CloudProviderAliyun       = "aliyun"
	CloudProviderAlibaba      = "alibaba"
	CloudProviderAliCloud     = "alicloud"
	CloudProviderAzure        = "azure"
	CloudProviderTencent      = "tencent"
	CloudProviderTencentShort = "tc"
	CloudProviderHwc          = "hwc"
)

var supportedStorageType = map[string]bool{
	Local:                     true,
	Minio:                     true,
	S3:                        true,
	CloudProviderAWS:          true,
	CloudProviderGCP:          true,
	CloudProviderGCPNative:    true,
	CloudProviderAli:          true,
	CloudProviderAliyun:       true,
	CloudProviderAlibaba:      true,
	CloudProviderAliCloud:     true,
	CloudProviderAzure:        true,
	CloudProviderTencent:      true,
	CloudProviderTencentShort: true,
	CloudProviderHwc:          true,
}

// Load loads configuration from YAML file and applies environment variable overrides
func Load(yamlPath string) (*BackupParams, error) {
	cfg := &BackupParams{}
	setDefaults(cfg)

	v := viper.New()
	v.SetConfigFile(yamlPath)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	applyEnvOverrides(cfg)
	applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func setDefaults(cfg *BackupParams) {
	cfg.LogCfg.Level = "info"
	cfg.LogCfg.Console = true
	cfg.LogCfg.File.Filename = "logs/backup.log"
	cfg.LogCfg.File.MaxSize = 300

	cfg.HTTPCfg.Enabled = true

	cfg.CloudConfig.Address = "https://api.cloud.zilliz.com"

	cfg.MilvusCfg.EtcdConfig.Endpoints = "localhost:2379"
	cfg.MilvusCfg.EtcdConfig.RootPath = "by-dev"
	cfg.MilvusCfg.RPCChannelName = "by-dev-replicate-msg"

	cfg.MinioCfg.StorageType = Minio
	cfg.MinioCfg.CloudProvider = CloudProviderAWS
	cfg.MinioCfg.Address = "localhost"
	cfg.MinioCfg.Port = "9000"
	cfg.MinioCfg.AccessKeyID = "minioadmin"
	cfg.MinioCfg.SecretAccessKey = "minioadmin"
	cfg.MinioCfg.BucketName = "a-bucket"
	cfg.MinioCfg.RootPath = "files"
	cfg.MinioCfg.BackupRootPath = "backup"

	cfg.BackupCfg.Parallelism.BackupCollection = 1
	cfg.BackupCfg.Parallelism.BackupSegment = 1024
	cfg.BackupCfg.Parallelism.CopyData = 128
	cfg.BackupCfg.Parallelism.RestoreCollection = 1
	cfg.BackupCfg.Parallelism.ImportJob = 768
	cfg.BackupCfg.GCPause.Address = "http://localhost:9091"
}

func applyDefaults(cfg *BackupParams) {
	// Minio root path should not start with /
	cfg.MinioCfg.RootPath = strings.TrimLeft(cfg.MinioCfg.RootPath, "/")

	// Backup storage defaults to primary storage
	if cfg.MinioCfg.BackupStorageType == "" {
		cfg.MinioCfg.BackupStorageType = cfg.MinioCfg.StorageType
	}
	if cfg.MinioCfg.BackupAddress == "" {
		cfg.MinioCfg.BackupAddress = cfg.MinioCfg.Address
	}
	if cfg.MinioCfg.BackupPort == "" {
		cfg.MinioCfg.BackupPort = cfg.MinioCfg.Port
	}
	if cfg.MinioCfg.BackupRegion == "" {
		cfg.MinioCfg.BackupRegion = cfg.MinioCfg.Region
	}
	if cfg.MinioCfg.BackupAccessKeyID == "" {
		cfg.MinioCfg.BackupAccessKeyID = cfg.MinioCfg.AccessKeyID
	}
	if cfg.MinioCfg.BackupSecretAccessKey == "" {
		cfg.MinioCfg.BackupSecretAccessKey = cfg.MinioCfg.SecretAccessKey
	}
	if cfg.MinioCfg.BackupToken == "" {
		cfg.MinioCfg.BackupToken = cfg.MinioCfg.Token
	}
	if cfg.MinioCfg.BackupBucketName == "" {
		cfg.MinioCfg.BackupBucketName = cfg.MinioCfg.BucketName
	}
	if !cfg.MinioCfg.BackupUseIAM && cfg.MinioCfg.UseIAM {
		cfg.MinioCfg.BackupUseIAM = cfg.MinioCfg.UseIAM
	}
	if cfg.MinioCfg.BackupIAMEndpoint == "" {
		cfg.MinioCfg.BackupIAMEndpoint = cfg.MinioCfg.IAMEndpoint
	}
}

// envMapping defines environment variable to config field mappings
var envMapping = []struct {
	env   string
	field func(*BackupParams) *string
}{
	{"MILVUS_ADDRESS", func(c *BackupParams) *string { return &c.MilvusCfg.Address }},
	{"MILVUS_PORT", func(c *BackupParams) *string { return &c.MilvusCfg.Port }},
	{"MILVUS_USER", func(c *BackupParams) *string { return &c.MilvusCfg.User }},
	{"MILVUS_PASSWORD", func(c *BackupParams) *string { return &c.MilvusCfg.Password }},

	{"MINIO_ADDRESS", func(c *BackupParams) *string { return &c.MinioCfg.Address }},
	{"MINIO_PORT", func(c *BackupParams) *string { return &c.MinioCfg.Port }},
	{"MINIO_ACCESS_KEY", func(c *BackupParams) *string { return &c.MinioCfg.AccessKeyID }},
	{"MINIO_SECRET_KEY", func(c *BackupParams) *string { return &c.MinioCfg.SecretAccessKey }},
	{"MINIO_BUCKET_NAME", func(c *BackupParams) *string { return &c.MinioCfg.BucketName }},
	{"MINIO_ROOT_PATH", func(c *BackupParams) *string { return &c.MinioCfg.RootPath }},
	{"MINIO_BACKUP_BUCKET_NAME", func(c *BackupParams) *string { return &c.MinioCfg.BackupBucketName }},
	{"MINIO_BACKUP_ROOT_PATH", func(c *BackupParams) *string { return &c.MinioCfg.BackupRootPath }},
}

func applyEnvOverrides(cfg *BackupParams) {
	for _, m := range envMapping {
		if val := os.Getenv(m.env); val != "" {
			field := m.field(cfg)
			oldVal := *field
			*field = val

			if isSensitiveEnv(m.env) {
				log.Info("config overridden by env",
					zap.String("env", m.env),
					zap.String("from", MaskSensitiveValue(oldVal)),
					zap.String("to", MaskSensitiveValue(val)))
			} else if oldVal != val {
				log.Info("config overridden by env",
					zap.String("env", m.env),
					zap.String("from", oldVal),
					zap.String("to", val))
			}
		}
	}
}

func isSensitiveEnv(env string) bool {
	lower := strings.ToLower(env)
	return strings.Contains(lower, "password") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "key") ||
		strings.Contains(lower, "token")
}

func validate(cfg *BackupParams) error {
	if cfg.MilvusCfg.Address == "" {
		return fmt.Errorf("milvus.address is required")
	}
	if cfg.MilvusCfg.Port == "" {
		return fmt.Errorf("milvus.port is required")
	}

	if !supportedStorageType[cfg.MinioCfg.StorageType] {
		return fmt.Errorf("unsupported storage type: %s", cfg.MinioCfg.StorageType)
	}
	if !supportedStorageType[cfg.MinioCfg.CloudProvider] {
		return fmt.Errorf("unsupported cloud provider: %s", cfg.MinioCfg.CloudProvider)
	}
	if cfg.MinioCfg.BackupStorageType != "" && !supportedStorageType[cfg.MinioCfg.BackupStorageType] {
		return fmt.Errorf("unsupported backup storage type: %s", cfg.MinioCfg.BackupStorageType)
	}

	return nil
}

// MaskSensitiveValue masks sensitive values, showing only first 2 and last 2 characters
func MaskSensitiveValue(value string) string {
	if len(value) < 6 {
		return "***"
	}
	return value[:2] + "***" + value[len(value)-2:]
}

// MaskedConfig returns a copy of config with sensitive values masked (for display)
func (c *BackupParams) MaskedConfig() *BackupParams {
	masked := *c

	masked.MilvusCfg.Password = MaskSensitiveValue(c.MilvusCfg.Password)

	masked.MinioCfg.AccessKeyID = MaskSensitiveValue(c.MinioCfg.AccessKeyID)
	masked.MinioCfg.SecretAccessKey = MaskSensitiveValue(c.MinioCfg.SecretAccessKey)
	masked.MinioCfg.Token = MaskSensitiveValue(c.MinioCfg.Token)
	masked.MinioCfg.GcpCredentialJSON = MaskSensitiveValue(c.MinioCfg.GcpCredentialJSON)
	masked.MinioCfg.BackupAccessKeyID = MaskSensitiveValue(c.MinioCfg.BackupAccessKeyID)
	masked.MinioCfg.BackupSecretAccessKey = MaskSensitiveValue(c.MinioCfg.BackupSecretAccessKey)
	masked.MinioCfg.BackupToken = MaskSensitiveValue(c.MinioCfg.BackupToken)
	masked.MinioCfg.BackupGcpCredentialJSON = MaskSensitiveValue(c.MinioCfg.BackupGcpCredentialJSON)

	masked.CloudConfig.APIKey = MaskSensitiveValue(c.CloudConfig.APIKey)

	return &masked
}
