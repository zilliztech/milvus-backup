package paramtable

import (
	"strconv"
	"strings"
)

// BackupParams
type BackupParams struct {
	BaseTable

	HTTPCfg   HTTPConfig
	MilvusCfg MilvusConfig
	MinioCfg  MinioConfig
	BackupCfg BackupConfig
}

func (p *BackupParams) InitOnce() {
	p.once.Do(func() {
		p.Init()
	})
}

func (p *BackupParams) Init() {
	p.BaseTable.Init()

	p.HTTPCfg.init(&p.BaseTable)
	p.MilvusCfg.init(&p.BaseTable)
	p.MinioCfg.init(&p.BaseTable)
	p.BackupCfg.init(&p.BaseTable)
}

type BackupConfig struct {
	Base *BaseTable

	MaxSegmentGroupSize int64

	BackupCollectionParallelism int
	BackupCopyDataParallelism   int
	RestoreParallelism          int

	KeepTempFiles bool

	GcPauseEnable  bool
	GcPauseSeconds int
	GcPauseAddress string
}

func (p *BackupConfig) init(base *BaseTable) {
	p.Base = base

	p.initMaxSegmentGroupSize()
	p.initBackupCollectionParallelism()
	p.initRestoreParallelism()
	p.initBackupCopyDataParallelism()
	p.initKeepTempFiles()
	p.initGcPauseEnable()
	p.initGcPauseSeconds()
	p.initGcPauseAddress()
}

func (p *BackupConfig) initMaxSegmentGroupSize() {
	size, err := p.Base.ParseDataSizeWithDefault("backup.maxSegmentGroupSize", "2g")
	if err != nil {
		panic(err)
	}
	p.MaxSegmentGroupSize = size
}

func (p *BackupConfig) initBackupCollectionParallelism() {
	size := p.Base.ParseIntWithDefault("backup.parallelism.backupCollection", 1)
	p.BackupCollectionParallelism = size
}

func (p *BackupConfig) initRestoreParallelism() {
	size := p.Base.ParseIntWithDefault("backup.parallelism.restoreCollection", 1)
	p.RestoreParallelism = size
}

func (p *BackupConfig) initBackupCopyDataParallelism() {
	size := p.Base.ParseIntWithDefault("backup.parallelism.copydata", 128)
	p.BackupCopyDataParallelism = size
}

func (p *BackupConfig) initKeepTempFiles() {
	keepTempFiles := p.Base.LoadWithDefault("backup.keepTempFiles", "false")
	p.KeepTempFiles, _ = strconv.ParseBool(keepTempFiles)
}

func (p *BackupConfig) initGcPauseEnable() {
	enable := p.Base.LoadWithDefault("backup.gcPause.enable", "false")
	p.GcPauseEnable, _ = strconv.ParseBool(enable)
}

func (p *BackupConfig) initGcPauseSeconds() {
	seconds := p.Base.ParseIntWithDefault("backup.gcPause.seconds", 7200)
	p.GcPauseSeconds = seconds
}

func (p *BackupConfig) initGcPauseAddress() {
	address := p.Base.LoadWithDefault("backup.gcPause.address", "http://localhost:9091")
	p.GcPauseAddress = address
}

type MilvusConfig struct {
	Base *BaseTable

	Address string
	Port    string

	User     string
	Password string

	TLSMode int

	// tls credentials for validate server
	CACertPath string
	ServerName string

	// tls credentials for validate client, eg: mTLS
	MTLSCertPath string
	MTLSKeyPath  string
}

func (p *MilvusConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initPort()

	p.initUser()
	p.initPassword()

	p.initTLSMode()

	p.initCACertPath()
	p.initServerName()

	p.initMTLSCertPath()
	p.initMTLSKeyPath()
}

func (p *MilvusConfig) initAddress() {
	address, err := p.Base.Load("milvus.address")
	if err != nil {
		panic(err)
	}
	p.Address = address
}

func (p *MilvusConfig) initPort() {
	port, err := p.Base.Load("milvus.port")
	if err != nil {
		panic(err)
	}
	p.Port = port
}

func (p *MilvusConfig) initUser() {
	user, err := p.Base.Load("milvus.user")
	if err != nil {
		p.User = ""
	}
	p.User = user
}

func (p *MilvusConfig) initPassword() {
	password, err := p.Base.Load("milvus.password")
	if err != nil {
		p.Password = ""
	}
	p.Password = password
}

func (p *MilvusConfig) initCACertPath() {
	caCertPath := p.Base.LoadWithDefault("milvus.caCertPath", "")
	p.CACertPath = caCertPath
}

func (p *MilvusConfig) initServerName() {
	serverName := p.Base.LoadWithDefault("milvus.serverName", "")
	p.ServerName = serverName
}

func (p *MilvusConfig) initMTLSCertPath() {
	// for backward compatibility, if mTLS cert path is not set, use tls mode 1.
	// WARN: This behavior will be removed in the version after v0.6.0
	mtlsCertPath := p.Base.LoadWithDefault("milvus.mtlsCertPath", "")
	p.MTLSCertPath = mtlsCertPath
}

func (p *MilvusConfig) initMTLSKeyPath() {
	// for backward compatibility, if mTLS key path is not set, use tls mode 1 instead of 2.
	// WARN: This behavior will be removed in the version after v0.6.0
	mtlsKeyPath := p.Base.LoadWithDefault("milvus.mtlsKeyPath", "")
	p.MTLSKeyPath = mtlsKeyPath
}

func (p *MilvusConfig) initTLSMode() {
	// for backward compatibility, if mTLS cert path is not set, use tls mode 1 instead of 2.
	// WARN: This behavior will be removed in the version after v0.6.0
	p.TLSMode = p.Base.ParseIntWithDefault("milvus.tlsMode", 0)
}

// /////////////////////////////////////////////////////////////////////////////
// --- minio ---
const (
	Local                     = "local"
	Minio                     = "minio"
	S3                        = "s3"
	CloudProviderAWS          = "aws"
	CloudProviderGCP          = "gcp"
	CloudProviderGCPNative    = "gcpnative"
	CloudProviderAli          = "ali"
	CloudProviderAliyun       = "aliyun"
	CloudProviderAzure        = "azure"
	CloudProviderTencent      = "tencent"
	CloudProviderTencentShort = "tc"
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
	CloudProviderAzure:        true,
	CloudProviderTencent:      true,
	CloudProviderTencentShort: true,
}

type MinioConfig struct {
	Base *BaseTable

	StorageType string
	// Deprecated
	CloudProvider     string
	Address           string
	Port              string
	AccessKeyID       string
	SecretAccessKey   string
	GcpCredentialJSON string
	UseSSL            bool
	BucketName        string
	RootPath          string
	UseIAM            bool
	IAMEndpoint       string

	BackupStorageType       string
	BackupAddress           string
	BackupPort              string
	BackupAccessKeyID       string
	BackupSecretAccessKey   string
	BackupGcpCredentialJSON string
	BackupUseSSL            bool
	BackupBucketName        string
	BackupRootPath          string
	BackupUseIAM            bool
	BackupIAMEndpoint       string

	CrossStorage bool
}

func (p *MinioConfig) init(base *BaseTable) {
	p.Base = base

	p.initStorageType()
	p.initAddress()
	p.initPort()
	p.initAccessKeyID()
	p.initSecretAccessKey()
	p.initGcpCredentialJSON()
	p.initUseSSL()
	p.initBucketName()
	p.initRootPath()
	p.initUseIAM()
	p.initCloudProvider()
	p.initIAMEndpoint()

	p.initBackupStorageType()
	p.initBackupAddress()
	p.initBackupPort()
	p.initBackupAccessKeyID()
	p.initBackupSecretAccessKey()
	p.initBackupGcpCredentialJSON()
	p.initBackupUseSSL()
	p.initBackupBucketName()
	p.initBackupRootPath()
	p.initBackupUseIAM()
	p.initBackupIAMEndpoint()

	p.initCrossStorage()
}

func (p *MinioConfig) initAddress() {
	endpoint := p.Base.LoadWithDefault("minio.address", DefaultMinioAddress)
	p.Address = endpoint
}

func (p *MinioConfig) initPort() {
	port := p.Base.LoadWithDefault("minio.port", DefaultMinioPort)
	p.Port = port
}

func (p *MinioConfig) initAccessKeyID() {
	keyID := p.Base.LoadWithDefault("minio.accessKeyID", DefaultMinioAccessKey)
	p.AccessKeyID = keyID
}

func (p *MinioConfig) initSecretAccessKey() {
	key := p.Base.LoadWithDefault("minio.secretAccessKey", DefaultMinioSecretAccessKey)
	p.SecretAccessKey = key
}

func (p *MinioConfig) initGcpCredentialJSON() {
	gcpCredentialJSON := p.Base.LoadWithDefault("minio.gcpCredentialJSON", DefaultGcpCredentialJSON)
	p.GcpCredentialJSON = gcpCredentialJSON
}

func (p *MinioConfig) initUseSSL() {
	usessl := p.Base.LoadWithDefault("minio.useSSL", DefaultMinioUseSSL)
	var err error
	p.UseSSL, err = strconv.ParseBool(usessl)
	if err != nil {
		panic("parse bool useIAM:" + err.Error())
	}
}

func (p *MinioConfig) initBucketName() {
	bucketName := p.Base.LoadWithDefault("minio.bucketName", DefaultMinioBucketName)
	p.BucketName = bucketName
}

func (p *MinioConfig) initRootPath() {
	rootPath := p.Base.LoadWithDefault("minio.rootPath", DefaultMinioRootPath)
	p.RootPath = strings.TrimLeft(rootPath, "/")
}

func (p *MinioConfig) initUseIAM() {
	useIAM := p.Base.LoadWithDefault("minio.useIAM", DefaultMinioUseIAM)
	var err error
	p.UseIAM, err = strconv.ParseBool(useIAM)
	if err != nil {
		panic("parse bool useIAM:" + err.Error())
	}
}

func (p *MinioConfig) initCloudProvider() {
	p.CloudProvider = p.Base.LoadWithDefault("minio.cloudProvider", DefaultMinioCloudProvider)
	if !supportedStorageType[p.CloudProvider] {
		panic("unsupported cloudProvider:" + p.CloudProvider)
	}
}

func (p *MinioConfig) initIAMEndpoint() {
	iamEndpoint := p.Base.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
	p.IAMEndpoint = iamEndpoint
}

func (p *MinioConfig) initStorageType() {
	engine := p.Base.LoadWithDefault("storage.storageType",
		p.Base.LoadWithDefault("minio.storageType",
			p.Base.LoadWithDefault("minio.cloudProvider", DefaultStorageType)))
	if !supportedStorageType[engine] {
		panic("unsupported storage type:" + engine)
	}
	p.StorageType = engine
}

func (p *MinioConfig) initBackupStorageType() {
	engine := p.Base.LoadWithDefault("storage.backupStorageType",
		p.Base.LoadWithDefault("minio.backupStorageType",
			p.StorageType))
	if !supportedStorageType[engine] {
		panic("unsupported storage type:" + engine)
	}
	p.BackupStorageType = engine
}

func (p *MinioConfig) initBackupAddress() {
	endpoint := p.Base.LoadWithDefault("minio.backupAddress",
		p.Base.LoadWithDefault("minio.address", DefaultMinioAddress))
	p.BackupAddress = endpoint
}

func (p *MinioConfig) initBackupPort() {
	port := p.Base.LoadWithDefault("minio.backupPort",
		p.Base.LoadWithDefault("minio.port", DefaultMinioPort))
	p.BackupPort = port
}

func (p *MinioConfig) initBackupUseSSL() {
	usessl := p.Base.LoadWithDefault("minio.backupUseSSL",
		p.Base.LoadWithDefault("minio.useSSL", DefaultMinioUseSSL))
	var err error
	p.BackupUseSSL, err = strconv.ParseBool(usessl)
	if err != nil {
		panic("parse bool backupUseSSL:" + err.Error())
	}
}

func (p *MinioConfig) initBackupUseIAM() {
	useIAM := p.Base.LoadWithDefault("minio.backupUseIAM",
		p.Base.LoadWithDefault("minio.useIAM", DefaultMinioUseIAM))
	var err error
	p.BackupUseIAM, err = strconv.ParseBool(useIAM)
	if err != nil {
		panic("parse bool backupUseIAM:" + err.Error())
	}
}

func (p *MinioConfig) initBackupIAMEndpoint() {
	iamEndpoint := p.Base.LoadWithDefault("minio.backupIamEndpoint",
		p.Base.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint))
	p.BackupIAMEndpoint = iamEndpoint
}

func (p *MinioConfig) initBackupAccessKeyID() {
	keyID := p.Base.LoadWithDefault("minio.backupAccessKeyID",
		p.Base.LoadWithDefault("minio.accessKeyID", DefaultMinioAccessKey))
	p.BackupAccessKeyID = keyID
}

func (p *MinioConfig) initBackupSecretAccessKey() {
	key := p.Base.LoadWithDefault("minio.backupSecretAccessKey",
		p.Base.LoadWithDefault("minio.secretAccessKey", DefaultMinioSecretAccessKey))
	p.BackupSecretAccessKey = key
}

func (p *MinioConfig) initBackupGcpCredentialJSON() {
	gcpCredentialJSON := p.Base.LoadWithDefault("minio.backupGcpCredentialJSON", DefaultGcpCredentialJSON)
	p.BackupGcpCredentialJSON = gcpCredentialJSON
}

func (p *MinioConfig) initBackupBucketName() {
	bucketName := p.Base.LoadWithDefault("minio.backupBucketName",
		p.Base.LoadWithDefault("minio.bucketName", DefaultMinioBackupBucketName))
	p.BackupBucketName = bucketName
}

func (p *MinioConfig) initBackupRootPath() {
	rootPath := p.Base.LoadWithDefault("minio.backupRootPath",
		p.Base.LoadWithDefault("minio.rootPath", DefaultMinioBackupRootPath))
	p.BackupRootPath = rootPath
}

func (p *MinioConfig) initCrossStorage() {
	crossStorage := p.Base.LoadWithDefault("minio.crossStorage", "false")
	var err error
	p.CrossStorage, err = strconv.ParseBool(crossStorage)
	if err != nil {
		panic("parse bool CrossStorage:" + err.Error())
	}
}

type HTTPConfig struct {
	Base *BaseTable

	Enabled        bool
	DebugMode      bool
	SimpleResponse bool
}

func (p *HTTPConfig) init(base *BaseTable) {
	p.Base = base

	p.initHTTPEnabled()
	p.initHTTPDebugMode()
	p.initHTTPSimpleResponse()
}

func (p *HTTPConfig) initHTTPEnabled() {
	p.Enabled = p.Base.ParseBool("http.enabled", true)
}

func (p *HTTPConfig) initHTTPDebugMode() {
	p.DebugMode = p.Base.ParseBool("http.debug_mode", false)
}

func (p *HTTPConfig) initHTTPSimpleResponse() {
	p.SimpleResponse = p.Base.ParseBool("http.simpleResponse", false)
}
