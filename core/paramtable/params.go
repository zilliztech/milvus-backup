package paramtable

import (
	"strconv"
	"strings"
)

type BackupParams struct {
	BaseTable

	LogCfg      LogConfig
	HTTPCfg     HTTPConfig
	CloudConfig CloudConfig
	MilvusCfg   MilvusConfig
	MinioCfg    MinioConfig
	BackupCfg   BackupConfig
}

func (p *BackupParams) Init() {
	p.BaseTable.Init()

	p.LogCfg.init(&p.BaseTable)
	p.HTTPCfg.init(&p.BaseTable)
	p.CloudConfig.init(&p.BaseTable)
	p.MilvusCfg.init(&p.BaseTable)
	p.MinioCfg.init(&p.BaseTable)
	p.BackupCfg.init(&p.BaseTable)
}

type BackupConfig struct {
	Base *BaseTable

	MaxSegmentGroupSize int64

	BackupCollectionParallelism int
	BackupCopyDataParallelism   int64
	RestoreParallelism          int
	ImportJobParallelism        int64

	KeepTempFiles bool

	GcPauseEnable  bool
	GcPauseAddress string
}

func (p *BackupConfig) init(base *BaseTable) {
	p.Base = base

	p.initMaxSegmentGroupSize()
	p.initBackupCollectionParallelism()
	p.initRestoreParallelism()
	p.initBackupCopyDataParallelism()
	p.initImportJobParallelism()
	p.initKeepTempFiles()
	p.initGcPauseEnable()
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
	p.BackupCopyDataParallelism = int64(size)
}

func (p *BackupConfig) initImportJobParallelism() {
	num := p.Base.ParseIntWithDefault("backup.parallelism.importJob", 768)
	p.ImportJobParallelism = int64(num)
}

func (p *BackupConfig) initKeepTempFiles() {
	keepTempFiles := p.Base.LoadWithDefault("backup.keepTempFiles", "false")
	p.KeepTempFiles, _ = strconv.ParseBool(keepTempFiles)
}

func (p *BackupConfig) initGcPauseEnable() {
	enable := p.Base.LoadWithDefault("backup.gcPause.enable", "false")
	p.GcPauseEnable, _ = strconv.ParseBool(enable)
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

	RPCChanelName string
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

	p.initRPCChanelName()
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

func (p *MilvusConfig) initRPCChanelName() {
	rpcChanelName := p.Base.LoadWithDefault("milvus.rpcChannelName", "by-dev-replicate-msg")
	p.RPCChanelName = rpcChanelName
}

const (
	DefaultCloudAddress = "https://api.cloud.zilliz.com"
)

type CloudConfig struct {
	Base *BaseTable

	Address string
	APIKey  string
}

func (p *CloudConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initAPIKey()
}

func (p *CloudConfig) initAddress() {
	address := p.Base.LoadWithDefault("cloud.address", DefaultCloudAddress)
	p.Address = address
}

func (p *CloudConfig) initAPIKey() {
	apikey := p.Base.LoadWithDefault("cloud.apikey", "")
	p.APIKey = apikey
}

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

type MinioConfig struct {
	Base *BaseTable

	StorageType string
	// Deprecated
	CloudProvider     string
	Address           string
	Port              string
	Region            string
	AccessKeyID       string
	SecretAccessKey   string
	Token             string
	GcpCredentialJSON string
	UseSSL            bool
	BucketName        string
	RootPath          string
	UseIAM            bool
	IAMEndpoint       string

	BackupStorageType       string
	BackupAddress           string
	BackupPort              string
	BackupRegion            string
	BackupAccessKeyID       string
	BackupSecretAccessKey   string
	BackupToken             string
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
	p.initRegion()
	p.initToken()
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
	p.initBackupRegion()
	p.initBackupToken()
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

func (p *MinioConfig) initRegion() {
	region := p.Base.LoadWithDefault("minio.region", "")
	p.Region = region
}

func (p *MinioConfig) initAccessKeyID() {
	keyID := p.Base.LoadWithDefault("minio.accessKeyID", DefaultMinioAccessKey)
	p.AccessKeyID = keyID
}

func (p *MinioConfig) initSecretAccessKey() {
	key := p.Base.LoadWithDefault("minio.secretAccessKey", DefaultMinioSecretAccessKey)
	p.SecretAccessKey = key
}

func (p *MinioConfig) initToken() {
	token := p.Base.LoadWithDefault("minio.token", "")
	p.Token = token
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

func (p *MinioConfig) initBackupRegion() {
	region := p.Base.LoadWithDefault("minio.backupRegion",
		p.Base.LoadWithDefault("minio.region", ""))
	p.BackupRegion = region
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

func (p *MinioConfig) initBackupToken() {
	token := p.Base.LoadWithDefault("minio.backupToken",
		p.Base.LoadWithDefault("minio.token", ""))
	p.BackupToken = token
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

type LogConfig struct {
	Base *BaseTable

	Level   string
	Console bool
	File    struct {
		RootPath   string
		Filename   string
		MaxSize    int
		MaxDays    int
		MaxBackups int
	}
}

func (p *LogConfig) init(base *BaseTable) {
	p.Base = base

	p.initLevel()
	p.initConsole()
	p.initFilename()
	p.initFileMaxSize()
	p.initFileMaxDays()
	p.initFileMaxBackups()
}

func (p *LogConfig) initLevel() {
	level := p.Base.LoadWithDefault("log.level", DefaultLogLevel)
	p.Level = level
}

func (p *LogConfig) initConsole() {
	console := p.Base.LoadWithDefault("log.console", "true")
	var err error
	p.Console, err = strconv.ParseBool(console)
	if err != nil {
		panic("parse bool console:" + err.Error())
	}
}

func (p *LogConfig) initFilename() {
	filename := p.Base.LoadWithDefault("log.file.filename", "logs/backup.log")
	p.File.Filename = filename
}

func (p *LogConfig) initFileMaxSize() {
	maxSizeStr := p.Base.LoadWithDefault("log.file.maxSize", "300")
	maxSize, err := strconv.Atoi(maxSizeStr)
	if err != nil {
		panic("parse int log.file.maxSize:" + err.Error())
	}
	p.File.MaxSize = maxSize
}

func (p *LogConfig) initFileMaxDays() {
	maxDaysStr := p.Base.LoadWithDefault("log.file.maxDays", "0")
	maxDays, err := strconv.Atoi(maxDaysStr)
	if err != nil {
		panic("parse int log.file.maxDays:" + err.Error())
	}
	p.File.MaxDays = maxDays
}

func (p *LogConfig) initFileMaxBackups() {
	maxBackupsStr := p.Base.LoadWithDefault("log.file.maxBackups", "0")
	maxBackups, err := strconv.Atoi(maxBackupsStr)
	if err != nil {
		panic("parse int log.file.maxBackups:" + err.Error())
	}
	p.File.MaxBackups = maxBackups
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
