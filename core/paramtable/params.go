package paramtable

import (
	"strconv"
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

	BackupParallelism         int
	RestoreParallelism        int
	BackupCopyDataParallelism int
	KeepTempFiles             bool
}

func (p *BackupConfig) init(base *BaseTable) {
	p.Base = base

	p.initMaxSegmentGroupSize()
	p.initBackupParallelism()
	p.initRestoreParallelism()
	p.initBackupCopyDataParallelism()
	p.initKeepTempFiles()
}

func (p *BackupConfig) initMaxSegmentGroupSize() {
	size, err := p.Base.ParseDataSizeWithDefault("backup.maxSegmentGroupSize", "2g")
	if err != nil {
		panic(err)
	}
	p.MaxSegmentGroupSize = size
}

func (p *BackupConfig) initBackupParallelism() {
	size := p.Base.ParseIntWithDefault("backup.parallelism", 1)
	p.BackupParallelism = size
}

func (p *BackupConfig) initRestoreParallelism() {
	size := p.Base.ParseIntWithDefault("restore.parallelism", 1)
	p.RestoreParallelism = size
}

func (p *BackupConfig) initBackupCopyDataParallelism() {
	size := p.Base.ParseIntWithDefault("backup.copydata.parallelism", 10)
	p.BackupCopyDataParallelism = size
}

func (p *BackupConfig) initKeepTempFiles() {
	keepTempFiles := p.Base.LoadWithDefault("backup.keepTempFiles", "false")
	p.KeepTempFiles, _ = strconv.ParseBool(keepTempFiles)
}

type MilvusConfig struct {
	Base *BaseTable

	Address              string
	Port                 string
	User                 string
	Password             string
	AuthorizationEnabled bool
	TLSMode              int
}

func (p *MilvusConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initPort()
	p.initUser()
	p.initPassword()
	p.initAuthorizationEnabled()
	p.initTLSMode()
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

func (p *MilvusConfig) initAuthorizationEnabled() {
	p.AuthorizationEnabled = p.Base.ParseBool("milvus.authorizationEnabled", false)
}

func (p *MilvusConfig) initTLSMode() {
	p.TLSMode = p.Base.ParseIntWithDefault("milvus.tlsMode", 0)
}

// /////////////////////////////////////////////////////////////////////////////
// --- minio ---
const (
	Minio               = "minio"
	CloudProviderAWS    = "aws"
	CloudProviderGCP    = "gcp"
	CloudProviderAliyun = "ali"
	CloudProviderAzure  = "azure"
)

var supportedCloudProvider = map[string]bool{
	Minio:               true,
	CloudProviderAWS:    true,
	CloudProviderGCP:    true,
	CloudProviderAliyun: true,
	CloudProviderAzure:  true,
}

type MinioConfig struct {
	Base *BaseTable

	Address         string
	Port            string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	RootPath        string
	UseIAM          bool
	CloudProvider   string
	IAMEndpoint     string

	BackupAccessKeyID     string
	BackupSecretAccessKey string
	BackupBucketName      string
	BackupRootPath        string

	StorageType string
}

func (p *MinioConfig) init(base *BaseTable) {
	p.Base = base

	p.initStorageType()
	p.initAddress()
	p.initPort()
	p.initAccessKeyID()
	p.initSecretAccessKey()
	p.initUseSSL()
	p.initBucketName()
	p.initRootPath()
	p.initUseIAM()
	p.initCloudProvider()
	p.initIAMEndpoint()

	p.initBackupAccessKeyID()
	p.initBackupSecretAccessKey()
	p.initBackupBucketName()
	p.initBackupRootPath()
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

func (p *MinioConfig) initUseSSL() {
	usessl := p.Base.LoadWithDefault("minio.useSSL", DefaultMinioUseSSL)
	p.UseSSL, _ = strconv.ParseBool(usessl)
}

func (p *MinioConfig) initBucketName() {
	bucketName := p.Base.LoadWithDefault("minio.bucketName", DefaultMinioBucketName)
	p.BucketName = bucketName
}

func (p *MinioConfig) initRootPath() {
	rootPath := p.Base.LoadWithDefault("minio.rootPath", DefaultMinioRootPath)
	p.RootPath = rootPath
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
	if !supportedCloudProvider[p.CloudProvider] {
		panic("unsupported cloudProvider:" + p.CloudProvider)
	}
}

func (p *MinioConfig) initIAMEndpoint() {
	iamEndpoint := p.Base.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
	p.IAMEndpoint = iamEndpoint
}

func (p *MinioConfig) initBackupAccessKeyID() {
	keyID := p.Base.LoadWithDefault("minio.backupAccessKeyID", DefaultMinioAccessKey)
	p.BackupAccessKeyID = keyID
}

func (p *MinioConfig) initBackupSecretAccessKey() {
	key := p.Base.LoadWithDefault("minio.backupSecretAccessKey", DefaultMinioSecretAccessKey)
	p.BackupSecretAccessKey = key
}

func (p *MinioConfig) initBackupBucketName() {
	bucketName := p.Base.LoadWithDefault("minio.backupBucketName", DefaultMinioBackupBucketName)
	p.BackupBucketName = bucketName
}

func (p *MinioConfig) initBackupRootPath() {
	rootPath := p.Base.LoadWithDefault("minio.backupRootPath", DefaultMinioBackupRootPath)
	p.BackupRootPath = rootPath
}

func (p *MinioConfig) initStorageType() {
	engine := p.Base.LoadWithDefault("storage.type",
		p.Base.LoadWithDefault("minio.type", DefaultStorageType))
	if !supportedCloudProvider[engine] {
		panic("unsupported storage type:" + engine)
	}
	p.StorageType = engine
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
