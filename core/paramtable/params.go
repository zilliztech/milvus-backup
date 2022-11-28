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

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
const (
	CloudProviderAWS = "aws"
	CloudProviderGCP = "gcp"
)

var supportedCloudProvider = map[string]bool{
	CloudProviderAWS: true,
	CloudProviderGCP: true,
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

	BackupBucketName string
	BackupRootPath   string
}

func (p *MinioConfig) init(base *BaseTable) {
	p.Base = base

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
	keyID, err := p.Base.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
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

func (p *MinioConfig) initBackupBucketName() {
	bucketName := p.Base.LoadWithDefault("minio.backupBucketName", DefaultMinioBackupBucketName)
	p.BackupBucketName = bucketName
}

func (p *MinioConfig) initBackupRootPath() {
	rootPath := p.Base.LoadWithDefault("minio.backupRootPath", DefaultMinioBackupRootPath)
	p.BackupRootPath = rootPath
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
