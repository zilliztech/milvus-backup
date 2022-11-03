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

	Address string
	Port    string
}

func (p *MilvusConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initPort()
}

func (p *MilvusConfig) initAddress() {
	endpoint, err := p.Base.Load("milvus.address")
	if err != nil {
		panic(err)
	}
	p.Address = endpoint
}

func (p *MilvusConfig) initPort() {
	endpoint, err := p.Base.Load("milvus.port")
	if err != nil {
		panic(err)
	}
	p.Port = endpoint
}

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
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
	IAMEndpoint     string
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
	p.initIAMEndpoint()
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
	p.UseIAM, _ = strconv.ParseBool(useIAM)
}

func (p *MinioConfig) initIAMEndpoint() {
	iamEndpoint := p.Base.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
	p.IAMEndpoint = iamEndpoint
}

type HTTPConfig struct {
	Base *BaseTable

	Enabled   bool
	DebugMode bool
}

func (p *HTTPConfig) init(base *BaseTable) {
	p.Base = base

	p.initHTTPEnabled()
	p.initHTTPDebugMode()
}

func (p *HTTPConfig) initHTTPEnabled() {
	p.Enabled = p.Base.ParseBool("proxy.http.enabled", true)
}

func (p *HTTPConfig) initHTTPDebugMode() {
	p.DebugMode = p.Base.ParseBool("proxy.http.debug_mode", false)
}
