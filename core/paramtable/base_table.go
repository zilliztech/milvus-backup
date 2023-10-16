// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/spf13/cast"
	"github.com/spf13/viper"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	memkv "github.com/zilliztech/milvus-backup/internal/kv/mem"
	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/logutil"
	"github.com/zilliztech/milvus-backup/internal/util/typeutil"
)

// UniqueID is type alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

const (
	DefaultBackupYaml = "backup.yaml"

	DefaultMinioAddress         = "localhost"
	DefaultMinioPort            = "9000"
	DefaultMinioAccessKey       = "minioadmin"
	DefaultMinioSecretAccessKey = "minioadmin"
	DefaultMinioUseSSL          = "false"
	DefaultMinioBucketName      = "a-bucket"
	DefaultMinioRootPath        = "files"
	DefaultMinioUseIAM          = "false"
	DefaultMinioCloudProvider   = "aws"
	DefaultMinioIAMEndpoint     = ""

	DefaultLogLevel = "WARNING"

	DefaultMinioBackupBucketName = "a-bucket"
	DefaultMinioBackupRootPath   = "backup"

	DefaultMilvusAddress              = "localhost"
	DefaultMilvusPort                 = "19530"
	DefaultMilvusAuthorizationEnabled = "false"
	DefaultMilvusTlsMode              = "0"
	DefaultMilvusUser                 = "root"
	DefaultMilvusPassword             = "Milvus"
)

var defaultYaml = DefaultBackupYaml

// BaseTable the basics of paramtable
type BaseTable struct {
	once      sync.Once
	params    *memkv.MemoryKV
	configDir string

	RoleName   string
	Log        log.Config
	LogCfgFunc func(log.Config)
}

// GlobalInitWithYaml initializes the param table with the given yaml.
// We will update the global DefaultYaml variable directly, once and for all.
// GlobalInitWithYaml shall be called at the very beginning before initiating the base table.
// GlobalInitWithYaml should be called only in standalone and embedded Milvus.
func (gp *BaseTable) GlobalInitWithYaml(yaml string) {
	gp.once.Do(func() {
		defaultYaml = yaml
		gp.Init()
	})
}

// Init initializes the param table.
func (gp *BaseTable) Init() {
	gp.params = memkv.NewMemoryKV()
	gp.configDir = gp.initConfPath()
	gp.loadFromYaml(defaultYaml)
	gp.tryLoadFromEnv()
	gp.InitLogCfg()
	gp.SetLogConfig()
	gp.SetLogger()
}

// GetConfigDir returns the config directory
func (gp *BaseTable) GetConfigDir() string {
	return gp.configDir
}

// LoadFromKVPair saves given kv pair to paramtable
func (gp *BaseTable) LoadFromKVPair(kvPairs []*backuppb.KeyValuePair) error {
	for _, pair := range kvPairs {
		err := gp.Save(pair.Key, pair.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gp *BaseTable) initConfPath() string {
	// check if user set conf dir through env
	configDir, find := syscall.Getenv("MILVUSCONF")
	if !find {
		if _, err := os.Stat(defaultYaml); err == nil {
			return path.Dir(defaultYaml)
		}

		runPath, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		configDir = runPath + "/configs/"
		if _, err := os.Stat(configDir); err != nil {
			_, fpath, _, _ := runtime.Caller(0)
			// TODO, this is a hack, need to find better solution for relative path
			configDir = path.Dir(fpath) + "/../../configs/"
		}
	}
	return configDir
}

func (gp *BaseTable) loadFromYaml(file string) {
	if err := gp.LoadYaml(file); err != nil {
		panic(err)
	}
}

func (gp *BaseTable) tryLoadFromEnv() {
	gp.loadMinioConfig()
	gp.loadMilvusConfig()
}

// Load loads an object with @key.
func (gp *BaseTable) Load(key string) (string, error) {
	return gp.params.Load(strings.ToLower(key))
}

// LoadWithPriority loads an object with multiple @keys, return the first successful value.
// If all keys not exist, return error.
// This is to be compatible with old configuration file.
func (gp *BaseTable) LoadWithPriority(keys []string) (string, error) {
	for _, key := range keys {
		if str, err := gp.params.Load(strings.ToLower(key)); err == nil {
			return str, nil
		}
	}
	return "", fmt.Errorf("invalid keys: %v", keys)
}

// LoadWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (gp *BaseTable) LoadWithDefault(key, defaultValue string) string {
	return gp.params.LoadWithDefault(strings.ToLower(key), defaultValue)
}

// LoadWithDefault2 loads an object with multiple @keys, return the first successful value.
// If all keys not exist, return @defaultValue.
// This is to be compatible with old configuration file.
func (gp *BaseTable) LoadWithDefault2(keys []string, defaultValue string) string {
	for _, key := range keys {
		if str, err := gp.params.Load(strings.ToLower(key)); err == nil {
			return str
		}
	}
	return defaultValue
}

// LoadRange loads objects with range @startKey to @endKey with @limit number of objects.
func (gp *BaseTable) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	return gp.params.LoadRange(strings.ToLower(key), strings.ToLower(endKey), limit)
}

func (gp *BaseTable) LoadYaml(fileName string) error {
	config := viper.New()
	configFile := fmt.Sprintf("%s/%s", strings.TrimRight(gp.configDir, "/"), path.Base(fileName))
	if _, err := os.Stat(configFile); err != nil {
		panic("cannot access config file: " + configFile)
	}

	config.SetConfigFile(configFile)
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}

	for _, key := range config.AllKeys() {
		val := config.Get(key)
		str, err := cast.ToStringE(val)
		if err != nil {
			switch val := val.(type) {
			case []interface{}:
				str = str[:0]
				for _, v := range val {
					ss, err := cast.ToStringE(v)
					if err != nil {
						panic(err)
					}
					if str == "" {
						str = ss
					} else {
						str = str + "," + ss
					}
				}

			default:
				panic("undefined config type, key=" + key)
			}
		}
		err = gp.params.Save(strings.ToLower(key), str)
		if err != nil {
			panic(err)
		}

	}

	return nil
}

func (gp *BaseTable) Get(key string) string {
	return gp.params.Get(strings.ToLower(key))
}

func (gp *BaseTable) Remove(key string) error {
	return gp.params.Remove(strings.ToLower(key))
}

func (gp *BaseTable) Save(key, value string) error {
	return gp.params.Save(strings.ToLower(key), value)
}

func (gp *BaseTable) ParseBool(key string, defaultValue bool) bool {
	valueStr := gp.LoadWithDefault(key, strconv.FormatBool(defaultValue))
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseFloat(key string) float64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseFloatWithDefault(key string, defaultValue float64) float64 {
	valueStr := gp.LoadWithDefault(key, fmt.Sprintf("%f", defaultValue))
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt64(key string) int64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt64WithDefault(key string, defaultValue int64) int64 {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(defaultValue, 10))
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt32(key string) int32 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseInt(valueStr, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(value)
}

func (gp *BaseTable) ParseInt32WithDefault(key string, defaultValue int32) int32 {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(int64(defaultValue), 10))
	value, err := strconv.ParseInt(valueStr, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(value)
}

func (gp *BaseTable) ParseInt(key string) int {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseIntWithDefault(key string, defaultValue int) int {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(int64(defaultValue), 10))
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseDataSizeWithDefault(key string, defaultValue string) (int64, error) {
	valueStr := strings.ToLower(gp.LoadWithDefault(key, defaultValue))
	if strings.HasSuffix(valueStr, "g") || strings.HasSuffix(valueStr, "gb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "g")[0], 10, 64)
		if err != nil {
			return 0, err
		}
		return size * 1024 * 1024 * 1024, nil
	} else if strings.HasSuffix(valueStr, "m") || strings.HasSuffix(valueStr, "mb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "m")[0], 10, 64)
		if err != nil {
			return 0, err
		}
		return size * 1024 * 1024, nil
	} else if strings.HasSuffix(valueStr, "k") || strings.HasSuffix(valueStr, "kb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "k")[0], 10, 64)
		if err != nil {
			return 0, err
		}
		return size * 1024, nil
	}

	size, err := strconv.ParseInt(strings.Split(valueStr, "k")[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// InitLogCfg init log of the base table
func (gp *BaseTable) InitLogCfg() {
	gp.Log = log.Config{}
	format := gp.LoadWithDefault("log.format", "text")
	gp.Log.Format = format
	level := gp.LoadWithDefault("log.level", "debug")
	gp.Log.Level = level
	gp.Log.Console = gp.ParseBool("log.console", false)
	gp.Log.File.Filename = gp.LoadWithDefault("log.file.rootPath", "backup.log")
	gp.Log.File.MaxSize = gp.ParseIntWithDefault("log.file.maxSize", 300)
	gp.Log.File.MaxBackups = gp.ParseIntWithDefault("log.file.maxBackups", 20)
	gp.Log.File.MaxDays = gp.ParseIntWithDefault("log.file.maxAge", 10)
}

// SetLogConfig set log config of the base table
func (gp *BaseTable) SetLogConfig() {
	gp.LogCfgFunc = func(cfg log.Config) {
		var err error
		grpclog, err := gp.Load("grpc.log.level")
		if err != nil {
			cfg.GrpcLevel = DefaultLogLevel
		} else {
			cfg.GrpcLevel = strings.ToUpper(grpclog)
		}
		logutil.SetupLogger(&cfg)
		defer log.Sync()
	}
}

// SetLogger
func (gp *BaseTable) SetLogger() {
	rootPath, err := gp.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if rootPath != "" {
		gp.Log.File.Filename = rootPath
	} else {
		gp.Log.File.Filename = ""
	}

	if gp.LogCfgFunc != nil {
		gp.LogCfgFunc(gp.Log)
	}
}

func (gp *BaseTable) loadMinioConfig() {
	minioAddress := os.Getenv("MINIO_ADDRESS")
	if minioAddress == "" {
		minioHost := gp.LoadWithDefault("minio.address", DefaultMinioAddress)
		minioAddress = minioHost
	}
	_ = gp.Save("minio.address", minioAddress)

	minioPort := os.Getenv("MINIO_PORT")
	if minioPort == "" {
		port := gp.LoadWithDefault("minio.port", DefaultMinioPort)
		minioPort = port
	}
	_ = gp.Save("minio.port", minioPort)

	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	if minioAccessKey == "" {
		minioAccessKey = gp.LoadWithDefault("minio.accessKeyID", DefaultMinioAccessKey)
	}
	_ = gp.Save("minio.accessKeyID", minioAccessKey)

	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
	if minioSecretKey == "" {
		minioSecretKey = gp.LoadWithDefault("minio.secretAccessKey", DefaultMinioSecretAccessKey)
	}
	_ = gp.Save("minio.secretAccessKey", minioSecretKey)

	minioUseSSL := os.Getenv("MINIO_USE_SSL")
	if minioUseSSL == "" {
		minioUseSSL = gp.LoadWithDefault("minio.useSSL", DefaultMinioUseSSL)
	}
	_ = gp.Save("minio.useSSL", minioUseSSL)

	minioBucketName := os.Getenv("MINIO_BUCKET_NAME")
	if minioBucketName == "" {
		minioBucketName = gp.LoadWithDefault("minio.bucketName", DefaultMinioBucketName)
	}
	_ = gp.Save("minio.bucketName", minioBucketName)

	minioUseIAM := os.Getenv("MINIO_USE_IAM")
	if minioUseIAM == "" {
		minioUseIAM = gp.LoadWithDefault("minio.useIAM", DefaultMinioUseIAM)
	}
	_ = gp.Save("minio.useIAM", minioUseIAM)

	minioIAMEndpoint := os.Getenv("MINIO_IAM_ENDPOINT")
	if minioIAMEndpoint == "" {
		minioIAMEndpoint = gp.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
	}
	_ = gp.Save("minio.iamEndpoint", minioIAMEndpoint)

	// extends.
	minioRootPath := os.Getenv("MINIO_ROOT_PATH")
	if minioRootPath == "" {
		minioRootPath = gp.LoadWithDefault("minio.rootPath", DefaultMinioRootPath)
	}
	_ = gp.Save("minio.rootPath", minioRootPath)

	minioBackupBucketName := os.Getenv("MINIO_BACKUP_BUCKET_NAME")
	if minioBackupBucketName == "" {
		minioBackupBucketName = gp.LoadWithDefault("minio.backupBucketName", DefaultMinioBackupBucketName)
	}
	_ = gp.Save("minio.backupBucketName", minioBucketName)

	minioBackupRootPath := os.Getenv("MINIO_BACKUP_ROOT_PATH")
	if minioBackupRootPath == "" {
		minioBackupRootPath = gp.LoadWithDefault("minio.backupRootPath", DefaultMinioBackupRootPath)
	}
	_ = gp.Save("minio.backupRootPath", minioBackupRootPath)
}

func (gp *BaseTable) loadMilvusConfig() {
	milvusAddress := os.Getenv("MILVUS_ADDRESS")
	if milvusAddress == "" {
		milvusAddress = gp.LoadWithDefault("milvus.address", DefaultMilvusAddress)
	}
	_ = gp.Save("milvus.address", milvusAddress)

	milvusPort := os.Getenv("MILVUS_PORT")
	if milvusPort == "" {
		milvusPort = gp.LoadWithDefault("milvus.port", DefaultMilvusPort)
	}
	_ = gp.Save("milvus.port", milvusPort)

	milvusAuthorizationEnabled := os.Getenv("MILVUS_AUTHORIZATION_ENABLED")
	if milvusAuthorizationEnabled == "" {
		milvusAuthorizationEnabled = gp.LoadWithDefault("milvus.authorizationEnabled", DefaultMilvusAuthorizationEnabled)
	}
	_ = gp.Save("milvus.authorizationEnabled", milvusAuthorizationEnabled)

	milvusTlsMode := os.Getenv("MILVUS_TLS_MODE")
	if milvusTlsMode == "" {
		milvusTlsMode = gp.LoadWithDefault("milvus.tlsMode", DefaultMilvusTlsMode)
	}
	_ = gp.Save("milvus.tlsMode", milvusTlsMode)

	milvusUser := os.Getenv("MILVUS_USER")
	if milvusUser == "" {
		milvusUser = gp.LoadWithDefault("milvus.user", DefaultMilvusUser)
	}
	_ = gp.Save("milvus.user", milvusUser)

	milvusPassword := os.Getenv("MILVUS_PASSWORD")
	if milvusPassword == "" {
		milvusPassword = gp.LoadWithDefault("milvus.password", DefaultMilvusPassword)
	}
	_ = gp.Save("milvus.password", milvusPassword)
}
