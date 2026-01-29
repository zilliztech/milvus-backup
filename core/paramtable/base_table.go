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
)

const (
	DefaultBackupYaml = "backup.yaml"

	DefaultMinioAddress         = "localhost"
	DefaultMinioPort            = "9000"
	DefaultMinioAccessKey       = "minioadmin"
	DefaultMinioSecretAccessKey = "minioadmin"
	DefaultGcpCredentialJSON    = ""
	DefaultMinioUseSSL          = "false"
	DefaultMinioBucketName      = "a-bucket"
	DefaultMinioRootPath        = "files"
	DefaultMinioUseIAM          = "false"
	DefaultMinioCloudProvider   = "aws"
	DefaultMinioIAMEndpoint     = ""

	DefaultLogLevel = "INFO"

	DefaultMinioBackupBucketName = "a-bucket"
	DefaultMinioBackupRootPath   = "backup"

	DefaultStorageType = "minio"
)

var defaultYaml = DefaultBackupYaml

// BaseTable the basics of paramtable
type BaseTable struct {
	once      sync.Once
	params    *memkv.MemoryKV
	configDir string
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
	gp.loadBackupConfig()
}

// Load loads an object with @key.
func (gp *BaseTable) Load(key string) (string, error) {
	return gp.params.Load(strings.ToLower(key))
}

// LoadWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (gp *BaseTable) LoadWithDefault(key, defaultValue string) string {
	return gp.params.LoadWithDefault(strings.ToLower(key), defaultValue)
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

func (gp *BaseTable) loadMinioConfig() {
	minioAddress := os.Getenv("MINIO_ADDRESS")
	if minioAddress != "" {
		_ = gp.Save("minio.address", minioAddress)
	}

	minioPort := os.Getenv("MINIO_PORT")
	if minioPort != "" {
		_ = gp.Save("minio.port", minioPort)
	}

	minioRegion := os.Getenv("MINIO_REGION")
	if minioRegion != "" {
		_ = gp.Save("minio.region", minioRegion)
	}

	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	if minioAccessKey != "" {
		_ = gp.Save("minio.accessKeyID", minioAccessKey)
	}

	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
	if minioSecretKey != "" {
		_ = gp.Save("minio.secretAccessKey", minioSecretKey)
	}

	minioToken := os.Getenv("MINIO_TOKEN")
	if minioToken != "" {
		_ = gp.Save("minio.token", minioToken)
	}

	gcpCredentialJSON := os.Getenv("GCP_KEY_JSON")
	if gcpCredentialJSON != "" {
		_ = gp.Save("minio.gcpCredentialJSON", gcpCredentialJSON)
	}

	minioUseSSL := os.Getenv("MINIO_USE_SSL")
	if minioUseSSL != "" {
		_ = gp.Save("minio.useSSL", minioUseSSL)
	}

	minioBucketName := os.Getenv("MINIO_BUCKET_NAME")
	if minioBucketName != "" {
		_ = gp.Save("minio.bucketName", minioBucketName)
	}

	minioUseIAM := os.Getenv("MINIO_USE_IAM")
	if minioUseIAM != "" {
		_ = gp.Save("minio.useIAM", minioUseIAM)
	}

	minioIAMEndpoint := os.Getenv("MINIO_IAM_ENDPOINT")
	if minioIAMEndpoint != "" {
		_ = gp.Save("minio.iamEndpoint", minioIAMEndpoint)
	}

	minioRootPath := os.Getenv("MINIO_ROOT_PATH")
	if minioRootPath != "" {
		_ = gp.Save("minio.rootPath", minioRootPath)
	}

	minioBackupBucketName := os.Getenv("MINIO_BACKUP_BUCKET_NAME")
	if minioBackupBucketName != "" {
		_ = gp.Save("minio.backupBucketName", minioBackupBucketName)
	}

	minioBackupRootPath := os.Getenv("MINIO_BACKUP_ROOT_PATH")
	if minioBackupRootPath != "" {
		_ = gp.Save("minio.backupRootPath", minioBackupRootPath)
	}

	minioBackupAddress := os.Getenv("MINIO_BACKUP_ADDRESS")
	if minioBackupAddress != "" {
		_ = gp.Save("minio.backupAddress", minioBackupAddress)
	}

	minioBackupPort := os.Getenv("MINIO_BACKUP_PORT")
	if minioBackupPort != "" {
		_ = gp.Save("minio.backupPort", minioBackupPort)
	}

	minioBackupRegion := os.Getenv("MINIO_BACKUP_REGION")
	if minioBackupRegion != "" {
		_ = gp.Save("minio.backupRegion", minioBackupRegion)
	}

	minioBackupAccessKey := os.Getenv("MINIO_BACKUP_ACCESS_KEY")
	if minioBackupAccessKey != "" {
		_ = gp.Save("minio.backupAccessKeyID", minioBackupAccessKey)
	}

	minioBackupSecretKey := os.Getenv("MINIO_BACKUP_SECRET_KEY")
	if minioBackupSecretKey != "" {
		_ = gp.Save("minio.backupSecretAccessKey", minioBackupSecretKey)
	}

	minioBackupToken := os.Getenv("MINIO_BACKUP_TOKEN")
	if minioBackupToken != "" {
		_ = gp.Save("minio.backupToken", minioBackupToken)
	}

	backupGcpCredentialJSON := os.Getenv("BACKUP_GCP_KEY_JSON")
	if backupGcpCredentialJSON != "" {
		_ = gp.Save("minio.backupGcpCredentialJSON", backupGcpCredentialJSON)
	}

	minioBackupUseSSL := os.Getenv("MINIO_BACKUP_USE_SSL")
	if minioBackupUseSSL != "" {
		_ = gp.Save("minio.backupUseSSL", minioBackupUseSSL)
	}

	minioBackupUseIAM := os.Getenv("MINIO_BACKUP_USE_IAM")
	if minioBackupUseIAM != "" {
		_ = gp.Save("minio.backupUseIAM", minioBackupUseIAM)
	}

	minioBackupIAMEndpoint := os.Getenv("MINIO_BACKUP_IAM_ENDPOINT")
	if minioBackupIAMEndpoint != "" {
		_ = gp.Save("minio.backupIamEndpoint", minioBackupIAMEndpoint)
	}
}

func (gp *BaseTable) loadMilvusConfig() {
	milvusAddress := os.Getenv("MILVUS_ADDRESS")
	if milvusAddress != "" {
		_ = gp.Save("milvus.address", milvusAddress)
	}

	milvusPort := os.Getenv("MILVUS_PORT")
	if milvusPort != "" {
		_ = gp.Save("milvus.port", milvusPort)
	}

	milvusTlsMode := os.Getenv("MILVUS_TLS_MODE")
	if milvusTlsMode != "" {
		_ = gp.Save("milvus.tlsMode", milvusTlsMode)
	}

	milvusUser := os.Getenv("MILVUS_USER")
	if milvusUser != "" {
		_ = gp.Save("milvus.user", milvusUser)
	}

	milvusPassword := os.Getenv("MILVUS_PASSWORD")
	if milvusPassword != "" {
		_ = gp.Save("milvus.password", milvusPassword)
	}

	milvusCACertPath := os.Getenv("MILVUS_CA_CERT_PATH")
	if milvusCACertPath != "" {
		_ = gp.Save("milvus.caCertPath", milvusCACertPath)
	}

	milvusServerName := os.Getenv("MILVUS_SERVER_NAME")
	if milvusServerName != "" {
		_ = gp.Save("milvus.serverName", milvusServerName)
	}

	milvusMtlsCertPath := os.Getenv("MILVUS_MTLS_CERT_PATH")
	if milvusMtlsCertPath != "" {
		_ = gp.Save("milvus.mtlsCertPath", milvusMtlsCertPath)
	}

	milvusMtlsKeyPath := os.Getenv("MILVUS_MTLS_KEY_PATH")
	if milvusMtlsKeyPath != "" {
		_ = gp.Save("milvus.mtlsKeyPath", milvusMtlsKeyPath)
	}

	milvusRPCChannelName := os.Getenv("MILVUS_RPC_CHANNEL_NAME")
	if milvusRPCChannelName != "" {
		_ = gp.Save("milvus.rpcChannelName", milvusRPCChannelName)
	}
}

func (gp *BaseTable) loadBackupConfig() {
	backupParallelismBackupCollection := os.Getenv("BACKUP_PARALLELISM_BACKUP_COLLECTION")
	if backupParallelismBackupCollection != "" {
		_ = gp.Save("backup.parallelism.backupCollection", backupParallelismBackupCollection)
	}

	backupParallelismCopydata := os.Getenv("BACKUP_PARALLELISM_COPYDATA")
	if backupParallelismCopydata != "" {
		_ = gp.Save("backup.parallelism.copydata", backupParallelismCopydata)
	}

	backupParallelismRestoreCollection := os.Getenv("BACKUP_PARALLELISM_RESTORE_COLLECTION")
	if backupParallelismRestoreCollection != "" {
		_ = gp.Save("backup.parallelism.restoreCollection", backupParallelismRestoreCollection)
	}

	backupKeepTempFiles := os.Getenv("BACKUP_KEEP_TEMP_FILES")
	if backupKeepTempFiles != "" {
		_ = gp.Save("backup.keepTempFiles", backupKeepTempFiles)
	}

	backupGCPauseEnable := os.Getenv("BACKUP_GC_PAUSE_ENABLE")
	if backupGCPauseEnable != "" {
		_ = gp.Save("backup.gcPause.enable", backupGCPauseEnable)
	}

	backupGCPauseAddress := os.Getenv("BACKUP_GC_PAUSE_ADDRESS")
	if backupGCPauseAddress != "" {
		_ = gp.Save("backup.gcPause.address", backupGCPauseAddress)
	}
}
