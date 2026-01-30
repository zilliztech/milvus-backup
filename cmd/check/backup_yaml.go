package check

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/milvus-backup/cmd/root"
	"github.com/zilliztech/milvus-backup/internal/cfg"
)

func newBackupYamlCmd(opt *root.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup_yaml",
		Short: "backup_yaml is a subcommand to check.  It prints the current backup config file in yaml format to stdio.",

		Run: func(cmd *cobra.Command, args []string) {
			params := opt.InitGlobalVars()

			printParams(params)
		},
	}

	return cmd
}

type YAMLConFig struct {
	Log struct {
		Level   string `yaml:"level"`
		Console bool   `yaml:"console"`
		File    struct {
			Filename   string `yaml:"filename"`
			MaxSize    int    `yaml:"maxSize"`
			MaxDays    int    `yaml:"maxDays"`
			MaxBackups int    `yaml:"maxBackups"`
		} `yaml:"file"`
	} `yaml:"log"`
	Cloud struct {
		Address string `yaml:"address"`
		APIKey  string `yaml:"apikey"`
	} `yaml:"cloud"`
	Milvus struct {
		Address        string `yaml:"address"`
		Port           int    `yaml:"port"`
		User           string `yaml:"user"`
		Password       string `yaml:"password"`
		TlsMode        int    `yaml:"tlsMode"`
		CACertPath     string `yaml:"caCertPath"`
		ServerName     string `yaml:"serverName"`
		MtlsCertPath   string `yaml:"mtlsCertPath"`
		MtlsKeyPath    string `yaml:"mtlsKeyPath"`
		RpcChannelName string `yaml:"rpcChannelName"`
		Etcd           struct {
			Endpoints string `yaml:"endpoints"`
			RootPath  string `yaml:"rootPath"`
		} `yaml:"etcd"`
	} `yaml:"milvus"`
	Minio struct {
		StorageType       string `yaml:"storageType"`
		Address           string `yaml:"address"`
		Port              int    `yaml:"port"`
		Region            string `yaml:"region"`
		AccessKeyID       string `yaml:"accessKeyID"`
		SecretAccessKey   string `yaml:"secretAccessKey"`
		Token             string `yaml:"token"`
		GcpCredentialJSON string `yaml:"gcpCredentialJSON"`
		UseSSL            bool   `yaml:"useSSL"`
		UseIAM            bool   `yaml:"useIAM"`
		IamEndpoint       string `yaml:"iamEndpoint"`
		BucketName        string `yaml:"bucketName"`
		RootPath          string `yaml:"rootPath"`

		BackupStorageType       string `yaml:"backupStorageType"`
		BackupAddress           string `yaml:"backupAddress"`
		BackupPort              int    `yaml:"backupPort"`
		BackupRegion            string `yaml:"backupRegion"`
		BackupAccessKeyID       string `yaml:"backupAccessKeyID"`
		BackupSecretAccessKey   string `yaml:"backupSecretAccessKey"`
		BackupToken             string `yaml:"backupToken"`
		BackupGcpCredentialJSON string `yaml:"backupGcpCredentialJSON"`
		BackupUseSSL            bool   `yaml:"backupUseSSL"`
		BackupUseIAM            bool   `yaml:"backupUseIAM"`
		BackupIamEndpoint       string `yaml:"backupIamEndpoint"`
		BackupBucketName        string `yaml:"backupBucketName"`
		BackupRootPath          string `yaml:"backupRootPath"`

		CrossStorage bool `yaml:"crossStorage"`
	} `yaml:"minio"`
	Backup struct {
		Parallelism struct {
			Copydata          int `yaml:"copydata"`
			BackupCollection  int `yaml:"backupCollection"`
			BackupSegment     int `yaml:"backupSegment"`
			RestoreCollection int `yaml:"restoreCollection"`
			ImportJob         int `yaml:"importJob"`
		} `yaml:"parallelism"`
		KeepTempFiles bool `yaml:"keepTempFiles"`
		GCPause       struct {
			Enable  bool   `yaml:"enable"`
			Address string `yaml:"address"`
		} `yaml:"gcPause"`
	} `yaml:"backup"`
}

func printParams(base *cfg.Config) {

	yml := YAMLConFig{}

	yml.Log.Level = base.Log.Level.Value()
	yml.Log.Console = base.Log.Console.Value()
	yml.Log.File.Filename = base.Log.File.Filename.Value()
	yml.Log.File.MaxSize = base.Log.File.MaxSize.Value()
	yml.Log.File.MaxDays = base.Log.File.MaxDays.Value()
	yml.Log.File.MaxBackups = base.Log.File.MaxBackups.Value()

	yml.Cloud.Address = base.Cloud.Address.Value()
	yml.Cloud.APIKey = base.Cloud.APIKey.Value()

	yml.Milvus.Address = base.Milvus.Address.Value()
	yml.Milvus.Port = base.Milvus.Port.Value()
	yml.Milvus.User = base.Milvus.User.Value()
	yml.Milvus.Password = base.Milvus.Password.Value()
	yml.Milvus.TlsMode = base.Milvus.TLSMode.Value()
	yml.Milvus.CACertPath = base.Milvus.CACertPath.Value()
	yml.Milvus.ServerName = base.Milvus.ServerName.Value()
	yml.Milvus.MtlsCertPath = base.Milvus.MTLSCertPath.Value()
	yml.Milvus.MtlsKeyPath = base.Milvus.MTLSKeyPath.Value()
	yml.Milvus.RpcChannelName = base.Milvus.RPCChannelName.Value()
	yml.Milvus.Etcd.Endpoints = base.Milvus.Etcd.Endpoints.Value()
	yml.Milvus.Etcd.RootPath = base.Milvus.Etcd.RootPath.Value()

	yml.Minio.StorageType = base.Minio.StorageType.Value()
	yml.Minio.Address = base.Minio.Address.Value()
	yml.Minio.Port = base.Minio.Port.Value()
	yml.Minio.Region = base.Minio.Region.Value()
	yml.Minio.AccessKeyID = base.Minio.AccessKeyID.Value()
	yml.Minio.SecretAccessKey = base.Minio.SecretAccessKey.Value()
	yml.Minio.Token = base.Minio.Token.Value()
	yml.Minio.GcpCredentialJSON = base.Minio.GcpCredentialJSON.Value()
	yml.Minio.UseSSL = base.Minio.UseSSL.Value()
	yml.Minio.UseIAM = base.Minio.UseIAM.Value()
	yml.Minio.IamEndpoint = base.Minio.IAMEndpoint.Value()
	yml.Minio.BucketName = base.Minio.BucketName.Value()
	yml.Minio.RootPath = base.Minio.RootPath.Value()

	yml.Minio.BackupStorageType = base.Minio.BackupStorageType.Value()
	yml.Minio.BackupAddress = base.Minio.BackupAddress.Value()
	yml.Minio.BackupPort = base.Minio.BackupPort.Value()
	yml.Minio.BackupRegion = base.Minio.BackupRegion.Value()
	yml.Minio.BackupAccessKeyID = base.Minio.BackupAccessKeyID.Value()
	yml.Minio.BackupSecretAccessKey = base.Minio.BackupSecretAccessKey.Value()
	yml.Minio.BackupToken = base.Minio.BackupToken.Value()
	yml.Minio.BackupGcpCredentialJSON = base.Minio.BackupGcpCredentialJSON.Value()
	yml.Minio.BackupUseSSL = base.Minio.BackupUseSSL.Value()
	yml.Minio.BackupUseIAM = base.Minio.BackupUseIAM.Value()
	yml.Minio.BackupIamEndpoint = base.Minio.BackupIAMEndpoint.Value()
	yml.Minio.BackupBucketName = base.Minio.BackupBucketName.Value()
	yml.Minio.BackupRootPath = base.Minio.BackupRootPath.Value()
	yml.Minio.CrossStorage = base.Minio.CrossStorage.Value()

	yml.Backup.Parallelism.Copydata = base.Backup.Parallelism.CopyData.Value()
	yml.Backup.Parallelism.BackupCollection = base.Backup.Parallelism.BackupCollection.Value()
	yml.Backup.Parallelism.BackupSegment = base.Backup.Parallelism.BackupSegment.Value()
	yml.Backup.Parallelism.RestoreCollection = base.Backup.Parallelism.RestoreCollection.Value()
	yml.Backup.Parallelism.ImportJob = base.Backup.Parallelism.ImportJob.Value()
	yml.Backup.KeepTempFiles = base.Backup.KeepTempFiles.Value()
	yml.Backup.GCPause.Enable = base.Backup.GCPause.Enable.Value()
	yml.Backup.GCPause.Address = base.Backup.GCPause.Address.Value()

	bytes, err := yaml.Marshal(yml)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n%s", strings.Repeat("-", 80), string(bytes))
}
