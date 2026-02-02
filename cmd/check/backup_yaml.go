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

	yml.Log.Level = base.Log.Level.Val
	yml.Log.Console = base.Log.Console.Val
	yml.Log.File.Filename = base.Log.File.Filename.Val
	yml.Log.File.MaxSize = base.Log.File.MaxSize.Val
	yml.Log.File.MaxDays = base.Log.File.MaxDays.Val
	yml.Log.File.MaxBackups = base.Log.File.MaxBackups.Val

	yml.Cloud.Address = base.Cloud.Address.Val
	yml.Cloud.APIKey = base.Cloud.APIKey.Val

	yml.Milvus.Address = base.Milvus.Address.Val
	yml.Milvus.Port = base.Milvus.Port.Val
	yml.Milvus.User = base.Milvus.User.Val
	yml.Milvus.Password = base.Milvus.Password.Val
	yml.Milvus.TlsMode = base.Milvus.TLSMode.Val
	yml.Milvus.CACertPath = base.Milvus.CACertPath.Val
	yml.Milvus.ServerName = base.Milvus.ServerName.Val
	yml.Milvus.MtlsCertPath = base.Milvus.MTLSCertPath.Val
	yml.Milvus.MtlsKeyPath = base.Milvus.MTLSKeyPath.Val
	yml.Milvus.RpcChannelName = base.Milvus.RPCChannelName.Val
	yml.Milvus.Etcd.Endpoints = base.Milvus.Etcd.Endpoints.Val
	yml.Milvus.Etcd.RootPath = base.Milvus.Etcd.RootPath.Val

	yml.Minio.StorageType = base.Minio.StorageType.Val
	yml.Minio.Address = base.Minio.Address.Val
	yml.Minio.Port = base.Minio.Port.Val
	yml.Minio.Region = base.Minio.Region.Val
	yml.Minio.AccessKeyID = base.Minio.AccessKeyID.Val
	yml.Minio.SecretAccessKey = base.Minio.SecretAccessKey.Val
	yml.Minio.Token = base.Minio.Token.Val
	yml.Minio.GcpCredentialJSON = base.Minio.GcpCredentialJSON.Val
	yml.Minio.UseSSL = base.Minio.UseSSL.Val
	yml.Minio.UseIAM = base.Minio.UseIAM.Val
	yml.Minio.IamEndpoint = base.Minio.IAMEndpoint.Val
	yml.Minio.BucketName = base.Minio.BucketName.Val
	yml.Minio.RootPath = base.Minio.RootPath.Val

	yml.Minio.BackupStorageType = base.Minio.BackupStorageType.Val
	yml.Minio.BackupAddress = base.Minio.BackupAddress.Val
	yml.Minio.BackupPort = base.Minio.BackupPort.Val
	yml.Minio.BackupRegion = base.Minio.BackupRegion.Val
	yml.Minio.BackupAccessKeyID = base.Minio.BackupAccessKeyID.Val
	yml.Minio.BackupSecretAccessKey = base.Minio.BackupSecretAccessKey.Val
	yml.Minio.BackupToken = base.Minio.BackupToken.Val
	yml.Minio.BackupGcpCredentialJSON = base.Minio.BackupGcpCredentialJSON.Val
	yml.Minio.BackupUseSSL = base.Minio.BackupUseSSL.Val
	yml.Minio.BackupUseIAM = base.Minio.BackupUseIAM.Val
	yml.Minio.BackupIamEndpoint = base.Minio.BackupIAMEndpoint.Val
	yml.Minio.BackupBucketName = base.Minio.BackupBucketName.Val
	yml.Minio.BackupRootPath = base.Minio.BackupRootPath.Val
	yml.Minio.CrossStorage = base.Minio.CrossStorage.Val

	yml.Backup.Parallelism.Copydata = base.Backup.Parallelism.CopyData.Val
	yml.Backup.Parallelism.BackupCollection = base.Backup.Parallelism.BackupCollection.Val
	yml.Backup.Parallelism.BackupSegment = base.Backup.Parallelism.BackupSegment.Val
	yml.Backup.Parallelism.RestoreCollection = base.Backup.Parallelism.RestoreCollection.Val
	yml.Backup.Parallelism.ImportJob = base.Backup.Parallelism.ImportJob.Val
	yml.Backup.KeepTempFiles = base.Backup.KeepTempFiles.Val
	yml.Backup.GCPause.Enable = base.Backup.GCPause.Enable.Val
	yml.Backup.GCPause.Address = base.Backup.GCPause.Address.Val

	bytes, err := yaml.Marshal(yml)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n%s", strings.Repeat("-", 80), string(bytes))
}
