package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/zilliztech/milvus-backup/core/paramtable"
	"gopkg.in/yaml.v3"
)

var configCmd = &cobra.Command{
	Use:   "backup_yaml",
	Short: "backup_yaml is a subcommand to check.  It prints the current backup config file in yaml format to stdio.",

	Run: func(cmd *cobra.Command, args []string) {
		var params paramtable.BackupParams
		params.GlobalInitWithYaml(config)
		params.Init()

		printParams(&params)
	},
}

type YAMLConFig struct {
	Log struct {
		Level   string `yaml:"level"`
		Console bool   `yaml:"console"`
		File    struct {
			RootPath string `yaml:"rootPath"`
		}
		Http struct {
			SimpleResponse bool `yaml:"simpleResponse"`
		} `yaml:"http"`
	} `yaml:"log"`
	Milvus struct {
		Address              string `yaml:"address"`
		Port                 int    `yaml:"port"`
		AuthorizationEnabled bool   `yaml:"authorizationEnabled"`
		TlsMode              int    `yaml:"tlsMode"`
		User                 string `yaml:"user"`
		Password             string `yaml:"password"`
		tlsCertPath          string `yaml:"tlsCertPath"`
		serverName           string `yaml:"serverName"`
	} `yaml:"milvus"`
	Minio struct {
		Address                 string `yaml:"address"`
		Port                    int    `yaml:"port"`
		AccessKeyID             string `yaml:"accessKeyID"`
		secretAccessKey         string `yaml:"secretAccessKey"`
		GcpCredentialJSON       string `yaml:"gcpCredentialJSON"`
		UseSSL                  bool   `yaml:"useSSL"`
		UseIAM                  bool   `yaml:"useIAM"`
		CloudProvider           string `yaml:"cloudProvider"`
		IamEndpoint             string `yaml:"iamEndpoint"`
		BucketName              string `yaml:"bucketName"`
		RootPath                string `yaml:"rootPath"`
		BackupGcpCredentialJSON string `yaml:"backupGcpCredentialJSON"`
		BackupBucketName        string `yaml:"backupBucketName"`
		BackupRootPath          string `yaml:"backupRootPath"`
	} `yaml:"minio"`
	Backup struct {
		MaxSegmentGroupSize string `yaml:"maxSegmentGroupSize"`
	} `yaml:"backup"`
}

func init() {
	checkCmd.AddCommand(configCmd)
}

func printParams(base *paramtable.BackupParams) {

	yml := YAMLConFig{}

	yml.Log.Level = base.BaseTable.LoadWithDefault("log.level", "debug")
	yml.Log.Console = base.ParseBool("log.console", false)
	yml.Log.File.RootPath = base.LoadWithDefault("log.file.rootPath", "backup.log")

	yml.Milvus.Address = base.LoadWithDefault("milvus.address", "localhost")
	yml.Milvus.Port = base.ParseIntWithDefault("milvus.port", 19530)
	yml.Milvus.AuthorizationEnabled = base.ParseBool("milvus.authorizationEnabled", false)
	yml.Milvus.TlsMode = base.ParseIntWithDefault("milvus.tlsMode", 0)
	yml.Milvus.User = base.BaseTable.LoadWithDefault("milvus.user", "")
	yml.Milvus.Password = base.BaseTable.LoadWithDefault("milvus.password", "")
	yml.Milvus.tlsCertPath = base.BaseTable.LoadWithDefault("milvus.tlsCertPath", "")
	yml.Milvus.serverName = base.BaseTable.LoadWithDefault("milvus.serverName", "localhost")

	yml.Minio.Address = base.LoadWithDefault("minio.address", "localhost")
	yml.Minio.Port = base.ParseIntWithDefault("minio.port", 9000)
	yml.Minio.AccessKeyID = base.BaseTable.LoadWithDefault("minio.accessKeyID", "")
	yml.Minio.secretAccessKey = base.BaseTable.LoadWithDefault("minio.secretAccessKey", "")
	yml.Minio.GcpCredentialJSON = base.BaseTable.LoadWithDefault("minio.gcpCredentialJSON", "")
	yml.Minio.UseSSL = base.ParseBool("minio.useSSL", false)
	yml.Minio.UseIAM = base.ParseBool("minio.useIAM", false)
	yml.Minio.CloudProvider = base.BaseTable.LoadWithDefault("minio.cloudProvider", "aws")
	yml.Minio.IamEndpoint = base.BaseTable.LoadWithDefault("minio.iamEndpoint", "")
	yml.Minio.BucketName = base.BaseTable.LoadWithDefault("minio.bucketName", "")
	yml.Minio.RootPath = base.LoadWithDefault("minio.rootPath", "")
	yml.Minio.BackupGcpCredentialJSON = base.BaseTable.LoadWithDefault("minio.backupGcpCredentialJSON", "")
	yml.Minio.BackupBucketName = base.LoadWithDefault("minio.backupBucketName", "")
	yml.Minio.BackupRootPath = base.LoadWithDefault("minio.backupRootPath", "")

	yml.Backup.MaxSegmentGroupSize = base.LoadWithDefault("backup.maxSegmentGroupSize", "5G")

	bytes, err := yaml.Marshal(yml)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n%s", strings.Repeat("-", 80), string(bytes))
}
