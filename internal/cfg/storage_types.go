package cfg

// Storage type constants.
// Keep values aligned with configs/backup.yaml and historical paramtable constants.
const (
	Local = "local"
	Minio = "minio"
	S3    = "s3"

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
