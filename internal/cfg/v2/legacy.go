package v2

import "strings"

// legacyKeys maps a v1 config file key to what replaced it in v2. A v2 file is
// never allowed to carry v1 keys, but recognizing them turns "unknown key"
// into an actionable migration error.
//
// Keys are lower-cased, matching the flattened form a config file is read in.
var legacyKeys = map[string]string{
	"milvus.address":        "milvus.grpc.address",
	"milvus.port":           "milvus.grpc.port",
	"milvus.tlsmode":        "milvus.grpc.tlsMode, with 0, 1, 2 written as disabled, server, mutual",
	"milvus.cacertpath":     "milvus.grpc.caCertPath",
	"milvus.servername":     "milvus.grpc.serverName",
	"milvus.mtlscertpath":   "milvus.grpc.mtlsCertPath",
	"milvus.mtlskeypath":    "milvus.grpc.mtlsKeyPath",
	"milvus.rpcchannelname": "milvus.replicate.rpcChannelName",

	"http.enabled":         "",
	"http.debugmode":       "server.debugMode",
	"http.swaggerbasepath": "server.swaggerBasePath",

	"log.file.filename": "log.file.path",
	"log.file.maxsize":  "log.file.maxSizeMiB",

	"cloud.address": "zillizCloud.endpoint",
	"cloud.apikey":  "zillizCloud.apiKey",

	"storage.storagetype":     "milvus.storage.provider",
	"minio.storagetype":       "milvus.storage.provider",
	"minio.cloudprovider":     "milvus.storage.provider",
	"minio.address":           "milvus.storage.address",
	"minio.port":              "milvus.storage.port",
	"minio.region":            "milvus.storage.region",
	"minio.usessl":            "milvus.storage.useSSL",
	"minio.bucketname":        "milvus.storage.bucketName",
	"minio.rootpath":          "milvus.storage.rootPath",
	"minio.accesskeyid":       "milvus.storage.auth.accessKeyID, or milvus.storage.accountName for azure",
	"minio.secretaccesskey":   "milvus.storage.auth.secretAccessKey, or milvus.storage.auth.accountKey for azure",
	"minio.token":             "milvus.storage.auth.sessionToken",
	"minio.gcpcredentialjson": "milvus.storage.auth.credentialsFile with milvus.storage.auth.type: serviceAccount",
	"minio.useiam":            "milvus.storage.auth.type: iam",
	"minio.iamendpoint":       "milvus.storage.auth.endpoint",

	"storage.backupstoragetype":     "backup.storage.provider",
	"minio.backupstoragetype":       "backup.storage.provider",
	"minio.backupaddress":           "backup.storage.address",
	"minio.backupport":              "backup.storage.port",
	"minio.backupregion":            "backup.storage.region",
	"minio.backupusessl":            "backup.storage.useSSL",
	"minio.backupbucketname":        "backup.storage.bucketName",
	"minio.backuprootpath":          "backup.storage.rootPath",
	"minio.backupaccesskeyid":       "backup.storage.auth.accessKeyID, or backup.storage.accountName for azure",
	"minio.backupsecretaccesskey":   "backup.storage.auth.secretAccessKey, or backup.storage.auth.accountKey for azure",
	"minio.backuptoken":             "backup.storage.auth.sessionToken",
	"minio.backupgcpcredentialjson": "backup.storage.auth.credentialsFile with backup.storage.auth.type: serviceAccount",
	"minio.backupuseiam":            "backup.storage.auth.type: iam",
	"minio.backupiamendpoint":       "backup.storage.auth.endpoint",

	"minio.crossstorage":              "transfer.mode",
	"minio.multipartcopythresholdmib": "transfer.multipartCopyThresholdMiB",

	"backup.parallelism.copydata":          "transfer.concurrency",
	"backup.parallelism.backupcollection":  "backup.concurrency.collections",
	"backup.parallelism.backupsegment":     "backup.concurrency.segments",
	"backup.parallelism.restorecollection": "restore.concurrency.collections",
	"backup.parallelism.importjob":         "restore.concurrency.importJobs",
	"backup.keeptempfiles":                 "restore.keepTempFiles",
	"backup.gcpause.enable":                "backup.pauseGC",
	"backup.gcpause.address":               "milvus.management.endpoint",
}

// legacyEnvKeys maps a v1 environment variable name to what replaced it in v2.
// Environment variables and --set paths are schema-versioned too, so a v1 name
// is reported the same way a v1 config key is.
//
// v2 keeps an environment variable for credentials only, so a v1 name that
// configured anything else is answered with the config key that replaced it:
// there is nothing left to export.
//
// Keys are lower-cased, matching how --set keys are looked up.
var legacyEnvKeys = map[string]string{
	"milvus_address":          "the milvus.grpc.address config key",
	"milvus_port":             "the milvus.grpc.port config key",
	"milvus_tls_mode":         "the milvus.grpc.tlsMode config key, with 0, 1, 2 written as disabled, server, mutual",
	"milvus_ca_cert_path":     "the milvus.grpc.caCertPath config key",
	"milvus_server_name":      "the milvus.grpc.serverName config key",
	"milvus_mtls_cert_path":   "the milvus.grpc.mtlsCertPath config key",
	"milvus_mtls_key_path":    "the milvus.grpc.mtlsKeyPath config key",
	"milvus_rpc_channel_name": "the milvus.replicate.rpcChannelName config key",

	"minio_address":      "the milvus.storage.address config key",
	"minio_port":         "the milvus.storage.port config key",
	"minio_region":       "the milvus.storage.region config key",
	"minio_use_ssl":      "the milvus.storage.useSSL config key",
	"minio_bucket_name":  "the milvus.storage.bucketName config key",
	"minio_root_path":    "the milvus.storage.rootPath config key",
	"minio_access_key":   "MILVUS_STORAGE_AUTH_ACCESS_KEY_ID, or MILVUS_STORAGE_ACCOUNT_NAME for azure",
	"minio_secret_key":   "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY, or MILVUS_STORAGE_AUTH_ACCOUNT_KEY for azure",
	"minio_token":        "MILVUS_STORAGE_AUTH_SESSION_TOKEN",
	"minio_use_iam":      "the milvus.storage.auth.type config key, set to iam",
	"minio_iam_endpoint": "the milvus.storage.auth.endpoint config key",
	"gcp_key_json":       "MILVUS_STORAGE_AUTH_CREDENTIALS_FILE",

	"minio_backup_address":      "the backup.storage.address config key",
	"minio_backup_port":         "the backup.storage.port config key",
	"minio_backup_region":       "the backup.storage.region config key",
	"minio_backup_use_ssl":      "the backup.storage.useSSL config key",
	"minio_backup_bucket_name":  "the backup.storage.bucketName config key",
	"minio_backup_root_path":    "the backup.storage.rootPath config key",
	"minio_backup_access_key":   "BACKUP_STORAGE_AUTH_ACCESS_KEY_ID, or BACKUP_STORAGE_ACCOUNT_NAME for azure",
	"minio_backup_secret_key":   "BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY, or BACKUP_STORAGE_AUTH_ACCOUNT_KEY for azure",
	"minio_backup_token":        "BACKUP_STORAGE_AUTH_SESSION_TOKEN",
	"minio_backup_use_iam":      "the backup.storage.auth.type config key, set to iam",
	"minio_backup_iam_endpoint": "the backup.storage.auth.endpoint config key",
	"backup_gcp_key_json":       "BACKUP_STORAGE_AUTH_CREDENTIALS_FILE",

	"backup_parallelism_copydata":           "the transfer.concurrency config key",
	"backup_parallelism_backup_collection":  "the backup.concurrency.collections config key",
	"backup_parallelism_restore_collection": "the restore.concurrency.collections config key",
	"backup_keep_temp_files":                "the restore.keepTempFiles config key",
	"backup_gc_pause_enable":                "the backup.pauseGC config key",
	"backup_gc_pause_address":               "the milvus.management.endpoint config key",
}

// Migration returns what replaced key in v2, and whether key is a known v1
// name at all. An empty replacement means the key was removed outright. key is
// a v1 config file key or environment variable name, matched case-insensitively.
func Migration(key string) (string, bool) {
	key = strings.ToLower(key)
	if to, ok := legacyKeys[key]; ok {
		return to, true
	}
	to, ok := legacyEnvKeys[key]

	return to, ok
}
