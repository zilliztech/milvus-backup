# Milvus Backup: Environment Variables

Every configuration parameter can also be set through an environment variable, which is the usual way to keep credentials out of the configuration file.

## How to use environment variables

Set them in your shell before running Milvus Backup:

```bash
export MILVUS_GRPC_PORT=29530
./milvus-backup server
```

A value is resolved from the first source that provides it:

```text
--set  >  environment variable  >  config file  >  default
```

`--set` accepts either spelling, so `--set MILVUS_USER=root` and `--set milvus.user=root` do the same thing.

Environment variables belong to the schema version the configuration file is written in. A file that declares `configVersion: v2` is resolved with the names below; a file written before that is read with the older schema and keeps the older variable names. `milvus-backup config migrate` reports the ones that need renaming, and `milvus-backup config show` prints where each resolved value actually came from.

## Supported environment variables

- [Log](#log)
- [Server](#server)
- [Milvus](#milvus)
- [Storage](#storage)
- [Backup, restore and transfer](#backup-restore-and-transfer)
- [Zilliz Cloud](#zilliz-cloud)

### Log

| Config key | Environment variable |
|------------|----------------------|
| `log.level` | `LOG_LEVEL` |
| `log.console` | `LOG_CONSOLE` |
| `log.file.path` | `LOG_FILE_PATH` |
| `log.file.maxSizeMiB` | `LOG_FILE_MAX_SIZE_MIB` |
| `log.file.maxDays` | `LOG_FILE_MAX_DAYS` |
| `log.file.maxBackups` | `LOG_FILE_MAX_BACKUPS` |

### Server

| Config key | Environment variable |
|------------|----------------------|
| `server.debugMode` | `SERVER_DEBUG_MODE` |
| `server.swaggerBasePath` | `SERVER_SWAGGER_BASE_PATH` |

### Milvus

| Config key | Environment variable |
|------------|----------------------|
| `milvus.user` | `MILVUS_USER` |
| `milvus.password` | `MILVUS_PASSWORD` |
| `milvus.grpc.address` | `MILVUS_GRPC_ADDRESS` |
| `milvus.grpc.port` | `MILVUS_GRPC_PORT` |
| `milvus.grpc.tlsMode` | `MILVUS_GRPC_TLS_MODE` |
| `milvus.grpc.caCertPath` | `MILVUS_GRPC_CA_CERT_PATH` |
| `milvus.grpc.serverName` | `MILVUS_GRPC_SERVER_NAME` |
| `milvus.grpc.mtlsCertPath` | `MILVUS_GRPC_MTLS_CERT_PATH` |
| `milvus.grpc.mtlsKeyPath` | `MILVUS_GRPC_MTLS_KEY_PATH` |
| `milvus.rest.endpoint` | `MILVUS_REST_ENDPOINT` |
| `milvus.management.endpoint` | `MILVUS_MANAGEMENT_ENDPOINT` |
| `milvus.replicate.rpcChannelName` | `MILVUS_REPLICATE_RPC_CHANNEL_NAME` |
| `milvus.etcd.endpoints` | `MILVUS_ETCD_ENDPOINTS` |
| `milvus.etcd.rootPath` | `MILVUS_ETCD_ROOT_PATH` |

`MILVUS_GRPC_TLS_MODE` takes `disabled`, `server` or `mutual`. `MILVUS_ETCD_ENDPOINTS` takes a comma-separated list.

### Storage

Both storage sections take the same parameters: `MILVUS_STORAGE_` describes the storage the Milvus deployment uses, and `BACKUP_STORAGE_` the backup destination.

| Config key | Environment variable |
|------------|----------------------|
| `milvus.storage.provider` | `MILVUS_STORAGE_PROVIDER` |
| `milvus.storage.address` | `MILVUS_STORAGE_ADDRESS` |
| `milvus.storage.port` | `MILVUS_STORAGE_PORT` |
| `milvus.storage.region` | `MILVUS_STORAGE_REGION` |
| `milvus.storage.useSSL` | `MILVUS_STORAGE_USE_SSL` |
| `milvus.storage.accountName` | `MILVUS_STORAGE_ACCOUNT_NAME` |
| `milvus.storage.bucketName` | `MILVUS_STORAGE_BUCKET_NAME` |
| `milvus.storage.rootPath` | `MILVUS_STORAGE_ROOT_PATH` |
| `milvus.storage.auth.type` | `MILVUS_STORAGE_AUTH_TYPE` |
| `milvus.storage.auth.accessKeyID` | `MILVUS_STORAGE_AUTH_ACCESS_KEY_ID` |
| `milvus.storage.auth.secretAccessKey` | `MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY` |
| `milvus.storage.auth.sessionToken` | `MILVUS_STORAGE_AUTH_SESSION_TOKEN` |
| `milvus.storage.auth.accountKey` | `MILVUS_STORAGE_AUTH_ACCOUNT_KEY` |
| `milvus.storage.auth.credentialsFile` | `MILVUS_STORAGE_AUTH_CREDENTIALS_FILE` |
| `milvus.storage.auth.endpoint` | `MILVUS_STORAGE_AUTH_ENDPOINT` |
| `backup.storage.provider` | `BACKUP_STORAGE_PROVIDER` |
| `backup.storage.address` | `BACKUP_STORAGE_ADDRESS` |
| `backup.storage.port` | `BACKUP_STORAGE_PORT` |
| `backup.storage.region` | `BACKUP_STORAGE_REGION` |
| `backup.storage.useSSL` | `BACKUP_STORAGE_USE_SSL` |
| `backup.storage.accountName` | `BACKUP_STORAGE_ACCOUNT_NAME` |
| `backup.storage.bucketName` | `BACKUP_STORAGE_BUCKET_NAME` |
| `backup.storage.rootPath` | `BACKUP_STORAGE_ROOT_PATH` |
| `backup.storage.auth.type` | `BACKUP_STORAGE_AUTH_TYPE` |
| `backup.storage.auth.accessKeyID` | `BACKUP_STORAGE_AUTH_ACCESS_KEY_ID` |
| `backup.storage.auth.secretAccessKey` | `BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY` |
| `backup.storage.auth.sessionToken` | `BACKUP_STORAGE_AUTH_SESSION_TOKEN` |
| `backup.storage.auth.accountKey` | `BACKUP_STORAGE_AUTH_ACCOUNT_KEY` |
| `backup.storage.auth.credentialsFile` | `BACKUP_STORAGE_AUTH_CREDENTIALS_FILE` |
| `backup.storage.auth.endpoint` | `BACKUP_STORAGE_AUTH_ENDPOINT` |

Anything the backup side does not set is inherited from the Milvus side, except `BACKUP_STORAGE_ROOT_PATH`, which always defaults to `backup`.

`*_PROVIDER` takes one of `local`, `minio`, `s3`, `aws`, `gcp`, `gcpnative`, `azure`, `aliyun`, `tencent` or `hwc`.

`*_AUTH_TYPE` selects which credentials apply. Naming one the chosen type does not use is rejected rather than ignored:

| Auth type | Credentials it uses |
|-----------|---------------------|
| `static` | `*_AUTH_ACCESS_KEY_ID` and `*_AUTH_SECRET_ACCESS_KEY`, optionally `*_AUTH_SESSION_TOKEN` |
| `sharedKey` | `*_ACCOUNT_NAME` and `*_AUTH_ACCOUNT_KEY` (Azure) |
| `serviceAccount` | `*_AUTH_CREDENTIALS_FILE` (Google Cloud) |
| `iam` | optionally `*_AUTH_ENDPOINT` |
| `default` | none — resolved by the provider SDK |

### Backup, restore and transfer

| Config key | Environment variable |
|------------|----------------------|
| `backup.concurrency.collections` | `BACKUP_CONCURRENCY_COLLECTIONS` |
| `backup.concurrency.segments` | `BACKUP_CONCURRENCY_SEGMENTS` |
| `backup.pauseGC` | `BACKUP_PAUSE_GC` |
| `restore.concurrency.collections` | `RESTORE_CONCURRENCY_COLLECTIONS` |
| `restore.concurrency.importJobs` | `RESTORE_CONCURRENCY_IMPORT_JOBS` |
| `restore.keepTempFiles` | `RESTORE_KEEP_TEMP_FILES` |
| `transfer.mode` | `TRANSFER_MODE` |
| `transfer.concurrency` | `TRANSFER_CONCURRENCY` |
| `transfer.multipartCopyThresholdMiB` | `TRANSFER_MULTIPART_COPY_THRESHOLD_MIB` |

`TRANSFER_MODE` takes `auto`, `direct` or `streaming`. See [storage transfer](transfer.md).

### Zilliz Cloud

| Config key | Environment variable |
|------------|----------------------|
| `zillizCloud.endpoint` | `ZILLIZ_CLOUD_ENDPOINT` |
| `zillizCloud.apiKey` | `ZILLIZ_CLOUD_API_KEY` |
