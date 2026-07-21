# Milvus Backup: Environment Variables

Milvus Backup supports environment variables for configuration and customization.
These variables allow you to override settings defined in your configuration file or inject secrets and runtime options.

## How to Use Environment Variables

You can set environment variables in your shell before running Milvus Backup. For example:

```bash
$ export MILVUS_PORT=29530
$ ./milvus-backup server
```

---

## Supported Environment Variables

> All variables are optional. If not set, defaults are used (see [`backup.yaml`](https://github.com/zilliztech/milvus-backup/blob/main/configs/backup.yaml)).
>
> For the latest list of supported environment variables, refer to `internal/cfg/cfg.go` (search for `EnvKeys`) in the source code.

- [Milvus Connection](#milvus-connection)
- [Milvus storage](#milvus-storage)
- [Backup storage](#backup-storage)
- [Transfer](#transfer)
- [Backup](#backup)
- [Restore](#restore)

### Milvus Connection

| Config Key           | Env Variable        | Description                                 |
|----------------------|---------------------|---------------------------------------------|
| milvus.address       | MILVUS_ADDRESS      | Milvus proxy address                        |
| milvus.port          | MILVUS_PORT         | Milvus proxy port                           |
| milvus.user          | MILVUS_USER         | Milvus user name                            |
| milvus.password      | MILVUS_PASSWORD     | Milvus password                             |
| milvus.tlsMode       | MILVUS_TLS_MODE     | TLS mode (0/1/2)                            |
| milvus.caCertPath    | MILVUS_CA_CERT_PATH | TLS CA certificate path                     |
| milvus.serverName    | MILVUS_SERVER_NAME  | Milvus server name (TLS)                    |
| milvus.mtlsCertPath  | MILVUS_MTLS_CERT_PATH| Mutual TLS certificate path                |
| milvus.mtlsKeyPath   | MILVUS_MTLS_KEY_PATH| Mutual TLS key path                         |
| milvus.rpcChannelName| MILVUS_RPC_CHANNEL_NAME | Replicate msg channel name               |

### Milvus storage

| Config Key                         | Env Variable                       | Description                     |
|------------------------------------|------------------------------------|---------------------------------|
| milvus.storage.provider            | MILVUS_STORAGE_PROVIDER            | Storage provider                |
| milvus.storage.address             | MILVUS_STORAGE_ADDRESS             | Storage endpoint address        |
| milvus.storage.port                | MILVUS_STORAGE_PORT                | Storage endpoint port           |
| milvus.storage.region              | MILVUS_STORAGE_REGION              | Region                          |
| milvus.storage.accessKeyID         | MILVUS_STORAGE_ACCESS_KEY          | Access key ID                   |
| milvus.storage.secretAccessKey     | MILVUS_STORAGE_SECRET_KEY          | Secret access key               |
| milvus.storage.token               | MILVUS_STORAGE_TOKEN               | Session token                   |
| milvus.storage.rootPath            | MILVUS_STORAGE_ROOT_PATH           | Root path                       |
| milvus.storage.useSSL              | MILVUS_STORAGE_USE_SSL             | Use SSL                         |
| milvus.storage.useIAM              | MILVUS_STORAGE_USE_IAM             | Use IAM credentials             |
| milvus.storage.iamEndpoint         | MILVUS_STORAGE_IAM_ENDPOINT        | IAM endpoint                    |
| milvus.storage.bucketName          | MILVUS_STORAGE_BUCKET_NAME         | Bucket name                     |
| milvus.storage.gcpCredentialJSON   | MILVUS_STORAGE_GCP_KEY_JSON        | GCP credentials JSON            |

### Backup storage

| Config Key                                | Env Variable                                | Description                              |
|-------------------------------------------|---------------------------------------------|------------------------------------------|
| backup.storage.provider                   | BACKUP_STORAGE_PROVIDER                     | Backup storage provider                  |
| backup.storage.address                    | BACKUP_STORAGE_ADDRESS                      | Backup storage endpoint address          |
| backup.storage.region                     | BACKUP_STORAGE_REGION                       | Backup storage region                    |
| backup.storage.port                       | BACKUP_STORAGE_PORT                         | Backup storage endpoint port             |
| backup.storage.accessKeyID                | BACKUP_STORAGE_ACCESS_KEY                   | Backup storage access key ID             |
| backup.storage.secretAccessKey            | BACKUP_STORAGE_SECRET_KEY                   | Backup storage secret access key         |
| backup.storage.token                      | BACKUP_STORAGE_TOKEN                        | Backup storage session token             |
| backup.storage.rootPath                   | BACKUP_STORAGE_ROOT_PATH                    | Backup root path                         |
| backup.storage.useSSL                     | BACKUP_STORAGE_USE_SSL                      | Use SSL                                  |
| backup.storage.useIAM                     | BACKUP_STORAGE_USE_IAM                      | Use IAM                                  |
| backup.storage.iamEndpoint                | BACKUP_STORAGE_IAM_ENDPOINT                 | IAM endpoint                             |
| backup.storage.bucketName                 | BACKUP_STORAGE_BUCKET_NAME                  | Backup bucket name                       |
| backup.storage.gcpCredentialJSON          | BACKUP_STORAGE_GCP_KEY_JSON                 | GCP credentials JSON                     |

The historical `MINIO_*`, `MINIO_BACKUP_*`, `GCP_KEY_JSON`, and `BACKUP_GCP_KEY_JSON` variables remain supported as compatibility aliases. The new variables take precedence when both forms are set.

### Transfer

| Config Key                        | Env Variable                         | Description                                      |
|-----------------------------------|--------------------------------------|--------------------------------------------------|
| transfer.mode                     | TRANSFER_MODE                        | `auto`, `direct`, or `streaming`                 |
| transfer.concurrency              | TRANSFER_CONCURRENCY                 | Maximum concurrently transferred objects         |
| transfer.multipartCopyThresholdMiB| TRANSFER_MULTIPART_COPY_THRESHOLD_MIB| Multipart direct-copy threshold in MiB           |

### Backup

| Config Key                     | Env Variable                    | Description                                   |
|--------------------------------|---------------------------------|-----------------------------------------------|
| backup.concurrency.collections | BACKUP_CONCURRENCY_COLLECTIONS  | Concurrent backup collection tasks            |
| backup.concurrency.segments    | BACKUP_CONCURRENCY_SEGMENTS     | Concurrent backup segment tasks               |
| backup.gcPause.enable          | BACKUP_GC_PAUSE_ENABLE          | Pause GC during backup                        |
| backup.gcPause.address         | BACKUP_GC_PAUSE_ADDRESS         | GC pause service address                      |

### Restore

| Config Key                     | Env Variable                    | Description                                   |
|--------------------------------|---------------------------------|-----------------------------------------------|
| restore.concurrency.collections| RESTORE_CONCURRENCY_COLLECTIONS | Concurrent restore collection tasks           |
| restore.concurrency.importJobs | RESTORE_CONCURRENCY_IMPORT_JOBS | Concurrent Milvus import jobs                 |
| restore.keepTempFiles          | RESTORE_KEEP_TEMP_FILES         | Keep temporary restore files                  |

The historical `BACKUP_PARALLELISM_*` and `BACKUP_KEEP_TEMP_FILES` variables remain supported as compatibility aliases.
