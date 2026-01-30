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
- [MinIO (Object Storage)](#minio-object-storage)
- [Logging](#logging)
- [HTTP API](#http-api)
- [Others](#others)

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

### MinIO (Object Storage)

| Config Key                  | Env Variable          | Description                                  |
|-----------------------------|-----------------------|----------------------------------------------|
| minio.address               | MINIO_ADDRESS         | MinIO server address                         |
| minio.port                  | MINIO_PORT            | MinIO server port                            |
| minio.region                | MINIO_REGION          | Region                                       |
| minio.accessKeyID           | MINIO_ACCESS_KEY      | MinIO access key ID                          |
| minio.secretAccessKey       | MINIO_SECRET_KEY      | MinIO secret access key                      |
| minio.token                 | MINIO_TOKEN           | Session token                                |
| minio.rootPath              | MINIO_ROOT_PATH       | Root path                                    |
| minio.useSSL                | MINIO_USE_SSL         | Use SSL for MinIO                            |
| minio.useIAM                | MINIO_USE_IAM         | Use IAM credentials                          |
| minio.iamEndpoint           | MINIO_IAM_ENDPOINT    | IAM endpoint                                 |
| minio.bucketName            | MINIO_BUCKET_NAME     | MinIO bucket name                            |
| minio.gcpCredentialJSON     | GCP_KEY_JSON          | Path to GCP credentials JSON for MinIO       |

#### Backup-specific MinIO Variables

| Config Key                        | Env Variable                  | Description                                  |
|-----------------------------------|-------------------------------|----------------------------------------------|
| minio.backupBucketName            | MINIO_BACKUP_BUCKET_NAME      | Backup bucket name                           |
| minio.backupAddress               | MINIO_BACKUP_ADDRESS          | Backup MinIO server address                  |
| minio.backupRegion                | MINIO_BACKUP_REGION           | Backup region                                |
| minio.backupPort                  | MINIO_BACKUP_PORT             | Backup MinIO server port                     |
| minio.backupAccessKeyID           | MINIO_BACKUP_ACCESS_KEY       | Backup MinIO access key ID                   |
| minio.backupSecretAccessKey       | MINIO_BACKUP_SECRET_KEY       | Backup MinIO secret access key               |
| minio.backupToken                 | MINIO_BACKUP_TOKEN            | Backup session token                         |
| minio.backupRootPath              | MINIO_BACKUP_ROOT_PATH        | Backup root path                             |
| minio.backupUseSSL                | MINIO_BACKUP_USE_SSL          | Use SSL for backup MinIO                     |
| minio.backupUseIAM                | MINIO_BACKUP_USE_IAM          | Use IAM for backup storage                   |
| minio.backupIamEndpoint           | MINIO_BACKUP_IAM_ENDPOINT     | Backup IAM endpoint                          |
| minio.backupGcpCredentialJSON     | BACKUP_GCP_KEY_JSON           | Path to GCP credentials JSON for backup      |

### Logging

| Config Key         | Env Variable     | Description                          |
|--------------------|------------------|--------------------------------------|
| log.level          | LOG_LEVEL        | Logging level (`info`, `debug`, etc) |
| log.console        | LOG_CONSOLE      | Print logs to console                |
| log.file.filename  | LOG_FILE_PATH    | Path to log file                     |

### HTTP API

| Config Key          | Env Variable          | Description                   |
|---------------------|-----------------------|-------------------------------|
| http.simpleResponse | HTTP_SIMPLE_RESPONSE  | Enable simple HTTP responses  |

### Others

| Config Key                  | Env Variable                  | Description                                   |
|-----------------------------|-------------------------------|-----------------------------------------------|
| backup.parallelism.backupCollection | BACKUP_PARALLELISM_BACKUP_COLLECTION | Backup collection parallelism         |
| backup.parallelism.copydata | BACKUP_PARALLELISM_COPYDATA   | Copy data parallelism                         |
| backup.parallelism.restoreCollection | BACKUP_PARALLELISM_RESTORE_COLLECTION | Restore collection parallelism     |
| backup.keepTempFiles        | BACKUP_KEEP_TEMP_FILES         | Keep temporary files after backup             |
| backup.gcPause.enable       | BACKUP_GC_PAUSE_ENABLE         | Pause GC during backup                        |
| backup.gcPause.address      | BACKUP_GC_PAUSE_ADDRESS        | GC pause service address                      |
