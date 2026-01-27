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
> For the latest list of supported environment variables, refer to [`paramtable/base_table.go`](https://github.com/zilliztech/milvus-backup/blob/main/core/paramtable/base_table.go) in the source code.

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
| milvus.caCertPath    | MILVUS_CA_CERT_PATH | TLS CA certificate path                     |
| milvus.serverName    | MILVUS_SERVER_NAME  | Milvus server name (TLS)                    |
| milvus.mtlsCertPath  | MILVUS_MTLS_CERT_PATH| Mutual TLS certificate path                |
| milvus.mtlsKeyPath   | MILVUS_MTLS_KEY_PATH| Mutual TLS key path                         |

### MinIO (Object Storage)

| Config Key                  | Env Variable          | Description                                  |
|-----------------------------|-----------------------|----------------------------------------------|
| minio.address               | MINIO_ADDRESS         | MinIO server address                         |
| minio.port                  | MINIO_PORT            | MinIO server port                            |
| minio.accessKeyID           | MINIO_ACCESS_KEY      | MinIO access key ID                          |
| minio.secretAccessKey       | MINIO_SECRET_KEY      | MinIO secret access key                      |
| minio.useSSL                | MINIO_USE_SSL         | Use SSL for MinIO                            |
| minio.bucketName            | MINIO_BUCKET_NAME     | MinIO bucket name                            |
| minio.gcpCredentialJSON     | GCP_KEY_JSON          | Path to GCP credentials JSON for MinIO       |

#### Backup-specific MinIO Variables

| Config Key                        | Env Variable                  | Description                                  |
|-----------------------------------|-------------------------------|----------------------------------------------|
| minio.backupBucketName            | MINIO_BACKUP_BUCKET_NAME      | Backup bucket name                           |
| minio.backupAddress               | MINIO_BACKUP_ADDRESS          | Backup MinIO server address                  |
| minio.backupPort                  | MINIO_BACKUP_PORT             | Backup MinIO server port                     |
| minio.backupAccessKeyID           | MINIO_BACKUP_ACCESS_KEY       | Backup MinIO access key ID                   |
| minio.backupSecretAccessKey       | MINIO_BACKUP_SECRET_KEY       | Backup MinIO secret access key               |
| minio.backupUseSSL                | MINIO_BACKUP_USE_SSL          | Use SSL for backup MinIO                     |

### Logging

| Config Key         | Env Variable     | Description                          |
|--------------------|------------------|--------------------------------------|
| log.level          | LOG_LEVEL        | Logging level (`info`, `debug`, etc) |
| log.console        | LOG_CONSOLE      | Print logs to console                |
| log.file.rootPath  | LOG_FILE_PATH    | Path to log file                     |

### HTTP API

| Config Key          | Env Variable          | Description                   |
|---------------------|-----------------------|-------------------------------|
| http.simpleResponse | HTTP_SIMPLE_RESPONSE  | Enable simple HTTP responses  |

### Others

| Config Key                  | Env Variable                  | Description                                   |
|-----------------------------|-------------------------------|-----------------------------------------------|
| backup.maxSegmentGroupSize  | BACKUP_MAX_SEGMENT_GROUP_SIZE | Maximum segment group size                    |
| backup.parallelism.backupCollection | BACKUP_PARALLELISM_BACKUP_COLLECTION | Backup collection parallelism         |
| backup.keepTempFiles        | BACKUP_KEEP_TEMP_FILES         | Keep temporary files after backup             |
