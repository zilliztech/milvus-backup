# Milvus Backup

<div align="left">
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="Discord" /></a>
  <img src="https://img.shields.io/github/license/zilliztech/milvus-backup" alt="License" />
</div>

Milvus Backup is a command-line tool and API service for backing up and restoring Milvus data. Backup and restore operations run while the Milvus cluster remains available.

## Compatibility

Use the [latest release](https://github.com/zilliztech/milvus-backup/releases) whenever possible. The latest version supports backups from Milvus 2.2 and later, and restores to Milvus 2.4 and later.

A backup can be restored only to the same or a newer Milvus version:

| Backup version | Restore to 2.4 | Restore to 2.5 | Restore to 2.6 |
|----------------|----------------|----------------|----------------|
| 2.2            | Supported      | Supported      | Supported      |
| 2.3            | Supported      | Supported      | Supported      |
| 2.4            | Supported      | Supported      | Supported      |
| 2.5            | —              | Supported      | Supported      |
| 2.6            | —              | —              | Supported      |

For example, a backup created from Milvus 2.5 cannot be restored to Milvus 2.4.

## Installation

Download a binary from the [release page](https://github.com/zilliztech/milvus-backup/releases), or install it with Homebrew on macOS:

```shell
brew install zilliztech/tap/milvus-backup
```

## Configuration

Milvus Backup must be able to connect to Milvus, the storage used by Milvus, and the backup destination. Copy the example configuration and update it for your deployment:

```shell
cp configs/backup.yaml backup.yaml
```

The main sections are:

- `milvus`: Milvus address, credentials, and TLS settings. The etcd settings are only required when using `--backup_index_extra`.
- `minio`: source storage and backup storage settings. Despite the section name, it also supports S3, AWS, GCP, Aliyun, Azure, Tencent Cloud, and local storage.
- `backup`: backup and restore concurrency, temporary file handling, and garbage collection pause settings.
- `log`: log level and output settings.

Use values that match the Milvus deployment. In common installations, the storage defaults differ:

| Field | Docker Compose | Helm |
|-------|----------------|------|
| `bucketName` | `a-bucket` | `milvus-bucket` |
| `rootPath` | `files` | `file` |

See [configs/backup.yaml](configs/backup.yaml) for all available settings. Configuration values can also be supplied through [environment variables](docs/user_guide/env_variables.md) or overridden with `--set`:

```shell
milvus-backup --set MILVUS_USER=root --set MILVUS_PASSWORD=Milvus list
```

## Command-line usage

Run the configuration check before creating a backup:

```shell
milvus-backup check
milvus-backup create -n my_backup
milvus-backup list
milvus-backup restore -n my_backup
```

The main commands are:

| Command | Description |
|---------|-------------|
| `check` | Validate connections and inspect the resolved configuration. |
| `create` | Create a backup. |
| `delete` | Delete a backup by name. |
| `get` | Show a backup by name. |
| `list` | List backups in object storage. |
| `restore` | Restore a backup. |
| `l0compact` | Convert L0 delete data into a restorable physical backup copy. |
| `migrate` | Migrate backup data to Zilliz Cloud. |
| `server` | Start the REST API server. |

Run `milvus-backup <command> --help` for command-specific flags. See the [CLI end-to-end guide](docs/user_guide/e2e_demo_cli.md) for a complete backup and restore example.

## API server

Start the REST API server with:

```shell
milvus-backup server
```

It listens on port `8080` by default. Use `-p` to select another port:

```shell
milvus-backup server -p 8443
```

The Swagger UI is available at:

```text
http://localhost:8080/api/v1/docs/index.html
```

See the [API demo](docs/user_guide/api_demo.md) for example requests. The Swagger UI reflects the current API and should be treated as the authoritative reference.

## Advanced features

- [Cross-storage backup](docs/user_guide/cross_storage.md): copy backup data between different storage systems, such as MinIO and AWS S3.
- [RBAC backup and restore](docs/user_guide/rbac.md): include Milvus RBAC metadata in a backup or restore operation.
- [Segment merging restore](docs/user_guide/mul_seg_restore.md): group small segments into fewer import jobs to improve restore performance.

### Cross-storage example

The source storage settings must match the storage used by Milvus. Configure the backup destination under the same `minio` section:

```yaml
minio:
  storageType: "minio"
  address: localhost
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: a-bucket
  rootPath: files

  backupStorageType: "aws"
  backupAddress: s3.us-east-1.amazonaws.com
  backupPort: 443
  backupRegion: us-east-1
  backupAccessKeyID: <your-access-key-id>
  backupSecretAccessKey: <your-secret-access-key>
  backupBucketName: <your-bucket-name>
  backupRootPath: backups
  backupUseSSL: true
  crossStorage: true
```

Do not commit storage credentials to the repository. Prefer environment variables or another secret-management mechanism in production.

## FAQ

See [docs/FAQ.md](docs/FAQ.md) for common issues and troubleshooting advice.

## Development

Build the binary:

```shell
make all
```

The resulting executable is written to `./milvus-backup`.

Run the test suite:

```shell
make test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a change.

## License

Milvus Backup is licensed under the Apache License 2.0.
