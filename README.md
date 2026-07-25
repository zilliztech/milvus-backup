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

- `milvus`: how to reach Milvus — credentials, the gRPC endpoint and its TLS settings — and under `milvus.storage`, the storage the deployment keeps its data in. The `milvus.etcd` settings are only required when using `--backup_index_extra`.
- `backup`: where backup data is written, under `backup.storage`, and how much of the backup runs in parallel.
- `restore`: restore concurrency and temporary file handling.
- `transfer`: how objects move between the two storage backends.
- `log`: log level and output settings.

Both storage sections describe a backend the same way, and `backup.storage` inherits anything it does not name from `milvus.storage`. Backing up into the same backend takes little more than a bucket name; `rootPath` is the exception, and always defaults to `backup`.

### Configuration examples

| File | Scenario |
|------|----------|
| [backup.yaml](configs/backup.yaml) | MinIO, with every setting spelled out |
| [backup-s3.yaml](configs/backup-s3.yaml) | Milvus on MinIO, backups on AWS S3 |
| [backup-gcp.yaml](configs/backup-gcp.yaml) | Google Cloud Storage with a service account |
| [backup-azure.yaml](configs/backup-azure.yaml) | Azure Blob Storage with an account key |
| [backup-iam.yaml](configs/backup-iam.yaml) | AWS S3 with an instance role, no keys in the file |
| [backup-local.yaml](configs/backup-local.yaml) | Milvus on MinIO, backups on a local disk |

Use values that match the Milvus deployment. In common installations, the storage defaults differ:

| Field | Docker Compose | Helm |
|-------|----------------|------|
| `milvus.storage.bucketName` | `a-bucket` | `milvus-bucket` |
| `milvus.storage.rootPath` | `files` | `file` |

Configuration values can also be supplied through [environment variables](docs/user_guide/env_variables.md) or overridden with `--set`:

```shell
milvus-backup --set MILVUS_USER=root --set MILVUS_PASSWORD=Milvus list
```

Run `milvus-backup config show` to print the resolved configuration along with where each value came from.

### Upgrading an existing configuration file

Configuration files carry a `configVersion`. A file written before it existed still loads — it is read with the older schema and translated, with a warning naming the file. Convert one with:

```shell
milvus-backup config migrate --config backup.yaml -o backup-v2.yaml
```

The migration report is written to stderr and lists everything that needs a decision, such as a secret that has to move to a renamed environment variable. The converted file goes to stdout, or to `-o`.

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

- [Storage transfer](docs/user_guide/transfer.md): copy backup data between different storage systems, such as MinIO and AWS S3.
- [RBAC backup and restore](docs/user_guide/rbac.md): include Milvus RBAC metadata in a backup or restore operation.
- [Segment merging restore](docs/user_guide/mul_seg_restore.md): group small segments into fewer import jobs to improve restore performance.

### Cross-storage example

The `milvus.storage` settings must match the storage used by Milvus. Describe the backup destination under `backup.storage`:

```yaml
configVersion: v2

milvus:
  storage:
    provider: minio
    address: localhost
    port: 9000
    bucketName: a-bucket
    rootPath: files
    auth:
      type: static
      accessKeyID: minioadmin
      secretAccessKey: minioadmin

backup:
  storage:
    provider: aws
    address: s3.us-east-1.amazonaws.com
    port: 443
    region: us-east-1
    useSSL: true
    bucketName: <your-bucket-name>
    rootPath: backups
    auth:
      type: static
      accessKeyID: <your-access-key-id>
      secretAccessKey: <your-secret-access-key>
```

The two ends are different backends, so the default `transfer.mode` of `auto` streams the objects through milvus-backup rather than asking either service to copy them. See [configs/backup-s3.yaml](configs/backup-s3.yaml) for the complete file.

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
