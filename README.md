# Milvus Backup

[![Release](https://img.shields.io/github/v/release/zilliztech/milvus-backup)](https://github.com/zilliztech/milvus-backup/releases)
[![CI](https://github.com/zilliztech/milvus-backup/actions/workflows/main.yaml/badge.svg)](https://github.com/zilliztech/milvus-backup/actions/workflows/main.yaml)
[![License](https://img.shields.io/github/license/zilliztech/milvus-backup)](LICENSE)
[![Discord](https://img.shields.io/badge/Discord-join%20chat-5865F2?logo=discord&logoColor=white)](https://discord.com/invite/8uyFbECzPX)

Milvus Backup is a command-line tool and API service for backing up and restoring Milvus data. Backup and restore operations run while the Milvus cluster remains available.

[Installation](#installation) · [Command-line usage](#command-line-usage) · [Compatibility](#compatibility) · [Configuration](#configuration) · [API server](#api-server) · [FAQ](docs/FAQ.md)

## Installation

Download a binary from the [release page](https://github.com/zilliztech/milvus-backup/releases), pull the [Docker image](https://hub.docker.com/r/milvusdb/milvus-backup) (`milvusdb/milvus-backup:latest`), or install with Homebrew on macOS:

```shell
brew install zilliztech/tap/milvus-backup
```

To build from source instead, run `make all`; the executable is written to `./milvus-backup`.

## Command-line usage

Milvus Backup reads its connection settings from a configuration file. Copy the example and update it for your deployment — [Configuration](#configuration) explains what to change:

```shell
cp configs/backup.yaml backup.yaml
```

Then validate the setup and run your first backup and restore:

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

### Selecting collections

`--filter` limits an operation to specific databases or collections. It takes a comma-separated list, where each entry is a collection in the default database (`coll1`), a collection in a named database (`db1.coll1`), or every collection in a database (`db1.*`):

```shell
# back up two collections
milvus-backup create -n my_backup --filter hello_milvus,db1.hello_milvus2

# back up every collection in db1 (quoted, so the shell does not expand the *)
milvus-backup create -n my_backup --filter 'db1.*'

# restore the whole backup
milvus-backup restore -n my_backup
```

On `restore`, `--filter` matches collection names as they will exist in the target Milvus **after** `--suffix` or `--rename` is applied:

```shell
# restores hello_milvus from the backup into a new hello_milvus_recover
milvus-backup restore -n my_backup -s _recover --filter hello_milvus_recover

# restores db1.coll1 into db2.coll1
milvus-backup restore -n my_backup -r db1.coll1:db2.coll1 --filter db2.coll1
```

Filtering by the original name matches nothing and restores nothing.

### Restore strategies

`--strategy` controls how `restore` loads data into the target cluster:

| Strategy | Backup format | Description |
|----------|---------------|-------------|
| `auto` (default) | Any | Binlog backups use `merged_seg` on Milvus 2.5+ (v2 import API) and `per_seg` on older versions. Snapshot backups use `restore_snap`, or `import_snap` when `--secondary` is set. |
| `merged_seg` | Binlog | Merge multiple segments into fewer import jobs ([multi-segment merged restore](docs/user_guide/mul_seg_restore.md)). Requires Milvus 2.5+. |
| `per_seg` | Binlog | Run one import job per segment. |
| `restore_snap` | Snapshot | Restore through Milvus's native snapshot restore API. |
| `import_snap` | Snapshot | Restore a snapshot through the v2 import API (requires Milvus 3.0.4+). |

> [!NOTE]
> The legacy `--use_v2_restore` flag is deprecated. Setting it to `true` still selects `merged_seg`, and an explicit `--strategy` takes precedence when both are given.

### Restoring a secondary or replicating cluster

**Bootstrapping a fresh secondary:** Use `restore secondary` and keep CDC stopped until the restore succeeds. See the [secondary restore guide](docs/user_guide/secondary_restore.md) for backup and restore commands.

**Restoring into a live CDC cluster:** When the target cluster replicates to secondaries, import jobs must be committed in two phases so the target and its secondaries do not diverge. Pass each secondary with a repeated `--secondary` flag in `address[,token]` form:

```shell
milvus-backup restore -n my_backup \
  --secondary 'http://10.0.0.2:19530,secondary_user:secondary_password' \
  --secondary 'http://10.0.0.3:19530,raw-api-token'
```

- **Authentication:** The optional token is either `username:password` or an API token; omit it to inherit the target's `milvus.user` and `milvus.password`.
- **How it works:** Each import job is held uncommitted until its mirror job appears on every secondary. Milvus Backup then commits the job on the primary target, which replicates the commit to the secondaries (secondaries reject direct client commits).
- **REST API:** Pass a `secondaries` object array (`[{"address": "...", "token": "..."}]`) to `POST /api/v1/restore`.
- **Prerequisites:**
  - Target must be **Milvus 3.0+** with `dataCoord.import.enableInReplicatingCluster=true` (restoring to such a target without `--secondary` is refused up front).
  - Snapshot backups use `import_snap` on this path and require **Milvus 3.0.4+** on the target and every secondary (`restore_snap` cannot be combined with secondaries).

See [two-phase commit restore](docs/user_guide/two_phase_commit_restore.md) for details.

## Compatibility

Use the [latest release](https://github.com/zilliztech/milvus-backup/releases) whenever possible. The latest version supports backups from Milvus 2.2 and later, and restores to Milvus 2.5 and later.

A backup can be restored only to the same or a newer Milvus version:

| Backup version | Restore to 2.5 | Restore to 2.6 | Restore to 3.0 |
|----------------|----------------|----------------|----------------|
| 2.2            | Supported      | Supported      | Supported      |
| 2.3            | Supported      | Supported      | Supported      |
| 2.4            | Supported      | Supported      | Supported      |
| 2.5            | Supported      | Supported      | Supported      |
| 2.6            | —              | Supported      | Supported      |
| 3.0            | —              | —              | Supported      |

For example, a backup created from Milvus 2.6 cannot be restored to Milvus 2.5.

### Backup formats

- **Snapshot format (default on Milvus 3.0+):** Milvus freezes each collection into a snapshot, and `milvus-backup` reads the snapshot manifest and copies its files to the backup storage. Like the binlog format, it can transfer data between any two supported storage backends according to `transfer.mode`.
- **Binlog format (default on Milvus 2.x, or via `--format=binlog`):** Directly backs up collection segments and metadata.

> [!IMPORTANT]
> **Cross-storage restore with snapshot backups:**
> - With `--strategy=restore_snap` (default on Milvus 3.0–3.0.3), restore uses a server-side copy within one storage service. The backup storage must be the **same provider** as the target Milvus storage (another bucket, account, or region of the same cloud provider works, but a different provider or a second self-hosted MinIO is refused).
> - On **Milvus 3.0.4+**, `--strategy=import_snap` lifts this restriction: the target reads the backup storage directly, so any reachable storage service works. `--strategy=auto` also selects `import_snap` automatically when secondaries are configured.

## Configuration

The configuration file tells Milvus Backup how to reach Milvus, the storage used by Milvus, and the backup destination. Its main sections are:

- `milvus`: how to reach Milvus — credentials, the gRPC endpoint and its TLS settings — and under `milvus.storage`, the storage the deployment keeps its data in. The `milvus.etcd` settings are only required when using `--backup_index_extra`. When connecting as a user other than `root`, see [Required Milvus privileges](docs/user_guide/privileges.md).
- `backup`: where backup data is written, under `backup.storage`, and how much of the backup runs in parallel.
- `restore`: restore concurrency and temporary file handling.
- `transfer`: how objects move between the two storage backends.
- `log`: log level and output settings.

Both storage sections share the same schema, and `backup.storage` inherits any omitted fields from `milvus.storage`. When backing up to the same storage backend, you typically only need to set `bucketName` (`rootPath` always defaults to `backup`).

### Configuration examples

| File | Scenario |
|------|----------|
| [backup.yaml](configs/backup.yaml) | MinIO, with every setting spelled out |
| [backup-s3.yaml](configs/backup-s3.yaml) | Milvus on MinIO, backups on AWS S3 |
| [backup-gcp.yaml](configs/backup-gcp.yaml) | Google Cloud Storage with a service account |
| [backup-azure.yaml](configs/backup-azure.yaml) | Azure Blob Storage with an account key |
| [backup-iam.yaml](configs/backup-iam.yaml) | AWS S3 with an instance role, no keys in the file |
| [backup-local.yaml](configs/backup-local.yaml) | Milvus on MinIO, backups on a local disk |
| [backup-local-milvus.yaml](configs/backup-local-milvus.yaml) | Milvus standalone with local storage (`COMMON_STORAGETYPE=local`), backups on MinIO |

Use values that match the Milvus deployment. In common installations, the storage defaults differ:

| Field | Docker Compose | Helm |
|-------|----------------|------|
| `milvus.storage.bucketName` | `a-bucket` | `milvus-bucket` |
| `milvus.storage.rootPath` | `files` | `file` |

Credentials can also be supplied through [environment variables](docs/user_guide/env_variables.md), and any value can be overridden for a single run with `--set`:

```shell
milvus-backup --set MILVUS_USER=root --set MILVUS_PASSWORD=Milvus list
milvus-backup --set milvus.grpc.address=milvus-proxy list
```

Run `milvus-backup config show` to print the resolved configuration along with where each value came from.

### Upgrading an existing configuration file

Configuration files declare a `configVersion`. Legacy v1 files without this field still load — Milvus Backup translates them automatically and logs a deprecation warning. To upgrade a file to v2, run:

```shell
milvus-backup config migrate --config backup.yaml -o backup-v2.yaml
```

The converted configuration is written to stdout (or to the path given by `-o`). Any items requiring manual review — such as renamed secret environment variables or v1 environment overrides that should move into the config file — are reported to stderr.

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

Because the source and destination use different storage providers, the default `transfer.mode` (`auto`) streams objects through `milvus-backup` instead of using server-side copy. See [configs/backup-s3.yaml](configs/backup-s3.yaml) for the complete file.

Do not commit storage credentials to the repository. Prefer environment variables or another secret-management mechanism in production.

## FAQ

See [docs/FAQ.md](docs/FAQ.md) for common issues and troubleshooting advice.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow, coding style, and how to run the test suite.

## License

Milvus Backup is licensed under the Apache License 2.0.
