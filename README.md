# Milvus-backup
<div class="column" align="left">
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="license"/></a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
</div>


Milvus-Backup is a tool that allows users to backup and restore Milvus data. This tool can be utilized either through the command line or an API server.

The Milvus-backup process has negligible impact on the performance of Milvus. Milvus cluster is fully functional and can operate normally while backup and restoration are in progress.

## Compatibility
Always use the **[latest release](https://github.com/zilliztech/milvus-backup/releases)** of `milvus-backup`, which is backward-compatible with Milvus **v2.2 and above**.

> The backup tool is designed to automatically adapt to version differences in Milvus.  
> Using the latest version ensures best compatibility, performance, and bug fixes.

The table below shows which Milvus versions can restore backups created from other versions:

| Backup From ↓ \ Restore To → | 2.4 | 2.5 | 2.6 |
|------------------------------|-----|-----|-----------|
| 2.2                          | ✅  | ✅  | ✅        |
| 2.3                          | ✅  | ✅  | ✅        |
| 2.4                          | ✅  | ✅  | ✅        |
| 2.5                          | ❌  | ✅  | ✅        |
| 2.6                          | ❌  | ❌  | ✅        |

> ✅ = Supported  ❌ = Not supported
>
> **Rules:**
> - Backup is supported from **Milvus 2.2+**
> - Restore is supported **to Milvus 2.4+**
> - A backup can only be restored to **the same or newer Milvus versions**
> - For example, backups created in Milvus 2.5 **cannot** be restored to 2.4


## Installation

* Download binary from [release page](https://github.com/zilliztech/milvus-backup/releases). Usually the latest is recommended.

* Use [homebrew](https://brew.sh/) to install on Mac
  ```shell
  brew install zilliztech/tap/milvus-backup
  ```

## Usage

Milvus-backup provides command line and API server for usage.

### Configuration
To use Milvus Backup, it must be able to access the Milvus proxy, the storage used by Milvus, and the backup destination. Configure these endpoints in [backup.yaml](configs/backup.yaml).

> [!NOTE]
>
> Ensure that `milvus.storage` matches the storage configuration of the Milvus deployment. Defaults can differ between Docker Compose, Kubernetes, and cloud deployments.
> |field|docker-compose |helm|
> |---|---|---|
> |bucketName|a-bucket|milvus-bucket|
> |rootPath|files|file|

> [!NOTE]
>
> There is also an option to override values in `backup.yaml` using environment variables. Please refer [env_variables.md](docs/user_guide/env_variables.md) to know more.

### Command Line

Milvus-backup establish CLI based on cobra. Use the following command to see all the usage.

```
milvus-backup is a backup&restore tool for milvus.

Usage:
  milvus-backup [flags]
  milvus-backup [command]

Available Commands:
  check       check if the connects is right.
  create      create subcommand create a backup.
  delete      delete subcommand delete backup by name.
  get         get subcommand get backup by name.
  help        Help about any command
  list        list subcommand shows all backup in the cluster.
  restore     restore subcommand restore a backup.
  server      server subcommand start milvus-backup RESTAPI server.

Flags:
      --config string   config YAML file of milvus (default "backup.yaml")
  -h, --help            help for milvus-backup

Use "milvus-backup [command] --help" for more information about a command.
```

Here is a [demo](docs/user_guide/e2e_demo_cli.md) for a complete backup and restore process.

### API Server

To start the RESTAPI server, use the following command after building:

```shell
./milvus-backup server
```

The server will listen on port 8080 by default. However, you can change it by using the `-p` parameter as shown below:

```shell
./milvus-backup server -p 443
```

We offer a [demo](docs/user_guide/api_demo.md) of the key APIs; however, please refer to the Swagger UI for the most up-to-date usage details, as the demo may occasionally become outdated.

#### swagger UI

We offer access to our Swagger UI, which displays comprehensive information for our APIs. To view it, simply go to

```
http://localhost:8080/api/v1/docs/index.html
```
---

## Backup.yaml Configurations

Below is a summary of the configurations supported in `backup.yaml`:

| **Section**       | **Field**                       | **Description**                                                                                              | **Default/Example**     |
|--------------------|---------------------------------|--------------------------------------------------------------------------------------------------------------|-------------------------|
| `log`             | `level`                         | Logging level. Supported: `debug`, `info`, `warn`, `error`, `panic`, `fatal`.                                | `info`                  |
|                   | `console`                       | Whether to print logs to the console.                                                                        | `true`                  |
|                   | `file.rootPath`                 | Path to the log file.                                                                                        | `logs/backup.log`       |
| `http`            | `enabled`                       | Whether to enable the HTTP server.                                                                           | `true`                  |
|                   | `debugMode`                     | Whether to enable Gin debug mode.                                                                            | `false`                 |
|                   | `swaggerBasePath`               | Override the Swagger UI base path. Useful when running behind a reverse proxy with a path prefix.            | (empty)                 |
| `milvus`          | `address`                       | Milvus proxy address.                                                                                        | `localhost`             |
|                   | `port`                          | Milvus proxy port.                                                                                           | `19530`                 |
|                   | `user`                          | Username for Milvus.                                                                                         | `root`                  |
|                   | `password`                      | Password for Milvus.                                                                                         | `Milvus`                |
|                   | `tlsMode`                       | TLS mode (0: none, 1: one-way, 2: two-way/mtls).                                                             | `0`                     |
|                   | `caCertPath`                    | Path to your ca certificate file                                                                             | `/path/to/certificate`  |
|                   | `serverName `                   | Server name                                                                                                  | `localhost`             |
|                   | `mtlsCertPath`                  | Path to your mtls certificate file                                                                           | `/path/to/certificate`  |
|                   | `mtlsKeyPath `                  | Path to your mtls key file                                                                                   | `/path/to/key`          |
| `milvus.storage`  | `provider`                      | Provider used by Milvus: `local`, `minio`, `s3`, `aws`, `gcp`, `gcpnative`, `ali`, `azure`, `tc`, or `hwc`. | `minio`                 |
|                   | `address`                       | Milvus storage endpoint address.                                                                             | `localhost`             |
|                   | `port`                          | Milvus storage endpoint port.                                                                                | `9000`                  |
|                   | `accessKeyID`                   | Milvus storage access key ID.                                                                                | `minioadmin`            |
|                   | `secretAccessKey`               | Milvus storage secret access key.                                                                            | `minioadmin`            |
|                   | `gcpCredentialJSON`             | Path to the GCP credential JSON key file for `gcpnative`.                                                    | `/path/to/json-key-file`|
|                   | `useSSL`                        | Whether to use SSL.                                                                                          | `false`                 |
|                   | `bucketName`                    | Bucket used by Milvus.                                                                                       | `a-bucket`              |
|                   | `rootPath`                      | Storage root path used by Milvus.                                                                            | `files`                 |
| `backup.storage`  | `provider`                      | Provider used to store backups.                                                                              | Inherits Milvus storage |
|                   | `address`                       | Backup storage endpoint address.                                                                             | Inherits Milvus storage |
|                   | `port`                          | Backup storage endpoint port.                                                                                | Inherits Milvus storage |
|                   | `bucketName`                    | Bucket used to store backups.                                                                                | Inherits Milvus storage |
|                   | `rootPath`                      | Root path used to store backups.                                                                             | Inherits Milvus storage |
| `transfer`        | `mode`                          | `auto`, `direct` (storage COPY API), or `streaming` (through milvus-backup).                                 | `auto`                  |
|                   | `concurrency`                   | Maximum number of objects transferred concurrently during backup, restore, check, or migrate.                | `128`                   |
|                   | `multipartCopyThresholdMiB`     | Threshold above which multipart direct copy is used.                                                         | `500`                   |
| `backup`          | `concurrency.collections`       | Maximum number of backup collection tasks executed concurrently.                                             | `4`                     |
|                   | `concurrency.segments`          | Maximum number of backup segment tasks executed concurrently.                                                | `1024`                  |
|                   | `gcPause.enable`                | Pause Milvus garbage collection during backup.                                                               | `true`                  |
|                   | `gcPause.address`               | Address for Milvus garbage collection API.                                                                   | `http://localhost:9091` |
| `restore`         | `concurrency.collections`       | Maximum number of restore collection tasks executed concurrently.                                            | `2`                     |
|                   | `concurrency.importJobs`        | Maximum number of Milvus import jobs submitted concurrently.                                                 | `768`                   |
|                   | `keepTempFiles`                 | Keep temporary restore files for debugging.                                                                  | `false`                 |

For more details, refer to the [backup.yaml](configs/backup.yaml) configuration file.

### Advanced features

1. [Cross Storage Backup](docs/user_guide/cross_storage.md): Data is read from the source storage and written to a different storage through the Milvus-backup service. Such as, S3 -> local, S3 a -> S3 b. 

2. [RBAC Backup&Restore](docs/user_guide/rbac.md): Enable backup and restore RBAC meta with extra parameter.

3. [Segment Merging Restore](docs/user_guide/mul_seg_restore.md): In segment merging restore mode (v2 restore), multiple segments are grouped into a single job and restored together. This significantly improves restore performance, especially in scenarios with many small segments.


## Examples

### Syncing Minio Backups to an AWS S3 Bucket

> **NOTE:** The following configuration is an example only. Replace the placeholders with your actual AWS and Minio settings.

To back up Milvus data to an AWS S3 bucket, you need to configure the `backup.yaml` file with the following settings:

```yaml
backup:
  storage:
    provider: "aws"
    address: s3.{your-aws-region}.amazonaws.com
    port: 443
    accessKeyID: <your-access-key-id>
    secretAccessKey: <your-secret-key>
    bucketName: "your-bucket-name"
    rootPath: "backups"
    useSSL: true

transfer:
  mode: auto
  concurrency: 128
```


## [FAQ](docs/FAQ.md)

## Development

### Build

For developers, Milvus-backup is easy to contribute to.

Execute `make all` will generate an executable binary `milvus-backup` in the `{project_path}/bin` directory.

### Test

Developers can also test it using an IDE. You can test it using the command line interface:

```shell
make test
```


## License
milvus-backup is licensed under the Apache License, Version 2.0.
