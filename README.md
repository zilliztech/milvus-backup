# Milvus-backup
<div class="column" align="left">
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="license"/></a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
</div>


Milvus-Backup is a tool that allows users to backup and restore Milvus data. This tool can be utilized either through the command line or an API server.

The Milvus-backup process has negligible impact on the performance of Milvus. Milvus cluster is fully functional and can operate normally while backup and restoration are in progress.

## Installation

* Download binary from [release page](https://github.com/zilliztech/milvus-backup/releases). Usually the latest is recommended.

* Use [homebrew](https://brew.sh/) to install on Mac
  ```shell
  brew install zilliztech/tap/milvus-backup
  ```

## Usage

Milvus-backup provides command line and API server for usage.

### Configuration
In order to use Milvus-Backup, access to Milvus proxy and Minio cluster is required. Configuration settings related to this access can be edited in [backup.yaml](configs/backup.yaml).

> [!NOTE]
>
> Please ensure that the configuration settings for Minio are accurate. There may be variations in the default value of Minio's configuration depending on how Milvus is deployed, either by docker-compose or k8s.
> |field|docker-compose |helm|
> |---|---|---|
> |bucketName|a-bucket|milvus-bucket|
> |rootPath|files|file|

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
| `http`            | `simpleResponse`                | Whether to enable simple HTTP responses.                                                                     | `true`                  |
| `milvus`          | `address`                       | Milvus proxy address.                                                                                        | `localhost`             |
|                   | `port`                          | Milvus proxy port.                                                                                           | `19530`                 |
|                   | `user`                          | Username for Milvus.                                                                                         | `root`                  |
|                   | `password`                      | Password for Milvus.                                                                                         | `Milvus`                |
|                   | `tlsMode`                       | TLS mode (0: none, 1: one-way, 2: two-way/mtls).                                                             | `0`                     |
|                   | `caCertPath`                    | Path to your ca certificate file                                                                             | `/path/to/certificate`  |
|                   | `serverName `                   | Server name                                                                                                  | `localhost`             |
|                   | `mtlsCertPath`                  | Path to your mtls certificate file                                                                           | `/path/to/certificate`  |
|                   | `mtlsKeyPath `                  | Path to your mtls key file                                                                                   | `/path/to/key`          |
| `minio`           | `storageType`                   | Storage type for Milvus (e.g., `local`, `minio`, `s3`, `aws`, `gcp`, `ali(aliyun)`, `azure`, `tc(tencent)`, `gcpnative`). Use `gcpnative` for the Google Cloud Platform provider.          | `minio`                 |
|                   | `address`                       | MinIO/S3 address.                                                                                            | `localhost`             |
|                   | `port`                          | MinIO/S3 port.                                                                                               | `9000`                  |
|                   | `accessKeyID`                   | MinIO/S3 access key ID.                                                                                      | `minioadmin`            |
|                   | `secretAccessKey`               | MinIO/S3 secret access key.                                                                                  | `minioadmin`            |
|                   | `gcpCredentialJSON`             | Path to your GCP credential JSON key file. Used only for the "gcpnative" cloud provider.                     | `/path/to/json-key-file`       |
|                   | `useSSL`                        | Whether to use SSL for MinIO/S3.                                                                             | `false`                 |
|                   | `bucketName`                    | Bucket name in MinIO/S3.                                                                                     | `a-bucket`              |
|                   | `rootPath`                      | Storage root path in MinIO/S3.                                                                               | `files`                 |
| `minio (backup)`  | `backupStorageType`             | Backup storage type (e.g., `local`, `minio`, `s3`, `aws`, `gcp`, `ali(aliyun)`, `azure`, `tc(tencent)`, `gcpnative`). Use `gcpnative` for the Google Cloud Platform provider.                       | `minio`                 |
|                   | `backupAddress`                 | Address of backup storagße.                                                                                   | `localhost`             |
|                   | `backupPort`                    | Port of backup storage.                                                                                      | `9000`                  |
|                   | `backupUseSSL`                  | Whether to use SSL for backup storage.                                                                       | `false`                 |
|                   | `backupAccessKeyID`             | Backup storage access key ID.                                                                                | `minioadmin`            |
|                   | `backupSecretAccessKey`         | Backup storage secret access key.                                                                            | `minioadmin`            |
|                   | `backupGcpCredentialJSON`       | Path to your GCP credential JSON key file. Used only for the "gcpnative" cloud provider.                     | `/path/to/json-key-file`       |
|                   | `backupBucketName`              | Bucket name for backups.                                                                                     | `a-bucket`              |
|                   | `backupRootPath`                | Root path to store backup data.                                                                              | `backup`                |
|                   | `crossStorage`                  | Enable cross-storage backups (e.g., MinIO to AWS S3).                                                        | `false`                 |
| `backup`          | `maxSegmentGroupSize`           | Maximum segment group size for backups.                                                                      | `2G`                    |
|                   | `parallelism.backupCollection`  | Collection-level parallelism for backup.                                                                     | `4`                     |
|                   | `parallelism.copydata`          | Thread pool size for copying data.                                                                           | `128`                   |
|                   | `parallelism.restoreCollection` | Collection-level parallelism for restore.                                                                    | `2`                     |
|                   | `keepTempFiles`                 | Whether to keep temporary files during restore (for debugging).                                              | `false`                 |
|                   | `gcPause.enable`                | Pause Milvus garbage collection during backup.                                                               | `true`                  |
|                   | `gcPause.seconds`               | Duration to pause garbage collection (in seconds).                                                           | `7200`                  |
|                   | `gcPause.address`               | Address for Milvus garbage collection API.                                                                   | `http://localhost:9091` |

For more details, refer to the [backup.yaml](configs/backup.yaml) configuration file.

### Advanced feature

1. [Cross Storage Backup](docs/user_guide/cross_storage.md): Data is read from the source storage and written to a different storage through the Milvus-backup service. Such as, S3 -> local, S3 a -> S3 b. 

2. [RBAC Backup&Restore](docs/user_guide/rbac.md): Enable backup and restore RBAC meta with extra parameter.

## Examples

### Syncing Minio Backups to an AWS S3 Bucket

> **NOTE:** The following configuration is an example only. Replace the placeholders with your actual AWS and Minio settings.

To back up Milvus data to an AWS S3 bucket, you need to configure the `backup.yaml` file with the following settings:

```yaml
# Backup storage configs: Configure the storage where you want to save the backup data
backupStorageType: "aws"                     # Specifies the storage type as AWS S3
backupAddress: s3.{your-aws-region}.amazonaws.com  # Address of AWS S3 (replace {your-aws-region} with your bucket's region)
backupPort: 443                              # Default port for AWS S3
backupAccessKeyID: <your-access-key-id>      # Access key ID for your AWS S3
backupSecretAccessKey: <your-secret-key>     # Secret access key for AWS S3
backupGcpCredentialJSON: "/path/to/file"     # Path to your GCP credential JSON key file. Used only for the "gcpnative" cloud provider.
backupBucketName: "your-bucket-name"         # Bucket name where the backups will be stored
backupRootPath: "backups"                    # Root path inside the bucket to store backups
backupUseSSL: true                           # Use SSL for secure connections (Required)
crossStorage: "true"                         # Required for minio to S3 backups
```


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
