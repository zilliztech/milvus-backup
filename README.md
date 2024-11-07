# Milvus-backup
<div class="column" align="left">
  <a href="https://discord.com/invite/8uyFbECzPX"><img height="20" src="https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="license"/></a>
  <img src="https://img.shields.io/github/license/milvus-io/milvus" alt="license"/>
</div>


<img src="https://github.com/zilliz-tech/milvus-backup/blob/main/docs/user_guide/figs/log.png" alt="milvus--backup-logo"/>

Milvus-Backup is a tool that allows users to backup and restore Milvus data. This tool can be utilized either through the command line or an API server.

The Milvus-backup process has negligible impact on the performance of Milvus. Milvus cluster is fully functional and can operate normally while backup and restoration are in progress.

## Installation

* Download binary from [release page](https://github.com/zilliztech/milvus-backup/releases). Usually the latest is recommended.

For Mac: 
* Use [homebrew](https://brew.sh/) to install
  ```shell
  brew install zilliztech/tap/milvus-backup
  ```

## Get started

Milvus-backup provides command line and API server for usage.

### Config
In order to use Milvus-Backup, access to Milvus proxy and Minio cluster is required. Configuration settings related to this access can be edited in `backup.yaml`.

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

### swagger UI

We offer access to our Swagger UI, which displays comprehensive information for our APIs. To view it, simply go to

```
http://localhost:8080/api/v1/docs/index.html
```


## Development

### Build

For developers, Milvus-backup is easy to contribute to.

Execute `make all` will generate an executable binary `milvus-backup` in the `{project_path}/bin` directory.

### Test

Developers can also test it using an IDE. `core/backup_context_test.go` contains test demos for all main interfaces. Alternatively, you can test it using the command line interface:

```shell
cd core
go test -v -test.run TestCreateBackup
```


## License
milvus-backup is licensed under the Apache License, Version 2.0.
