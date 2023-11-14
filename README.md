# Milvus-backup

Milvus-Backup is a tool that allows users to backup and restore Milvus data. This tool can be utilized either through the command line or an API server.

The Milvus-backup process has negligible impact on the performance of Milvus. Milvus cluster is fully functional and can operate normally while backup and restoration are in progress.

## Installation
* Download binary from [release page](https://github.com/zilliztech/milvus-backup/releases)

* Use [homebrew](https://brew.sh/) to install
  ```shell
  brew install zilliztech/tap/milvus-backup
  ```


## Compatibility
|      Milvus      |  Milvus-backup   |
|:----------------:|:----------------:|
| v2.3.0 and above | v0.4.0 and above | 
| v2.2.9 and above | v0.3.0 and above |
| v2.2.0 to v2.2.8 | v0.1.0 to v0.2.2 |

## Config
In order to use Milvus-Backup, access to Milvus proxy and Minio cluster is required. Configuration settings related to this access can be edited in `configs/backup.yaml`.

> **Note**
>
> Please ensure that the configuration settings for Minio are accurate. There may be variations in the default value of Minio's configuration depending on how Milvus is deployed, either by docker-compose or k8s.
>
> Please be advised that it is not possible to backup data to a local path. Backup data is stored in Minio or another object storage solution used by your Milvus instance.
> |field|docker-compose |helm|
> |---|---|---|
> |bucketName|a-bucket|milvus-bucket|
> |rootPath|files|file|

## Development

### Build

```
go get
go build
```

Will generate an executable binary `milvus-backup` in the project directory.

### Test

Developers can also test it using an IDE. `core/backup_context_test.go` contains test demos for all main interfaces. Alternatively, you can test it using the command line interface:

```shell
cd core
go test -v -test.run TestCreateBackup
```

## API server

To start the RESTAPI server, use the following command after building:

```shell
./milvus-backup server
```

The server will listen on port 8080 by default. However, you can change it by using the `-p` parameter as shown below:

```shell
./milvus-backup server -p 443
```

### swagger UI

We offer access to our Swagger UI, which displays comprehensive information for our APIs. To view it, simply go to

```
http://localhost:8080/api/v1/docs/index.html
```

### API Reference

### `/create`

Creates a backup for the cluster. Data of selected collections will be copied to a backup directory. You can specify a group of collection names to backup, or if left empty (by default), it will backup all collections.

```
curl --location --request POST 'http://localhost:8080/api/v1/create' \
--header 'Content-Type: application/json' \
--data-raw '{
  "async": true,
  "backup_name": "test_backup",
  "collection_names": [
    "test_collection1","test_collection2"
  ]
}'
```

### `/list`

Lists all backups that exist in the `backup` directory in MinIO.

```
curl --location --request GET 'http://localhost:8080/api/v1/list' \
--header 'Content-Type: application/json'
```

### `/get_backup`

Retrieves a backup by name.

```
curl --location --request GET 'http://localhost:8080/api/v1/get_backup?backup_id=test_backup_id&backup_name=test_backup' \
--header 'Content-Type: application/json'
```

### `/delete`

Deletes a backup by name.

```
curl --location --request DELETE 'http://localhost:8080/api/v1/delete?backup_name=test_api' \
--header 'Content-Type: application/json'
```

### `/restore`

Restores a backup by name. It recreates the collections in the cluster and recovers the data through bulk insert. For more details about bulk insert, please refer to:
https://milvus.io/docs/bulk_insert.md

Bulk inserts will be done by partition. Currently, concurrent bulk inserts are not supported.

```
curl --location --request POST 'http://localhost:8080/api/v1/restore' \
--header 'Content-Type: application/json' \
--data-raw '{
    "async": true,
    "collection_names": [
    "test_collection1"
  ],
    "collection_suffix": "_bak",
    "backup_name":"test_backup"
}'
```

### `/get_restore`

This is only available in the REST API. Retrieves restore task information by ID. We support async restore in the REST API, and you can use this method to get information on the restore execution status.

```
curl --location --request GET 'http://localhost:8080/api/v1/get_restore?id=test_restore_id' \
--header 'Content-Type: application/json'
```

## Command Line

Milvus-backup establish CLI based on cobra. Use the following command to see the usage.

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

## Demo

To try this demo, you should have a functional Milvus server installed and have pymilvus library installed.

Step 0: Check the connections

First of all, we can use `check` command to check whether connections to milvus and storage is normal:

```
./milvus-backup check
```

normal output:

```shell
Succeed to connect to milvus and storage.
Milvus version: v2.3
Storage:
milvus-bucket: a-bucket
milvus-rootpath: files
backup-bucket: a-bucket
backup-rootpath: backup
```

Step 1: Prepare the Data

Create a collection in Milvus called `hello_milvus` and insert some data using the following command:

```
python example/prepare_data.py
```

Step 2: Create a Backup

Use the following command to create a backup of the `hello_milvus` collection:

```
./milvus-backup create -n my_backup
```

Step 3: Restore the Backup

Restore the backup using the following command:

```
./milvus-backup restore -n my_backup -s _recover
```

This will create a new collection called `hello_milvus_recover` which contains the data from the original collection.

Step 4: Verify the Restored Data

Create an index on the restored collection using the following command:

```
python example/verify_data.py
```

This will perform a search on the `hello_milvus_recover` collection and verify that the restored data is correct.

That's it! You have successfully backed up and restored your Milvus collection.
