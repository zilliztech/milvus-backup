# Milvus-backup

Milvus-backup is a tool to backup and restore Milvus data. It can be used as a command line or an API server.

Milvus-backup needs to visit Milvus proxy and minio cluster. Related config can be edited in `configs/backup.yaml`. 
> **Note**
> Please make sure the config of Minio is correct. There are some differences for the default value of Minio config when the Milvus is deployed by docker-compose and k8s.

|field|docker-compose |k8s|
|---|---|---|
|bucketName|a-bucket|milvus-bucket|
|rootPath|files|file|

Milvus-backup has no large impact on Milvus. Milvus cluster can work as usual during backup and restore. 

# Interfaces

## Create
Create a backup for the cluster. Data of selected collections will be copied to a backup directory.
Support set a group of collection_names to backup, if empty(by default), will backup all collections.

## List
List will scan the `backup` directory in minio and return all the backups that exist in the cluster.

## GetBackup
Get backup by name.

## Delete
Delete backup by name.

## Restore
Restore backup by name, will recreate the collections in the cluster and recover the data through bulkinsert. For more details about BulkInsert, please see:
https://milvus.io/docs/bulk_load.md
BulkInserts will be done by partition. Currently we don't support concurrent BulkInserts.

## GetRestore
Only supported in rest API. Get restore task info by id. We support async restore in rest API. And we can use this method to get the restore executing state.

# Development

## Build

```
go get
go build
```
Will generate an executable binary `milvus-backup` in the project directory.

## Test

For developers, you can also test it with IDE. `core/backup_context_test.go` contains some test demos for all main interfaces.
Or you can test with cli.

```shell
cd core
go test -v -test.run TestCreateBackup
```

# Usage

## API server

After building, use the following command to start a RESTAPI server. 
```
./milvus-backup server
```
By default, the server will listen to 8080. You can change it by `-p` parameter:
```
./milvus-backup server -p 443
```

### APIs

#### swagger UI
We provide swagger UI to show details of APIs. You can see it by:

```
http://localhost:8080/api/v1/docs/index.html
```

#### create

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
#### list

```
curl --location --request GET 'http://localhost:8080/api/v1/list' \
--header 'Content-Type: application/json'
```

#### get_backup

```
curl --location --request GET 'http://localhost:8080/api/v1/get_backup?backup_id=test_backup_id&backup_name=test_backup' \
--header 'Content-Type: application/json'
```

#### delete

```
// DELETE method
curl --location --request DELETE 'http://localhost:8080/api/v1/delete?backup_name=test_api' \
--header 'Content-Type: application/json'
```

#### restore
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

#### get_restore
```
curl --location --request GET 'http://localhost:8080/api/v1/get_restore?id=test_restore_id' \
--header 'Content-Type: application/json'
```
## Command Line 

Milvus-backup establish CLI based on cobra. Use the following command to see the usage.

```
milvus-backup is a backup tool for milvus.

Usage:
  milvus-backup [flags]
  milvus-backup [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  create      create subcommand create a backup.
  delete      delete subcommand delete backup by name.
  get         get subcommand get backup by name.
  help        Help about any command
  list        list subcommand shows all backup in the cluster.
  restore     restore subcommand restore a backup.
  server      server subcommand start milvus-backup RESTAPI server.

Flags:
  -h, --help   help for milvus-backup

Use "milvus-backup [command] --help" for more information about a command.
```

## Demo

This demo is evolved from `hello_milvus`. To try this demo, you should have a healthy Milvus and pymilvus installed.

1, Prepare data. Create a collection `hello_milvus` and insert some data.

```
python example/prepare_data.py
```

2, Create backup

```
./milvus-backup create -n my_backup 
```

3, Restore backup. Set suffix `_recover`. `hello_milvus` will be restored with new name `hello_milvus_recover`. 
```
./milvus-backup restore -n my_backup -s _recover
```

4, Verfiy data. Create index and do search on the recovered collection `hello_milvus_recover`.

```
python example/verify_data.py
```
