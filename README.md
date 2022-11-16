# Milvus-backup

Milvus-backup is a tool to backup and recover Milvus data. It can be used as a command line or an API server.

Milvus-backup needs to visit Milvus proxy and minio cluster. Currently, Milvus-backup get these configs by reading `milvus.yaml` file. Same behaviour as Milvus.
A default `milvus.yaml` is in `milvus-backup/configs/`. You can replace it with the config of your cluster. Both command line and REST API server need it.

Milvus-backup has no large impact on Milvus. Milvus cluster can work as usual during backup. 

# Interfaces

## Create
Create a backup for the cluster. Data of selected collections will be copied to a backup directory(in the same bucket of the cluster).
The path tree of backups is like:
```
/backup/my_backup_0
├── meta
│   ├── backup_meta.json
│   ├── collection_meta.json
│   ├── partition_meta.json
│   └── segment_meta.json
└── binlogs
    ├── delta_log
    │   └── 434574377495035905
    │       └── 434574377495035906
    │           └── 434574378976149505
    │               └── 434574382554415105
    ├── insert_log
    │   └── 434574377495035905
    │       └── 434574377495035906
    │           ├── 434574378976149505
    │           │   ├── 0
    │           │   │   └── 434574379605819396
    │           │   ├── 1
    │           │   │   └── 434574379605819397
    │           │   ├── 100
    │           │   │   └── 434574379605819393
    │           │   ├── 101
    │           │   │   └── 434574379605819394
    │           │   └── 102
    │           │       └── 434574379605819395
    │           └── 434574378976149506
    │               ├── 0
    │               │   └── 434574379605819401
    │               ├── 1
    │               │   └── 434574379605819402
    │               ├── 100
    │               │   └── 434574379605819398
    │               ├── 101
    │               │   └── 434574379605819399
    │               └── 102
    │                   └── 434574379605819400
/backup/my_backup_1
├── meta
│  .......
└── binlogs
   ......
```

Support set a group of collection_names to backup, if empty(by default), will backup all collections.

## List
List will scan the `backup` directory in minio and return all the backups that exist in the cluster.

## Get
Get backup by name.

## Delete
Delete backup by name.

## Restore
Restore backup by name, will recreate the collections in the cluster and recover the data through bulkinsert. For more details about BulkInsert, please see:

BulkInserts will be done by partition. Currently we don't support concurrent BulkInserts.

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
By default, the server will listen to 8080. You can change it by `--port` parameter:
```
./milvus-backup server --port 443
```

### APIs

#### hello

Just test the service is active.
```
http://localhost:8080/api/v1/hello
```

```
Hello, This is backup service
```

#### create

```
curl --location --request POST 'http://localhost:8080/api/v1/create' \
--header 'Content-Type: application/json' \
--data-raw '{
    "backup_name":"test_api"
}'
```
#### list

```
http://localhost:8080/api/v1/list
```

#### get

```
http://localhost:8080/api/v1/get?backup_name=test_api
```

#### delete

```
// DELETE method
http://localhost:8080/api/v1/delete?backup_name=test_api
```

#### restore
```
curl --location --request POST 'http://localhost:8080/api/v1/restore' \
--header 'Content-Type: application/json' \
--data-raw '{
    "backup_name":"test_api"
}'
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
