# milvus-backup

## Introduction
milvus-backup is the tool to backup and recovery milvus data. It can use as a command line tool or a API server.

milvus-backup needs to visit Milvus proxy and minio cluster. Currently, milvus-backup get these configs by reading `milvus.yaml` file. Same behaviour like Milvus.
A default `milvus.yaml` is in `milvus-backup/configs/`. You can replace it with the config of your cluster. Both command line and REST API server needs it.

### Interfaces

#### Create
Create a backup for the cluster. All the segment data will copy to a backup directory(in the same minio cluster/bucket of the cluster).
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
    └── stats_log
        └── 434574377495035905
            └── 434574377495035906
                ├── 434574378976149505
                │   └── 100
                │       └── 434574379605819393
                └── 434574378976149506
                    └── 100
                        └── 434574379605819398
/backup/my_backup_1
├── meta
│  .......
└── binlogs
   ......
```

Support define collection_names to backup, if empty(default) to backup all.

#### List
List will scan the `backup` directory in minio and return all the backups exist in the cluster.

#### Get
Get backup by name.

#### Delete
Delete backup by name.

#### Load
Load backup by name, will recreate the collections in the cluster and recover the data through bulkload.

As backup is arranged in collection/partition/segment three-level hireachy. Bulkloads will be done by segment one after another.


## Build && Development

```
go build
```
Will generate an executable binary `milvus-backup` in the project directory.

For developer, you can also test it in IDE. `core/backup_context_test.go` contains some test demos for all main interfaces.

## API server

Use the following command can start a restAPI server. 
```
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn && ./milvus-backup server
```
In default, the server will listen to 8080. You can change it by:
```
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn && ./milvus-backup server --port 443
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

#### load
```
curl --location --request POST 'http://localhost:8080/api/v1/load' \
--header 'Content-Type: application/json' \
--data-raw '{
    "backup_name":"test_api"
}'
```

###

## Command Line 
milvus-backup establish CLI based on cobra. Use the following command to see the usage.
```
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn && ./milvus-backup --help
```

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
  server      server subcommand start milvus-backup RESTAPI server.

Flags:
  -h, --help   help for milvus-backup

Use "milvus-backup [command] --help" for more information about a command.
```

## Code structure

`internal` contains codes copied from milvus project.
Keep the file structure consistent with milvus.
Some minor adjustment are made for simplicity. 

`core` contains the backup tool logic.

## 

milvus-backup and milvus-go-sdk both contain milvus.proto.
It will throw error while running UTs. Set environment to enable UT.
```
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn
```
