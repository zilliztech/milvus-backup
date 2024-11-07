# API Demos

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
curl --location --request GET 'http://localhost:8080/api/v1/get_backup?backup_name=test_backup' \
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

