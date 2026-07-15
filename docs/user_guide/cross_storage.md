# Storage transfer modes

Milvus Backup can move objects in two different ways:

- `direct`: call the destination storage provider's native COPY API. This avoids sending object data through the Milvus Backup process, but requires both storage configurations to refer to a compatible backend.
- `streaming`: download objects from the source and upload them to the destination through the Milvus Backup process. Use this for different providers or different service endpoints.
- `auto` (default): use direct copy only when the source and destination have the same provider and endpoint; otherwise use streaming.

Configure the policy with `transfer.mode`. The legacy `minio.crossStorage` key remains supported: `true` maps to `streaming`, while `false` maps to `auto`.

Run `./milvus-backup check` after changing storage settings to verify that the selected transfer path works.

## MinIO to a local directory

```yaml
milvus:
  storage:
    provider: minio
    address: localhost
    port: 9000
    accessKeyID: minioadmin
    secretAccessKey: minioadmin
    bucketName: a-bucket
    rootPath: files

backup:
  storage:
    provider: local
    rootPath: /root/backup/

transfer:
  mode: auto
```

Because the providers differ, `auto` streams data through Milvus Backup.

## MinIO to S3

```yaml
milvus:
  storage:
    provider: minio
    address: localhost
    port: 9000
    accessKeyID: minioadmin
    secretAccessKey: minioadmin
    bucketName: a-bucket
    rootPath: files

backup:
  storage:
    provider: s3
    address: s3.example.com
    port: 443
    accessKeyID: s3AccessKey
    secretAccessKey: s3SecretAccessKey
    bucketName: s3-bucket
    rootPath: s3-backup-path
    useSSL: true

transfer:
  mode: auto
```

## MinIO service A to MinIO service B

```yaml
milvus:
  storage:
    provider: minio
    address: minio-a.example.com
    port: 9000
    accessKeyID: userA
    secretAccessKey: passwordA
    bucketName: a-bucket
    rootPath: files

backup:
  storage:
    provider: minio
    address: minio-b.example.com
    port: 9000
    accessKeyID: userB
    secretAccessKey: passwordB
    bucketName: b-bucket
    rootPath: backup

transfer:
  mode: auto
```

Although both endpoints use MinIO, their addresses differ, so `auto` selects streaming. Set `mode: direct` only when the destination storage service can perform a native COPY from the configured source.

## Choosing a mode

Use `auto` unless you have a specific reason to override it:

- Choose `direct` to require the storage COPY API. The operation fails rather than silently switching to streaming if direct copy is not possible.
- Choose `streaming` when storage-native COPY is unavailable, credentials are isolated, or traffic must pass through the Milvus Backup host.
- Choose `auto` for safe endpoint-aware selection.
