# Storage transfer

Milvus Backup can back up into a different storage system than the one Milvus itself uses — from MinIO to a local disk, or from in-house storage to a cloud provider.

Copying objects within one storage service is the fast path: the service does the work and the data never travels through milvus-backup. That only works when both ends are the same backend. Otherwise the objects have to be read from the source and written to the destination by milvus-backup itself.

`transfer.mode` decides which of the two happens:

| Mode | Behavior |
|------|----------|
| `auto` (default) | Ask the storage service to copy when both ends are the same backend, and stream through milvus-backup when they are not. |
| `direct` | Always ask the storage service to copy. |
| `streaming` | Always read from the source and write to the destination through milvus-backup. |

Two ends count as the same backend when they are the same provider reached at the same endpoint — the address, port, region, TLS setting and, for Azure, the storage account all have to match. Buckets and root paths may differ within one backend, since a service can copy between its own buckets.

`auto` fits almost every deployment. Reach for `streaming` when a copy fails despite both ends looking identical, such as when one bucket is reachable but not copyable from the other, and for `direct` only when you know the service can copy and want to keep the data off the milvus-backup host.

Run `milvus-backup check` to verify the configuration before creating a backup. `azure` is not supported as a streaming endpoint, and not every provider combination is fully tested.

## Examples

Only the parts that matter are shown. See [configs/](../../configs) for complete files.

### MinIO to a local disk

Local storage is a path, so it has no endpoint and no credentials.

```yaml
configVersion: v2

milvus:
  storage:
    provider: minio
    address: localhost
    port: 9000
    bucketName: "a-bucket"
    rootPath: "files"
    auth:
      type: static
      accessKeyID: minioadmin
      secretAccessKey: minioadmin

backup:
  storage:
    provider: local
    rootPath: "/root/backup"
```

The two ends are different backends, so `auto` streams. Complete file: [backup-local.yaml](../../configs/backup-local.yaml).

### MinIO to S3

```yaml
configVersion: v2

milvus:
  storage:
    provider: minio
    address: localhost
    port: 9000
    bucketName: "a-bucket"
    rootPath: "files"
    auth:
      type: static
      accessKeyID: minioadmin
      secretAccessKey: minioadmin

backup:
  storage:
    provider: s3
    address: s3.us-east-1.amazonaws.com
    port: 443
    region: us-east-1
    useSSL: true
    bucketName: "s3-bucket"
    rootPath: "s3-backup-path"
    auth:
      type: static
      accessKeyID: "<your-access-key-id>"
      secretAccessKey: "<your-secret-access-key>"
```

Complete file: [backup-s3.yaml](../../configs/backup-s3.yaml).

### MinIO A to MinIO B

Two deployments of the same provider are still two backends, because they answer at different addresses. `auto` compares the endpoint and streams, so nothing extra needs to be set:

```yaml
configVersion: v2

milvus:
  storage:
    provider: minio
    address: addressA
    port: 9000
    bucketName: "a-bucket"
    rootPath: "files"
    auth:
      type: static
      accessKeyID: userA
      secretAccessKey: passwdA

backup:
  storage:
    provider: minio
    address: addressB
    port: 9000
    bucketName: "b-bucket"
    rootPath: "backup"
    auth:
      type: static
      accessKeyID: userB
      secretAccessKey: passwdB
```

## FAQ

**When do I have to set `transfer.mode` myself?**

Rarely. `auto` streams whenever the two ends are not the same provider at the same endpoint, which covers backing up to a different service, a different region, or a different MinIO deployment.

Set `streaming` explicitly when both ends look like one backend but cannot copy between each other in practice — for instance when the credentials in use can read one bucket and write the other, but are not allowed to issue a server-side copy across them.

**What replaced `minio.crossStorage`?**

`transfer.mode`. `crossStorage: true` is `transfer.mode: streaming`, and `crossStorage: false` becomes `auto` rather than `direct`, because the old value did not actually force a server-side copy: it was overridden whenever the two providers differed. `auto` keeps that behavior and extends it to two deployments of the same provider, which the old flag had to be set by hand for.

Files written for the older schema are translated when they load, and `milvus-backup config migrate` converts one in place.
