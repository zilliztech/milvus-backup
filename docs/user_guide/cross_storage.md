# Cross storage backup & restore

Previously, Milvus-backup utilized the Copy API of the storage client to back up data.
This limited the backup capability to the same storage type as the Milvus cluster.
However, there's a significant demand for cross-storage backups—for instance,
backup data from Minio to a local disk or backup from in-house storage to cloud storage.

Starting from version v0.4.21, Milvus-backup now supports cross-storage backups.
In this process, data is read from the source storage and written to the target storage through the Milvus-backup service.

This feature is currently in Beta. `azure` is not supported. Not all storage types are fully tested.

## Usage

To enable cross-storage backup, you only need to adjust the configurations in backup.yaml.

You can use `./milvus-backup check` first to see if the cross copy is working.

For example

*Back up data from Minio to a local disk*:

```yaml
# Related configuration of minio, which is responsible for data persistence for Milvus.
minio:
  storageType: "minio"
  address: localhost
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  bucketName: "a-bucket"
  rootPath: "files"

  backupStorageType: "local"
  backupRootPath: "/root/backup/"
```

*Backup from Minio to S3*

```yaml
minio:
  storageType: "minio"
  address: localhost
  port: 9000
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
  useSSL: false
  useIAM: false
  iamEndpoint: ""
  bucketName: "a-bucket"
  rootPath: "files"

  backupStorageType: "s3"
  backupAddress: s3Address
  backupPort: 443
  backupAccessKeyID: s3AccessKey
  backupSecretAccessKey: s3SecretAccessKey
  backupBucketName: "s3-bucket"
  backupRootPath: "s3-backup-path"
```

*Backup from Minio A to Minio B*

If the two storage locations are of the same type but belong to different services,
you need to add an additional configuration crossStorage=true to explicitly indicate that it is a cross-storage backup or restore operation.
```yaml
minio:
  storageType: "minio"
  address: addressA
  port: 9000
  accessKeyID: userA
  secretAccessKey: passwdB
  useSSL: false
  useIAM: false
  iamEndpoint: ""
  bucketName: "a-bucket"
  rootPath: "files"

  backupStorageType: "minio"
  backupAddress: addressB
  backupPort: 9000
  backupAccessKeyID: userB
  backupSecretAccessKey: passwdB
  backupBucketName: "b-bucket"
  backupRootPath: "backup"

  # If you need to back up or restore data between two different storage systems, direct client-side copying is not supported. 
  # Set this option to true to enable data transfer through Milvus Backup.
  # Note: This option will be automatically set to true if `minio.storageType` and `minio.backupStorageType` differ.
  # However, if they are the same but belong to different services, you must manually set this option to `true`.
  crossStorage: "true"
```

## FAQ

Q: 
```
What is the meaning of the option `crossStorage`. 

Even if the storageType is the same, does it mean we need to set crossStorage to true when minio.address and backupaddress are different? 
```

A:
```
The crossStorage option determines how data is transferred:

When set to True, data is transferred via read and write operations managed by the milvus-backup process.

When set to False, data is directly copied using the storage system’s COPY API, bypassing the milvus-backup process.

Use crossStorage based on whether the two addresses are accessible to each other through the COPY API.
```