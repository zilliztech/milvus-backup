# Secondary restore

## Procedure

1. Point connection settings at the source, including etcd access, and create a complete backup:

   ```shell
   milvus-backup create -n my_backup --for secondary
   ```

   The preset forces `binlog`, `bulk_flush`, RBAC, and index extra metadata, overriding conflicting flags. Preserve `meta/full_meta.json`; it contains channel metadata and FlushAll messages needed for restore.

   A snapshot format backup can seed a secondary too, when the secondary runs Milvus 3.0.4 or newer (see [Snapshot format backups](#snapshot-format-backups)). The preset does not take it; pass the knobs it would have set yourself:

   ```shell
   milvus-backup create -n my_backup --format=snapshot --backup_index_extra --rbac
   ```

2. Configure a fresh target as a secondary of the source. Keep CDC delivery disabled and drain any in-flight replication requests. Do not run concurrent restores.
3. Point connection settings at the target, then run:

   ```shell
   milvus-backup restore secondary -n my_backup \
     --source_cluster_id SOURCE --target_cluster_id TARGET
   ```

4. Wait for success, which includes confirmation of the backed-up FlushAll messages. Then start CDC from the target's resulting checkpoints.

## Snapshot format backups

A secondary restore replays the source's DDL and then its data over the replication stream. With a binlog backup, the data goes out as one import message per segment batch pointing at the binlogs, which are staged into the target's storage first when the backup sits in another bucket. With a snapshot backup, each collection goes out as a single snapshot-sourced import message: the target expands it into the same import job the import api would create for `restore --strategy=import_snap`, reads the bundle itself, and folds the snapshot's deletes on its own side. No bytes move through `milvus-backup`, and a bundle on another storage service is read through the external spec, so nothing is staged.

What this needs:

- The secondary must run Milvus 3.0.4 or newer, where snapshot-sourced imports exist. The restore refuses an older secondary before any DDL is sent; take a binlog backup (`create --for secondary`) for it instead.
- The backup must still carry index extra metadata and the FlushAll messages: the snapshot replaces how the data is carried, not what the DDL replay and the replication start point are built from. A snapshot backup taken with the default strategy records the FlushAll messages; `--backup_index_extra` is what records the index attributes.
- The snapshot import reads StorageV3 segments only, the same limit as `restore --strategy=import_snap`. The restore reads every bundle's manifests up front and refuses a backup with data segments in another storage version before anything is sent, because the target would only find out after the DDL had been replayed, and a secondary that got that far has to be redeployed. A Milvus 3.0 source writes StorageV3 segments with `common.storage.useLoonFFI` enabled.

## Preconditions and limits

Before opening restore streams, the command requires a checkpoint from the expected source at time tick zero on every target physical channel. Missing configuration, missing checkpoints, or failed lookups stop the restore.

Restore messages start at time tick 1. If CDC advances the checkpoint first, restore messages can be acknowledged without being applied. A nonzero checkpoint from CDC or an earlier restore requires a new secondary; restarting Milvus or dropping collections does not clear it.

The check neither blocks CDC nor detects uncommitted replicated transactions. If CDC has already sent messages, use a new secondary even if checkpoints remain zero.
