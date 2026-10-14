# Secondary restore

## Procedure

1. Point connection settings at the source, including etcd access, and create a complete backup:

   ```shell
   milvus-backup create -n my_backup --for secondary
   ```

   The preset forces `binlog`, `bulk_flush`, RBAC, and index extra metadata, overriding conflicting flags. Preserve `meta/full_meta.json`; it contains channel metadata and FlushAll messages needed for restore.
2. Configure a fresh target as a secondary of the source. Keep CDC delivery disabled and drain any in-flight replication requests. Do not run concurrent restores.
3. Point connection settings at the target, then run:

   ```shell
   milvus-backup restore secondary -n my_backup \
     --source_cluster_id SOURCE --target_cluster_id TARGET
   ```

4. Wait for success, which includes confirmation of the backed-up FlushAll messages. Then start CDC from the target's resulting checkpoints.

## Preconditions and limits

Before opening restore streams, the command requires a checkpoint from the expected source at time tick zero on every target physical channel. Missing configuration, missing checkpoints, or failed lookups stop the restore.

Restore messages start at time tick 1. If CDC advances the checkpoint first, restore messages can be acknowledged without being applied. A nonzero checkpoint from CDC or an earlier restore requires a new secondary; restarting Milvus or dropping collections does not clear it.

The check neither blocks CDC nor detects uncommitted replicated transactions. If CDC has already sent messages, use a new secondary even if checkpoints remain zero.
