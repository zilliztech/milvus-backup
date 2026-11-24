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

A self-contained bundle can be copied to another bucket or prefix as a whole, as long as its internal layout stays intact. The preflight resolves its manifests against the copied metadata location, so copying only the metadata JSON is not enough.

What this needs:

- The secondary must run Milvus 3.0.4 or newer, where snapshot-sourced imports exist. The restore refuses an older secondary before any DDL is sent; take a binlog backup (`create --for secondary`) for it instead.
- The backup must still carry index extra metadata and the FlushAll messages: the snapshot replaces how the data is carried, not what the DDL replay and the replication start point are built from. A snapshot backup taken with the default strategy records the FlushAll messages; `--backup_index_extra` is what records the index attributes.
- The snapshot import reads StorageV3 segments only, the same limit as `restore --strategy=import_snap`. The restore reads every bundle's manifests up front and refuses a backup with data segments in another storage version before anything is sent, because the target would only find out after the DDL had been replayed, and a secondary that got that far has to be redeployed. A Milvus 3.0 source writes StorageV3 segments with `common.storage.useLoonFFI` enabled.

## Preconditions and limits

Before opening restore streams, the command requires a checkpoint from the expected source at time tick zero on every target physical channel. Missing configuration, missing checkpoints, or failed lookups stop the restore.

Restore messages start at time tick 1. If CDC advances the checkpoint first, restore messages can be acknowledged without being applied. Starting a new task against a nonzero checkpoint from CDC or an earlier restore requires a new secondary; restarting Milvus or dropping collections does not clear it.

The check neither blocks CDC nor detects uncommitted replicated transactions. If CDC has already sent messages, use a new secondary even if checkpoints remain zero.

## Resuming a failed secondary restore

Restore Resume is Alpha (experimental). See the [compatibility notice](restore_resume.md).

Set `"enableResume": true` in the original `POST /api/v1/restore_secondary` request. It defaults to false. `GET /api/v1/get_restore?id=<id>` reports `enableResume: true` from registration when enabled. After preflight and batch planning, a failed task can be resumed by sending `{"id":"<id>"}` to `POST /api/v1/resume_restore` using the same persistence location. Its recovery state is held in memory and saved after a prepared attempt fails. See [durable recovery state](restore_resume.md#durable-recovery-state) for storage configuration, CLI use, and the stain marker that blocks replay after an interrupted attempt.

Keep CDC stopped, with earlier replication requests drained, and reserve the target for this restore until verification succeeds or the restore is abandoned. Resume verifies the original source and channel mapping. A channel may be at its last confirmed position or at a position the restore sent but did not hear acknowledged. A position alone cannot identify a competing writer.

Unconfirmed broadcast fragments keep their original IDs, time ticks and per-channel order. Polling a saved binlog job and replaying its original broadcast use the existing staged files. Binlog batches share the collection directory `restore-temp-<taskID>-<db>-<collection>/`. A replacement import recopies and verifies its binlogs at their backup-relative paths under this directory. Independent secondary restores ignore `restore.keepTempFiles`. A successful binlog import cleans up its staging files. A failed import retains them only when Resume is enabled. A snapshot import keeps the original bundle, partition routing and per-channel source cutoff. The original FlushAll positions must be confirmed and final collection verification must pass before starting CDC.

Resume queries the saved ID of a submitted import even when the previous attempt recorded it as Failed. A confirmed Failed result for that ID permits one replacement; Completed is recorded and skipped; Pending, Importing, Uncommitted, or Committing keeps the original ID and continues waiting or committing. If an adopted job fails later while waiting, or the replacement fails, this execution ends and another explicit Resume is required. This applies to both Milvus 2.6 automatic commit and 3.0 explicit commit, including failures after commit was requested. New submissions use the original batch and a new message identity/time tick; replaying unconfirmed messages preserves their original identities. If an execution stops after saving a new job ID but before registering its import message, the next Resume submits with that same ID instead of allocating another one.

Lookup failures are separate from a job's Failed state. Network, HTTP, and business-level lookup errors receive up to ten attempts with exponential backoff from 200 ms to 3 seconds before failing the execution and retaining the ID. Cancellation stops retries; missing or invalid response status codes fail immediately. A missing job, including Failed with a reason identifying the queried job as missing, keeps its ID and is polled for approximately one minute. It does not authorize a replacement because the job might still be arriving or might already have completed and been garbage-collected.

**Secondary import replay has at-least-once semantics, not exactly-once semantics.** A terminal Failed result does not prove that all published data or compaction effects were rolled back. Retrying after automatic or explicit commit can leave duplicate data or residual effects from the old job, even though Milvus asynchronously cleans up its failed segments. Resume permits this replay; it does not provide transactional rollback or deduplication.

To abandon a failed job, use `POST /api/v1/cleanup_failed_restore` with its ID. This releases its recovery state and removes its remaining staged files. Abandonment is local and does not roll back the secondary or make it a fresh target for a new restore.
