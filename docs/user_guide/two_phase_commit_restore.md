# two-phase commit restore into a replicating cluster

## Background

When the restore target replicates to other clusters over Milvus CDC, every import job on the target is mirrored onto each secondary by the replication stream, and the data must become visible at the same logical point on all ends. A plain auto-commit import cannot guarantee that, so a replicating cluster running Milvus 3.0 or newer with `dataCoord.import.enableInReplicatingCluster=true` accepts only two-phase import jobs (`auto_commit=false`) and rejects the rest.

milvus-backup drives that two-phase flow when it knows the secondaries: the import job is held uncommitted until its mirror is staged on every secondary, then committed once on the restore target, and the replicated commit commits every mirror.

## How to use

Pass each secondary with a repeated `--secondary` flag in `address[,token]` form:

```shell
milvus-backup restore -n my_backup \
  --secondary 'http://10.0.0.2:19530,secondary_user:secondary_password' \
  --secondary 'http://10.0.0.3:19530,raw-api-token'
```

The token is the raw Milvus credential: use `username:password` for a Milvus user, or an API token directly. Do not add a `Bearer ` prefix or base64-encode it. The flag splits only at the first comma, so later commas belong to the token. Omit the comma and token to inherit the restore target's `milvus.user` and `milvus.password`:

```shell
milvus-backup restore -n my_backup --secondary http://10.0.0.2:19530
```

The RESTful API takes a `secondaries` object array on `POST /api/v1/restore`, with the same address and raw-token semantics:

```json
{
  "backup_name": "my_backup",
  "secondaries": [
    {"address": "http://10.0.0.2:19530", "token": "secondary_user:secondary_password"},
    {"address": "http://10.0.0.3:19530", "token": "raw-api-token"}
  ]
}
```

Snapshot-format backups are supported through `import_snap`. Leaving the strategy at `auto` selects it automatically whenever secondaries are configured; pinning `--strategy import_snap` also works. An explicit `--strategy restore_snap` is rejected because the snapshot restore api does not create an import job to commit two-phase.

When the primary and every secondary keep their own copy of the bundle — the split-bucket deployment, where each cluster stages the backup at the same key prefix in its own bucket — set `--snapshot_source own_bucket` (REST field `snapshot_source`, same values). The import then sends a bucket-relative object key instead of a complete URI, so neither a bucket name nor an endpoint travels the replication stream: every cluster reads the bundle from its own configured bucket with its own credential, and no external spec goes out. The key is the backup directory as each cluster's storage resolves it, `backup/my_backup` with the default `backup.storage.rootPath`. The value `shared`, like leaving the option unset, is today's behavior: one complete uri pointed at a single bucket. The option applies only to snapshot-format restores on the import path, and needs a Milvus that takes bucket-relative snapshot sources on every cluster (milvus-io/milvus#55620).

On a replicating 3.0+ target a restore *without* `--secondary` is refused up front, because the server no longer accepts plain auto-commit import jobs there; the same check runs when `--strategy per_seg` is combined with `--secondary`, since the per-segment path imports through the v1 grpc api, which has no two-phase commit.

> **Note**: Two-phase commit requires Milvus 3.0 or newer on the restore target and on every secondary. Snapshot imports raise that floor to Milvus 3.0.4 on every cluster.

## Prerequisites

- The restore target runs Milvus 3.0+ with `dataCoord.import.enableInReplicatingCluster=true`, and every address passed with `--secondary` is a replication target of it. Snapshot imports require Milvus 3.0.4+ on every cluster.
- **For binlog backups, every cluster must resolve the same object-storage paths.** An import job references bucket-relative file paths. milvus-backup stages the backup files once, into the restore target's storage, and each secondary's mirrored job reads the same paths from the secondary's own configured storage. The ends therefore need a shared bucket, or bucket content synced across clusters; a secondary whose storage does not see the staged files fails its mirror, and the restore reports the failure before committing anything.
- **For snapshot backups, every cluster must be able to read the snapshot bundle.** Each collection has one snapshot-sourced import whose metadata URI and external spec replicate to the mirrors. No files are staged by milvus-backup; the primary and every secondary read the bundle themselves, so the URI and credentials must work from each cluster. For an endpoint-less MinIO URI, the URI bucket must be every cluster's own `minio.bucketName`: copy the complete backup prefix into that bucket on each MinIO with its key layout intact. A separate bucket that merely shares a name across the clusters does not satisfy this check. Cross-service external specs remain subject to Milvus's provider and endpoint admission rules. With `snapshot_source=own_bucket` the URI constraint drops away instead: the import names no bucket at all, and the same key prefix staged in every cluster's own bucket is what each one reads.

## How it works

1. The job is submitted to the restore target with `auto_commit=false`; it imports and then holds at `Uncommitted`, with its segments invisible to queries.
2. The replication stream delivers the import message to every secondary, which creates a mirrored job under the same job id and runs the same import. The restore waits until every secondary's mirror holds at `Uncommitted` (a few minutes are allowed for a stream that is catching up).
3. Only then is the job committed, once, on the restore target. A replication target refuses client-side commits, so the secondaries are never called: the commit message the target broadcasts travels the replication stream and commits every mirror, and the restore waits for each mirror to complete.

With the default restore settings a failure before commit returns triggers a primary abort and cleanup of staged files.

Restore Resume is Alpha (experimental). See the [compatibility notice](restore_resume.md).

With `enableResume: true`, a query error or interrupted request leaves the job ID and staged files available for Resume. If the primary is Uncommitted and commit has never been requested, a confirmed failed mirror triggers a primary abort. If a mirror reports the current ID missing for five minutes, the attempt fails and keeps the primary job for another Resume. Both an explicit missing-ID lookup and a Failed response identifying that same ID as missing count toward the window. Seeing the mirror Pending or Importing resets the window. The attempt retains its ID even if the abort succeeds or its response is lost. The next Resume checks the primary first. Only that initial lookup can authorize a replacement. If an adopted job fails or disappears later while waiting, this execution ends with the same ID; another explicit Resume is required. The pre-commit abort rules still apply. Resume recopies the batch into the recorded staging directory before submitting a replacement only if the primary failed before commit and acknowledged the abort. If the primary reports the saved ID missing, Resume returns an error with the original ID retained. If the create response was lost before the ID was recorded, Resume may create another manual-commit job. An earlier untracked job can remain uncommitted.

Resume keeps the original primary ID once it sends a commit request or observes Committing or Completed on any cluster. From that point it will neither abort nor replace the job, even if the primary subsequently reports the ID missing. It may repeat the idempotent commit while the primary is still Uncommitted, or wait for the mirrors if the primary is already Committing or Completed. A failed mirror after commit is reported on the original ID. The backup server remembers each Completed observation for the current job ID, including mirrors already completed during the preparation check. Resume polls only the remaining clusters. A replacement job starts with no completion observations. The local job completes once every secondary and the primary have been confirmed Completed.

Explicit [Cleanup](restore_resume.md) releases the backup-side staging files. An outstanding uncommitted job on Milvus may still need to be aborted on the primary if the restore is abandoned.
