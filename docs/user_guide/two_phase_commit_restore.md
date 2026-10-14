# two-phase commit restore into a replicating cluster

## Background

When the restore target replicates to other clusters over Milvus CDC, every import job on the target is mirrored onto each replica by the replication stream, and the data must become visible at the same logical point on all ends. A plain auto-commit import cannot guarantee that, so a replicating cluster running Milvus 3.0 or newer with `dataCoord.import.enableInReplicatingCluster=true` accepts only two-phase import jobs (`auto_commit=false`) and rejects the rest.

milvus-backup drives that two-phase flow when it knows the replicas: the import job is held uncommitted until its mirror is staged on every replica, then committed once on the restore target, and the replicated commit commits every mirror.

## How to use

Pass the replicas' REST endpoints with the `--peers` flag (comma-separated or repeated):

```shell
milvus-backup restore -n my_backup --peers http://10.0.0.2:19530,http://10.0.0.3:19530
```

The RESTful API takes the same option as the `peers` string array of `POST /api/v1/restore`:

```json
{"backup_name": "my_backup", "peers": ["http://10.0.0.2:19530", "http://10.0.0.3:19530"]}
```

On a replicating 3.0+ target a restore *without* `--peers` is refused up front, because the server no longer accepts plain auto-commit import jobs there; the same check runs when `--strategy per_seg` is combined with `--peers`, since the per-segment path imports through the v1 grpc api, which has no two-phase commit.

> **Note**: Two-phase commit requires Milvus 3.0 or newer on the restore target and on every peer.

## Prerequisites

- The restore target runs Milvus 3.0+ with `dataCoord.import.enableInReplicatingCluster=true`, and every endpoint in `--peers` is a replication target of it.
- **Every cluster must resolve the same object-storage paths.** An import job references bucket-relative file paths. milvus-backup stages the backup files once, into the restore target's storage, and each peer's mirrored job reads the same paths from the peer's own configured storage. The ends therefore need a shared bucket, or bucket content synced across clusters; a peer whose storage does not see the staged files fails its mirror, and the restore reports the failure before committing anything.

## How it works

1. The job is submitted to the restore target with `auto_commit=false`; it imports and then holds at `Uncommitted`, with its segments invisible to queries.
2. The replication stream delivers the import message to every peer, which creates a mirrored job under the same job id and runs the same import. The restore waits until every peer's mirror holds at `Uncommitted` (a few minutes are allowed for a stream that is catching up).
3. Only then is the job committed, once, on the restore target. A replication target refuses client-side commits, so the peers are never called: the commit message the target broadcasts travels the replication stream and commits every mirror, and the restore waits for each mirror to complete.

A failure before the commit aborts the job on the restore target, and the replicated rollback fails the mirrors — no end keeps an uncommitted job behind. Past the commit there is no way back: a mirror that fails on its own does not take the replicated commit, and the restore reports the divergence, naming the peer.
