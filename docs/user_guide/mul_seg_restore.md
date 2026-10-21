# multi-segment merged restore

## Background

In restore, each Segment used to trigger a separate BulkInsert job. While this approach (called per-segment restore) is straightforward, it becomes inefficient when restoring Collections with a large number of Segments, due to the overhead of launching and scheduling a large number of jobs.

To address this, we've implemented multi-segment merged restore that allows restoring multiple Segments in a single BulkInsert job, significantly reducing scheduling overhead.

In internal testing, we've observed up to 10x performance improvements when using this mode to restore large Collection.

> **Note**: This mode talks to the v2 import api, which requires Milvus v2.5.0 or higher.

## How to use

Merged restore is selected with the `strategy` option, whose value names both the backup format it applies to and the path it takes: `per_seg` (binlog, one import job per segment), `merged_seg` (binlog, segments merged into fewer jobs), `restore_snap` (snapshot, Milvus's own restore api), and `import_snap` (snapshot, one v2 import job per collection reading the snapshot bundle; Milvus 3.0.4+). `auto`, like leaving the strategy unset, lets the restore pick one on its own: a binlog backup takes the merged path when the target Milvus supports the v2 import api, and falls back to the per-segment path otherwise; a snapshot backup takes the restore api — `import_snap` is only ever used when pinned explicitly.

### command line

To pin the merged mode explicitly, use the `--strategy` flag:

```shell
./milvus-backup restore -n <backup_name> --strategy merged_seg
```

### HTTP API

To pin the merged mode explicitly, set the `strategy` field in the request body.

The old `--use_v2_restore` flag and the `use_v2_restore` request field are deprecated: setting them to `true` still selects the merged mode, and an explicit `strategy` wins when both are given.

![mul_seg_restore](./figs/mul_seg_restore.png)

## How many Segments are merged

At most 256 Segments go into one BulkInsert job by default. Milvus rejects a request carrying more files than `dataCoord.import.maxImportFileNumPerReq` allows, so a deployment that lowered that limit has to lower this one to match:

```yaml
restore:
  maxSegmentsPerImportJob: 256
```

## Snapshot restores: restore api vs import api

Both snapshot strategies restore a snapshot-format backup; they differ in who creates the collection and how the data moves.

`restore_snap` (Milvus 3.0+) hands the whole restore to Milvus's restore api: the server creates the collection from the bundle's schema, restores its indexes and partitions, and copies the data inside one storage service. It is the faster path — a physical copy with no row rewrite — but it cannot cap the shard count. When the backup storage is not the same service as the target's, milvus-backup first stages the bundle into a temp prefix of the target's storage (streaming it through the milvus-backup host) and restores from there.

`import_snap` (Milvus 3.0.4+) creates the schema, partitions and indexes through the ordinary restore DDL, then submits one import job per collection that reads the snapshot bundle: the server decodes the rows, folds the snapshot's L0 deletes, and writes new segments. Because milvus-backup creates the collection, shard overrides, `skip_params`, the index options and `meta_only` behave as they do for binlog restores; and because Milvus reads the source directly under the backup storage's own credentials, a backup on a different storage service needs no staging copy.
