# FAQ

### Backup fails with `field xxx has different file num to other fields`

This error means the backup tool detected inconsistent binlog files in a segment — some fields have more files than others.

**Why does this happen?**

Starting from v0.5.4, milvus-backup first tries to fetch the accurate binlog list from Milvus via the RESTful API (`GetSegmentInfo`, available since Milvus 2.5.8). If this API call fails or is unavailable, the tool falls back to listing files directly from object storage. The fallback may pick up orphaned binlog files left by DataNode upload retries during transient network errors. These extra files cause a count mismatch between fields, and the backup fails with this error as a safety check.

**How to fix it?**

1. **Upgrade Milvus to >= 2.5.8 and milvus-backup to the latest version.** This enables the precise binlog list API, completely avoiding the problem.
2. **If already on Milvus >= 2.5.8**, check the backup logs for:
   ```
   get segment info via proxy node failed, pls check whether milvus restful api is enabled
   ```
   This means the RESTful API is unreachable. Common causes:
   - A Layer-7 load balancer that only forwards gRPC but not HTTP/1.1 traffic. You need to configure routing rules to support both protocols on the same port (e.g., route `/milvus.proto.milvus.MilvusService/` to gRPC backend, and `/` to HTTP backend).
   - Network policies or firewalls blocking HTTP access to the Milvus proxy.
3. **If you cannot upgrade Milvus**, wait for Milvus GC to clean up the orphaned binlogs (or trigger a manual compaction), then retry the backup.

**Related issues:** [#913](https://github.com/zilliztech/milvus-backup/issues/913), [#635](https://github.com/zilliztech/milvus-backup/issues/635), [#476](https://github.com/zilliztech/milvus-backup/issues/476)

---

### Why doesn't backup include index data?

Milvus-backup only backs up **raw data** (insert binlogs, delta binlogs) and **index metadata** (index type, parameters, field info). It does **not** back up the actual index files. This is by design — vector index formats evolve rapidly and their internal data structures are not guaranteed to be compatible across Milvus versions, so rebuilding from raw data is the only reliable approach.

During restore, only the data is recovered by default — indexes are **not** rebuilt. Since Milvus requires an index to load a collection, you will need to create indexes before the collection can be loaded and queried.

If you want the restore process to automatically rebuild indexes using the original index metadata stored in the backup, pass the `--rebuild_index` flag:

```shell
milvus-backup restore --name <backup_name> --rebuild_index
```

This tells the restore process to re-create the indexes based on the backed-up index metadata after the data is restored, so the collection is ready to load immediately.
