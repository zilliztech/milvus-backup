# Create backup storage preflight

`POST /api/v1/create` checks source storage before registering a backup task,
for both synchronous and asynchronous requests. The CLI create command uses
the same admission check. It lists the configured source
bucket under `<milvus.storage.rootPath>/insert_log/` using the same storage client and
credentials that the backup will use. The request's `backup_root_path` override
only affects the destination. Legacy v1 configuration is translated by the config loader.

The check reuses `ListPrefix`, calls `Next` once to surface listing errors, and
closes the iterator. Cloud providers use their existing default page sizes;
local storage retains its existing eager listing behavior. An empty result succeeds. The list check has a 10-second context deadline,
or the caller's earlier deadline. `strategy=meta_only` (including the legacy
`meta_only` flag when it selects that strategy) skips the source data check.

If the source list check fails, the response remains
HTTP 200 with a nonzero **business code**, for example:

```json
{
  "request_id": "same-request-id",
  "code": 503,
  "msg": "source storage list preflight failed (...): ...",
  "data": null
}
```

`503` is `Storage_Not_Ready`. The rejected request does not register a new task,
start backup execution, or write backup metadata. A caller can retry with the
same backup name and request ID. Callers must check the response body rather
than treating HTTP 200 alone as acceptance. Use bounded retries with backoff;
persistent configuration or permission errors also fail this check.

This change does not remove existing tasks or make requests idempotent after a
task has been accepted. It checks List access at admission time; it does not
prove Get/Put/Copy permissions or guarantee that permissions remain available
throughout execution. A successful later retry alone does not establish IAM
propagation delay as the root cause.

Storage client initialization keeps its existing error handling.
