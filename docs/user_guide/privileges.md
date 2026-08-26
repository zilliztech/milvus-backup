# Required Milvus privileges

Milvus Backup connects to Milvus as the user configured under `milvus.user` /
`milvus.password` (default `root`). Which privileges that user needs depends on
the deployment.

## Default deployments

With the default configuration nothing needs to be granted: milvus-backup
connects as `root`, and Milvus skips privilege checks for `root` while
`common.security.rootShouldBindRole` is `false` (the Milvus default).

## Restoring as a non-root user

Restore writes data through Milvus bulk import in binlog mode (`backup=true`,
or `l0_import=true` for L0 segments). Starting with Milvus versions that
include [milvus-io/milvus#51894](https://github.com/milvus-io/milvus/pull/51894)
(unreleased at the time of writing), these imports require the cluster-level
`ImportBinlog` privilege on top of the collection-level `Import` privilege.

This affects deployments that run milvus-backup as a user other than `root`,
or that set `common.security.rootShouldBindRole=true`. Without the grant,
restore fails with a `PrivilegeNotPermitted` error naming `ImportBinlog`.

Grant the privilege to a role of the connecting user. The grant must be
cluster-scoped — both the database name and the collection name must be `*`.
Milvus accepts a narrower scope without an error, but the grant then never
matches and restore keeps failing:

```go
// milvus Go client
client.OperatePrivilegeV2(ctx,
    milvusclient.NewGrantPrivilegeV2Option(roleName, "ImportBinlog", "*").WithDbName("*"))
```

Roles carrying the built-in `admin` or `ClusterAdmin` privilege group already
include `ImportBinlog` and need no extra grant.

Backup (`create`) is unaffected: the privilege only guards import.
