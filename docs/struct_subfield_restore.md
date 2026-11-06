# Restore struct sub-field schemas

Backups retain source field, struct, and function IDs together with `element_nullable`, the recursive `type_schema`, collection schema `version`, and namespace enablement. `type_schema` stores the serialized upstream `schemapb.TypeSchema` message; metadata JSON encodes those bytes as base64. Collection properties saved from DescribeCollectionResponse are merged into the schema and override duplicate schema property keys. Other schema properties remain unchanged.

## Enable source field ID preservation

The first Milvus release supporting the public `CreateCollection` `preserve_field_ids` contract has not been established. The capability override defaults to `false`. Enable it only after verifying that the target implements that contract and supports struct sub-field evolution, nullable elements, and nested array sub-fields:

```sh
./milvus-backup --set milvus.preserveFieldIDs=true restore -n backup_name
./milvus-backup --set milvus.preserveFieldIDs=true restore -n backup_name --strategy import_snap
```

The equivalent config is:

```yaml
milvus:
  preserveFieldIDs: true
```

For binlog backups, capture the hidden source `$meta` and `$namespace_id` IDs before restoring. The same setting enables this capture independently of index-extra backup, using the configured `milvus.etcd.endpoints` and `milvus.etcd.rootPath`:

```sh
./milvus-backup --set milvus.preserveFieldIDs=true create -n backup_name --format binlog
```

Existing `--backup_index_extra` / REST `with_index_extra` backups also capture these hidden schemas. Snapshot-format backups capture them from the exported bundle and need no etcd access for schema capture. A binlog backup taken without either option logs a warning if it omits hidden IDs.

Preserved-ID restore sends source IDs for all fields, structs, sub-fields, hidden fields, and functions. It retains `max_field_id`, including IDs of deleted trailing fields. Skipping `max_field_id` is rejected on this path. Sub-field names are sent bare, system fields are omitted, and the internal `__struct_sub_field_added__` marker is removed. The import rewrites rows using the current schema.

A nonempty collection missing hidden field IDs fails before a requested target collection drop. Recreate its backup with schema ID capture enabled. Unknown binlog field IDs are not assigned to hidden fields: they can belong to deleted fields or sub-fields. A binlog collection proven empty by its metadata may use ordinary creation when its hidden IDs were not recorded; that request omits `preserve_field_ids`, creates all visible fields together, and lets Milvus allocate hidden IDs. Snapshot metadata does not enumerate segments at this level, so it must retain its bundle's hidden schemas.

With the capability disabled, the existing ordinary restore remains available for older schemas. Nullable elements, nested arrays, and struct field ID evolution that ordinary creation cannot preserve fail explicitly. Gaps and the `max_field_id` high-water mark detect incompatible struct evolution. A sub-field appended to the last struct can leave IDs contiguous, so a nonzero schema version also requires the capability. This check is conservative: unrelated schema changes on a struct-bearing collection can require the override too, because DescribeCollection removes the sub-field origin markers. Older metadata without a schema version keeps its existing behavior.

## Restore paths

- Binlog and `import_snap` share the preserved-ID collection DDL task.
- `restore_snap` continues to let Milvus recreate the collection from the stored bundle schema and copy its segments.
- Secondary restore writes final source IDs directly into its WAL messages. It retains the new schema attributes and checks target support before broadcasting new struct shapes.
- Manifest-based storage version 3 segments require snapshot backup format; binlog backup rejects them.

## Integration scaffold

`tests/testcases/test_restore_struct_evolution.py` provides an opt-in round-trip scaffold for all three primary restore paths. Set `MILVUS_BACKUP_STRUCT_EVOLUTION_COLLECTION` to a disposable synthetic source collection prepared with the matching Milvus and SDK versions. Its preparation must create nullable scalar and vector elements and a nested array sub-field, flush rows, add a nullable sub-field, flush more rows, drop a different sub-field while its old segment still exists, and retain a deleted trailing ID. Index and load the source before running the test. Set the backup service's `milvus.preserveFieldIDs` to `true` and configure source etcd for its binlog run.

The scaffold compares visible schemas and every queried row after binlog, snapshot copy, and snapshot import restore. Creating the source fixture and running the real scenarios require the Milvus image that implements the contract.
