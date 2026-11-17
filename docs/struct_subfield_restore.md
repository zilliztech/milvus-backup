# Struct sub-field schema restore

A struct array's sub-fields, nested arrays (`Array<Array<T>>`), and
per-element nullability live in schema attributes that a plain
`DescribeCollection` round trip used to drop: `element_nullable` and the
recursive `type_schema` (a serialized `schemapb.TypeSchema`). Backups now
record both on every field, visible or struct sub-field, so a restore can
recreate the fields as the source had them.

## What is captured

- `FieldSchema.element_nullable` — nullability of an array's elements.
- `FieldSchema.type_schema` — the recursive type representation, stored as
  serialized `schemapb.TypeSchema` bytes (base64 in the metadata JSON).

Both are additive: metadata written by older versions of this tool still
restores, decoding to absent attributes. The collection schema `version`
recording from earlier releases is unchanged.

## Restore paths

- **Binlog and `import_snap`** primary restores build the
  `CreateCollection`/`AddField` requests from the backup metadata and send the
  attributes as recorded. The restore request's `skip_params` lists also apply
  to the type params nested inside a `type_schema`.
- **Secondary** restore bypasses the proxy, so its `CreateCollection` and
  import WAL messages pre-qualify sub-field names (`struct[sub]`) and carry
  the same attributes.
- **`restore_snap`** already recreates the collection from the snapshot
  bundle's internal schema and is unaffected.

The create request is fully decoded before a `--drop-collections` style
target removal runs, so undecodable backup metadata fails while the target
collection still stands.

## Target support

Sending the attributes is backward compatible: a Milvus that does not know
them ignores them, which reproduces the previous behavior. A Milvus with
`TypeSchema` support ([milvus-io/milvus#55227](https://github.com/milvus-io/milvus/pull/55227))
honors them and recreates the fields faithfully. Restoring an
element-nullable or nested-array schema to an older target therefore
succeeds with those attributes lost.

Field IDs are out of scope here. A collection whose struct sub-fields were
added or dropped after creation cannot be recreated with matching IDs by
ordinary creation, and restore refuses such a backup before importing rows
rather than silently reading the wrong columns
([milvus-backup#1388](https://github.com/zilliztech/milvus-backup/pull/1388)).
