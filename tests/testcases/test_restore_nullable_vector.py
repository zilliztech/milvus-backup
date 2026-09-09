"""Backup/restore round-trip coverage for nullable vector fields.

PRD reference: Zilliz Cloud Nullable Vector (Milvus 2.6.18 / 3.0.0).
These tests assert that:
  1. The `nullable=True` flag on vector fields survives backup -> restore.
  2. NULL vector values round-trip as NULL (not zero vectors, not omitted rows).
  3. Mixed NULL / non-NULL data preserves per-row vector content.
  4. AddCollectionField of a nullable vector field is captured by backup and
     restored as a nullable field on the target collection.
  5. Search on a restored nullable vector field skips NULL rows.

The tests use MilvusClient (not ORM) because:
  - dict-form inserts with `None` for vector fields are the official
    nullable-vector ingest path documented in the user guide;
  - describe_collection on MilvusClient exposes per-field `nullable` directly.
"""


import numpy as np
import ml_dtypes
import pytest
from pymilvus import DataType

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log


PREFIX = "restore_nullable_vec"
BACKUP_PREFIX = "backup_nullable_vec"
SUFFIX = "_bak"


def _vector_value(data_type: DataType, dim: int, seed: int):
    """Generate a deterministic non-NULL vector value for the given dtype."""
    rng = np.random.default_rng(seed)
    if data_type == DataType.FLOAT_VECTOR:
        return [np.float32(x) for x in rng.random(dim)]
    if data_type == DataType.FLOAT16_VECTOR:
        return np.asarray(rng.random(dim), dtype=np.float16)
    if data_type == DataType.BFLOAT16_VECTOR:
        return np.asarray(rng.random(dim), dtype=ml_dtypes.bfloat16)
    if data_type == DataType.INT8_VECTOR:
        return np.asarray(rng.integers(-128, 127, size=dim), dtype=np.int8)
    if data_type == DataType.BINARY_VECTOR:
        # BINARY_VECTOR expects bytes; dim is in bits, so dim // 8 bytes.
        return bytes(rng.integers(0, 256, size=dim // 8, dtype=np.uint8).tolist())
    if data_type == DataType.SPARSE_FLOAT_VECTOR:
        # Sparse vectors are dict[int -> float] in pymilvus.
        return {int(i): float(rng.random()) for i in rng.choice(10_000, size=8, replace=False)}
    raise AssertionError(f"unsupported vector data type: {data_type}")


def _add_vector_field(schema, data_type: DataType, name: str, dim: int, nullable: bool):
    kwargs = {"nullable": nullable}
    if data_type != DataType.SPARSE_FLOAT_VECTOR:
        kwargs["dim"] = dim
    schema.add_field(name, data_type, **kwargs)


def _flush(client, collection_name):
    """pymilvus flush() blocks until the underlying FlushAll returns, so a
    helper here is mostly for readability and future hardening if we need
    to add row-count polling."""
    client.flush(collection_name=collection_name)


def _describe_field(client, collection_name, field_name):
    info = client.describe_collection(collection_name=collection_name)
    for f in info["fields"]:
        if f["name"] == field_name:
            return f
    raise AssertionError(f"field {field_name} not found in {collection_name}")


# Vector dtypes whose Milvus support for `nullable=True` is in scope per PRD.
# BINARY_VECTOR dim must be multiple of 8; SPARSE has no dim param.
_VECTOR_CASES = [
    pytest.param(DataType.FLOAT_VECTOR, 128, id="float_vector"),
    pytest.param(DataType.FLOAT16_VECTOR, 128, id="float16_vector"),
    pytest.param(DataType.BFLOAT16_VECTOR, 128, id="bfloat16_vector"),
    pytest.param(DataType.INT8_VECTOR, 128, id="int8_vector"),
    pytest.param(DataType.BINARY_VECTOR, 128, id="binary_vector"),
    pytest.param(DataType.SPARSE_FLOAT_VECTOR, 0, id="sparse_float_vector"),
]


class TestRestoreNullableVector(TestcaseBase):
    """Backup/restore behavior for vector fields declared with nullable=True."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data_type,dim", _VECTOR_CASES)
    def test_restore_nullable_vector_round_trip(self, data_type, dim):
        """Round-trip a collection that holds mixed NULL / non-NULL vectors.

        Verifies (a) schema nullable flag preserved, (b) per-row NULL preserved.
        """
        self._connect()
        collection_name = cf.gen_unique_str(PREFIX)
        backup_name = cf.gen_unique_str(BACKUP_PREFIX)
        vec_field = "embedding"
        nb = 200

        schema = self.milvus_client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        _add_vector_field(schema, data_type, vec_field, dim, nullable=True)
        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)
        log.info(f"created collection {collection_name} with nullable {data_type.name}")

        src_field = _describe_field(self.milvus_client, collection_name, vec_field)
        assert src_field.get("nullable") is True, (
            f"source field must be nullable, got {src_field}"
        )

        # Half rows NULL, half rows with real vectors. id parity drives NULL.
        rows = []
        for i in range(nb):
            row = {"id": i}
            if i % 2 == 0:
                row[vec_field] = _vector_value(data_type, dim, seed=i)
            else:
                row[vec_field] = None
            rows.append(row)
        self.milvus_client.insert(collection_name=collection_name, data=rows)
        _flush(self.milvus_client, collection_name)

        # Sanity: source query reports NULL for odd ids.
        src_query = self.milvus_client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", vec_field],
            limit=nb,
        )
        src_by_id = {r["id"]: r for r in src_query}
        assert len(src_by_id) == nb
        for i in range(nb):
            if i % 2 == 0:
                assert src_by_id[i][vec_field] is not None, (
                    f"source id={i} should have a vector, got NULL"
                )
            else:
                assert src_by_id[i][vec_field] is None, (
                    f"source id={i} should be NULL, got {src_by_id[i][vec_field]!r}"
                )

        # Backup and restore with suffix.
        res = self.client.create_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
        })
        assert res.get("msg", "") == "success", f"create_backup failed: {res}"
        res = self.client.restore_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "collection_suffix": SUFFIX,
        })
        assert res.get("msg", "") == "success", f"restore_backup failed: {res}"

        restored = collection_name + SUFFIX
        listed, _ = self.utility_wrap.list_collections()
        assert restored in listed

        # (a) schema nullable flag preserved on target.
        dst_field = _describe_field(self.milvus_client, restored, vec_field)
        assert dst_field.get("nullable") is True, (
            f"restored field nullable flag lost: {dst_field}"
        )
        assert dst_field.get("type") == src_field.get("type"), (
            f"vector type changed: {src_field} -> {dst_field}"
        )

        # (b) per-row NULL preserved.
        dst_query = self.milvus_client.query(
            collection_name=restored,
            filter="id >= 0",
            output_fields=["id", vec_field],
            limit=nb,
        )
        dst_by_id = {r["id"]: r for r in dst_query}
        assert len(dst_by_id) == nb, (
            f"row count mismatch: src={nb}, dst={len(dst_by_id)}"
        )
        for i in range(nb):
            if i % 2 == 0:
                assert dst_by_id[i][vec_field] is not None, (
                    f"restored id={i} lost its vector"
                )
            else:
                assert dst_by_id[i][vec_field] is None, (
                    f"restored id={i} should remain NULL, got "
                    f"{dst_by_id[i][vec_field]!r}"
                )

    @pytest.mark.tags(CaseLabel.L1)
    def test_restore_nullable_vector_search_skips_null(self):
        """Search on the restored nullable vector should skip NULL rows."""
        self._connect()
        collection_name = cf.gen_unique_str(PREFIX)
        backup_name = cf.gen_unique_str(BACKUP_PREFIX)
        vec_field = "embedding"
        dim = 64
        nb = 100
        non_null_count = nb // 2

        schema = self.milvus_client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)

        rng = np.random.default_rng(seed=42)
        rows = []
        for i in range(nb):
            rows.append({
                "id": i,
                vec_field: [np.float32(x) for x in rng.random(dim)] if i % 2 == 0 else None,
            })
        self.milvus_client.insert(collection_name=collection_name, data=rows)
        _flush(self.milvus_client, collection_name)

        res = self.client.create_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
        })
        assert res.get("msg", "") == "success", f"create_backup failed: {res}"
        res = self.client.restore_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "collection_suffix": SUFFIX,
        })
        assert res.get("msg", "") == "success", f"restore_backup failed: {res}"

        restored = collection_name + SUFFIX
        # Build index + load on the restored collection so we can search.
        index_params = self.milvus_client.prepare_index_params()
        index_params.add_index(
            field_name=vec_field,
            index_type="FLAT",
            metric_type="L2",
        )
        self.milvus_client.create_index(
            collection_name=restored,
            index_params=index_params,
        )
        self.milvus_client.load_collection(restored)

        # Request topK >= nb, expect at most non_null_count results back.
        query_vec = [np.float32(x) for x in rng.random(dim)]
        search_res = self.milvus_client.search(
            collection_name=restored,
            data=[query_vec],
            anns_field=vec_field,
            limit=nb,
            output_fields=["id"],
        )
        hits = list(search_res[0])
        assert len(hits) <= non_null_count, (
            f"search should skip NULL rows; expected <= {non_null_count} hits, "
            f"got {len(hits)}"
        )
        # Every hit must come from an even id (the non-NULL set).
        for hit in hits:
            hit_id = hit["id"]
            assert hit_id % 2 == 0, f"hit id={hit_id} should have been NULL"

    @pytest.mark.tags(CaseLabel.L1)
    def test_restore_add_nullable_vector_field(self):
        """add_collection_field with a nullable vector field round-trips through backup."""
        self._connect()
        collection_name = cf.gen_unique_str(PREFIX)
        backup_name = cf.gen_unique_str(BACKUP_PREFIX)
        dim = 64
        nb_initial = 100
        nb_after = 50

        schema = self.milvus_client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("base_vector", DataType.FLOAT_VECTOR, dim=dim)
        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)

        rng = np.random.default_rng(seed=7)
        initial_rows = [
            {"id": i, "base_vector": [np.float32(x) for x in rng.random(dim)]}
            for i in range(nb_initial)
        ]
        self.milvus_client.insert(collection_name=collection_name, data=initial_rows)
        _flush(self.milvus_client, collection_name)

        # AddCollectionField of a nullable vector. Non-nullable should be
        # rejected by Milvus per PRD; we only test the supported path here.
        new_vec = "added_vector"
        self.milvus_client.add_collection_field(
            collection_name=collection_name,
            field_name=new_vec,
            data_type=DataType.FLOAT_VECTOR,
            dim=dim,
            nullable=True,
        )

        # Existing rows are implicitly NULL on the new field; new rows split
        # 50/50 between populated and NULL.
        new_rows = []
        for i in range(nb_initial, nb_initial + nb_after):
            row = {"id": i, "base_vector": [np.float32(x) for x in rng.random(dim)]}
            if i % 2 == 0:
                row[new_vec] = [np.float32(x) for x in rng.random(dim)]
            else:
                row[new_vec] = None
            new_rows.append(row)
        self.milvus_client.insert(collection_name=collection_name, data=new_rows)
        _flush(self.milvus_client, collection_name)

        res = self.client.create_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
        })
        assert res.get("msg", "") == "success", f"create_backup failed: {res}"
        res = self.client.restore_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "collection_suffix": SUFFIX,
        })
        assert res.get("msg", "") == "success", f"restore_backup failed: {res}"

        restored = collection_name + SUFFIX
        added = _describe_field(self.milvus_client, restored, new_vec)
        assert added.get("nullable") is True, (
            f"restored added vector field must remain nullable: {added}"
        )

        # Sample-check NULL semantics on the added field.
        dst_query = self.milvus_client.query(
            collection_name=restored,
            filter="id >= 0",
            output_fields=["id", new_vec],
            limit=nb_initial + nb_after,
        )
        dst_by_id = {r["id"]: r for r in dst_query}
        assert len(dst_by_id) == nb_initial + nb_after

        # Rows that existed before the add must be NULL on the new field.
        for i in range(nb_initial):
            assert dst_by_id[i][new_vec] is None, (
                f"pre-add row id={i} should be NULL on new field, got "
                f"{dst_by_id[i][new_vec]!r}"
            )
        # Rows inserted after add: even => non-NULL, odd => NULL.
        for i in range(nb_initial, nb_initial + nb_after):
            if i % 2 == 0:
                assert dst_by_id[i][new_vec] is not None, (
                    f"post-add row id={i} should hold a vector"
                )
            else:
                assert dst_by_id[i][new_vec] is None, (
                    f"post-add row id={i} should be NULL"
                )

    @pytest.mark.tags(CaseLabel.L2)
    def test_restore_nullable_vector_with_skip_create_collection(self):
        """When the user pre-creates the target with a non-nullable vector
        field but the backup contains NULL vectors, restore must surface a
        clear error rather than silently masking the mismatch.

        Marked L2 because it exercises the negative path.
        """
        self._connect()
        collection_name = cf.gen_unique_str(PREFIX)
        backup_name = cf.gen_unique_str(BACKUP_PREFIX)
        dim = 32
        nb = 40
        vec_field = "embedding"

        schema = self.milvus_client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)

        rng = np.random.default_rng(seed=1)
        rows = [
            {
                "id": i,
                vec_field: None if i % 2 else [np.float32(x) for x in rng.random(dim)],
            }
            for i in range(nb)
        ]
        self.milvus_client.insert(collection_name=collection_name, data=rows)
        _flush(self.milvus_client, collection_name)

        res = self.client.create_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
        })
        assert res.get("msg", "") == "success", f"create_backup failed: {res}"

        # Pre-create restore target with a NON-nullable vector field.
        restored = collection_name + SUFFIX
        target_schema = self.milvus_client.create_schema(
            auto_id=False, enable_dynamic_field=False
        )
        target_schema.add_field("id", DataType.INT64, is_primary=True)
        target_schema.add_field(vec_field, DataType.FLOAT_VECTOR, dim=dim, nullable=False)
        self.milvus_client.create_collection(
            collection_name=restored, schema=target_schema
        )

        res = self.client.restore_backup({
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "collection_suffix": SUFFIX,
            "skipCreateCollection": True,
        })
        # We expect this to fail with a clear, actionable error. Once the
        # backup tool gains a pre-flight nullable schema check the message
        # should mention the field name; today the failure point is BulkInsert.
        assert res.get("msg", "") != "success", (
            "restore must fail when target field nullable=false but backup "
            f"contains NULL vectors; got: {res}"
        )
