import json
import os
import subprocess

import numpy as np
import pandas as pd
import pytest

from base.client_base import TestcaseBase
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log

# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BINARY = os.path.join(REPO_ROOT, "milvus-backup")


def run_l0compact(name, output=None, expect_success=True):
    """Invoke the `milvus-backup l0compact` CLI from the repo root."""
    cmd = [BINARY, "l0compact", "-n", name] + (["-o", output] if output else [])
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=300
    )
    log.info(f"l0compact rc={proc.returncode} out={proc.stdout} err={proc.stderr}")
    if expect_success:
        assert proc.returncode == 0, proc.stderr
    return proc


c_name_prefix = "l0compact"
backup_prefix = "l0c_backup"
default_dim = ct.default_dim


class TestL0Compact(TestcaseBase):
    """E2E regression suite for the `milvus-backup l0compact` CLI.

    Each test builds a collection, inserts a known PK range, deletes a known
    subset (which forms L0 delete-only segments once flushed), backs it up,
    runs `l0compact` to fold L0 into per-segment deltalogs, restores the washed
    backup and asserts the restored collection reflects the deletes exactly.
    """

    # ------------------------------------------------------------------
    # small building blocks (kept DRY, parameterized by pk type / scope)
    # ------------------------------------------------------------------
    @staticmethod
    def _schema(string_pk):
        if string_pk:
            return cf.gen_string_pk_default_collection_schema()
        return cf.gen_default_collection_schema()

    @staticmethod
    def _pk_field(string_pk):
        return ct.default_string_field_name if string_pk else ct.default_int64_field_name

    @staticmethod
    def _pk_val(i, string_pk):
        return str(i) if string_pk else i

    def _pk_expr(self, ids, string_pk):
        if string_pk:
            vals = ", ".join(f'"{i}"' for i in ids)
        else:
            vals = ", ".join(str(i) for i in ids)
        return f"{self._pk_field(string_pk)} in [{vals}]"

    @staticmethod
    def _gen_data(start, nb, string_pk):
        """Generate a dataframe whose PKs are the contiguous range [start, start+nb).

        For int64 PK we reuse the framework generator (int64 PKs = range).
        For VarChar PK the schema has no json field, so we build the 4 columns
        (int64, float, varchar-pk, float_vector) explicitly, PK = str(i).
        """
        if not string_pk:
            return cf.gen_default_dataframe_data(nb=nb, start=start)
        return pd.DataFrame(
            {
                ct.default_int64_field_name: [i for i in range(start, start + nb)],
                ct.default_float_field_name: [
                    np.float32(i) for i in range(start, start + nb)
                ],
                ct.default_string_field_name: [
                    str(i) for i in range(start, start + nb)
                ],
                ct.default_float_vec_field_name: cf.gen_vectors(nb, default_dim),
            }
        )

    def _new_collection(self, string_pk=False, extra_partitions=None):
        self._connect()
        name = cf.gen_unique_str(c_name_prefix)
        coll_w = self.init_collection_wrap(name=name, schema=self._schema(string_pk))
        for p in extra_partitions or []:
            coll_w.create_partition(partition_name=p)
        return coll_w, name

    @staticmethod
    def _insert_segment(coll_w, start, nb, string_pk, partition_name=None):
        """Insert one contiguous PK range and flush it into its own sealed segment."""
        df = TestL0Compact._gen_data(start, nb, string_pk)
        coll_w.insert(df, partition_name=partition_name)
        coll_w.flush()

    def _index_and_load(self, coll_w):
        try:
            coll_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        except Exception as e:  # index may already exist (e.g. restored collection)
            log.warning(f"create_index skipped: {e}")
        coll_w.load()

    def _delete_pks(self, coll_w, ids, string_pk, partition_name=None):
        """Delete a set of PKs, then flush so the deletes seal into L0."""
        coll_w.delete(self._pk_expr(ids, string_pk), partition_name=partition_name)
        coll_w.flush()

    def _connect_existing(self, name):
        """Wrap an already-existing (restored) collection and register it for teardown."""
        w = ApiCollectionWrapper()
        w.init_collection(name=name)
        self.collection_object_list.append(w)
        return w

    def _backup(self, name):
        bk = cf.gen_unique_str(backup_prefix)
        res = self.client.create_backup(
            {"async": False, "backup_name": bk, "collection_names": [name]}
        )
        log.info(f"create_backup {bk}: {res}")
        return bk

    def _restore(self, backup_name, src_name, suffix):
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": backup_name,
                "collection_names": [src_name],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup {backup_name} suffix {suffix}: {res}")
        restored = src_name + suffix
        return self._connect_existing(restored), restored

    def _assert_pk_state(
        self, w, string_pk, absent_ids=None, present_ids=None, expected_count=None
    ):
        """Assert the restored collection reflects the deletes exactly."""
        pk_field = self._pk_field(string_pk)
        self._index_and_load(w)
        if expected_count is not None:
            res, _ = w.query("", output_fields=["count(*)"])
            count = res[0]["count(*)"]
            log.info(f"restored count={count}, expected={expected_count}")
            assert count == expected_count, f"count {count} != expected {expected_count}"
        if absent_ids:
            res, _ = w.query(
                self._pk_expr(absent_ids, string_pk), output_fields=[pk_field]
            )
            assert len(res) == 0, f"deleted PKs still present: {res}"
        if present_ids:
            res, _ = w.query(
                self._pk_expr(present_ids, string_pk), output_fields=[pk_field]
            )
            got = {r[pk_field] for r in res}
            expected = {self._pk_val(i, string_pk) for i in present_ids}
            assert expected.issubset(got), f"surviving PKs missing: {expected - got}"

    def _wash_restore_assert(
        self,
        name,
        string_pk=False,
        absent_ids=None,
        present_ids=None,
        expected_count=None,
    ):
        """The core reused flow: backup -> l0compact -> restore(_l0c) -> assert."""
        bk = self._backup(name)
        run_l0compact(bk, bk + "_l0c")
        w, restored = self._restore(bk + "_l0c", name, "_l0c")
        self._assert_pk_state(
            w,
            string_pk,
            absent_ids=absent_ids,
            present_ids=present_ids,
            expected_count=expected_count,
        )
        return bk, w, restored

    # ------------------------------------------------------------------
    # structural-meta helpers (case 8)
    # ------------------------------------------------------------------
    @staticmethod
    def _read_full_meta(backup_name):
        """Best-effort read of full_meta.json from the MinIO host volume.

        The get_backup HTTP API does not expose L0 segments, so we read the raw
        meta from disk. Layout may differ in CI, so skip (not fail) if absent.
        """
        path = os.path.join(
            REPO_ROOT,
            "deployment",
            "standalone",
            "volumes",
            "minio",
            "a-bucket",
            "backup",
            backup_name,
            "meta",
            "full_meta.json",
        )
        if not os.path.exists(path):
            pytest.skip(
                f"full_meta.json not found at {path}; MinIO volume layout differs in CI"
            )
        with open(path) as f:
            return json.load(f)

    @staticmethod
    def _meta_has_l0(meta):
        """True if the backup meta contains any L0 segment.

        L0 lives either as collection-level `l0_segments` or as an `is_l0`
        segment inside a partition's `segment_backups`.
        """
        for coll in meta.get("collection_backups", []):
            if coll.get("l0_segments"):
                return True
            for part in coll.get("partition_backups", []):
                for seg in part.get("segment_backups", []):
                    if seg.get("is_l0"):
                        return True
        return False

    # ==================================================================
    # 1. Int64 PK, partition-level L0
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_int64_pk_partition_level(self):
        coll_w, name = self._new_collection(extra_partitions=["part_a"])
        # _default holds [0, 1000), part_a holds [1000, 2000)
        self._insert_segment(coll_w, 0, 1000, False)
        self._insert_segment(coll_w, 1000, 1000, False, partition_name="part_a")
        self._index_and_load(coll_w)
        # delete a subset scoped to a single partition -> partition-level L0
        deleted = list(range(1000, 1100))
        self._delete_pks(coll_w, deleted, False, partition_name="part_a")
        self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=[1000, 1050, 1099],
            present_ids=[0, 999, 1100, 1999],
            expected_count=2000 - len(deleted),
        )

    # ==================================================================
    # 2. VarChar PK
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_varchar_pk(self):
        coll_w, name = self._new_collection(string_pk=True)
        self._insert_segment(coll_w, 0, 2000, True)
        self._index_and_load(coll_w)
        deleted = list(range(100, 200))
        self._delete_pks(coll_w, deleted, True)
        self._wash_restore_assert(
            name,
            string_pk=True,
            absent_ids=[100, 150, 199],
            present_ids=[0, 99, 200, 1999],
            expected_count=2000 - len(deleted),
        )

    # ==================================================================
    # 3. Collection-level L0 (cross-partition)
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_collection_level_cross_partition(self):
        coll_w, name = self._new_collection(extra_partitions=["p0", "p1"])
        self._insert_segment(coll_w, 0, 1000, False, partition_name="p0")
        self._insert_segment(coll_w, 1000, 1000, False, partition_name="p1")
        self._index_and_load(coll_w)
        # delete PKs spanning both partitions with partition_name=None
        deleted = list(range(500, 550)) + list(range(1500, 1550))
        self._delete_pks(coll_w, deleted, False, partition_name=None)
        self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=[500, 549, 1500, 1549],
            present_ids=[0, 999, 1000, 1999],
            expected_count=2000 - len(deleted),
        )

    # ==================================================================
    # 4. No-op deletes (existing + non-existent PKs, no over-deletion)
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_noop_deletes(self):
        coll_w, name = self._new_collection()
        self._insert_segment(coll_w, 0, 1000, False)
        self._index_and_load(coll_w)
        existing = list(range(100, 150))          # 50 real PKs
        non_existent = list(range(5000, 5050))    # never inserted
        self._delete_pks(coll_w, existing + non_existent, False)
        self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=[100, 149],
            present_ids=[0, 99, 150, 999],
            expected_count=1000 - len(existing),
        )

    # ==================================================================
    # 5. Multiple data segments (delete spread across sealed segments)
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_multiple_data_segments(self):
        coll_w, name = self._new_collection()
        # three sealed data segments with disjoint PK ranges
        self._insert_segment(coll_w, 0, 500, False)
        self._insert_segment(coll_w, 500, 500, False)
        self._insert_segment(coll_w, 1000, 500, False)
        self._index_and_load(coll_w)
        deleted = [10, 20, 510, 520, 1010, 1020]  # one per pair, across segments
        self._delete_pks(coll_w, deleted, False)
        self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=deleted,
            present_ids=[0, 499, 500, 999, 1000, 1499],
            expected_count=1500 - len(deleted),
        )

    # ==================================================================
    # 6. Idempotency (washing a backup that already has no L0)
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_idempotency(self):
        coll_w, name = self._new_collection()
        self._insert_segment(coll_w, 0, 1000, False)
        self._index_and_load(coll_w)
        deleted = list(range(100, 150))
        self._delete_pks(coll_w, deleted, False)
        # first wash
        bk, _, _ = self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=[100, 149],
            present_ids=[0, 99, 150, 999],
            expected_count=1000 - len(deleted),
        )
        # second wash on an already-washed backup (no L0 left) must still succeed
        run_l0compact(bk + "_l0c", bk + "_l0c2")
        w2, _ = self._restore(bk + "_l0c2", name, "_l0c2")
        self._assert_pk_state(
            w2,
            False,
            absent_ids=[100, 149],
            present_ids=[0, 99, 150, 999],
            expected_count=1000 - len(deleted),
        )

    # ==================================================================
    # 7. No-L0 negative control (clean backup must be untouched)
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_no_l0_negative_control(self):
        coll_w, name = self._new_collection()
        self._insert_segment(coll_w, 0, 1000, False)  # no deletes at all
        bk = self._backup(name)
        run_l0compact(bk, bk + "_l0c")
        _, restored = self._restore(bk + "_l0c", name, "_l0c")
        # a wash of a delete-free backup must not corrupt anything
        self.compare_collections(name, restored, verify_by_query=True)

    # ==================================================================
    # 8. Structural check: prove the transform in the backup meta JSON
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_structural_meta(self):
        coll_w, name = self._new_collection()
        self._insert_segment(coll_w, 0, 1000, False)
        self._index_and_load(coll_w)
        self._delete_pks(coll_w, list(range(100, 200)), False)
        bk = self._backup(name)
        run_l0compact(bk, bk + "_l0c")
        src_meta = self._read_full_meta(bk)
        washed_meta = self._read_full_meta(bk + "_l0c")
        assert self._meta_has_l0(src_meta), "source backup should contain L0 segments"
        assert not self._meta_has_l0(
            washed_meta
        ), "l0compacted backup must contain no L0 segments"

    # ==================================================================
    # 9. (soft) un-washed contrast: document the problem l0compact solves
    # ==================================================================
    @pytest.mark.tags(CaseLabel.L2)
    def test_l0compact_unwashed_contrast(self):
        coll_w, name = self._new_collection()
        self._insert_segment(coll_w, 0, 1000, False)
        self._index_and_load(coll_w)
        deleted = list(range(100, 150))
        self._delete_pks(coll_w, deleted, False)
        # washed restore is the correct baseline
        bk, _, _ = self._wash_restore_assert(
            name,
            string_pk=False,
            absent_ids=[100, 149],
            present_ids=[0, 99, 150, 999],
            expected_count=1000 - len(deleted),
        )
        # now try restoring the ORIGINAL (un-washed) backup. This may be rejected
        # by the l0ImportDisabled guard or may resurrect deleted rows depending on
        # the milvus version, so we only LOG the outcome (no hard assert).
        try:
            res = self.client.restore_backup(
                {
                    "async": False,
                    "backup_name": bk,
                    "collection_names": [name],
                    "collection_suffix": "_raw",
                }
            )
            log.info(f"un-washed restore response: {res}")
            raw = name + "_raw"
            list_res, _ = self.utility_wrap.list_collections()
            if raw in list_res:
                w = self._connect_existing(raw)
                self._index_and_load(w)
                q, _ = w.query(
                    self._pk_expr(deleted, False),
                    output_fields=[ct.default_int64_field_name],
                )
                log.info(
                    f"un-washed restore: {len(q)} of {len(deleted)} deleted PKs "
                    f"reappeared (this is the problem l0compact solves)"
                )
            else:
                log.info("un-washed restore did not create a collection (likely rejected)")
        except Exception as e:
            log.info(f"un-washed restore raised (likely l0ImportDisabled guard): {e}")
