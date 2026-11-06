"""Opt-in integration scaffold for an image-matched struct-evolution fixture.

Prepare a disposable synthetic source collection containing:
* element_nullable scalar and vector sub-fields, including NULL elements;
* nested Array<T> sub-fields, including empty and NULL inner arrays;
* flushed segments from before and after adding a nullable sub-field;
* a dropped sub-field whose old column still exists in a segment;
* a deleted trailing field ID retained by max_field_id.

Index/load the source, set MILVUS_BACKUP_STRUCT_EVOLUTION_COLLECTION to its
name, and configure the backup service with milvus.preserveFieldIDs=true.
Source construction remains with the image-matched schema-evolution fixture
until that Milvus API and its SDK release are available together.
"""

import os

import pytest

from base.client_base import TestcaseBase
from common import common_func as cf


_SOURCE = os.environ.get("MILVUS_BACKUP_STRUCT_EVOLUTION_COLLECTION")
pytestmark = pytest.mark.skipif(
    not _SOURCE,
    reason="requires a prepared struct-evolution collection and supporting Milvus image",
)


def _rows(client, collection):
    iterator = client.query_iterator(
        collection_name=collection, filter="", output_fields=["*"], batch_size=256
    )
    rows = []
    try:
        while True:
            batch = iterator.next()
            if not batch:
                break
            rows.extend(batch)
    finally:
        iterator.close()
    return sorted(rows, key=lambda row: row["id"])


class TestRestoreStructEvolution(TestcaseBase):
    @pytest.mark.parametrize(
        "backup_format,strategy",
        [("binlog", "merged_seg"), ("snapshot", "restore_snap"), ("snapshot", "import_snap")],
    )
    def test_round_trip(self, backup_format, strategy):
        self._connect()
        source_schema = self.milvus_client.describe_collection(_SOURCE)
        expected = _rows(self.milvus_client, _SOURCE)
        assert expected, "the prepared fixture must include pre-evolution and post-evolution rows"
        backup = cf.gen_unique_str("struct_evolution")
        suffix = "_" + strategy + "_" + backup[-8:]
        restored = _SOURCE + suffix
        try:
            response = self.client.create_backup({
                "backup_name": backup,
                "filter": {"default": {"colls": [_SOURCE]}},
                "format": backup_format,
                "async": True,
            })
            assert response["code"] == 0, response
            assert self.client.wait_create_backup_complete(backup, timeout=600)
            response = self.client.restore_backup({
                "backup_name": backup,
                "collection_suffix": suffix,
                "strategy": strategy,
                "restoreIndex": True,
                "async": True,
            })
            assert response["code"] == 0, response
            assert self.client.wait_restore_complete(response["data"]["id"], timeout=600)
            restored_schema = self.milvus_client.describe_collection(restored)
            assert restored_schema["fields"] == source_schema["fields"]
            if "struct_array_fields" in source_schema:
                assert restored_schema["struct_array_fields"] == source_schema["struct_array_fields"]
            self.milvus_client.load_collection(restored, timeout=600)
            assert _rows(self.milvus_client, restored) == expected
        finally:
            if self.milvus_client.has_collection(restored):
                self.milvus_client.drop_collection(restored)
            self.client.delete_backup(backup)
