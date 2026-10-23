import time

import pytest
from pymilvus import Collection, connections, utility

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log

prefix = "secondaries_restore"
backup_prefix = "backup"


class TestRestoreSecondaries(TestcaseBase):
    """Restful restore against a replicating target, with and without the
    secondaries option. The dual-cluster environment turns the upstream into a
    replication source: a plain restore must be refused there, a secondaries
    restore must commit two-phase, and the restored data must converge on the
    downstream secondary through replication."""

    def _create_binlog_backup(self, nb):
        # master reports the snapshot feature, so pin the binlog format the
        # same way the cli secondary job does
        self._connect()
        name = cf.gen_unique_str(prefix)
        self.prepare_data(
            name, nb=nb, is_binary=False, auto_id=False, check_function=True
        )
        backup_name = cf.gen_unique_str(backup_prefix)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_names": [name],
            "format": "binlog",
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        assert res.get("code", 0) == 0
        return name, backup_name

    def _wait_secondary_converged(self, uri, collection_name, timeout=300):
        # the replicated commit lands on the secondary asynchronously, so poll
        # until the mirrored collection is there and holds rows
        connections.connect("secondaries_downstream", uri=uri, token="root:Milvus")
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    if utility.has_collection(
                        collection_name, using="secondaries_downstream"
                    ):
                        entities = Collection(
                            collection_name, using="secondaries_downstream"
                        ).num_entities
                        if entities > 0:
                            log.info(
                                f"collection {collection_name} converged on the "
                                f"secondary with {entities} entities"
                            )
                            return True
                except Exception as e:
                    log.debug(f"secondary {collection_name} not ready yet: {e}")
                time.sleep(10)
            return False
        finally:
            connections.disconnect("secondaries_downstream")

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_secondaries_commits_two_phase(self, secondary_milvus_uri):
        name, backup_name = self._create_binlog_backup(nb=2000)
        suffixed = name + "_secondaries"
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_secondaries",
            "secondaries": [secondary_milvus_uri],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with secondaries response: {res}")
        assert res.get("code", 0) == 0, res.get("msg")

        res, _ = self.utility_wrap.list_collections()
        assert suffixed in res
        assert Collection(suffixed).num_entities == 2000

        assert self._wait_secondary_converged(secondary_milvus_uri, suffixed), (
            f"collection {suffixed} did not converge on the downstream secondary"
        )

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_without_secondaries_refused_on_replicating_target(self):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_no_secondaries",
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore without secondaries response: {res}")
        assert res.get("code", 0) != 0
        assert "secondaries" in res.get("msg", "")
        # the refusal is up front, before any ddl runs
        res, _ = self.utility_wrap.list_collections()
        assert name + "_no_secondaries" not in res

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_invalid_secondary_rejected(self):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_bad_secondary",
            "secondaries": ["127.0.0.1:19500"],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with invalid secondary response: {res}")
        assert res.get("code", 0) != 0
        assert "invalid secondary" in res.get("msg", "")
        # rejected while validating the request, before any job or ddl
        res, _ = self.utility_wrap.list_collections()
        assert name + "_bad_secondary" not in res

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_per_seg_and_secondaries_refused(self, secondary_milvus_uri):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_per_seg",
            "strategy": "per_seg",
            "secondaries": [secondary_milvus_uri],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with per_seg and secondaries response: {res}")
        assert res.get("code", 0) != 0
        assert "--secondaries" in res.get("msg", "")
        res, _ = self.utility_wrap.list_collections()
        assert name + "_per_seg" not in res
