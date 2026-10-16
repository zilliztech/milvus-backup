import time

import pytest
from pymilvus import Collection, connections, utility

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log

prefix = "peers_restore"
backup_prefix = "backup"


class TestRestorePeers(TestcaseBase):
    """Restful restore against a replicating target, with and without the
    peers option. The dual-cluster environment turns the upstream into a
    replication source: a plain restore must be refused there, a peers restore
    must commit two-phase, and the restored data must converge on the
    downstream peer through replication."""

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

    def _wait_peer_converged(self, uri, collection_name, timeout=300):
        # the replicated commit lands on the peer asynchronously, so poll until
        # the mirrored collection is there and holds rows
        connections.connect("peers_downstream", uri=uri, token="root:Milvus")
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    if utility.has_collection(collection_name, using="peers_downstream"):
                        entities = Collection(
                            collection_name, using="peers_downstream"
                        ).num_entities
                        if entities > 0:
                            log.info(
                                f"collection {collection_name} converged on the "
                                f"peer with {entities} entities"
                            )
                            return True
                except Exception as e:
                    log.debug(f"peer {collection_name} not ready yet: {e}")
                time.sleep(10)
            return False
        finally:
            connections.disconnect("peers_downstream")

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_peers_commits_two_phase(self, secondary_milvus_uri):
        name, backup_name = self._create_binlog_backup(nb=2000)
        suffixed = name + "_peers"
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_peers",
            "peers": [secondary_milvus_uri],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with peers response: {res}")
        assert res.get("code", 0) == 0, res.get("msg")

        res, _ = self.utility_wrap.list_collections()
        assert suffixed in res
        assert Collection(suffixed).num_entities == 2000

        assert self._wait_peer_converged(secondary_milvus_uri, suffixed), (
            f"collection {suffixed} did not converge on the downstream peer"
        )

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_without_peers_refused_on_replicating_target(self):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_no_peers",
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore without peers response: {res}")
        assert res.get("code", 0) != 0
        assert "peers" in res.get("msg", "")
        # the refusal is up front, before any ddl runs
        res, _ = self.utility_wrap.list_collections()
        assert name + "_no_peers" not in res

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_invalid_peer_rejected(self):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_bad_peer",
            "peers": ["127.0.0.1:19500"],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with invalid peer response: {res}")
        assert res.get("code", 0) != 0
        assert "invalid peer" in res.get("msg", "")
        # rejected while validating the request, before any job or ddl
        res, _ = self.utility_wrap.list_collections()
        assert name + "_bad_peer" not in res

    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_restore_with_per_seg_and_peers_refused(self, secondary_milvus_uri):
        name, backup_name = self._create_binlog_backup(nb=1000)
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": "_per_seg",
            "strategy": "per_seg",
            "peers": [secondary_milvus_uri],
        }
        res = self.client.restore_backup(payload)
        log.info(f"restore with per_seg and peers response: {res}")
        assert res.get("code", 0) != 0
        assert "per_seg" in res.get("msg", "")
        res, _ = self.utility_wrap.list_collections()
        assert name + "_per_seg" not in res
