import pytest
from pymilvus import MilvusClient
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient

prefix = "secondary_restore"
backup_prefix = "backup"


class TestRestoreSecondary(TestcaseBase):
    """Test case for secondary restore"""

    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_async", [False])
    @pytest.mark.tags(CaseLabel.SECONDARY)
    def test_secondary_restore_basic(
        self, is_async, nb, backup_uri_secondary, secondary_milvus_uri
    ):
        # prepare data with index and load so the backup captures index extra
        # and the restore replicates the load state to the downstream
        self._connect()
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        for is_binary in [False, False]:
            names_origin.append(cf.gen_unique_str(prefix))
            self.prepare_data(
                names_origin[-1],
                nb=nb,
                is_binary=is_binary,
                auto_id=False,
                check_function=True,
            )

        # create backup on the upstream via the upstream backup server
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_origin,
            "with_index_extra": True,
            "format": "binlog",
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        assert res.get("code", 0) == 0

        # the backup must carry the etcd index extra attributes; without them
        # the secondary restore rebuilds the index with field_id=0 and empty
        # user_index_params, breaking the downstream index state
        backup = self.client.get_backup(back_up_name)
        for coll in backup["data"]["collection_backups"]:
            assert len(coll["index_infos"]) > 0, (
                f"collection {coll['collection_name']} has no index info"
            )
            for index in coll["index_infos"]:
                assert index["field_id"] != 0
                assert len(index["user_index_params"]) > 0

        # restore secondary into the downstream via the downstream backup server
        downstream = MilvusBackupClient(f"{backup_uri_secondary}/api/v1")
        payload = {
            "async": is_async,
            "backup_name": back_up_name,
            "sourceClusterID": "backup-test-upstream",
            "targetClusterID": "backup-test-downstream",
        }
        res = downstream.restore_secondary(payload)
        log.info(f"restore secondary response: {res}")
        assert res.get("code", 0) == 0

        if is_async:
            restore_id = res["data"]["id"]
            success = downstream.wait_restore_complete(restore_id)
            assert success

        # verify collections still exist on upstream
        res, _ = self.utility_wrap.list_collections()
        for name in names_origin:
            assert name in res

        # verify the collections and data were actually restored on downstream
        down_client = MilvusClient(uri=secondary_milvus_uri, token="root:Milvus")
        down_collections = down_client.list_collections()
        for name in names_origin:
            assert name in down_collections
        for name in names_origin:
            count = down_client.query(
                collection_name=name, output_fields=["count(*)"], limit=1
            )
            assert count[0]["count(*)"] > 0
