import pytest
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
    def test_secondary_restore_basic(self, is_async, nb):
        # prepare data
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
                check_function=False,
            )

        # create backup
        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        assert res["code"] == 200

        # restore secondary
        payload = {
            "async": is_async,
            "backup_name": back_up_name,
            "source_cluster_id": "backup-test-upstream",
            "target_cluster_id": "backup-test-downstream",
        }
        res = self.client.restore_secondary(payload)
        log.info(f"restore secondary response: {res}")
        assert res["code"] == 200

        if is_async:
            restore_id = res["data"]["id"]
            success = self.client.wait_restore_complete(restore_id)
            assert success

        # verify collections exist on upstream (same instance in API test)
        res, _ = self.utility_wrap.list_collections()
        for name in names_origin:
            assert name in res
