import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log

prefix = "restore_backup"
backup_prefix = "backup"
suffix = "_bak"


class TestGetRestore(TestcaseBase):
    """Test case of end to end"""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("restore_num", [1, 2, 3])
    def test_milvus_get_restore(self, restore_num, is_async):
        # prepare data
        back_up_name = cf.gen_unique_str(backup_prefix)
        names_origin = [cf.gen_unique_str(prefix)]
        for name in names_origin:
            self.prepare_data(name, check_function=False)
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup
        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        restore_ids = []
        for i in range(restore_num):
            restore_collections = names_need_backup
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix + str(i),
                "collection_names": restore_collections,
            }
            t0 = time.time()
            res = self.client.restore_backup(payload)
            log.info(f"restore backup response: {res}")
            restore_id = res["data"]["id"]
            restore_ids.append(restore_id)
            log.info(f"restore_backup: {res}")
            if is_async:
                res = self.client.wait_restore_complete(restore_id)
                assert res is True
            t1 = time.time()
            log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        for id in restore_ids:
            restore = self.client.get_restore(id)
            assert restore["data"]["state_code"] == 2
