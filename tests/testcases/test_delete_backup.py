import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
prefix = "delete_backup"
backup_prefix = "backup"
suffix = "_bak"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestDeleteBackup(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("backup_num", [1, 2, 3])
    def test_milvus_delete_backup(self, backup_num, is_async):
        # prepare data
        names_origin = [cf.gen_unique_str(prefix)]
        for name in names_origin:
            self.prepare_data(name)
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup
        back_up_names = []
        for i in range(backup_num):
            back_up_name = cf.gen_unique_str(backup_prefix)
            log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
            back_up_names.append(back_up_name)
            names_need_backup = names_origin
            payload = {"async": is_async, "backup_name": back_up_name, "collection_names": names_need_backup}
            res = client.create_backup(payload)
            log.info(f"create backup response: {res}")
            if is_async:
                res = client.wait_create_backup_complete(back_up_name)
                assert res is True

        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert set(back_up_names).issubset(set(all_backup))
        # delete backup
        for back_up_name in back_up_names:
            res = client.delete_backup(back_up_name)
            assert res["msg"] == "success"
            time.sleep(1)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        for back_up_name in back_up_names:
            assert back_up_name not in all_backup

