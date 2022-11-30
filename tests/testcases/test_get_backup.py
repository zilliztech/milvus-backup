import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
prefix = "get_backup"
backup_prefix = "backup"
suffix = "_bak"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestGetBackup(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("backup_num", [1, 2, 3])
    def test_milvus_get_backup(self, backup_num, is_async):
        # prepare data
        names_origin = [cf.gen_unique_str(prefix)]
        back_up_names = [cf.gen_unique_str(backup_prefix) for i in range(backup_num)]
        for name in names_origin:
            self.prepare_data(name)
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_names}")

        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        for back_up_name in back_up_names:
            payload = {"async": is_async, "backup_name": back_up_name, "collection_names": names_origin}
            res = client.create_backup(payload)
            log.info(f"create backup response: {res}")
            if is_async:
                res = client.wait_create_backup_complete(back_up_name)
                assert res is True
        for back_up_name in back_up_names:
            backup = client.get_backup(back_up_name)
            assert backup["data"]["state_code"] == 2
            assert backup["data"]["name"] == back_up_name
            backup_collections = [backup["collection_name"]for backup in backup["data"]["collection_backups"]]
            assert set(names_origin).issubset(backup_collections)









