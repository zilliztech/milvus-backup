import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
prefix = "create_backup"
backup_prefix = "backup"
suffix = "_bak"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestCreateBackup(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_backup", [1, 2, 3])
    @pytest.mark.parametrize("collection_type", ["binary", "float", "all"])
    def test_milvus_create_backup(self, collection_type, collection_need_to_backup, is_async):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], is_binary=is_binary)
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], is_binary=is_binary)
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], is_binary=is_binary)
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup
        names_to_backup = []
        if collection_need_to_backup == "all":
            names_to_backup = names_origin
            payload = {"async": is_async, "backup_name": back_up_name}
        else:
            names_need_backup = names_origin[:collection_need_to_backup]
            payload = {"async": is_async, "backup_name": back_up_name, "collection_names": names_need_backup}
        res = client.create_backup(payload)
        log.info(f"create backup response: {res}")
        if is_async:
            res = client.wait_create_backup_complete(back_up_name)
            assert res is True
        res = client.list_backup()
        log.info(f"list backup response: {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [backup["collection_name"]for backup in backup["data"]["collection_backups"]]
        if isinstance(collection_need_to_backup, int):
            assert len(backup_collections) == collection_need_to_backup
        assert set(names_to_backup).issubset(backup_collections)









