import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
prefix = "restore_backup"
backup_prefix = "backup"
suffix = "_bak"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestRestoreBackup(TestcaseBase):
    """ Test case of end to end"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nb", [0, 3000])
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_restore", [1, 2, 3])
    @pytest.mark.parametrize("collection_type", ["binary", "float", "all"])
    def test_milvus_restore_back(self, collection_type, collection_need_to_restore, is_async, nb):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, check_function=False)
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, check_function=False)
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, check_function=False)
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup

        names_need_backup = names_origin
        payload = {"async": False, "backup_name": back_up_name, "collection_names": names_need_backup}
        res = client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [backup["collection_name"]for backup in backup["data"]["collection_backups"]]
        restore_collections = backup_collections
        if collection_need_to_restore == "all":
            payload = {"async": False, "backup_name": back_up_name,
                       "collection_suffix": suffix}
        else:
            restore_collections = names_need_backup[:collection_need_to_restore]
            payload = {"async": False, "backup_name": back_up_name,
                       "collection_suffix": suffix, "collection_names": restore_collections}
        t0 = time.time()
        res = client.restore_backup(payload)
        restore_id = res["data"]["id"]
        log.info(f"restore_backup: {res}")
        if is_async:
            res = client.wait_restore_complete(restore_id)
            assert res is True
        t1 = time.time()
        log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        res, _ = self.utility_wrap.list_collections()
        for name in restore_collections:
            assert name + suffix in res
        for name in restore_collections:
            self.compare_collections(name, name+suffix)

