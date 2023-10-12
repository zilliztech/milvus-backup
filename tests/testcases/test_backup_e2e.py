import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient

c_name_prefix = "e2e_backup"
backup_prefix = "backup"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestE2e(TestcaseBase):
    """ Test case of end to end"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_backup_default(self):
        # prepare data
        name_origin = cf.gen_unique_str(c_name_prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        suffix = "_bak"
        log.info(f"name_origin:{name_origin}, back_up_name: {back_up_name}")
        self.prepare_data(name_origin)
        res, _ = self.utility_wrap.list_collections()
        log.info(f"list collection: {res}")
        # backup
        res = client.create_backup({"async": False, "backup_name": back_up_name, "collection_names": [name_origin]})
        log.info(f"create_backup {res}")
        res = client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [backup["collection_name"]for backup in backup["data"]["collection_backups"]]
        assert name_origin in backup_collections
        res = client.restore_backup({"async": False, "backup_name": back_up_name, "collection_names": [name_origin],
                                     "collection_suffix": suffix})
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        self.compare_collections(name_origin, name_origin + suffix)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup
