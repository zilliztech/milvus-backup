import time
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
prefix = "backup"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestE2e(TestcaseBase):
    """ Test case of end to end"""
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_backup_default(self):
        # create
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(prefix)
        self.prepare_data(name_origin)
        res, _ = self.utility_wrap.list_collections()
        res = [r for r in res if prefix in r]
        log.info(f"all collections: {res}")
        # backup
        # list backup
        res = client.list_backup()
        log.info(f"list backup {res}")
        backup_list = [r["name"] for r in res["backup_infos"]]
        res = client.create_backup({"backup_name": back_up_name, "collection_names": [name_origin]})
        log.info(f"create backup {res}")
        res = client.list_backup()
        log.info(f"list backup {res}")









