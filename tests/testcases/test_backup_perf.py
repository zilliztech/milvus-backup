from time import sleep
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
from checker import Op, BackupCreateChecker, BackupRestoreChecker, start_monitor_threads

c_name_prefix = "perf_backup"
backup_prefix = "backup"
client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestPerf(TestcaseBase):
    """ Test case of performance"""

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_create_backup_perf(self):
        # prepare data
        total_nb = 1000000
        cnt = 10
        collection_to_backup = cf.gen_unique_str(c_name_prefix)
        for i in range(cnt):
            self.prepare_data(collection_to_backup, nb=total_nb // cnt)
        collections_to_backup = [collection_to_backup]
        checkers = {
            Op.create: BackupCreateChecker(collections_to_backup)
        }
        start_monitor_threads(checkers)
        log.info("*********************Perf Test Start**********************")
        sleep(360)
        for k, v in checkers.items():
            v.check_result()
        for k, v in checkers.items():
            v.terminate()
        sleep(10)
        log.info("*********************Perf Test End**********************")

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_restore_backup_perf(self):
        # prepare data
        total_nb = 1000000
        cnt = 10
        collection_to_backup = cf.gen_unique_str(c_name_prefix)
        for i in range(cnt):
            self.prepare_data(collection_to_backup, nb=total_nb // cnt)
        collections_to_backup = [collection_to_backup]
        backup_name = cf.gen_unique_str(backup_prefix)
        suffix = "_bak"

        client.create_backup({"async": False, "backup_name": backup_name, "collection_names": collections_to_backup})
        checkers = {
            Op.restore: BackupRestoreChecker(backup_name, suffix, collections_to_backup)
        }
        start_monitor_threads(checkers)
        log.info("*********************Perf Test Start**********************")
        sleep(360)
        for k, v in checkers.items():
            v.check_result()
        for k, v in checkers.items():
            v.terminate()
        sleep(10)
        log.info("*********************Perf Test End**********************")
