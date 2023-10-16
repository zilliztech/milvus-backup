from time import sleep
import pytest
import time
from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
from checker import BackupCreateChecker, BackupRestoreChecker

c_name_prefix = "perf_backup"
backup_prefix = "backup"
client = MilvusBackupClient("http://localhost:8080/api/v1")


@pytest.mark.tags(CaseLabel.Perf)
class TestPerf(TestcaseBase):
    """ Test case of performance"""

    prepare_data_done = False

    def setup_perf(self, nb=1000):
        log.info(f"*****************Test Perf Setup With nb {nb}*****************")
        if self.prepare_data_done:
            log.info(f"*****************Test Perf Setup With nb {nb} Done, Skip*****************")
            return
        else:
            log.info(f"*****************Test Perf Setup With nb {nb} Start*****************")
        total_nb = nb
        cnt = 10
        coll_num = 1
        collections_to_backup = []
        for i in range(coll_num):
            collection_to_backup = cf.gen_unique_str(c_name_prefix)
            for j in range(cnt):
                self.prepare_data(collection_to_backup, nb=total_nb // cnt)
            collections_to_backup.append(collection_to_backup)
        backup_name = cf.gen_unique_str(backup_prefix)
        client.create_backup({"async": False, "backup_name": backup_name, "collection_names": collections_to_backup})
        self.collections_to_backup = collections_to_backup
        self.backup_name = backup_name
        self.prepare_data_done = True

    def backup_perf(self):
        log.info("*****************Test Backup Perf Start*****************")
        t0 = time.perf_counter()
        res, result = BackupCreateChecker(self.collections_to_backup).run_task()
        t1 = time.perf_counter()
        log.info(f"create backup time: {t1 - t0} with {res}, {result}")
        return res, result

    def restore_perf(self):
        log.info("*****************Test Restore Perf Start*****************")
        t0 = time.perf_counter()
        res, result= BackupRestoreChecker(self.backup_name, "_bak", self.collections_to_backup).run_task()
        t1 = time.perf_counter()
        log.info(f"create backup time: {t1 - t0} with {res}, {result}")
        return res, result

    @pytest.mark.parametrize("nb", [100000])
    def test_milvus_create_backup_perf(self, benchmark, nb):
        self.setup_perf(nb=nb)
        res, result = benchmark.pedantic(self.backup_perf, iterations=1, rounds=5)
        assert result is True

    @pytest.mark.parametrize("nb", [100000])
    def test_milvus_restore_backup_perf(self, benchmark, nb):
        self.setup_perf(nb=nb)
        res, result = benchmark.pedantic(self.restore_perf, setup=self.setup_perf, iterations=1, rounds=5)
        assert result is True
