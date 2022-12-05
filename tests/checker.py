from enum import Enum
import functools
import time
from utils.util_log import test_log as log
from datetime import datetime
from common import common_func as cf
import threading
from api.milvus_backup import MilvusBackupClient

client = MilvusBackupClient("http://localhost:8080/api/v1")


class Op(Enum):
    create = 'create'
    restore = 'restore'


def start_monitor_threads(checkers={}):
    """start monitor threads"""
    for k, ch in checkers.items():
        ch._keep_running = True
        t = threading.Thread(target=ch.keep_running, name=k, daemon=True)
        t.start()


DEFAULT_FMT = '[start time:{start_time}][time cost:{elapsed:0.8f}s][operation_name:{operation_name}] -> {result!r}'


def trace(fmt=DEFAULT_FMT, prefix='chaos-test', flag=True):
    def decorate(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            start_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            t0 = time.perf_counter()
            res, result = func(self, *args, **kwargs)
            elapsed = time.perf_counter() - t0
            end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            if flag:
                operation_name = func.__name__
                log_str = f"[{prefix}]" + fmt.format(**locals())
                # TODO: add report function in this place, like uploading to influxdb
                # it is better a async way to do this, in case of blocking the request processing
                log.info(log_str)
            if result:
                self.rsp_times.append(elapsed)
                self.average_time = (elapsed + self.average_time * self._succ) / (self._succ + 1)
                self._succ += 1
            else:
                self._fail += 1
            return res, result

        return inner_wrapper

    return decorate


def exception_handler():
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            try:
                res, result = func(self, *args, **kwargs)
                return res, result
            except Exception as e:
                log_row_length = 300
                e_str = str(e)
                log_e = e_str[0:log_row_length] + \
                        '......' if len(e_str) > log_row_length else e_str
                log.error(log_e)
                return e, False

        return inner_wrapper

    return wrapper


class Checker:
    """
    count operations and success rate and average time
    """

    def __init__(self):
        self._succ = 0
        self._fail = 0
        self._keep_running = True
        self.rsp_times = []
        self.average_time = 0

    def total(self):
        return self._succ + self._fail

    def succ_rate(self):
        return self._succ / self.total() if self.total() != 0 else 0

    def check_result(self):
        succ_rate = self.succ_rate()
        total = self.total()
        rsp_times = self.rsp_times
        average_time = 0 if len(rsp_times) == 0 else sum(
            rsp_times) / len(rsp_times)
        max_time = 0 if len(rsp_times) == 0 else max(rsp_times)
        min_time = 0 if len(rsp_times) == 0 else min(rsp_times)
        checker_name = self.__class__.__name__
        checkers_result = f"{checker_name}, succ_rate: {succ_rate:.2f}, total: {total:03d}, average_time: {average_time:.4f}, max_time: {max_time:.4f}, min_time: {min_time:.4f}"
        log.info(checkers_result)
        log.info(f"{checker_name} rsp times: {self.rsp_times}")
        return checkers_result

    def terminate(self):
        self._keep_running = False
        self.reset()

    def reset(self):
        self._succ = 0
        self._fail = 0
        self.rsp_times = []
        self.average_time = 0


class BackupCreateChecker(Checker):
    """
    A checker to check whether backup create is successful
    """

    def __init__(self, collections_to_backup):
        super().__init__()
        self.collections_to_backup = collections_to_backup

    @trace()
    def create(self, backup_name, collections_to_backup):
        res = client.create_backup(
            {"async": False, "backup_name": backup_name, "collection_names": collections_to_backup})
        return res, res["msg"] == "success"

    @exception_handler()
    def run_task(self):
        res, result = self.create(cf.gen_unique_str("backup"), self.collections_to_backup)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()


class BackupRestoreChecker(Checker):
    """
    A checker to check whether backup restore is successful
    """

    def __init__(self, backup_name, suffix, collections_to_restore):
        super().__init__()
        self.backup_name = backup_name
        self.suffix = suffix
        self.collections_to_restore = collections_to_restore

    @trace()
    def restore(self, backup_name, suffix, collections_to_restore):
        res = client.restore_backup({"async": False, "backup_name": backup_name, "collection_suffix": suffix,
                                     "collection_names": collections_to_restore})
        time.sleep(20)
        log.info(res)
        return res, res['msg'] == "success"

    @exception_handler()
    def run_task(self):
        res, result = self.restore(self.backup_name, cf.gen_unique_str(self.suffix), self.collections_to_restore)
        return res, result

    def keep_running(self):
        while self._keep_running:
            self.run_task()
