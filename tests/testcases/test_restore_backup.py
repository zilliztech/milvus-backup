import random
import time
import pytest
import json
import numpy as np
import jax.numpy as jnp
import random
from collections import defaultdict
from pymilvus import db, list_collections, Collection, DataType
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from api.milvus_backup import MilvusBackupClient
from faker import Faker
fake_en = Faker("en_US")
prefix = "restore_backup"
backup_prefix = "backup"
suffix = "_bak"

client = MilvusBackupClient("http://localhost:8080/api/v1")


class TestRestoreBackup(TestcaseBase):
    """ Test case of end to end"""

    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True, False])
    @pytest.mark.parametrize("enable_partition", [False])
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_restore", [1, 2, 3])
    @pytest.mark.parametrize("collection_type", ["binary", "float", "all"])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back(self, collection_type, collection_need_to_restore, is_async, is_auto_id, enable_partition, nb):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
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
            self.compare_collections(name, name+suffix, verify_by_query=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True])
    @pytest.mark.parametrize("enable_partition", [True])
    @pytest.mark.parametrize("is_async", [True])
    @pytest.mark.parametrize("collection_need_to_restore", [3])
    @pytest.mark.parametrize("collection_type", ["all"])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_multi_partition(self, collection_type, collection_need_to_restore, is_async, is_auto_id, enable_partition, nb):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(names_origin[-1], nb=nb, is_binary=is_binary, auto_id=is_auto_id, check_function=False, enable_partition=enable_partition)
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

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_db_support(self):
        # prepare data
        self._connect()
        names_origin = []
        for i in range(2):
            db_name = cf.gen_unique_str("db")
            db.create_database(db_name)
            db.using_database(db_name)
            collection_name = cf.gen_unique_str(prefix)
            self.prepare_data(name=collection_name, db_name=db_name, nb=3000, is_binary=False, auto_id=True, check_function=False)
            assert collection_name in self.utility_wrap.list_collections()[0]
            names_origin.append(f"{db_name}.{collection_name}")
        log.info(f"name_origin:{names_origin}")
        # create backup
        back_up_name = cf.gen_unique_str(backup_prefix)
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_origin,
        }
        log.info(f"payload: {payload}")
        res = client.create_backup(payload)
        log.info(f"create backup response: {res}")
        res = client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        payload = {"async": False, "backup_name": back_up_name,
                   # "collection_names": names_origin,
                   "collection_suffix": suffix}
        log.info(f"restore payload: {payload}")
        res = client.restore_backup(payload)
        log.info(f"restore_backup: {res}")
        for name in names_origin:
            db_name = name.split(".")[0]
            collection_name = name.split(".")[1]
            db.using_database(db_name)

            res, _ = self.utility_wrap.list_collections()
            log.info(f"collection list in db {db_name}: {res}")
            assert collection_name + suffix in res
            self.compare_collections(collection_name, collection_name + suffix)

    @pytest.mark.parametrize("include_partition_key", [True, False])
    @pytest.mark.parametrize("include_dynamic", [True, False])
    @pytest.mark.parametrize("include_json", [True, False])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_new_feature_support(self, include_json, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        if include_json:
            fields = [cf.gen_int64_field(name="int64", is_primary=True),
                      cf.gen_int64_field(name="key"),
                      cf.gen_json_field(name="json"),
                      cf.gen_float_vec_field(name="float_vector", dim=128),
                      ]
        else:
            fields = [cf.gen_int64_field(name="int64", is_primary=True),
                      cf.gen_int64_field(name="key"),
                      cf.gen_float_vec_field(name="float_vector", dim=128),
                      ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        if include_json:
            data = [
                [i for i in range(nb)],
                [i % 3 for i in range(nb)],
                [{f"key_{str(i)}": i} for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        else:
            data = [
                [i for i in range(nb)],
                [i % 3 for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        collection_w.insert(data=data)

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
        output_fields = None
        if not include_json:
            output_fields = [ct.default_int64_field_name]
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.parametrize("include_json", [True])
    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_multi_json_datatype(self, include_json, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        if include_json:
            fields = [cf.gen_int64_field(name="int64", is_primary=True),
                      cf.gen_int64_field(name="key"),
                      cf.gen_json_field(name="json"),
                      cf.gen_float_vec_field(name="float_vector", dim=128),
                      ]
        else:
            fields = [cf.gen_int64_field(name="int64", is_primary=True),
                      cf.gen_int64_field(name="key"),
                      cf.gen_float_vec_field(name="float_vector", dim=128),
                      ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        json_value = [
                    1,
                    1.0,
                    "1",
                    [1, 2, 3],
                    ["1", "2", "3"],
                    [1, 2, "3"],
                    {"key": "value"},
                ]
        if include_json:
            data = [
                [i for i in range(nb)],
                [i % 3 for i in range(nb)],
                [json_value[i%len(json_value)] for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        else:
            data = [
                [i for i in range(nb)],
                [i % 3 for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        collection_w.insert(data=data)

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
        output_fields = None
        if not include_json:
            output_fields = [ct.default_int64_field_name]
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup


    @pytest.mark.parametrize("drop_db", [True, False])
    @pytest.mark.parametrize("str_json", [True, False])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_with_db_collections(self, drop_db, str_json):
        # prepare data
        self._connect()
        names_origin = []
        db_collections = defaultdict(list)
        for i in range(2):
            db_name = cf.gen_unique_str("db")
            db.create_database(db_name)
            db.using_database(db_name)
            for j in range(2):
                collection_name = cf.gen_unique_str(prefix)
                self.prepare_data(name=collection_name, db_name=db_name, nb=3000, is_binary=False, auto_id=True)
                assert collection_name in self.utility_wrap.list_collections()[0]
                names_origin.append(f"{db_name}.{collection_name}")
                db_collections[db_name].append(collection_name)
        db_collections = dict(db_collections)
        log.info(f"db_collections:{db_collections}")
        log.info(f"name_origin:{names_origin}")
        # create backup
        back_up_name = cf.gen_unique_str(backup_prefix)
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "db_collections": json.dumps(db_collections) if str_json else db_collections,
        }
        log.info(f"payload: {payload}")
        res = client.create_backup(payload)
        log.info(f"create backup response: {res}")
        res = client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        if drop_db:
            # delete db to check that restore can create db if not exist
            for db_name in db_collections:
                db.using_database(db_name)
                all_collections = list_collections()
                for c in all_collections:
                    collection = Collection(name=c)
                    collection.drop()
                db.drop_database(db_name)
        payload = {"async": False, "backup_name": back_up_name,
                   "db_collections": db_collections,
                   "collection_suffix": suffix}
        log.info(f"restore payload: {payload}")
        res = client.restore_backup(payload)
        log.info(f"restore_backup: {res}")
        for name in names_origin:
            db_name = name.split(".")[0]
            collection_name = name.split(".")[1]
            db.using_database(db_name)
            res, _ = self.utility_wrap.list_collections()
            log.info(f"collection list in db {db_name}: {res}")
            assert collection_name + suffix in res
            if not drop_db:
                self.compare_collections(collection_name, collection_name + suffix)

    @pytest.mark.parametrize("include_partition_key", [True, False])
    @pytest.mark.parametrize("include_dynamic", [True, False])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_array_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        collection_w.insert(data=data)
        if include_dynamic:
            data = [
                {
                    "int64": i,
                    "key": i % 3,
                    "json": {f"key_{str(i)}": i},
                    "var_array": [str(x) for x in range(10)],
                    "int_array": [int(x) for x in range(10)],
                    "float_vector": [np.float32(i) for i in range(128)],
                    f"dynamic_{str(i)}": i
                } for i in range(nb, nb*2)
            ]
            collection_w.insert(data=data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup


    @pytest.mark.parametrize("include_partition_key", [True, False])
    @pytest.mark.parametrize("include_dynamic", [True, False])
    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_multi_vector_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector_0", dim=128),
                    cf.gen_float_vec_field(name="float_vector_1", dim=128),
                    cf.gen_float_vec_field(name="float_vector_2", dim=128),
                    ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)]
        ]
        collection_w.insert(data=data)
        if include_dynamic:
            data = [
                {
                    "int64": i,
                    "key": i % 3,
                    "json": {f"key_{str(i)}": i},
                    "var_array": [str(x) for x in range(10)],
                    "int_array": [int(x) for x in range(10)],
                    "float_vector_0": [np.float32(i) for i in range(128)],
                    "float_vector_1": [np.float32(i) for i in range(128)],
                    "float_vector_2": [np.float32(i) for i in range(128)],
                    f"dynamic_{str(i)}": i
                } for i in range(nb, nb*2)
            ]
            collection_w.insert(data=data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup


    @pytest.mark.parametrize("include_partition_key", [True, False])
    @pytest.mark.parametrize("include_dynamic", [True, False])
    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_f16_bf16_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    cf.gen_float16_vec_field(name="float16_vector", dim=128),
                    cf.gen_brain_float16_vec_field(name="brain_float16_vector", dim=128),
                    ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [np.array([random.random() for _ in range(128)], dtype=np.float16) for _ in range(nb)],
            [np.array(jnp.array([random.random() for _ in range(128)], dtype=jnp.bfloat16))for _ in range(nb)]
        ]
        collection_w.insert(data=data)
        if include_dynamic:
            data = [
                {
                    "int64": i,
                    "key": i % 3,
                    "json": {f"key_{str(i)}": i},
                    "var_array": [str(x) for x in range(10)],
                    "int_array": [int(x) for x in range(10)],
                    "float_vector": [np.float32(i) for i in range(128)],
                    "float16_vector": np.array([random.random() for _ in range(128)], dtype=np.float16),
                    "brain_float16_vector": np.array(jnp.array([random.random() for _ in range(128)], dtype=jnp.bfloat16)),
                    f"dynamic_{str(i)}": i
                } for i in range(nb, nb*2)
            ]
            collection_w.insert(data=data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.parametrize("enable_text_match", [True])
    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_sparse_vector_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_string_field(name="text", enable_match=True),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    cf.gen_sparse_vec_field(name="sparse_vector"),
                    ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic,
                                                      partition_key_field=partition_key)
        else:
            default_schema = cf.gen_collection_schema(fields,
                                                      enable_dynamic_field=include_dynamic)

        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        rng = np.random.default_rng()
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [fake_en.text() for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [{
            d: rng.random() for d in random.sample(range(1000), random.randint(20, 30))
        } for _ in range(nb)],
        ]
        collection_w.insert(data=data)
        if include_dynamic:
            data = [
                {
                    "int64": i,
                    "key": i % 3,
                    "json": {f"key_{str(i)}": i},
                    "var_array": [str(x) for x in range(10)],
                    "int_array": [int(x) for x in range(10)],
                    "float_vector": [np.float32(i) for i in range(128)],
                    "sparse_vector": {
            d: rng.random() for d in random.sample(range(1000), random.randint(20, 30))
        },
                    f"dynamic_{str(i)}": i
                } for i in range(nb, nb*2)
            ]
            collection_w.insert(data=data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields)
        res = client.delete_backup(back_up_name)
        res = client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_with_delete(self):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    ]
        default_schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        pk = res.primary_keys
        # delete first 100 rows
        delete_ids = pk[:100]
        collection_w.delete(expr=f"int64 in {delete_ids}")
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields, verify_by_query=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_with_upsert(self):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    ]
        default_schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        # upsert first 100 rows by pk
        upsert_data = [
            [i for i in range(100)],
            [i % 3 for i in range(100, 200)],
            [{f"key_{str(i)}": i} for i in range(100, 200)],
            [[str(x) for x in range(10, 20)] for _ in range(100)],
            [[int(x) for x in range(10)] for _ in range(100)],
            [[np.float32(i) for i in range(128, 128*2)] for _ in range(100)],
        ]
        res, result = collection_w.upsert(data=upsert_data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields, verify_by_query=True)

    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_dup_pk(self):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                    cf.gen_int64_field(name="key"),
                    cf.gen_json_field(name="json"),
                    cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                    cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                    cf.gen_float_vec_field(name="float_vector", dim=128),
                    ]
        default_schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(name=name_origin, schema=default_schema, active_trace=True)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb, nb*2)],
            [{f"key_{str(i)}": i} for i in range(nb, nb*2)],
            [[str(x) for x in range(10)] for i in range(nb, nb*2)],
            [[int(x) for x in range(10)] for i in range(nb, nb*2)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
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
        output_fields = None
        self.compare_collections(name_origin, name_origin + suffix, output_fields=output_fields, verify_by_query=True)
