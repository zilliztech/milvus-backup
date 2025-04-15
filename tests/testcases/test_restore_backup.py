import time
import pytest
import json
import numpy as np
import jax.numpy as jnp
import random
from collections import defaultdict
from pymilvus import db, list_collections, Collection, DataType, Function, FunctionType
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log
from utils.util_common import analyze_documents
from utils.util_pymilvus import create_index_for_vector_fields, create_json_path_index_for_json_fields
from api.milvus_backup import MilvusBackupClient
from faker import Faker

fake_en = Faker("en_US")
prefix = "restore_backup"
backup_prefix = "backup"
suffix = "_bak"


class TestRestoreBackup(TestcaseBase):
    """Test case of end to end"""

    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True])
    @pytest.mark.parametrize("enable_partition", [False])
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_restore", [3])
    @pytest.mark.parametrize("collection_type", ["all"])
    @pytest.mark.parametrize("use_v2_restore", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back(
        self,
        use_v2_restore,
        collection_type,
        collection_need_to_restore,
        is_async,
        is_auto_id,
        enable_partition,
        nb,
        nullable
    ):

        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                    nullable=nullable
                )
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                    nullable=nullable
                )
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                )
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup
        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        restore_collections = backup_collections
        if collection_need_to_restore == "all":
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
            }
        else:
            restore_collections = names_need_backup[:collection_need_to_restore]
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
                "collection_names": restore_collections,
            }
        payload["useV2Restore"] = use_v2_restore
        t0 = time.time()
        res = self.client.restore_backup(payload)
        restore_id = res["data"]["id"]
        log.info(f"restore_backup: {res}")
        if is_async:
            res = self.client.wait_restore_complete(restore_id)
            assert res is True
        t1 = time.time()
        log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        res, _ = self.utility_wrap.list_collections()
        for name in restore_collections:
            assert name + suffix in res
        for name in restore_collections:
            self.compare_collections(name, name + suffix, verify_by_query=True)

    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True])
    @pytest.mark.parametrize("enable_partition", [False])
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_restore", [3])
    @pytest.mark.parametrize("collection_type", ["all"])
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_index(
        self,
        collection_type,
        collection_need_to_restore,
        is_async,
        is_auto_id,
        enable_partition,
        nb,
        nullable
    ):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                    nullable=nullable
                )
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                    nullable=nullable
                )
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                )
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True

        # create index for source collections
        for name in names_origin:
            c = Collection(name)
            create_index_for_vector_fields(c)
        # create json path index for json field of source collections
        for name in names_origin:
            c = Collection(name)
            create_json_path_index_for_json_fields(c)

        # create backup
        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        restore_collections = backup_collections
        if collection_need_to_restore == "all":
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
            }
        else:
            restore_collections = names_need_backup[:collection_need_to_restore]
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
                "collection_names": restore_collections,
            }
        payload["restoreIndex"] = True
        t0 = time.time()
        res = self.client.restore_backup(payload)
        restore_id = res["data"]["id"]
        log.info(f"restore_backup: {res}")
        if is_async:
            res = self.client.wait_restore_complete(restore_id)
            assert res is True
        t1 = time.time()
        log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        res, _ = self.utility_wrap.list_collections()
        for name in restore_collections:
            assert name + suffix in res
        for name in restore_collections:
            self.compare_indexes(name, name + suffix)
        for name in restore_collections:
            self.compare_collections(name, name + suffix, verify_by_query=True, skip_index=True)


    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True])
    @pytest.mark.parametrize("enable_partition", [True])
    @pytest.mark.parametrize("is_async", [True])
    @pytest.mark.parametrize("collection_need_to_restore", [1])
    @pytest.mark.parametrize("collection_type", ["all"])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_multi_partition(
        self,
        collection_type,
        collection_need_to_restore,
        is_async,
        is_auto_id,
        enable_partition,
        nb,
    ):
        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        if collection_type == "all":
            for is_binary in [True, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                )
        if collection_type == "float":
            for is_binary in [False, False, False]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                )
        if collection_type == "binary":
            for is_binary in [True, True, True]:
                names_origin.append(cf.gen_unique_str(prefix))
                self.prepare_data(
                    names_origin[-1],
                    nb=nb,
                    is_binary=is_binary,
                    auto_id=is_auto_id,
                    check_function=False,
                    enable_partition=enable_partition,
                )
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup

        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        restore_collections = backup_collections
        if collection_need_to_restore == "all":
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
            }
        else:
            restore_collections = names_need_backup[:collection_need_to_restore]
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
                "collection_names": restore_collections,
            }
        t0 = time.time()
        res = self.client.restore_backup(payload)
        restore_id = res["data"]["id"]
        log.info(f"restore_backup: {res}")
        if is_async:
            res = self.client.wait_restore_complete(restore_id)
            assert res is True
        t1 = time.time()
        log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        res, _ = self.utility_wrap.list_collections()
        for name in restore_collections:
            assert name + suffix in res
        for name in restore_collections:
            self.compare_collections(name, name + suffix)

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
            self.prepare_data(
                name=collection_name,
                db_name=db_name,
                nb=3000,
                is_binary=False,
                auto_id=True,
                check_function=False,
            )
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
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        payload = {
            "async": False,
            "backup_name": back_up_name,
            # "collection_names": names_origin,
            "collection_suffix": suffix,
        }
        log.info(f"restore payload: {payload}")
        res = self.client.restore_backup(payload)
        log.info(f"restore_backup: {res}")
        for name in names_origin:
            db_name = name.split(".")[0]
            collection_name = name.split(".")[1]
            db.using_database(db_name)

            res, _ = self.utility_wrap.list_collections()
            log.info(f"collection list in db {db_name}: {res}")
            assert collection_name + suffix in res
            self.compare_collections(collection_name, collection_name + suffix)

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.parametrize("include_json", [True])
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_new_dynamic_schema_and_partition_key(self, include_json, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        if include_json:
            fields = [
                cf.gen_int64_field(name="int64", is_primary=True),
                cf.gen_int64_field(name="key"),
                cf.gen_json_field(name="json"),
                cf.gen_float_vec_field(name="float_vector", dim=128),
            ]
        else:
            fields = [
                cf.gen_int64_field(name="int64", is_primary=True),
                cf.gen_int64_field(name="key"),
                cf.gen_float_vec_field(name="float_vector", dim=128),
            ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
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

        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        if not include_json:
            output_fields = [ct.default_int64_field_name]
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.parametrize("include_json", [True])
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_restore_back_with_multi_json_datatype(
        self, include_json, include_dynamic, include_partition_key
    ):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        if include_json:
            fields = [
                cf.gen_int64_field(name="int64", is_primary=True),
                cf.gen_int64_field(name="key"),
                cf.gen_json_field(name="json"),
                cf.gen_float_vec_field(name="float_vector", dim=128),
            ]
        else:
            fields = [
                cf.gen_int64_field(name="int64", is_primary=True),
                cf.gen_int64_field(name="key"),
                cf.gen_float_vec_field(name="float_vector", dim=128),
            ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
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
                [json_value[i % len(json_value)] for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        else:
            data = [
                [i for i in range(nb)],
                [i % 3 for i in range(nb)],
                [[np.float32(i) for i in range(128)] for _ in range(nb)],
            ]
        collection_w.insert(data=data)

        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        if not include_json:
            output_fields = [ct.default_int64_field_name]
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("drop_db", [True])
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
                self.prepare_data(
                    name=collection_name,
                    db_name=db_name,
                    nb=3000,
                    is_binary=False,
                    auto_id=True,
                )
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
            "db_collections": json.dumps(db_collections)
            if str_json
            else db_collections,
        }
        log.info(f"payload: {payload}")
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        res = self.client.list_backup()
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
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "db_collections": db_collections,
            "collection_suffix": suffix,
        }
        log.info(f"restore payload: {payload}")
        res = self.client.restore_backup(payload)
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

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_restore_back_with_array_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
            cf.gen_int64_field(name="key"),
            cf.gen_json_field(name="json"),
            cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
            cf.gen_array_field(name="int_array", element_type=DataType.INT64),
            cf.gen_float_vec_field(name="float_vector", dim=128),
        ]
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
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
                    f"dynamic_{str(i)}": i,
                }
                for i in range(nb, nb * 2)
            ]
            collection_w.insert(data=data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_restore_back_with_multi_vector_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
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
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
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
                    "float_vector_0": [np.float32(i) for i in range(128)],
                    "float_vector_1": [np.float32(i) for i in range(128)],
                    "float_vector_2": [np.float32(i) for i in range(128)],
                    f"dynamic_{str(i)}": i,
                }
                for i in range(nb, nb * 2)
            ]
            collection_w.insert(data=data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_with_f16_bf16_datatype(self, include_dynamic, include_partition_key):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
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
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
            [
                np.array([random.random() for _ in range(128)], dtype=np.float16)
                for _ in range(nb)
            ],
            [
                np.array(
                    jnp.array([random.random() for _ in range(128)], dtype=jnp.bfloat16)
                )
                for _ in range(nb)
            ],
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
                    "float16_vector": np.array(
                        [random.random() for _ in range(128)], dtype=np.float16
                    ),
                    "brain_float16_vector": np.array(
                        jnp.array(
                            [random.random() for _ in range(128)], dtype=jnp.bfloat16
                        )
                    ),
                    f"dynamic_{str(i)}": i,
                }
                for i in range(nb, nb * 2)
            ]
            collection_w.insert(data=data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name not in all_backup

    @pytest.mark.parametrize("include_partition_key", [True])
    @pytest.mark.parametrize("include_dynamic", [True])
    @pytest.mark.parametrize("enable_text_match", [True])
    @pytest.mark.parametrize("enable_full_text_search", [True])
    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_sparse_vector_text_match_datatype(self, include_dynamic, include_partition_key, enable_text_match, enable_full_text_search):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [cf.gen_int64_field(name="int64", is_primary=True),
                  cf.gen_int64_field(name="key"),
                  cf.gen_string_field(name="text", enable_match=enable_text_match, enable_analyzer=True),
                  cf.gen_json_field(name="json"),
                  cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
                  cf.gen_array_field(name="int_array", element_type=DataType.INT64),
                  cf.gen_float_vec_field(name="float_vector", dim=128),
                  cf.gen_sparse_vec_field(name="sparse_vector"),
                  # cf.gen_sparse_vec_field(name="bm25_sparse_vector"),
                  ]
        if enable_full_text_search:
            fields.append(cf.gen_sparse_vec_field(name="bm25_sparse_vector"))
        if include_partition_key:
            partition_key = "key"
            default_schema = cf.gen_collection_schema(
                fields,
                enable_dynamic_field=include_dynamic,
                partition_key_field=partition_key,
            )
        else:
            default_schema = cf.gen_collection_schema(
                fields, enable_dynamic_field=include_dynamic
            )
        if enable_full_text_search:
            bm25_function = Function(
                name="text_bm25_emb",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["bm25_sparse_vector"],
                params={},
            )
            default_schema.add_function(bm25_function)
        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        nb = 3000
        rng = np.random.default_rng()

        data = []
        for i in range(nb):
            tmp = {
                "int64": i,
                "key": i % 3,
                "text": fake_en.text(),
                "json": {f"key_{str(i)}": i},
                "var_array": [str(x) for x in range(10)],
                "int_array": [int(x) for x in range(10)],
                "float_vector": [np.float32(i) for i in range(128)],
                "sparse_vector": {
                    d: rng.random() for d in random.sample(range(1000), random.randint(20, 30))
                },
            }
            if include_dynamic:
                tmp[f"dynamic_{str(i)}"] = i
            data.append(tmp)
        texts = [d["text"] for d in data]
        collection_w.insert(data=data)
        res = self.client.create_backup({"async": False, "backup_name": back_up_name, "collection_names": [name_origin]})
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin, name_origin + suffix, output_fields=output_fields
        )
        # check text match and full text search in restored collection
        word_freq = analyze_documents(texts)
        token = word_freq.most_common(1)[0][0]
        c = Collection(name=name_origin + suffix)
        create_index_for_vector_fields(c)
        c.load()
        if enable_text_match:
            res = c.query(
                expr=f"text_match(text, '{token}')",
                output_fields=["text"],
                limit=1
            )
            assert len(res) == 1
            for r in res:
                assert token.lower() in r["text"].lower()
        if enable_full_text_search:
            search_data = [fake_en.text()+f" {token} "]
            res = c.search(
                data=search_data,
                anns_field="bm25_sparse_vector",
                output_fields=["text"],
                param={},
                limit=1
            )
            assert len(res) == 1
            for r in res:
                assert len(r) == 1
        res = self.client.delete_backup(back_up_name)
        res = self.client.list_backup()
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
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
            cf.gen_int64_field(name="key"),
            cf.gen_json_field(name="json"),
            cf.gen_string_field(name="text", enable_match=True, enable_analyzer=True),
            cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
            cf.gen_array_field(name="int_array", element_type=DataType.INT64),
            cf.gen_float_vec_field(name="float_vector", dim=128),
            cf.gen_sparse_vec_field(name="bm25_vector"),
        ]
        default_schema = cf.gen_collection_schema(fields)

        # Add BM25 function
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["bm25_vector"],
            params={},
        )
        default_schema.add_function(bm25_function)

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )

        create_index_for_vector_fields(collection_w)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [fake_en.text() for _ in range(nb)],  # Text data for BM25
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        collection_w.flush()
        pk = res.primary_keys
        # delete first 100 rows
        delete_ids = pk[:100]
        collection_w.delete(expr=f"int64 in {delete_ids}")
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin,
            name_origin + suffix,
            output_fields=output_fields,
            verify_by_query=True,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_with_upsert(self):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
            cf.gen_int64_field(name="key"),
            cf.gen_json_field(name="json"),
            cf.gen_string_field(name="text", enable_match=True, enable_analyzer=True),
            cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
            cf.gen_array_field(name="int_array", element_type=DataType.INT64),
            cf.gen_float_vec_field(name="float_vector", dim=128),
            cf.gen_sparse_vec_field(name="bm25_vector"),
        ]
        default_schema = cf.gen_collection_schema(fields)

        # Add BM25 function
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["bm25_vector"],
            params={},
        )
        default_schema.add_function(bm25_function)

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        # create index for float_vector and bm25_sparse_vector
        create_index_for_vector_fields(collection_w)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [fake_en.text() for _ in range(nb)],  # Text data for BM25
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        collection_w.flush()
        # upsert first 100 rows by pk
        upsert_data = [
            [i for i in range(100)],
            [i % 3 for i in range(100, 200)],
            [{f"key_{str(i)}": i} for i in range(100, 200)],
            [fake_en.text() for _ in range(100)],  # Text data for BM25
            [[str(x) for x in range(10, 20)] for _ in range(100)],
            [[int(x) for x in range(10)] for _ in range(100)],
            [[np.float32(i) for i in range(128, 128 * 2)] for _ in range(100)],
        ]
        res, result = collection_w.upsert(data=upsert_data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin,
            name_origin + suffix,
            output_fields=output_fields,
            verify_by_query=True,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_with_dup_pk(self):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
            cf.gen_int64_field(name="key"),
            cf.gen_json_field(name="json"),
            cf.gen_string_field(name="text", enable_match=True, enable_analyzer=True),
            cf.gen_array_field(name="var_array", element_type=DataType.VARCHAR),
            cf.gen_array_field(name="int_array", element_type=DataType.INT64),
            cf.gen_float_vec_field(name="float_vector", dim=128),
            cf.gen_sparse_vec_field(name="bm25_sparse_vector"),
        ]
        default_schema = cf.gen_collection_schema(fields)

        # Add BM25 function
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["bm25_sparse_vector"],
            params={},
        )
        default_schema.add_function(bm25_function)

        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        create_index_for_vector_fields(collection_w)
        nb = 3000
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb)],
            [{f"key_{str(i)}": i} for i in range(nb)],
            [fake_en.text() for _ in range(nb)],  # Text data for BM25
            [[str(x) for x in range(10)] for i in range(nb)],
            [[int(x) for x in range(10)] for i in range(nb)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        collection_w.flush()
        data = [
            [i for i in range(nb)],
            [i % 3 for i in range(nb, nb * 2)],
            [{f"key_{str(i)}": i} for i in range(nb, nb * 2)],
            [fake_en.text() for _ in range(nb, nb * 2)],  # Text data for BM25
            [[str(x) for x in range(10)] for i in range(nb, nb * 2)],
            [[int(x) for x in range(10)] for i in range(nb, nb * 2)],
            [[np.float32(i) for i in range(128)] for _ in range(nb)],
        ]
        res, result = collection_w.insert(data=data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin,
            name_origin + suffix,
            output_fields=output_fields,
            verify_by_query=True,
        )

    @pytest.mark.tags(CaseLabel.MASTER)
    def test_milvus_restore_back_with_text_embedding(self, tei_endpoint):
        self._connect()
        name_origin = cf.gen_unique_str(prefix)
        back_up_name = cf.gen_unique_str(backup_prefix)
        dim = 768
        fields = [
            cf.gen_int64_field(name="int64", is_primary=True),
            cf.gen_string_field(name="text"),
            cf.gen_float_vec_field(name="dense", dim=dim),
        ]
        default_schema = cf.gen_collection_schema(fields)
        text_embedding_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["text"],
            output_field_names="dense",
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        default_schema.add_function(text_embedding_function)
        collection_w = self.init_collection_wrap(
            name=name_origin, schema=default_schema, active_trace=True
        )
        create_index_for_vector_fields(collection_w)
        nb = 3000
        data = [
            {
                "int64": i,
                "text": fake_en.text(),
            } for i in range(nb)
        ]
        batch_size = 100
        for i in range(0, nb, batch_size):
            collection_w.insert(data=data[i:i+batch_size])
        collection_w.flush()
        # delete first 100 rows
        delete_ids = [i for i in range(100)]
        collection_w.delete(expr=f"int64 in {delete_ids}")
        # upsert last 100 rows by pk
        upsert_data = [
            {
                "int64": i,
                "text": fake_en.text(),
            } for i in range(nb-100, nb)
        ]
        collection_w.upsert(data=upsert_data)
        res = self.client.create_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
            }
        )
        log.info(f"create_backup {res}")
        res = self.client.list_backup()
        log.info(f"list_backup {res}")
        if "data" in res:
            all_backup = [r["name"] for r in res["data"]]
        else:
            all_backup = []
        assert back_up_name in all_backup
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        assert name_origin in backup_collections
        res = self.client.restore_backup(
            {
                "async": False,
                "backup_name": back_up_name,
                "collection_names": [name_origin],
                "collection_suffix": suffix,
            }
        )
        log.info(f"restore_backup: {res}")
        res, _ = self.utility_wrap.list_collections()
        assert name_origin + suffix in res
        output_fields = None
        self.compare_collections(
            name_origin,
            name_origin + suffix,
            output_fields=output_fields,
            verify_by_query=True,
        )

    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("is_auto_id", [True])
    @pytest.mark.parametrize("enable_partition", [False])
    @pytest.mark.parametrize("is_async", [True, False])
    @pytest.mark.parametrize("collection_need_to_restore", [3])
    @pytest.mark.parametrize("use_v2_restore", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("is_all_data_type", [True])
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_restore_back_all_supported_data_type(
        self,
        use_v2_restore,
        collection_need_to_restore,
        is_async,
        is_auto_id,
        enable_partition,
        nb,
        nullable,
        is_all_data_type
    ):

        # prepare data
        names_origin = []
        back_up_name = cf.gen_unique_str(backup_prefix)
        names_origin.append(cf.gen_unique_str(prefix))
        self.prepare_data(
            names_origin[-1],
            nb=nb,
            auto_id=is_auto_id,
            check_function=False,
            enable_partition=enable_partition,
            nullable=nullable,
            is_all_data_type = is_all_data_type
            )
        log.info(f"name_origin:{names_origin}, back_up_name: {back_up_name}")
        for name in names_origin:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True
        # create backup
        names_need_backup = names_origin
        payload = {
            "async": False,
            "backup_name": back_up_name,
            "collection_names": names_need_backup,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        backup = self.client.get_backup(back_up_name)
        assert backup["data"]["name"] == back_up_name
        backup_collections = [
            backup["collection_name"] for backup in backup["data"]["collection_backups"]
        ]
        restore_collections = backup_collections
        if collection_need_to_restore == "all":
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
            }
        else:
            restore_collections = names_need_backup[:collection_need_to_restore]
            payload = {
                "async": False,
                "backup_name": back_up_name,
                "collection_suffix": suffix,
                "collection_names": restore_collections,
            }
        payload["useV2Restore"] = use_v2_restore
        t0 = time.time()
        res = self.client.restore_backup(payload)
        restore_id = res["data"]["id"]
        log.info(f"restore_backup: {res}")
        if is_async:
            res = self.client.wait_restore_complete(restore_id)
            assert res is True
        t1 = time.time()
        log.info(f"restore {restore_collections} cost time: {t1 - t0}")
        res, _ = self.utility_wrap.list_collections()
        for name in restore_collections:
            assert name + suffix in res
        for name in restore_collections:
            self.compare_collections(name, name + suffix, verify_by_query=True)