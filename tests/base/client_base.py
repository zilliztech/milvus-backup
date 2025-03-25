import sys
import time
import pytest
from pymilvus import DefaultConfig, DataType, db

sys.path.append("..")
from base.connections_wrapper import ApiConnectionsWrapper
from base.collection_wrapper import ApiCollectionWrapper
from base.partition_wrapper import ApiPartitionWrapper
from base.index_wrapper import ApiIndexWrapper
from base.utility_wrapper import ApiUtilityWrapper
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from api.milvus_backup import MilvusBackupClient
from utils.util_pymilvus import create_index_for_vector_fields

class Base:
    """Initialize class object"""

    client = None
    connection_wrap = None
    collection_wrap = None
    partition_wrap = None
    index_wrap = None
    utility_wrap = None
    collection_schema_wrap = None
    field_schema_wrap = None
    collection_object_list = []

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        log.info("[teardown_class] Start teardown class...")

    def setup_method(self, method):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        log.info("[setup_method] Start setup test case %s." % method.__name__)
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        try:
            """ Drop collection before disconnect """
            if not self.connection_wrap.has_connection(
                alias=DefaultConfig.DEFAULT_USING
            )[0]:
                self.connection_wrap.connect(
                    alias=DefaultConfig.DEFAULT_USING,
                    host=cf.param_info.param_host,
                    port=cf.param_info.param_port,
                )

            if self.collection_wrap.collection is not None:
                if self.collection_wrap.collection.name.startswith("alias"):
                    log.info(
                        f"collection {self.collection_wrap.collection.name} is alias, skip drop operation"
                    )
                else:
                    self.collection_wrap.drop(check_task=ct.CheckTasks.check_nothing)

            collection_list = self.utility_wrap.list_collections()[0]
            for collection_object in self.collection_object_list:
                if (
                    collection_object.collection is not None
                    and collection_object.name in collection_list
                ):
                    collection_object.drop(check_task=ct.CheckTasks.check_nothing)

        except Exception as e:
            log.debug(str(e))

        # # delete backups after test
        # try:
        #     for backup_name in self.client.backup_list:
        #         self.client.delete_backup(backup_name)
        # except Exception as e:
        #     log.error(f"delete backup failed with error: {e}")


class TestcaseBase(Base):
    """
    Additional methods;
    Public methods that can be used for test cases.
    """

    @pytest.fixture(scope="function", autouse=True)
    def inti_client(self, backup_uri):
        endpoint = f"{backup_uri}/api/v1"
        self.client = MilvusBackupClient(endpoint)

    def _connect(self):
        """Add a connection and create the connect"""
        if cf.param_info.param_user and cf.param_info.param_password:
            res, is_succ = self.connection_wrap.connect(
                alias=DefaultConfig.DEFAULT_USING,
                host=cf.param_info.param_host,
                port=cf.param_info.param_port,
                user=cf.param_info.param_user,
                password=cf.param_info.param_password,
                secure=cf.param_info.param_secure,
            )
        else:
            res, is_succ = self.connection_wrap.connect(
                alias=DefaultConfig.DEFAULT_USING,
                host=cf.param_info.param_host,
                port=cf.param_info.param_port,
            )
        return res

    def init_collection_wrap(
        self,
        name=None,
        schema=None,
        shards_num=2,
        check_task=None,
        check_items=None,
        **kwargs,
    ):
        name = cf.gen_unique_str("coll_") if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[
            0
        ]:
            self._connect()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(
            name=name,
            schema=schema,
            shards_num=shards_num,
            check_task=check_task,
            check_items=check_items,
            **kwargs,
        )
        self.collection_object_list.append(collection_w)
        return collection_w

    def init_multi_fields_collection_wrap(self, name=cf.gen_unique_str()):
        vec_fields = [cf.gen_float_vec_field(ct.another_float_vec_field_name)]
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        return collection_w, df

    def init_partition_wrap(
        self,
        collection_wrap=None,
        name=None,
        description=None,
        check_task=None,
        check_items=None,
        **kwargs,
    ):
        name = cf.gen_unique_str("partition_") if name is None else name
        description = (
            cf.gen_unique_str("partition_des_") if description is None else description
        )
        collection_wrap = (
            self.init_collection_wrap() if collection_wrap is None else collection_wrap
        )
        partition_wrap = ApiPartitionWrapper()
        partition_wrap.init_partition(
            collection_wrap.collection,
            name,
            description,
            check_task=check_task,
            check_items=check_items,
            **kwargs,
        )
        return partition_wrap

    def insert_data_general(
        self,
        prefix="test",
        insert_data=False,
        nb=ct.default_nb,
        partition_num=0,
        is_binary=False,
        is_all_data_type=False,
        auto_id=False,
        dim=ct.default_dim,
        primary_field=ct.default_int64_field_name,
        is_flush=True,
        name=None,
        **kwargs,
    ):
        """ """
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        if name is not None:
            collection_name = name
        vectors = []
        binary_raw_vectors = []
        insert_ids = []
        time_stamp = 0
        # 1 create collection
        default_schema = cf.gen_default_collection_schema(
            auto_id=auto_id, dim=dim, primary_field=primary_field
        )
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        log.info("init_collection_general: collection creation")
        collection_w = self.init_collection_wrap(
            name=collection_name, schema=default_schema, **kwargs
        )
        pre_entities = collection_w.num_entities
        if insert_data:
            collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp = (
                cf.insert_data(
                    collection_w,
                    nb,
                    is_binary,
                    is_all_data_type,
                    auto_id=auto_id,
                    dim=dim,
                )
            )
            if is_flush:
                collection_w.flush()
                assert collection_w.num_entities == nb + pre_entities

        return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp

    def init_collection_general(
        self,
        prefix="test",
        insert_data=False,
        nb=ct.default_nb,
        partition_num=0,
        is_binary=False,
        is_all_data_type=False,
        auto_id=False,
        dim=ct.default_dim,
        is_index=False,
        primary_field=ct.default_int64_field_name,
        is_flush=True,
        name=None,
        **kwargs,
    ):
        """
        target: create specified collections
        method: 1. create collections (binary/non-binary, default/all data type, auto_id or not)
                2. create partitions if specified
                3. insert specified (binary/non-binary, default/all data type) data
                   into each partition if any
                4. not load if specifying is_index as True
        expected: return collection and raw data, insert ids
        """
        log.info("Test case of search interface: initialize before test case")
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        if name is not None:
            collection_name = name
        vectors = []
        binary_raw_vectors = []
        insert_ids = []
        time_stamp = 0
        # 1 create collection
        default_schema = cf.gen_default_collection_schema(
            auto_id=auto_id, dim=dim, primary_field=primary_field
        )
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        log.info("init_collection_general: collection creation")
        collection_w = self.init_collection_wrap(
            name=collection_name, schema=default_schema, **kwargs
        )
        # 2 add extra partitions if specified (default is 1 partition named "_default")
        if partition_num > 0:
            cf.gen_partitions(collection_w, partition_num)
        # 3 insert data if specified
        if insert_data:
            collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp = (
                cf.insert_data(
                    collection_w,
                    nb,
                    is_binary,
                    is_all_data_type,
                    auto_id=auto_id,
                    dim=dim,
                )
            )
            if is_flush:
                assert collection_w.is_empty is False
                assert collection_w.num_entities == nb
            # This condition will be removed after auto index feature
            if not is_index:
                if is_binary:
                    collection_w.create_index(
                        ct.default_binary_vec_field_name, ct.default_bin_flat_index
                    )
                else:
                    collection_w.create_index(
                        ct.default_float_vec_field_name, ct.default_flat_index
                    )
                collection_w.load()
        elif not is_index:
            if is_binary:
                collection_w.create_index(
                    ct.default_binary_vec_field_name, ct.default_bin_flat_index
                )
            else:
                collection_w.create_index(
                    ct.default_float_vec_field_name, ct.default_flat_index
                )

        return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp

    def insert_entities_into_two_partitions_in_half(self, half, prefix="query"):
        """
        insert default entities into two partitions(partition_w and _default) in half(int64 and float fields values)
        :param half: half of nb
        :return: collection wrap and partition wrap
        """
        self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        # insert [0, half) into partition_w
        df_partition = cf.gen_default_dataframe_data(nb=half, start=0)
        partition_w.insert(df_partition)
        # insert [half, nb) into _default
        df_default = cf.gen_default_dataframe_data(nb=half, start=half)
        collection_w.insert(df_default)
        # flush
        collection_w.num_entities
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index
        )
        collection_w.load(partition_names=[partition_w.name, "_default"])
        return collection_w, partition_w, df_partition, df_default

    def collection_insert_multi_segments_one_shard(
        self, collection_prefix, num_of_segment=2, nb_of_segment=1, is_dup=True
    ):
        """
        init collection with one shard, insert data into two segments on one shard (they can be merged)
        :param collection_prefix: collection name prefix
        :param num_of_segment: number of segments
        :param nb_of_segment: number of entities per segment
        :param is_dup: whether the primary keys of each segment is duplicated
        :return: collection wrap and partition wrap
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(collection_prefix), shards_num=1
        )

        for i in range(num_of_segment):
            start = 0 if is_dup else i * nb_of_segment
            df = cf.gen_default_dataframe_data(nb_of_segment, start=start)
            collection_w.insert(df)
            assert collection_w.num_entities == nb_of_segment * (i + 1)
        return collection_w

    def prepare_data(
        self,
        name=None,
        db_name="default",
        nb=ct.default_nb,
        dim=ct.default_dim,
        is_binary=False,
        auto_id=False,
        primary_field=ct.default_int64_field_name,
        is_flushed=True,
        check_function=False,
        enable_partition=False,
        nullable=False,
        is_all_data_type = False
    ):
        """
        prepare data for test case
        """
        self._connect()
        db.using_database(db_name)
        prefix = "backup_e2e_"
        name = cf.gen_unique_str(prefix) if name is None else name
        default_schema = cf.gen_default_collection_schema(
            auto_id=auto_id, dim=dim, primary_field=primary_field, nullable=nullable
        )
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(
                auto_id=auto_id, dim=dim, primary_field=primary_field, nullable=nullable
            )
        collection_w = self.init_collection_wrap(
            name=name, schema=default_schema, active_trace=True
        )
        # create partitions
        if enable_partition:
            for i in range(3):
                self.init_partition_wrap(collection_wrap=collection_w)
        assert collection_w.name == name
        if nb > 0:
            cf.insert_data(
                collection_w, nb=nb, is_binary=is_binary, auto_id=auto_id, dim=dim, nullable=nullable,
                is_all_data_type=is_all_data_type
            )
        if is_flushed:
            collection_w.flush(timeout=180)
        if check_function:
            if is_binary:
                collection_w.create_index(
                    ct.default_binary_vec_field_name,
                    ct.default_bin_flat_index,
                    index_name=cf.gen_unique_str(),
                )
            else:
                collection_w.create_index(
                    ct.default_float_vec_field_name,
                    ct.default_index,
                    index_name=cf.gen_unique_str(),
                )

            collection_w.create_index(
                field_name=ct.default_string_field_name,
                index_params={},
                index_name=cf.gen_unique_str(),
            )
            collection_w.load()
            if is_binary:
                search_raw_vector, search_vectors = cf.gen_binary_vectors(5, dim)
                search_params = ct.default_search_binary_params
                search_field = ct.default_binary_vec_field_name
            else:
                search_vectors = cf.gen_vectors(5, dim)
                search_params = ct.default_search_params
                search_field = ct.default_float_vec_field_name
            collection_w.search(
                data=search_vectors,
                anns_field=search_field,
                param=search_params,
                limit=5,
            )
            term_expr = f"{ct.default_int64_field_name} in [1001,1201,4999,2999]"
            res, _ = collection_w.query(term_expr)

    def verify_data(
        self,
        name=None,
        dim=ct.default_dim,
        is_binary=False,
        auto_id=False,
        primary_field=ct.default_int64_field_name,
    ):
        collection_w, _ = self.collection_wrap.init_collection(name=name)
        default_schema = cf.gen_default_collection_schema(
            auto_id=auto_id, dim=dim, primary_field=primary_field
        )
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(
                auto_id=auto_id, dim=dim, primary_field=primary_field
            )
        collection_w = self.init_collection_wrap(
            name=name, schema=default_schema, active_trace=True
        )

        if is_binary:
            collection_w.create_index(
                ct.default_binary_vec_field_name,
                ct.default_bin_flat_index,
                index_name=cf.gen_unique_str(),
            )
        else:
            collection_w.create_index(
                ct.default_float_vec_field_name,
                ct.default_index,
                index_name=cf.gen_unique_str(),
            )

        collection_w.create_index(
            field_name=ct.default_string_field_name,
            index_params={},
            index_name=cf.gen_unique_str(),
        )
        collection_w.load()
        if is_binary:
            search_raw_vector, search_vectors = cf.gen_binary_vectors(5, dim)
            search_params = ct.default_search_binary_params
            search_field = ct.default_binary_vec_field_name
        else:
            search_vectors = cf.gen_vectors(5, dim)
            search_params = ct.default_search_params
            search_field = ct.default_float_vec_field_name
        collection_w.search(
            data=search_vectors, anns_field=search_field, param=search_params, limit=5
        )
        term_expr = f"{ct.default_int64_field_name} in [1001,1201,4999,2999]"
        collection_w.query(term_expr)

    def is_binary_by_schema(self, schema):
        fields = schema.fields
        for field in fields:
            if field.dtype == DataType.BINARY_VECTOR:
                return True
            if field.dtype == DataType.FLOAT_VECTOR:
                return False

    def compare_indexes(self, src_name, dist_name):
        collection_src, _ = self.collection_wrap.init_collection(name=src_name)
        collection_dist, _ = self.collection_wrap.init_collection(name=dist_name)
        src_indexes_info = [x.to_dict() for x in collection_src.indexes]
        dist_indexes_info = [x.to_dict() for x in collection_dist.indexes]
        log.info(f"collection_src indexes: {src_indexes_info}")
        log.info(f"collection_dist indexes: {dist_indexes_info}")
        # compare indexes info with same field name for src and dist
        for src_index_info in src_indexes_info:
            for dist_index_info in dist_indexes_info:
                if src_index_info["field"] == dist_index_info["field"]:
                    src_index_info.pop("collection")
                    dist_index_info.pop("collection")
                    assert src_index_info == dist_index_info

    def compare_collections(
        self, src_name, dist_name, output_fields=None, verify_by_query=False, skip_index=False
    ):
        if output_fields is None:
            output_fields = ["*"]
        collection_src, _ = self.collection_wrap.init_collection(name=src_name)
        collection_dist, _ = self.collection_wrap.init_collection(name=dist_name)
        log.info(f"collection_src schema: {collection_src.schema}")
        log.info(f"collection_dist schema: {collection_dist.schema}")
        assert collection_src.schema == collection_dist.schema
        # get partitions
        partitions_src = collection_src.partitions
        partitions_dist = collection_dist.partitions
        log.info(
            f"partitions_src: {partitions_src}, partitions_dist: {partitions_dist}"
        )
        assert len(partitions_src) == len(partitions_dist)
        # get num entities
        src_num = collection_src.num_entities
        dist_num = collection_dist.num_entities
        log.info(f"src_num: {src_num}, dist_num: {dist_num}")
        if not verify_by_query:
            assert src_num == dist_num, f"srs_num: {src_num}, dist_num: {dist_num}"
            return
        for coll in [collection_src, collection_dist]:
            if not skip_index:
                try:
                    create_index_for_vector_fields(coll)
                except Exception as e:
                    log.error(f"collection {coll.name} create index failed with error: {e}")
            coll.load()
            time.sleep(5)
        # get entities by count
        src_count = collection_src.query(expr="", output_fields=["count(*)"])
        dist_count = collection_dist.query(expr="", output_fields=["count(*)"])
        log.info(f"src count: {src_count}, dist count: {dist_count}")
        src_res = collection_src.query(
            expr=f"{ct.default_int64_field_name} >= 0", output_fields=output_fields
        )
        # log.info(f"src res: {len(src_res)}, src res: {src_res[-1]}")
        dist_res = collection_dist.query(
            expr=f"{ct.default_int64_field_name} >= 0", output_fields=output_fields
        )
        # log.info(f"dist res: {len(dist_res)}, dist res: {dist_res[-1]}")
        assert len(dist_res) == len(src_res)

        # sort by primary key and compare
        src_res = sorted(src_res, key=lambda x: x[ct.default_int64_field_name])
        dist_res = sorted(dist_res, key=lambda x: x[ct.default_int64_field_name])
        src_pk = [r[ct.default_int64_field_name] for r in src_res]
        dist_pk = [r[ct.default_int64_field_name] for r in dist_res]
        diff = list(set(src_pk).difference(set(dist_pk)))
        log.info(f"pk diff: {diff}")
        assert len(diff) == 0
        for i in range(len(src_res)):
            assert src_res[i] == dist_res[i], f"src: {src_res[i]}, dist: {dist_res[i]}"
        for coll in [collection_src, collection_dist]:
            try:
                coll.release()
            except Exception as e:
                log.error(f"collection {coll.name} release failed with error: {e}")

    def check_collection_binary(self, name):
        collection_w, _ = self.collection_wrap.init_collection(name=name)
        field_types = [field.dtype for field in collection_w.schema.fields]
        if 5 in field_types:
            is_binary = True
        else:
            is_binary = False
        return is_binary
