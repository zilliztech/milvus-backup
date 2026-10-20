import time
import os
import numpy as np
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
import argparse


def main(uri, token):
    fmt = "\n=== {:30} ===\n"
    search_latency_fmt = "search latency = {:.4f}s"
    num_entities, dim = 3000, 8
    rng = np.random.default_rng(seed=19530)
    entities = [
        # provide the pk field because `auto_id` is set to False
        [i for i in range(num_entities)],
        rng.random(num_entities).tolist(),  # field random, only supports list
        rng.random((num_entities, dim)),    # field embeddings, supports numpy.ndarray and list
    ]

    ################################################################################
    # 1. get recovered collection hello_milvus_recover
    print(fmt.format("start connecting to Milvus"))
    print(fmt.format(f"Milvus uri: {uri}"))
    connections.connect("default", uri=uri, token=token)

    recover_collections = ["hello_milvus_recover", "hello_milvus2_recover"]

    for recover_collection_name in recover_collections:
        has = utility.has_collection(recover_collection_name)
        print(f"Does collection {recover_collection_name} exist in Milvus: {has}")
        recover_collection = Collection(recover_collection_name)
        print(recover_collection.schema)
        recover_collection.flush()

        print(f"Number of entities in Milvus: {recover_collection_name} : {recover_collection.num_entities}")  # check the num_entites

        ################################################################################
        # 4. create index
        # We are going to create an IVF_FLAT index for hello_milvus_recover collection.
        # create_index() can only be applied to `FloatVector` and `BinaryVector` fields.
        print(fmt.format("Start Creating index IVF_FLAT"))
        index = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 128},
        }

        recover_collection.create_index("embeddings", index)

        ################################################################################
        # 5. search, query, and hybrid search
        # After data were inserted into Milvus and indexed, you can perform:
        # - search based on vector similarity
        # - query based on scalar filtering(boolean, int, etc.)
        # - hybrid search based on vector similarity and scalar filtering.
        #

        # Before conducting a search or a query, you need to load the data in `hello_milvus` into memory.
        print(fmt.format("Start loading"))
        recover_collection.load()

        # -----------------------------------------------------------------------------
        # search based on vector similarity
        print(fmt.format("Start searching based on vector similarity"))
        vectors_to_search = entities[-1][-2:]
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10},
        }

        start_time = time.time()
        result = recover_collection.search(vectors_to_search, "embeddings", search_params, limit=3, output_fields=["random"])
        end_time = time.time()

        for hits in result:
            for hit in hits:
                print(f"hit: {hit}, random field: {hit.entity.get('random')}")
        print(search_latency_fmt.format(end_time - start_time))

        # -----------------------------------------------------------------------------
        # query based on scalar filtering(boolean, int, etc.)
        print(fmt.format("Start querying with `random > 0.5`"))

        start_time = time.time()
        result = recover_collection.query(expr="random > 0.5", output_fields=["random", "embeddings"])
        end_time = time.time()

        print(f"query result:\n-{result[0]}")
        print(search_latency_fmt.format(end_time - start_time))

        # -----------------------------------------------------------------------------
        # hybrid search
        print(fmt.format("Start hybrid searching with `random > 0.5`"))

        start_time = time.time()
        result = recover_collection.search(vectors_to_search, "embeddings", search_params, limit=3, expr="random > 0.5", output_fields=["random"])
        end_time = time.time()

        for hits in result:
            for hit in hits:
                print(f"hit: {hit}, random field: {hit.entity.get('random')}")
        print(search_latency_fmt.format(end_time - start_time))

        ###############################################################################
        # 7. drop collection
        # Finally, drop the hello_milvus, hello_milvus_recover collection
        print(fmt.format(f"Drop collection {recover_collection_name}"))
        utility.drop_collection(recover_collection_name)


L0_RECOVER_COLLECTION = "hello_milvus_l0_recover"
L0_TOTAL_ENTITIES = 3000
L0_DELETED_PKS = list(range(1000, 1100))


def main_l0(uri, token):
    fmt = "\n=== {:30} ===\n"
    print(fmt.format("start connecting to Milvus"))
    print(fmt.format(f"Milvus uri: {uri}"))
    connections.connect("default", uri=uri, token=token)

    has = utility.has_collection(L0_RECOVER_COLLECTION)
    print(f"Does collection {L0_RECOVER_COLLECTION} exist in Milvus: {has}")
    assert has, f"{L0_RECOVER_COLLECTION} not found after restore"
    recover_collection = Collection(L0_RECOVER_COLLECTION)
    print(recover_collection.schema)
    recover_collection.flush()

    print(fmt.format("Start Creating index IVF_FLAT"))
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    recover_collection.create_index("embeddings", index)

    print(fmt.format("Start loading"))
    recover_collection.load()

    expected = L0_TOTAL_ENTITIES - len(L0_DELETED_PKS)
    result = recover_collection.query("", output_fields=["count(*)"])
    count = result[0]["count(*)"]
    print(f"Number of entities in {L0_RECOVER_COLLECTION}: {count}, expected: {expected}")
    assert count == expected, f"count {count} != expected {expected}"

    expr = "pk in [{}]".format(", ".join(str(pk) for pk in L0_DELETED_PKS))
    result = recover_collection.query(expr, output_fields=["pk"])
    print(f"Deleted rows still present: {len(result)}")
    assert len(result) == 0, f"deleted PKs reappeared after restore: {result[:10]}"

    survivors = [0, 999, 1100, L0_TOTAL_ENTITIES - 1]
    expr = "pk in [{}]".format(", ".join(str(pk) for pk in survivors))
    result = recover_collection.query(expr, output_fields=["pk"])
    print(f"Surviving rows found: {len(result)} of {len(survivors)}")
    assert len(result) == len(survivors), f"surviving PKs missing: {result}"

    print(fmt.format(f"Drop collection {L0_RECOVER_COLLECTION}"))
    utility.drop_collection(L0_RECOVER_COLLECTION)


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="verify data")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19530", help="Milvus server uri")
    args.add_argument("--token",type=str, default="root:Milvus", help="Milvus server token")
    args.add_argument("--scenario", choices=["base", "l0"], default="base", help="base: hello_milvus_recover / hello_milvus2_recover. l0: hello_milvus_l0_recover, restored from an l0compacted backup.")
    args = args.parse_args()
    if args.scenario == "base":
        main(args.uri, args.token)
    else:
        main_l0(args.uri, args.token)