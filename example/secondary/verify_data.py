import time
import numpy as np
from pymilvus import (
    connections,
    utility,
    Collection,
)
import argparse


def main(uri, token):
    fmt = "\n=== {:30} ===\n"
    search_latency_fmt = "search latency = {:.4f}s"
    num_entities, dim = 3000, 8
    rng = np.random.default_rng(seed=19530)
    entities = [
        [i for i in range(num_entities)],
        rng.random(num_entities).tolist(),
        rng.random((num_entities, dim)),
    ]

    print(fmt.format("start connecting to Milvus"))
    print(fmt.format(f"Milvus uri: {uri}"))
    connections.connect("default", uri=uri, token=token)

    # Secondary restore uses original collection names (no _recover suffix)
    collections = ["hello_milvus", "hello_milvus2"]

    for collection_name in collections:
        has = utility.has_collection(collection_name)
        print(f"Does collection {collection_name} exist in Milvus: {has}")
        assert has, f"Collection {collection_name} does not exist"
        collection = Collection(collection_name)
        print(collection.schema)

        # In secondary mode, the downstream Milvus only accepts replicate stream
        # messages. Flush, create_index, and load are already done by the
        # secondary restore process via the replicate stream, so we skip them
        # here and go straight to search/query verification.

        print(f"Number of entities in Milvus: {collection_name} : {collection.num_entities}")

        print(fmt.format("Start searching based on vector similarity"))
        vectors_to_search = entities[-1][-2:]
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10},
        }

        start_time = time.time()
        result = collection.search(vectors_to_search, "embeddings", search_params, limit=3, output_fields=["random"])
        end_time = time.time()

        for hits in result:
            for hit in hits:
                print(f"hit: {hit}, random field: {hit.entity.get('random')}")
        print(search_latency_fmt.format(end_time - start_time))

        print(fmt.format("Start querying with `random > 0.5`"))

        start_time = time.time()
        result = collection.query(expr="random > 0.5", output_fields=["random", "embeddings"])
        end_time = time.time()

        print(f"query result:\n-{result[0]}")
        print(search_latency_fmt.format(end_time - start_time))

        print(fmt.format("Start hybrid searching with `random > 0.5`"))

        start_time = time.time()
        result = collection.search(vectors_to_search, "embeddings", search_params, limit=3, expr="random > 0.5", output_fields=["random"])
        end_time = time.time()

        for hits in result:
            for hit in hits:
                print(f"hit: {hit}, random field: {hit.entity.get('random')}")
        print(search_latency_fmt.format(end_time - start_time))

        # Note: drop_collection is also blocked in secondary mode, skip it.
        print(fmt.format(f"Verified collection {collection_name}"))


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="verify secondary restore data")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19500", help="Milvus server uri")
    args.add_argument("--token", type=str, default="root:Milvus", help="Milvus server token")
    args = args.parse_args()
    main(args.uri, args.token)
