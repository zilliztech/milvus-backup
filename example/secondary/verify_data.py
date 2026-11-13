import time
import numpy as np
from pymilvus import (
    connections,
    utility,
    Collection,
)
import argparse

# Row counts produced by example/prepare_data.py: hello_milvus,
# hello_milvus_dynamic, and hello_milvus_added carry 3000 rows each;
# hello_milvus2 inserts the same 3000-row batch twice (auto_id=True), so it
# carries 6000. The downstream must match exactly — a missing segment and a
# double-replayed one (the inflated-num_entities failure class) must both
# fail here.
EXPECTED_ROW_COUNTS = {
    "hello_milvus": 3000,
    "hello_milvus2": 6000,
    "hello_milvus_dynamic": 3000,
    "hello_milvus_added": 3000,
}


def verify_row_counts(timeout_seconds=120, poll_seconds=5):
    """Assert the downstream row counts equal the fixture counts.

    The replicate/import replay lands on the downstream asynchronously, so
    poll until every collection reports its expected count. A count that
    never converges — e.g. a fabricated checkpoint inflating num_entities —
    fails the assertion with both sides in the message.
    """
    deadline = time.time() + timeout_seconds
    counts = {}
    while True:
        counts = {name: Collection(name).num_entities for name in EXPECTED_ROW_COUNTS}
        if counts == EXPECTED_ROW_COUNTS:
            print(f"Row counts aligned with the fixture: {counts}")
            return
        if time.time() >= deadline:
            break
        print(
            f"Row counts not converged yet: {counts} != {EXPECTED_ROW_COUNTS}, "
            f"retrying in {poll_seconds}s"
        )
        time.sleep(poll_seconds)
    raise AssertionError(
        f"Row counts diverged after secondary restore: downstream {counts} "
        f"!= expected {EXPECTED_ROW_COUNTS}"
    )


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

    # Secondary restore uses original collection names (no _recover suffix).
    # hello_milvus_dynamic exercises the dynamic-schema path; hello_milvus_added
    # exercises the add_collection_field path. Both target the schema
    # round-trip described in zilliztech/milvus-backup#1013.
    collections = [
        "hello_milvus",
        "hello_milvus2",
        "hello_milvus_dynamic",
        "hello_milvus_added",
    ]

    verify_row_counts()

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

        expected_count = EXPECTED_ROW_COUNTS[collection_name]

        # Read every row back instead of trusting the server-side counter:
        # the pk set catches missing and wrong rows that a matching count
        # alone would still hide. Milvus refuses an empty expression without
        # a limit, so ask for twice the fixture — a duplicated row still lands
        # in the read. hello_milvus2 uses auto_id, so its pks are
        # server-generated and only the cardinality is assertable.
        rows = collection.query("", output_fields=["pk"], limit=2 * expected_count)
        pks = {row["pk"] for row in rows}
        if collection_name == "hello_milvus2":
            assert len(rows) == expected_count and len(pks) == expected_count, (
                f"{collection_name}: queried {len(rows)} rows with {len(pks)} "
                f"distinct pks, expected {expected_count} distinct"
            )
        else:
            missing = sorted(set(range(expected_count)) - pks)
            unexpected = sorted(pks - set(range(expected_count)))
            assert pks == set(range(expected_count)), (
                f"{collection_name}: pk set diverged — missing {missing[:10]}, "
                f"unexpected {unexpected[:10]}"
            )
        print(f"PK set of {collection_name}: {len(pks)} pks match the fixture")

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
