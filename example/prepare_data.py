# prepare_data.py - Prepare test data for Milvus backup/restore testing
# Supports two scenarios:
# 1. Single-stage: All data inserted at once (for testing backup/restore of old version data)
# 2. Multi-stage: Data inserted in stages (for testing cross-version backup/restore with incremental data)
#
# Usage:
#   Single-stage mode (default): python prepare_data.py
#   Multi-stage mode: python prepare_data.py --stage 1  # then later: --stage 2
import time
import numpy as np
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
import argparse



def main(uri="http://127.0.0.1:19530", token="root:Milvus", stage=None, total_entities=3000):
    fmt = "\n=== {:30} ===\n"
    dim = 8

    #################################################################################
    # 1. connect to Milvus
    # Add a new connection alias `default` for Milvus server in `localhost:19530`
    # Actually the "default" alias is a buildin in PyMilvus.
    # If the address of Milvus is the same as `localhost:19530`, you can omit all
    # parameters and call the method as: `connections.connect()`.
    #
    # Note: the `using` parameter of the following methods is default to "default".
    print(fmt.format("start connecting to Milvus"))

    print(fmt.format(f"Milvus uri: {uri}"))
    connections.connect("default", uri=uri, token=token)

    has = utility.has_collection("hello_milvus")
    print(f"Does collection hello_milvus exist in Milvus: {has}")

    #################################################################################
    # 2. create collection
    # We're going to create a collection with 3 fields.
    # +-+------------+------------+------------------+------------------------------+
    # | | field name | field type | other attributes |       field description      |
    # +-+------------+------------+------------------+------------------------------+
    # |1|    "pk"    |    Int64   |  is_primary=True |      "primary field"         |
    # | |            |            |   auto_id=False  |                              |
    # +-+------------+------------+------------------+------------------------------+
    # |2|  "random"  |    Double  |                  |      "a double field"        |
    # +-+------------+------------+------------------+------------------------------+
    # |3|"embeddings"| FloatVector|     dim=8        |  "float vector with dim 8"   |
    # +-+------------+------------+------------------+------------------------------+
    fields = [
        FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="random", dtype=DataType.DOUBLE),
        FieldSchema(name="var", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]

    schema = CollectionSchema(fields, "hello_milvus")

    print(fmt.format("Create collection `hello_milvus`"))
    hello_milvus = Collection("hello_milvus", schema, consistency_level="Strong")

    ################################################################################
    # 3. insert data
    # We are going to insert rows of data into the collection
    # Data to be inserted must be organized in fields.
    #
    # The insert() method returns:
    # - either automatically generated primary keys by Milvus if auto_id=True in the schema;
    # - or the existing primary key field from the entities if auto_id=False in the schema.

    # Only insert data to hello_milvus when stage is None or 1
    if stage != 2:
        print(fmt.format("Start inserting entities to hello_milvus"))
        rng = np.random.default_rng(seed=19530)
        
        # hello_milvus always inserts all data when inserting
        num_entities = total_entities
        pk_list = [i for i in range(num_entities)]
        random_list = rng.random(num_entities).tolist()
        var_list = [str(i) for i in range(num_entities)]  # Always use original format
        embeddings_list = rng.random((num_entities, dim))
        
        # Split data into 10 batches for insertion
        batch_size = num_entities // 10
        if batch_size == 0:
            batch_size = 1
            
        for j in range(10):
            start_idx = j * batch_size
            end_idx = (j + 1) * batch_size if j < 9 else num_entities
            if start_idx >= num_entities:
                break
                
            # Prepare batch data
            batch_entities = [
                pk_list[start_idx:end_idx],
                random_list[start_idx:end_idx],
                var_list[start_idx:end_idx],
                embeddings_list[start_idx:end_idx].tolist() if isinstance(embeddings_list, np.ndarray) else embeddings_list[start_idx:end_idx]
            ]
            
            # Insert batch data
            hello_milvus.insert(batch_entities)
            time.sleep(1)  # Add delay to prevent inserting too quickly
            print(f"epoch {j+1}/10")
        hello_milvus.flush()
    else:
        print("Stage 2: Skipping data insertion to hello_milvus")
        rng = np.random.default_rng(seed=19530)  # Initialize rng for hello_milvus2
    
    print(f"Number of entities in hello_milvus: {hello_milvus.num_entities}")

    # create another collection
    fields2 = [
        FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="random", dtype=DataType.DOUBLE),
        FieldSchema(name="var", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]

    schema2 = CollectionSchema(fields2, "hello_milvus2")

    print(fmt.format("Create collection `hello_milvus2`"))
    hello_milvus2 = Collection("hello_milvus2", schema2, consistency_level="Strong")

    # For hello_milvus2, apply stage-based data generation
    if stage is None:
        # Original scenario: all data in one go
        num_entities2 = total_entities
        entity_offset2 = 0
    elif stage == 1:
        # Multi-stage scenario: first half of data
        num_entities2 = total_entities // 2
        entity_offset2 = 0
    else:  # stage == 2
        # Multi-stage scenario: second half of data
        num_entities2 = total_entities - (total_entities // 2)
        entity_offset2 = total_entities // 2
    
    if stage is None:
        var_list2 = [str(i) for i in range(num_entities2)]  # Original format
    else:
        var_list2 = [f"stage{stage}_entity_{i + entity_offset2}" for i in range(num_entities2)]
    
    entities2 = [
        rng.random(num_entities2).tolist(),  # field random, only supports list
        var_list2,
        rng.random((num_entities2, dim)),  # field embeddings, supports numpy.ndarray and list
    ]

    hello_milvus2.insert(entities2)
    hello_milvus2.flush()
    hello_milvus2.insert(entities2)
    hello_milvus2.flush()

    if stage is None:
        print(f"Number of entities in hello_milvus2: {hello_milvus2.num_entities}")
    else:
        print(f"Stage {stage} - Number of entities in hello_milvus2: {hello_milvus2.num_entities}")
        print(fmt.format(f"Stage {stage} completed for hello_milvus2"))
        print(f"Stage {stage} inserted {num_entities2} entities starting from offset {entity_offset2}")


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="prepare data for backup/restore testing")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19530", help="Milvus server uri")
    args.add_argument("--token", type=str, default="root:Milvus", help="Milvus server token")
    args.add_argument("--stage", type=int, choices=[1, 2], required=False, help="Stage 1 or 2 for multi-stage data preparation (only affects hello_milvus2). Omit for single-stage mode")
    args.add_argument("--total-entities", type=int, default=3000, help="Total number of entities (hello_milvus always gets all, hello_milvus2 respects stage)")
    args = args.parse_args()
    main(args.uri, args.token, args.stage, args.total_entities)
