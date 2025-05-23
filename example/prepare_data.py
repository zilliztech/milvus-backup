# hello_milvus.py demonstrates the basic operations of PyMilvus, a Python SDK of Milvus.
# 1. connect to Milvus
# 2. create collection
# 3. insert data
# 4. create index
# 5. search, query, and hybrid search on entities
# 6. delete entities by PK
# 7. drop collection
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



def main(uri="http://127.0.0.1:19530", token="root:Milvus"):
    fmt = "\n=== {:30} ===\n"
    num_entities, dim = 3000, 8

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
    # We are going to insert 3000 rows of data into `hello_milvus`
    # Data to be inserted must be organized in fields.
    #
    # The insert() method returns:
    # - either automatically generated primary keys by Milvus if auto_id=True in the schema;
    # - or the existing primary key field from the entities if auto_id=False in the schema.

    print(fmt.format("Start inserting entities"))
    rng = np.random.default_rng(seed=19530)
    # Prepare data
    pk_list = [i for i in range(num_entities)]
    random_list = rng.random(num_entities).tolist()
    var_list = [str(i) for i in range(num_entities)]
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
        insert_result = hello_milvus.insert(batch_entities)
        time.sleep(1)  # Add delay to prevent inserting too quickly
        print(f"epoch {j+1}/10")
    hello_milvus.flush()
    print(f"Number of entities in hello_milvus: {hello_milvus.num_entities}")  # check the num_entites

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

    entities2 = [
        rng.random(num_entities).tolist(),  # field random, only supports list
        [str(i) for i in range(num_entities)],
        rng.random((num_entities, dim)),  # field embeddings, supports numpy.ndarray and list
    ]

    insert_result2 = hello_milvus2.insert(entities2)
    hello_milvus2.flush()
    insert_result2 = hello_milvus2.insert(entities2)
    hello_milvus2.flush()

    print(f"Number of entities in hello_milvus2: {hello_milvus2.num_entities}")  # check the num_entities


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="prepare data")
    args.add_argument("--uri", type=str, default="http://127.0.0.1:19530", help="Milvus server uri")
    args.add_argument("--token", type=str, default="root:Milvus", help="Milvus server token")
    args = args.parse_args()
    main(args.uri, args.token)
