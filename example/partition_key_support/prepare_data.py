# hello_milvus.py demonstrates the basic operations of PyMilvus, a Python SDK of Milvus.
# 1. connect to Milvus
# 2. create collection
# 3. insert data
# 4. create index
# 5. search, query, and hybrid search on entities
# 6. delete entities by PK
# 7. drop collection

import os
import time
import random
import string
import numpy as np
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)

fmt = "\n=== {:30} ===\n"
dim = 8

print(fmt.format("start connecting to Milvus"))
host = os.environ.get('MILVUS_HOST')
if host == None:
    host = "localhost"
print(fmt.format(f"Milvus host: {host}"))
connections.connect("default", host=host, port="19530")

has = utility.has_collection("hello_milvus")
print(f"Does collection hello_milvus exist in Milvus: {has}")

default_fields = [
    FieldSchema(name="count", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="key", dtype=DataType.INT64),
    FieldSchema(name="random", dtype=DataType.DOUBLE),
    FieldSchema(name="var", dtype=DataType.VARCHAR, max_length=10000, is_primary=False),
    FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dim)
]
default_schema = CollectionSchema(fields=default_fields, description="test partition-key collection", partition_key_field="key")
hello_milvus = Collection(name="hello_milvus_pk", schema=default_schema, shard_num=1, num_partitions=20)

nb = 20

rng = np.random.default_rng(seed=19530)
random_data = rng.random(nb).tolist()

vec_data = [[random.random() for _ in range(dim)] for _ in range(nb)]
_len = int(20)
_str = string.ascii_letters + string.digits
_s = _str
print("_str size ", len(_str))

for i in range(int(_len / len(_str))):
    _s += _str
    print("append str ", i)
values = [''.join(random.sample(_s, _len - 1)) for _ in range(nb)]
index = 0
while index < 20:
    # insert data
    data = [
        [index * nb + i for i in range(nb)],
        [(index * nb + i) % 256 for i in range(nb)],
        random_data,
        values,
        vec_data,
    ]
    res = hello_milvus.insert(data)
    print("insert done", index)
    index += 1
hello_milvus.flush()

print(f"Number of entities in Milvus: {hello_milvus.num_entities}")  # check the num_entites

# 4. create index
print(fmt.format("Start Creating index IVF_FLAT"))
index = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 128},
}

hello_milvus.create_index("embeddings", index)
