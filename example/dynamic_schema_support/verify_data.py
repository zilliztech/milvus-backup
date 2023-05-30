import os
import time
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

hello_milvus = Collection("hello_milvus_recover")

print(fmt.format("Start Creating index for recovered collection"))
index = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 128},
}
hello_milvus.create_index("embeddings", index)


print(fmt.format("Start loading"))
hello_milvus.load()
# -----------------------------------------------------------------------------
# search based on vector similarity
print(fmt.format("Start searching based on vector similarity"))

rng = np.random.default_rng(seed=19530)
vectors_to_search = rng.random((1, dim))
search_params = {
    "metric_type": "L2",
    "params": {"nprobe": 10},
}

start_time = time.time()

print(fmt.format(f"Start search with retrieve all $meta fields."))
result = hello_milvus.search(vectors_to_search, "embeddings", search_params, limit=3, output_fields=["pk", "$meta"])
end_time = time.time()

for hits in result:
    for hit in hits:
        print(f"hit: {hit}")


expr = f'pk in ["1" , "2"] || g == 1'

print(fmt.format(f"Start query with expr `{expr}`"))
result = hello_milvus.query(expr=expr, output_fields=["random", "a", "g"])
for hit in result:
    print("hit:", hit)