from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import numpy as np
import random
import time

connections.connect()
dim = 128
int64_field = FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True)
double_field = FieldSchema(name="nullableFid", dtype=DataType.DOUBLE, nullable=True, is_primary=False, is_clustering_key=True)
int32_field = FieldSchema(name="int32", dtype=DataType.INT64, default_value=10)
string_field = FieldSchema(name="string", dtype=DataType.VARCHAR, max_length=1000, default_value="10")
float_vector = FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim, mmap_enabled=True)
schema = CollectionSchema(fields=[int64_field, double_field, int32_field, string_field, float_vector])
utility.drop_collection("null_test")
collection = Collection("null_test", schema=schema)
res = collection.schema
print(res)


nb = 10000
slice = 100
for i in range(int(nb/slice)):
    vectors = [[random.random() for _ in range(dim)] for _ in range(slice)]
    data = [[j for j in range(i*slice,(i+1)*slice)], [None for _ in range(slice)],[], ["1" for _ in range(slice)], vectors]
    collection.insert(data=data)
    print("inserted %d %d" %(i, slice))


collection.flush()
res = collection.num_entities
print(res)