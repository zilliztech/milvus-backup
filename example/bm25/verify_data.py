# hello_bm25.py demonstrates how to insert raw data only into Milvus and perform
# sparse vector based ANN search using BM25 algorithm.
# 1. connect to Milvus
# 2. create collection
# 3. insert data
# 4. create index
# 5. search, query, and filtering search on entities
# 6. delete entities by PK
# 7. drop collection
import time

from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, Function, DataType, FunctionType,
    Collection,
)

fmt = "\n=== {:30} ===\n"
search_latency_fmt = "search latency = {:.4f}s"

#################################################################################
# 1. connect to Milvus
# Add a new connection alias `default` for Milvus server in `localhost:19530`
print(fmt.format("start connecting to Milvus"))
connections.connect("default", host="localhost", port="19530")

has = utility.has_collection("hello_bm25")
print(f"Does collection hello_bm25 exist in Milvus: {has}")

#################################################################################
# 2. create collection
# We're going to create a collection with 2 explicit fields and a function.
# +-+------------+------------+------------------+------------------------------+
# | | field name | field type | other attributes |       field description      |
# +-+------------+------------+------------------+------------------------------+
# |1|    "id"    |   INT64    |  is_primary=True |      "primary field"         |
# | |            |            |   auto_id=False  |                              |
# +-+------------+------------+------------------+------------------------------+
# |2| "document" | VarChar    |                  |     "raw text document"      |
# +-+------------+------------+------------------+------------------------------+
#
# Function 'bm25' is used to convert raw text document to a sparse vector representation
# and store it in the 'sparse' field.
# +-+------------+-------------------+-----------+------------------------------+
# | | field name | field type        | other attr|       field description      |
# +-+------------+-------------------+-----------+------------------------------+
# |3|  "sparse"  |SPARSE_FLOAT_VECTOR|           |                              |
# +-+------------+-------------------+-----------+------------------------------+
#
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
    FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=1000, enable_analyzer=True),
]

bm25_function = Function(
    name="bm25",
    function_type=FunctionType.BM25,
    input_field_names=["document"],
    output_field_names="sparse",
)

schema = CollectionSchema(fields, "hello_bm25 demo")
schema.add_function(bm25_function)

print(fmt.format("Create collection `hello_bm25`"))
hello_bm25 = Collection("hello_bm25")

num_entities = 6

entities = [
    [f"This is a test document {i + hello_bm25.num_entities}" for i in range(num_entities)],
]


# Before conducting a search or a query, you need to load the data in `hello_bm25` into memory.
print(fmt.format("Start loading"))
hello_bm25.load()

# -----------------------------------------------------------------------------
print(fmt.format("Start searching based on BM25 texts relevance using sparse vector ANN search"))
texts_to_search = entities[-1][-2:]
print(fmt.format(f"texts_to_search: {texts_to_search}"))
search_params = {
    "metric_type": "BM25",
    "params": {},
}

start_time = time.time()
result = hello_bm25.search(texts_to_search, "sparse", search_params, limit=3, output_fields=["document"], consistency_level="Strong")
end_time = time.time()

for hits, text in zip(result, texts_to_search):
    print(f"result of text: {text}")
    for hit in hits:
        print(f"\thit: {hit}, document field: {hit.entity.get('document')}")
print(search_latency_fmt.format(end_time - start_time))

# -----------------------------------------------------------------------------
# query based on scalar filtering(boolean, int, etc.)
filter_id = 1
print(fmt.format(f"Start querying with `id > {filter_id}`"))

start_time = time.time()
result = hello_bm25.query(expr=f"id > {filter_id}", output_fields=["document"])
end_time = time.time()

print(f"query result:\n-{result[0]}")
print(search_latency_fmt.format(end_time - start_time))

# -----------------------------------------------------------------------------
# pagination
r1 = hello_bm25.query(expr=f"id > {filter_id}", limit=3, output_fields=["document"])
r2 = hello_bm25.query(expr=f"id > {filter_id}", offset=1, limit=2, output_fields=["document"])
print(f"query pagination(limit=3):\n\t{r1}")
print(f"query pagination(offset=1, limit=2):\n\t{r2}")


# -----------------------------------------------------------------------------
# scalar filtering search
print(fmt.format(f"Start filtered searching with `id > {filter_id}`"))

start_time = time.time()
result = hello_bm25.search(texts_to_search, "sparse", search_params, limit=3, expr=f"id > {filter_id}", output_fields=["document"])
end_time = time.time()

for hits, text in zip(result, texts_to_search):
    print(f"result of text: {text}")
    for hit in hits:
        print(f"\thit: {hit}, document field: {hit.entity.get('document')}")
print(search_latency_fmt.format(end_time - start_time))

###############################################################################
# 6. delete entities by PK
# You can delete entities by their PK values using boolean expressions.
# 
# expr = f'id in [{ids[0]}, {ids[1]}]'
# print(fmt.format(f"Start deleting with expr `{expr}`"))
# 
# result = hello_bm25.query(expr=expr, output_fields=["document"])
# print(f"query before delete by expr=`{expr}` -> result: \n- {result[0]}\n- {result[1]}\n")
# 
# hello_bm25.delete(expr)
# 
# result = hello_bm25.query(expr=expr, output_fields=["document"])
# print(f"query after delete by expr=`{expr}` -> result: {result}\n")
# 
# ###############################################################################
# # 7. upsert by PK
# # You can upsert data to replace existing data.
# target_id = ids[2]
# print(fmt.format(f"Start upsert operation for id {target_id}"))
# 
# # Query before upsert
# result_before = hello_bm25.query(expr=f"id == {target_id}", output_fields=["id", "document"])
# print(f"Query before upsert (id={target_id}):\n{result_before}")
# 
# # Prepare data for upsert
# upsert_data = [
#     [target_id],
#     ["This is an upserted document for testing purposes."]
# ]
# 
# # Perform upsert operation
# hello_bm25.upsert(upsert_data)
# 
# # Query after upsert
# result_after = hello_bm25.query(expr=f"id == {target_id}", output_fields=["id", "document"])
# print(f"Query after upsert (id={target_id}):\n{result_after}")
# 
# 
# ###############################################################################
# # 7. drop collection
# # Finally, drop the hello_bm25 collection
# print(fmt.format("Drop collection `hello_bm25`"))
# utility.drop_collection("hello_bm25")