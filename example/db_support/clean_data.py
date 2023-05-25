import os
from pymilvus import (
    connections,
    db,
    utility,
)

fmt = "\n=== {:30} ===\n"

print(fmt.format("start connecting to Milvus"))
host = os.environ.get('MILVUS_HOST')
if host == None:
    host = "localhost"
print(fmt.format(f"Milvus host: {host}"))
connections.connect("default", host=host, port="19530")

db.using_database(db_name="db1")
print(fmt.format("Drop collection `db1.hello_milvus`"))
utility.drop_collection("hello_milvus")

print(fmt.format(f"Drop collection `db1.hello_milvus_recover`"))
utility.drop_collection("hello_milvus_recover")

db.using_database(db_name="db2")
print(fmt.format("Drop collection `db2.hello_milvus2`"))
utility.drop_collection("hello_milvus2")

print(fmt.format(f"Drop collection `db2.hello_milvus2_recover`"))
utility.drop_collection("hello_milvus2_recover")