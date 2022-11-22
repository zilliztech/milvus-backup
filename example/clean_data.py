from pymilvus import (
    connections,
    utility,
)

fmt = "\n=== {:30} ===\n"

print(fmt.format("start connecting to Milvus"))
connections.connect("default", host="localhost", port="19530")
recover_collection_name = "hello_milvus_recover"

print(fmt.format("Drop collection `hello_milvus`"))
utility.drop_collection("hello_milvus")

print(fmt.format("Drop collection `hello_milvus2`"))
utility.drop_collection("hello_milvus2")

print(fmt.format(f"Drop collection `hello_milvus_recover`"))
utility.drop_collection(recover_collection_name)

print(fmt.format(f"Drop collection `hello_milvus2_recover`"))
utility.drop_collection("hello_milvus2_recover")