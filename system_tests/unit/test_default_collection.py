from backup_restore.datasets.default_collection import prepare_default_collection


class RecordingMilvusClient:
    def __init__(self):
        self.collection = None
        self.rows = []
        self.flushed = []

    def create_collection(self, **kwargs):
        self.collection = kwargs

    def insert(self, collection_name, data):
        self.rows.extend(data)

    def flush(self, collection_name):
        self.flushed.append(collection_name)


def test_default_collection_contains_deterministic_scalar_and_vector_data():
    client = RecordingMilvusClient()

    dataset = prepare_default_collection(
        client,
        collection_name="source_collection",
        row_count=3,
        dimension=4,
    )

    assert dataset.rows == [
        {
            "id": 0,
            "category": "group-0",
            "value": 0,
            "vector": [0.0, 1.0, 2.0, 3.0],
        },
        {
            "id": 1,
            "category": "group-1",
            "value": 10,
            "vector": [1.0, 2.0, 3.0, 4.0],
        },
        {
            "id": 2,
            "category": "group-2",
            "value": 20,
            "vector": [2.0, 3.0, 4.0, 5.0],
        },
    ]
    assert client.rows == dataset.rows
    assert client.collection["collection_name"] == "source_collection"
    assert [field.name for field in client.collection["schema"].fields] == [
        "id",
        "category",
        "value",
        "vector",
    ]
    index = client.collection["index_params"][0].to_dict()
    assert index["index_name"] == "vector_idx"
    assert index["index_type"] == "FLAT"
    assert client.flushed == ["source_collection"]
