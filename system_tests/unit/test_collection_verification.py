from backup_restore.verification.collection import assert_collection_restored


class ObservableMilvusClient:
    def __init__(self, collection_name, collection_id):
        self.collection_name = collection_name
        self.loaded = []
        self._description = {
            "collection_name": collection_name,
            "collection_id": collection_id,
            "auto_id": False,
            "enable_dynamic_field": False,
            "fields": [
                {"field_id": 100, "name": "id", "type": 5, "is_primary": True},
                {"field_id": 101, "name": "category", "type": 21},
                {"field_id": 102, "name": "value", "type": 5},
                {"field_id": 103, "name": "vector", "type": 101},
            ],
        }
        self._rows = [
            {
                "id": 1,
                "category": "group-1",
                "value": 10,
                "vector": [1.0, 2.0],
            },
            {
                "id": 0,
                "category": "group-0",
                "value": 0,
                "vector": [0.0, 1.0],
            },
        ]

    def describe_collection(self, collection_name):
        return self._description

    def list_partitions(self, collection_name):
        return ["_default"]

    def list_indexes(self, collection_name):
        return ["vector_idx"]

    def describe_index(self, collection_name, index_name):
        return {
            "index_name": "vector_idx",
            "field_name": "vector",
            "index_type": "FLAT",
            "metric_type": "L2",
            "state": 3,
            "total_rows": 2,
        }

    def load_collection(self, collection_name):
        self.loaded.append(collection_name)

    def query(self, collection_name, **kwargs):
        return self._rows

    def search(self, collection_name, **kwargs):
        return [[{"id": 0, "distance": 0.0}, {"id": 1, "distance": 2.0}]]


def test_restored_collection_matches_source_observable_state():
    source = ObservableMilvusClient("source_collection", collection_id=10)
    target = ObservableMilvusClient("restored_collection", collection_id=20)

    assert_collection_restored(
        source=source,
        target=target,
        source_collection="source_collection",
        target_collection="restored_collection",
        query_vector=[0.0, 1.0],
        expected_row_count=2,
    )

    assert source.loaded == ["source_collection"]
    assert target.loaded == ["restored_collection"]
