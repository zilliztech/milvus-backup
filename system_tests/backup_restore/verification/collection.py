from __future__ import annotations

from typing import Any

_SCHEMA_KEYS = ("auto_id", "enable_dynamic_field", "consistency_level", "num_shards")
_FIELD_KEYS = (
    "name",
    "type",
    "params",
    "is_primary",
    "auto_id",
    "is_partition_key",
    "is_clustering_key",
    "nullable",
    "default_value",
    "element_type",
)
_INDEX_KEYS = ("index_name", "field_name", "index_type", "metric_type", "params")


def assert_collection_restored(
    *,
    source: Any,
    target: Any,
    source_collection: str,
    target_collection: str,
    query_vector: list[float],
    expected_row_count: int,
) -> None:
    source_schema = _schema(source.describe_collection(source_collection))
    target_schema = _schema(target.describe_collection(target_collection))
    assert target_schema == source_schema, (
        f"restored schema differs from source: source={source_schema!r}, "
        f"target={target_schema!r}"
    )

    source_partitions = sorted(source.list_partitions(source_collection))
    target_partitions = sorted(target.list_partitions(target_collection))
    assert target_partitions == source_partitions, (
        f"restored partitions differ from source: source={source_partitions!r}, "
        f"target={target_partitions!r}"
    )

    source_indexes = _indexes(source, source_collection)
    target_indexes = _indexes(target, target_collection)
    assert target_indexes == source_indexes, (
        f"restored indexes differ from source: source={source_indexes!r}, "
        f"target={target_indexes!r}"
    )

    source.load_collection(source_collection)
    target.load_collection(target_collection)
    query_options = {
        "filter": "id >= 0",
        "output_fields": ["id", "category", "value", "vector"],
        "limit": expected_row_count,
        "consistency_level": "Strong",
    }
    source_rows = sorted(
        source.query(source_collection, **query_options), key=lambda row: row["id"]
    )
    target_rows = sorted(
        target.query(target_collection, **query_options), key=lambda row: row["id"]
    )
    assert len(source_rows) == expected_row_count
    assert len(target_rows) == expected_row_count
    assert target_rows == source_rows, (
        "restored scalar or vector data differs from source"
    )

    search_options = {
        "data": [query_vector],
        "anns_field": "vector",
        "limit": min(5, expected_row_count),
        "search_params": {"metric_type": "L2", "params": {}},
        "consistency_level": "Strong",
    }
    source_ids = _search_ids(source.search(source_collection, **search_options))
    target_ids = _search_ids(target.search(target_collection, **search_options))
    assert target_ids == source_ids, (
        f"restored search results differ from source: source={source_ids!r}, "
        f"target={target_ids!r}"
    )


def _schema(description: dict[str, Any]) -> dict[str, Any]:
    schema = {key: description[key] for key in _SCHEMA_KEYS if key in description}
    schema["fields"] = [
        {key: field[key] for key in _FIELD_KEYS if key in field}
        for field in description["fields"]
    ]
    return schema


def _indexes(client: Any, collection_name: str) -> list[dict[str, Any]]:
    return sorted(
        (
            {key: description[key] for key in _INDEX_KEYS if key in description}
            for description in (
                client.describe_index(collection_name, index_name)
                for index_name in client.list_indexes(collection_name)
            )
        ),
        key=lambda index: index["index_name"],
    )


def _search_ids(results: list[list[dict[str, Any]]]) -> list[int | str]:
    return [hit["id"] for hit in results[0]]
