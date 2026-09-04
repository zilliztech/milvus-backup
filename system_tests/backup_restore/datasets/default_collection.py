from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pymilvus import DataType, MilvusClient


@dataclass(frozen=True)
class DefaultCollection:
    name: str
    dimension: int
    rows: list[dict[str, Any]]


def prepare_default_collection(
    client: Any,
    *,
    collection_name: str,
    row_count: int,
    dimension: int,
) -> DefaultCollection:
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name="category", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="value", datatype=DataType.INT64)
    schema.add_field(
        field_name="vector",
        datatype=DataType.FLOAT_VECTOR,
        dim=dimension,
    )

    index_params = MilvusClient.prepare_index_params()
    index_params.add_index(
        field_name="vector",
        index_name="vector_idx",
        index_type="FLAT",
        metric_type="L2",
    )

    rows = [
        {
            "id": row_id,
            "category": f"group-{row_id % 3}",
            "value": row_id * 10,
            "vector": [float(row_id + offset) for offset in range(dimension)],
        }
        for row_id in range(row_count)
    ]

    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=index_params,
    )
    client.insert(collection_name=collection_name, data=rows)
    client.flush(collection_name=collection_name)

    return DefaultCollection(
        name=collection_name,
        dimension=dimension,
        rows=rows,
    )
