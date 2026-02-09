from __future__ import annotations

from typing import Any, Dict, List, Tuple
import pandas as pd

REQUIRED_COLUMNS = [
    "table_name",
    "column_name",
    "data_type",
    "description",
    "pii",
    "synonyms",
    "is_primary_key",
    "is_foreign_key",
    "references_table",
    "references_column",
    "semantic_role",
    "recommended_aggregation",
    "is_snapshot_table",
    "snapshot_date_column",
    "data_classification",
    "example_values",
]


def _to_bool(v: Any) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "evet"}


def _to_str(v: Any) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def _split_synonyms(v: Any) -> List[str]:
    s = _to_str(v)
    if not s:
        return []
    # öneri: "müşteri|musteri|customer" gibi
    s = s.replace(";", "|").replace(",", "|")
    parts = [p.strip() for p in s.split("|")]
    return [p for p in parts if p]


def build_catalog_from_df(df: pd.DataFrame) -> Dict[str, Any]:
    tables: Dict[str, Dict[str, Any]] = {}
    fk_edges: List[Dict[str, str]] = []
    columns_count = 0

    for _, row in df.iterrows():
        table = _to_str(row["table_name"])
        col = _to_str(row["column_name"])
        if not table or not col:
            continue

        columns_count += 1

        meta = {
            "table_name": table,
            "column_name": col,
            "data_type": _to_str(row["data_type"]),
            "description": _to_str(row["description"]),
            "pii": _to_bool(row["pii"]),
            "synonyms": _split_synonyms(row["synonyms"]),
            "is_primary_key": _to_bool(row["is_primary_key"]),
            "is_foreign_key": _to_bool(row["is_foreign_key"]),
            "references_table": _to_str(row["references_table"]),
            "references_column": _to_str(row["references_column"]),
            "semantic_role": _to_str(row["semantic_role"]),
            "recommended_aggregation": _to_str(row["recommended_aggregation"]),
            "is_snapshot_table": _to_bool(row["is_snapshot_table"]),
            "snapshot_date_column": _to_str(row["snapshot_date_column"]),
            "data_classification": _to_str(row["data_classification"]),
            "example_values": _to_str(row["example_values"]),
        }

        if table not in tables:
            tables[table] = {
                "table_name": table,
                "columns": [],
                "is_snapshot_table": False,
                "snapshot_date_column": "",
            }

        tables[table]["columns"].append(meta)

        if meta["is_snapshot_table"]:
            tables[table]["is_snapshot_table"] = True
            if meta["snapshot_date_column"]:
                tables[table]["snapshot_date_column"] = meta["snapshot_date_column"]

        if meta["is_foreign_key"] and meta["references_table"] and meta["references_column"]:
            fk_edges.append(
                {
                    "from_table": table,
                    "from_column": col,
                    "to_table": meta["references_table"],
                    "to_column": meta["references_column"],
                }
            )

    column_index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in tables.values():
        for c in t["columns"]:
            column_index[(c["table_name"], c["column_name"])] = c

    return {
        "tables": tables,
        "fk_edges": fk_edges,
        "column_index": column_index,
        "columns_count": columns_count,
    }
