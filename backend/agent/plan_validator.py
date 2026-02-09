from __future__ import annotations
from typing import Any, Dict, List, Set, Tuple

def validate_plan_against_catalog(plan: Dict[str, Any], relevant_columns: List[Dict[str, Any]]) -> None:
    allowed: Set[Tuple[str, str]] = set((c["table_name"], c["column_name"]) for c in relevant_columns)
    allowed_tables: Set[str] = set(t for t, _ in allowed)

    def check_col(t: str, c: str):
        if (t, c) not in allowed:
            raise ValueError(f"Plan uses non-candidate column: {t}.{c}")

    # intent
    intent = plan.get("intent")
    if intent not in {"aggregate", "list_rows"}:
        raise ValueError("Invalid intent")

    # aggregate
    if intent == "aggregate":
        mc = plan["metric"]["column"]
        check_col(mc["table"], mc["column"])

    # list_rows
    if intent == "list_rows":
        sel = plan.get("select", [])
        if not sel:
            raise ValueError("list_rows intent requires select[]")
        for s in sel:
            check_col(s["table"], s["column"])

    # filters
    for f in plan.get("filters", []):
        check_col(f["table"], f["column"])

    # group_by
    for g in plan.get("group_by", []):
        check_col(g["table"], g["column"])

    # time_filter
    tf = plan.get("time_filter") or {}
    if tf.get("type") == "snapshot":
        check_col(tf["table"], tf["snapshot_date_column"])

    # joins: sadece allowed tablolar arasında olmalı (kolonlar candidates'ta olmayabilir diye tablo bazlı kontrol)
    for j in plan.get("joins", []):
        if j["from_table"] not in allowed_tables or j["to_table"] not in allowed_tables:
            raise ValueError("Join uses non-candidate table")
