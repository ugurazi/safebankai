from __future__ import annotations
from typing import Any, Dict, List, Tuple, Set


def _pairs_from_plan(plan: Dict[str, Any]) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    intent = plan.get("intent", "aggregate")

    if intent == "list_rows":
        for s in plan.get("select") or []:
            t, c = s.get("table"), s.get("column")
            if t and c:
                pairs.add((t, c))

    if intent == "aggregate":
        mc = ((plan.get("metric") or {}).get("column") or {})
        t, c = mc.get("table"), mc.get("column")
        if t and c:
            pairs.add((t, c))

        for g in plan.get("group_by") or []:
            t, c = g.get("table"), g.get("column")
            if t and c:
                pairs.add((t, c))

    tf = plan.get("time_filter") or {}
    if tf.get("type") == "snapshot":
        t, c = tf.get("table"), tf.get("snapshot_date_column")
        if t and c:
            pairs.add((t, c))

    for f in plan.get("filters") or []:
        t, c = f.get("table"), f.get("column")
        if t and c:
            pairs.add((t, c))

    return pairs


def _all_catalog_columns(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    build_catalog_from_df farklı şekillerde saklıyor olabilir.
    Burada hepsini normalize edip tek liste yapıyoruz.
    """
    # 1) direkt kolon listesi
    if isinstance(catalog.get("columns"), list):
        return catalog["columns"]

    # 2) farklı isim
    if isinstance(catalog.get("all_columns"), list):
        return catalog["all_columns"]

    # 3) tables altında kolonlar
    cols: List[Dict[str, Any]] = []
    tables = catalog.get("tables")

    # tables dict ise: { "MUST_TUM": { "columns":[...] } } veya { "MUST_TUM":[...] }
    if isinstance(tables, dict):
        for _, tv in tables.items():
            if isinstance(tv, dict) and isinstance(tv.get("columns"), list):
                cols.extend(tv["columns"])
            elif isinstance(tv, list):
                cols.extend(tv)

    # tables list ise: [{"table_name":"...","columns":[...]}]
    elif isinstance(tables, list):
        for t in tables:
            if isinstance(t, dict) and isinstance(t.get("columns"), list):
                cols.extend(t["columns"])

    return cols


def used_columns_from_plan(
    plan: Dict[str, Any],
    catalog: Dict[str, Any],
    relevant_columns: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    wanted = _pairs_from_plan(plan)

    # relevant index
    rel_index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in relevant_columns:
        key = (c.get("table_name"), c.get("column_name"))
        if key[0] and key[1]:
            rel_index[key] = c

    # catalog index (fallback)
    cat_index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in _all_catalog_columns(catalog):
        key = (c.get("table_name"), c.get("column_name"))
        if key[0] and key[1]:
            cat_index[key] = c

    out: List[Dict[str, Any]] = []
    for key in wanted:
        if key in rel_index:
            out.append(rel_index[key])
        elif key in cat_index:
            out.append(cat_index[key])

    return out
