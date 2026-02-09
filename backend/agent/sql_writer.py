from __future__ import annotations

from typing import Any, Dict, List, Set


def _sql_quote(value: Any) -> str:
    """
    Planner bazen value'yu "'X'" gibi zaten tırnaklı döndürebilir.
    Biz tek bir kez güvenli şekilde tırnaklayacağız.
    """
    if value is None:
        return "NULL"

    s = str(value).strip()

    # dış tek/double tırnakları soy
    if len(s) >= 2 and ((s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"')):
        s = s[1:-1].strip()

    # iç tek tırnakları escape et (SQL standard: '' )
    s = s.replace("'", "''")

    return f"'{s}'"


def _tables_used_in_plan(plan: Dict[str, Any]) -> Set[str]:
    """
    Plan'da gerçekten kullanılan tabloları çıkarır.
    Bu sayede SELECT/GROUP BY/FILTER/TIME_FILTER içinde hiç kullanılmayan dimension join'lerini atarız.
    """
    used: Set[str] = set()
    intent = plan.get("intent", "aggregate")

    # list_rows select
    if intent == "list_rows":
        for s in plan.get("select") or []:
            t = s.get("table")
            if t:
                used.add(t)

    # aggregate metric + group_by
    if intent == "aggregate":
        mc = ((plan.get("metric") or {}).get("column") or {})
        if mc.get("table"):
            used.add(mc["table"])

        for g in plan.get("group_by") or []:
            if g.get("table"):
                used.add(g["table"])

    # time_filter
    tf = plan.get("time_filter") or {}
    if tf.get("type") == "snapshot" and tf.get("table"):
        used.add(tf["table"])

    # filters
    for f in plan.get("filters") or []:
        if f.get("table"):
            used.add(f["table"])

    return used


def write_sql_mysql(plan: Dict[str, Any]) -> str:
    intent = plan.get("intent", "aggregate")

    # helper alias mapping
    alias_map: Dict[str, str] = {}
    next_alias = 1

    def alias_for(table: str) -> str:
        nonlocal next_alias
        if table not in alias_map:
            alias_map[table] = "m" if not alias_map else f"t{next_alias}"
            next_alias += 1
        return alias_map[table]

    def qname(t: str, c: str) -> str:
        return f"{alias_for(t)}.{c}"

    used_tables = _tables_used_in_plan(plan)

    # ---------- LIST ROWS ----------
    if intent == "list_rows":
        select_cols = plan.get("select") or []
        if not select_cols:
            select_cols = [{"table": "MUST_TUM", "column": "MUST_NO"}]

        base_table = select_cols[0]["table"]
        alias_for(base_table)

        select_parts = [f"{qname(s['table'], s['column'])} AS {s['column']}" for s in select_cols]
        sql = f"SELECT {', '.join(select_parts)}\nFROM {base_table} {alias_map[base_table]}\n"

        # joins (✅ prune)
        for j in plan.get("joins", []):
            ft, fc = j["from_table"], j["from_column"]
            tt, tc = j["to_table"], j["to_column"]

            # hedef tablo hiç kullanılmıyorsa join atla
            if tt not in used_tables:
                continue

            alias_for(ft)
            alias_for(tt)
            sql += f"JOIN {tt} {alias_map[tt]} ON {qname(ft, fc)} = {qname(tt, tc)}\n"

        # where
        where_parts: List[str] = []
        tf = plan.get("time_filter") or {}
        if tf.get("type") == "snapshot":
            where_parts.append(
                f"{qname(tf['table'], tf['snapshot_date_column'])} = {_sql_quote(tf['date'])}"
            )

        for f in plan.get("filters", []):
            t, c, op, val = f["table"], f["column"], f["op"], f["value"]

            # snapshot_date_column için redundant filtreyi at
            if tf.get("type") == "snapshot" and t == tf.get("table") and c == tf.get("snapshot_date_column"):
                continue

            where_parts.append(f"{qname(t, c)} {op} {_sql_quote(val)}")

        if where_parts:
            sql += "WHERE " + "\n  AND ".join(where_parts) + "\n"

        sql = sql.rstrip() + "\nLIMIT 1000;"
        return sql

    # ---------- AGGREGATE ----------
    metric_col = plan["metric"]["column"]
    base_table = metric_col["table"]
    alias_for(base_table)

    select_parts: List[str] = []

    # group_by columns in select
    for gb in plan.get("group_by", []):
        select_parts.append(f"{qname(gb['table'], gb['column'])} AS {gb['column']}")

    mtype = plan["metric"]["type"]
    distinct = plan["metric"].get("distinct", False)
    col_expr = qname(metric_col["table"], metric_col["column"])

    if mtype == "count":
        metric_expr = f"COUNT(DISTINCT {col_expr})" if distinct else f"COUNT({col_expr})"
    elif mtype == "sum":
        metric_expr = f"SUM({col_expr})"
    elif mtype == "avg":
        metric_expr = f"AVG({col_expr})"
    else:
        metric_expr = f"COUNT(DISTINCT {col_expr})"

    select_parts.append(f"{metric_expr} AS result")

    sql = f"SELECT {', '.join(select_parts)}\nFROM {base_table} {alias_map[base_table]}\n"

    # joins (✅ prune)
    for j in plan.get("joins", []):
        ft, fc = j["from_table"], j["from_column"]
        tt, tc = j["to_table"], j["to_column"]

        # hedef tablo hiç kullanılmıyorsa join atla
        if tt not in used_tables:
            continue

        alias_for(ft)
        alias_for(tt)
        sql += f"JOIN {tt} {alias_map[tt]} ON {qname(ft, fc)} = {qname(tt, tc)}\n"

    # where
    where_parts: List[str] = []
    tf = plan.get("time_filter") or {}
    if tf.get("type") == "snapshot":
        where_parts.append(
            f"{qname(tf['table'], tf['snapshot_date_column'])} = {_sql_quote(tf['date'])}"
        )

    for f in plan.get("filters", []):
        t, c, op, val = f["table"], f["column"], f["op"], f["value"]

        if tf.get("type") == "snapshot" and t == tf.get("table") and c == tf.get("snapshot_date_column"):
            continue

        where_parts.append(f"{qname(t, c)} {op} {_sql_quote(val)}")

    if where_parts:
        sql += "WHERE " + "\n  AND ".join(where_parts) + "\n"

    if plan.get("group_by"):
        gb_expr = ", ".join([qname(g["table"], g["column"]) for g in plan["group_by"]])
        sql += f"GROUP BY {gb_expr}\n"

    sql = sql.rstrip() + "\nLIMIT 1000;"
    return sql
