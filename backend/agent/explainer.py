from __future__ import annotations
from typing import Any, Dict, List


def explain(plan: Dict[str, Any]) -> str:
    intent = plan.get("intent", "aggregate")

    # ---------- LIST ROWS ----------
    if intent == "list_rows":
        sel = plan.get("select") or []
        if sel:
            cols = ", ".join([f"{s['table']}.{s['column']}" for s in sel])
            parts: List[str] = [f"Satır listeleme sorgusu üretildi. Seçilen kolonlar: {cols}."]
        else:
            parts = ["Satır listeleme sorgusu üretildi."]

        tf = plan.get("time_filter") or {}
        if tf.get("type") == "snapshot":
            parts.append(
                f"Snapshot filtresi uygulandı: "
                f"{tf['table']}.{tf['snapshot_date_column']} = {tf['date']}."
            )

        fs = plan.get("filters") or []
        if fs:
            parts.append(f"Uygulanan filtre sayısı: {len(fs)}.")

        return " ".join(parts)

    # ---------- AGGREGATE ----------
    m = plan.get("metric", {})
    mc = m.get("column") or {}

    metric_name = m.get("type", "count").upper()
    metric_expr = (
        f"{metric_name}(DISTINCT {mc.get('table')}.{mc.get('column')})"
        if m.get("distinct")
        else f"{metric_name}({mc.get('table')}.{mc.get('column')})"
    )

    parts: List[str] = [f"Agregasyon sorgusu üretildi: {metric_expr}."]

    tf = plan.get("time_filter") or {}
    if tf.get("type") == "snapshot":
        parts.append(
            f"Snapshot filtresi uygulandı: "
            f"{tf['table']}.{tf['snapshot_date_column']} = {tf['date']}."
        )

    fs = plan.get("filters") or []
    if fs:
        parts.append(f"Uygulanan filtre sayısı: {len(fs)}.")

    gb = plan.get("group_by") or []
    if gb:
        gb_cols = ", ".join([f"{g['table']}.{g['column']}" for g in gb])
        parts.append(f"Gruplama şu kolonlara göre yapıldı: {gb_cols}.")

    return " ".join(parts)
