from __future__ import annotations

from typing import Any, Dict, List, Tuple


def sanitize_plan(plan: Dict[str, Any], relevant_columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    LLM planını güvenli ve tutarlı hale getirir.
    - snapshot ise: time_filter '=' olmalı, '>=' gibi filtreleri kaldır
    - filters içinde snapshot_date_column'a dair redundant/çakışan koşulları kaldır
    - metric yoksa default count(distinct MUST_NO) yapmaya çalış
    - group_by yoksa ama soru 'bazında' diyorsa tahmin edilebilir (ileride)
    """

    # 1) time_filter normalize
    tf = plan.get("time_filter") or {}
    tf_type = tf.get("type", "none")

    snapshot_col = None
    snapshot_table = None
    snapshot_date = None

    if tf_type == "snapshot":
        snapshot_table = tf.get("table")
        snapshot_col = tf.get("snapshot_date_column")
        snapshot_date = tf.get("date")

    # 2) filters normalize
    filters = plan.get("filters", [])
    if not isinstance(filters, list):
        filters = []

    cleaned_filters: List[Dict[str, Any]] = []
    for f in filters:
        try:
            t = f.get("table")
            c = f.get("column")
            op = (f.get("op") or "").strip()
            val = f.get("value")

            # Snapshot sorusunda: snapshot_date_column için >=, <=, between gibi şeyleri at
            if tf_type == "snapshot" and snapshot_table and snapshot_col and snapshot_date:
                if t == snapshot_table and c == snapshot_col:
                    # sadece "=" eşitliği bırak, diğerlerini sil
                    if op != "=":
                        continue
                    # "=" bile olsa, time_filter zaten ekleyeceği için bunu da silebiliriz (redundant)
                    # ama bazı durumlarda planner time_filter'i boş bırakıp filtreye koyabilir.
                    # Biz time_filter varsa filtreyi kaldırıyoruz.
                    continue

            cleaned_filters.append(f)
        except Exception:
            # bozuk filtreyi komple at
            continue

    plan["filters"] = cleaned_filters
    return plan
