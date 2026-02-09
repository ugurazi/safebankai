from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from agent.ollama_client import OllamaClient


PLANNER_SYSTEM = """
You are a Text-to-SQL planning agent.

OUTPUT:
- Output ONLY valid JSON. No markdown. No extra text.

RULES:
- Use ONLY the provided schema candidates. Do not invent tables/columns.
- Decide intent:
  - If question includes words like "listele", "list", "göster", "getir", "ver", "tablo" then intent="list_rows".
  - Otherwise intent="aggregate".
- For intent="aggregate":
  - If question asks "müşteri adedi", use COUNT(DISTINCT MUST_NO) when available.
  - If question asks "şube bazında", GROUP BY SUBE_KOD.
- For intent="list_rows":
  - Select 3-6 non-PII columns that help identify the rows (prefer MUST_NO, SUBE_KOD, IL_KOD, ISKOLU_KOD, snapshot_date).
  - NEVER select PII columns like MUST_TC if pii=true.
- If the user provides a date like 31.12.2025, convert it to YYYY-MM-DD.
- If question includes "özel bankacılık", apply filter ISKOLU_KOD = 'X' when candidate indicates example_values 'X'.
- If is_snapshot_table=true and user gave a specific date, apply time_filter on snapshot_date_column for that date.
- If question asks "şube adı" or "şube ismi" and SUBE_DIM.SUBE_AD is available among candidates,
  then group by SUBE_DIM.SUBE_AD and join MUST_TUM.SUBE_KOD -> SUBE_DIM.SUBE_KOD.
- Prefer human-readable dimension names for grouping if available:
  - branch: SUBE_DIM.SUBE_AD
  - city: IL_DIM.IL_AD (if exists)
"""


def _extract_allowed_pairs(relevant_columns: List[Dict[str, Any]]) -> set[Tuple[str, str]]:
    return set((c["table_name"], c["column_name"]) for c in relevant_columns)


def _is_list_question(q: str) -> bool:
    q = q.lower()
    keywords = ["listele", "list", "göster", "getir", "ver", "tablo", "kayıt", "satır"]
    return any(k in q for k in keywords)


def plan_question(
    question: str,
    relevant_columns: List[Dict[str, Any]],
    ollama: OllamaClient | None = None
) -> Dict[str, Any]:
    ollama = ollama or OllamaClient()

    qlower = question.lower()

    # candidates
    candidates = []
    for c in relevant_columns:
        candidates.append(
            {
                "table": c["table_name"],
                "column": c["column_name"],
                "data_type": c.get("data_type", ""),
                "description": c.get("description", ""),
                "synonyms": c.get("synonyms", []),
                "pii": bool(c.get("pii", False)),
                "is_primary_key": c.get("is_primary_key", False),
                "is_foreign_key": c.get("is_foreign_key", False),
                "references_table": c.get("references_table", ""),
                "references_column": c.get("references_column", ""),
                "semantic_role": c.get("semantic_role", ""),
                "is_snapshot_table": c.get("is_snapshot_table", False),
                "snapshot_date_column": c.get("snapshot_date_column", ""),
                "example_values": c.get("example_values", ""),
            }
        )

    # ✅ retriever boşsa deterministic fallback
    if not candidates:
        if _is_list_question(question):
            return {
                "intent": "list_rows",
                "select": [
                    {"table": "MUST_TUM", "column": "MUST_NO"},
                    {"table": "MUST_TUM", "column": "ISKOLU_KOD"},
                    {"table": "MUST_TUM", "column": "SUBE_KOD"},
                    {"table": "MUST_TUM", "column": "IL_KOD"},
                    {"table": "MUST_TUM", "column": "snapshot_date"},
                ],
                "filters": [],
                "group_by": [],
                "time_filter": {"type": "none", "date": "", "table": "", "snapshot_date_column": ""},
                "joins": [],
            }
        return {
            "intent": "aggregate",
            "metric": {"type": "count", "distinct": True, "column": {"table": "MUST_TUM", "column": "MUST_NO"}},
            "filters": [],
            "group_by": [],
            "time_filter": {"type": "none", "date": "", "table": "", "snapshot_date_column": ""},
            "joins": [],
        }

    prompt = f"""
USER_QUESTION:
{question}

SCHEMA_CANDIDATES (only use these):
{json.dumps(candidates, ensure_ascii=False, indent=2)}

Return a JSON plan with this exact shape:

{{
  "intent": "aggregate|list_rows",

  "metric": {{
    "type": "count|sum|avg",
    "distinct": true|false,
    "column": {{"table":"...","column":"..."}}
  }},

  "select": [
    {{"table":"...","column":"..."}}
  ],

  "filters": [
    {{"table":"...","column":"...","op":"=","value":"..."}}
  ],

  "group_by": [
    {{"table":"...","column":"..."}}
  ],

  "time_filter": {{
    "type": "snapshot|none",
    "date": "YYYY-MM-DD",
    "table": "...",
    "snapshot_date_column": "..."
  }},

  "joins": [
    {{"from_table":"...","from_column":"...","to_table":"...","to_column":"..."}}
  ]
}}

Notes:
- If intent="aggregate", you MUST fill metric and may fill group_by/time_filter/filters.
- If intent="list_rows", you MUST fill select and may fill time_filter/filters. metric/group_by should be empty or omitted.
"""

    raw = ollama.generate(prompt=prompt, system=PLANNER_SYSTEM, temperature=0.0).strip()

    # JSON extract
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"Planner did not return JSON. Raw: {raw[:300]}")
    plan = json.loads(raw[start:end + 1])

    allowed = _extract_allowed_pairs(relevant_columns)
    def ok_pair(t: str, c: str) -> bool:
        return (t, c) in allowed

    # intent normalize
    intent = plan.get("intent")
    if intent not in {"aggregate", "list_rows"}:
        intent = "list_rows" if _is_list_question(question) else "aggregate"
    plan["intent"] = intent

    # select sanitize (list_rows)
    if intent == "list_rows":
        sel = plan.get("select") or []
        good_sel = []
        for s in sel:
            t, c = s.get("table"), s.get("column")
            if t and c and ok_pair(t, c):
                good_sel.append({"table": t, "column": c})
        if not good_sel:
            safe = [c for c in candidates if not c.get("pii")]
            safe = safe[:5]
            good_sel = [{"table": x["table"], "column": x["column"]} for x in safe]
        plan["select"] = good_sel

    # metric sanity (aggregate)
    if intent == "aggregate":
        mc = ((plan.get("metric") or {}).get("column") or {})
        if not ok_pair(mc.get("table", ""), mc.get("column", "")):
            # fallback: MUST_NO varsa onu seç
            if ok_pair("MUST_TUM", "MUST_NO"):
                plan["metric"] = {"type": "count", "distinct": True, "column": {"table": "MUST_TUM", "column": "MUST_NO"}}

    # ✅✅ HARD ENFORCE: "şube adı/isimi" => SUBE_DIM.SUBE_AD ile group + join
    wants_branch_name = ("şube adı" in qlower) or ("sube adi" in qlower) or ("şube ismi" in qlower) or ("sube ismi" in qlower)
    if wants_branch_name and intent == "aggregate":
        need_pairs = {
            ("SUBE_DIM", "SUBE_AD"),
            ("SUBE_DIM", "SUBE_KOD"),
            ("MUST_TUM", "SUBE_KOD"),
        }
        if all(p in allowed for p in need_pairs):
            # group_by override
            plan["group_by"] = [{"table": "SUBE_DIM", "column": "SUBE_AD"}]

            # join ensure
            joins = plan.get("joins") or []
            already = any(
                j.get("from_table") == "MUST_TUM" and j.get("from_column") == "SUBE_KOD"
                and j.get("to_table") == "SUBE_DIM" and j.get("to_column") == "SUBE_KOD"
                for j in joins
            )
            if not already:
                joins.append({"from_table": "MUST_TUM", "from_column": "SUBE_KOD", "to_table": "SUBE_DIM", "to_column": "SUBE_KOD"})
            plan["joins"] = joins

    return plan
