from __future__ import annotations

import re
from typing import Any, Dict, List

FORBIDDEN = ["insert", "update", "delete", "drop", "truncate", "alter", "create", "grant", "revoke"]

HARD_FORBIDDEN_WRITE = ["drop", "truncate", "grant", "revoke"]


def validate_sql_readonly(sql: str) -> None:
    s = (sql or "").strip().lower()

    if not s.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    # çok basit forbidden check
    for kw in FORBIDDEN:
        if re.search(rf"\b{kw}\b", s):
            raise ValueError(f"Forbidden keyword detected: {kw}")

    # ; ile birden fazla statement riskini azalt
    if ";" in s[:-1]:
        raise ValueError("Multiple statements are not allowed.")


def validate_sql_write(sql: str) -> None:
    """
    Director-only endpointlerde çağır.
    Amaç: yazma sorgularını belli bir güvenlik çerçevesinde tutmak.
    """
    s = (sql or "").strip().lower()
    if not s:
        raise ValueError("Empty SQL")

    # multi statement engeli (tek sql)
    if ";" in s[:-1]:
        raise ValueError("Multiple statements are not allowed.")

    # hard forbidden
    for kw in HARD_FORBIDDEN_WRITE:
        if re.search(rf"\b{kw}\b", s):
            raise ValueError(f"Hard-forbidden keyword detected: {kw}")

    allowed_starts = ("update", "insert", "delete", "alter", "create")
    if not s.startswith(allowed_starts):
        raise ValueError("Only INSERT/UPDATE/DELETE/ALTER/CREATE are allowed in write mode.")

    # UPDATE/DELETE için WHERE zorunlu
    if s.startswith("update") and not re.search(r"\bwhere\b", s):
        raise ValueError("UPDATE must include WHERE.")
    if s.startswith("delete") and not re.search(r"\bwhere\b", s):
        raise ValueError("DELETE must include WHERE.")


def enforce_limit(sql: str, default_limit: int = 1000) -> str:
    s = sql.strip()
    if re.search(r"\blimit\b", s, flags=re.IGNORECASE):
        return s
    return s.rstrip(";") + f" LIMIT {default_limit};"


def drop_pii_columns(selected_columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Retriever çıktısında PII işaretli kolonları LLM'e bile göstermemek istersen."""
    return [c for c in selected_columns if not c.get("pii", False)]
