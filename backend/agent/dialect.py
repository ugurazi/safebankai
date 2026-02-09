from __future__ import annotations

from typing import Optional
import sqlglot


def to_mysql(sql: str, source_dialect: Optional[str] = None) -> str:
    """
    Eğer source_dialect biliyorsan (postgres, tsql, bigquery vs) yaz.
    Bilmiyorsan None bırak; çoğu zaman zaten MySQL ürettireceğiz.
    """
    if not sql:
        return sql

    if source_dialect:
        out = sqlglot.transpile(sql, read=source_dialect, write="mysql")
        return out[0] if out else sql

    return sql
