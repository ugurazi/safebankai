from __future__ import annotations

import os
from typing import Any, Dict, List
import mysql.connector


def _get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "app"),
        password=os.getenv("DB_PASSWORD", "app"),
        database=os.getenv("DB_NAME", "safebank"),
    )


def execute_mysql(sql: str) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass
