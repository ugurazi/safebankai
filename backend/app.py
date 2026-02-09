from __future__ import annotations

print("🔥🔥🔥 RUNNING THIS app.py FILE 🔥🔥🔥")
print(__file__)

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import uuid
import io
import os
import json
import re

# auth helpers
import base64
import hmac
import hashlib
import time

# admin sql execute needs this
import mysql.connector

from catalog.loader import REQUIRED_COLUMNS, build_catalog_from_df
from catalog.retriever import retrieve_relevant_columns

# 🔥 guard: readonly + write
from agent.guard import (
    validate_sql_readonly,
    validate_sql_write,
    enforce_limit,
    drop_pii_columns,
)

from agent.planner import plan_question
from agent.sql_writer import write_sql_mysql
from agent.dialect import to_mysql
from agent.plan_sanitizer import sanitize_plan
from db.mysql import execute_mysql
from agent.explainer import explain
from agent.used_columns import used_columns_from_plan

# ✅ Ollama
from agent.ollama_client import OllamaClient


app = FastAPI(
    title="SafeBank AI Agent",
    version="0.7",
    response_model_exclude_none=True,
)

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8000",  # Flask UI
        "http://localhost:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Demo için RAM
CATALOG_STORE: Dict[str, Dict[str, Any]] = {}

# ✅ Ollama client (lokal)
OLLAMA = OllamaClient(
    base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
    model=os.getenv("OLLAMA_MODEL", "llama3.1:latest"),
)

# =========================
# Models
# =========================
class UploadCatalogResponse(BaseModel):
    catalog_id: str
    tables_count: int
    columns_count: int


class QueryRequest(BaseModel):
    catalog_id: str
    question: str = Field(..., min_length=3)
    execute: bool = False  # default: sadece SQL döndür


class QueryResponse(BaseModel):
    catalog_id: str
    question: str
    sql: str
    explanation: str
    risk_flags: List[str]
    used_columns: List[Dict[str, Any]]
    rows: Optional[List[Dict[str, Any]]] = None


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    role: str
    username: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    catalog_id: Optional[str] = None
    execute: bool = True  # chat default execute


class ChatResponse(BaseModel):
    text: str
    chartData: Optional[Dict[str, Any]] = None
    sql: Optional[str] = None
    risk_flags: Optional[List[str]] = None
    rows: Optional[List[Dict[str, Any]]] = None


class AdminSQLRequest(BaseModel):
    sql: str


class AdminSQLResponse(BaseModel):
    ok: bool
    affected_rows: Optional[int] = None


class AdminBatchSQLRequest(BaseModel):
    statements: List[str] = Field(..., min_items=1)


class AdminBatchSQLResponse(BaseModel):
    ok: bool
    results: List[Dict[str, Any]]


class AdminNLPExecuteRequest(BaseModel):
    message: str = Field(..., min_length=3)
    catalog_id: Optional[str] = None
    dry_run: bool = False  # True: sadece SQL üret, DB'ye basma


class AdminNLPExecuteResponse(BaseModel):
    ok: bool
    catalog_id: str
    statements: List[str]
    results: Optional[List[Dict[str, Any]]] = None


# =========================
# Auth (NO signup / NO admin dashboard)
# =========================
def _parse_users() -> Dict[str, Dict[str, str]]:
    """
    AUTH_USERS env:
      "stajyer:123:intern,yonetmen:123:director"
    """
    raw = os.getenv("AUTH_USERS", "")
    users: Dict[str, Dict[str, str]] = {}
    for item in [x.strip() for x in raw.split(",") if x.strip()]:
        parts = item.split(":")
        if len(parts) != 3:
            continue
        u, p, r = parts
        users[u] = {"password": p, "role": r}
    return users


def _sign(payload: str) -> str:
    secret = os.getenv("AUTH_SECRET", "dev-secret")
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode("utf-8").rstrip("=")


def make_token(username: str, role: str, ttl_seconds: int = 60 * 60 * 8) -> str:
    exp = int(time.time()) + ttl_seconds
    payload = f"{username}|{role}|{exp}"
    sig = _sign(payload)
    token = base64.urlsafe_b64encode(f"{payload}|{sig}".encode("utf-8")).decode("utf-8").rstrip("=")
    return token


def verify_token(token: str) -> Dict[str, str]:
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        username, role, exp_str, sig = decoded.split("|")
        payload = f"{username}|{role}|{exp_str}"

        if _sign(payload) != sig:
            raise ValueError("bad sig")
        if int(exp_str) < int(time.time()):
            raise ValueError("expired")

        return {"username": username, "role": role}
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid/expired token")


def _require_bearer_if_configured(request: Request) -> Dict[str, str]:
    """
    - AUTH_DISABLED=1 ise auth bypass (director)
    - aksi halde Bearer token zorunlu + verify
    """
    if os.getenv("AUTH_DISABLED", "0") in {"1", "true", "TRUE", "yes", "YES"}:
        return {"username": "demo", "role": "director"}

    auth = request.headers.get("Authorization") or ""
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth.replace("Bearer ", "", 1).strip()
    return verify_token(token)


@app.post("/auth/login", response_model=LoginResponse)
def auth_login(req: LoginRequest):
    users = _parse_users()
    u = users.get(req.username)
    if not u or u["password"] != req.password:
        raise HTTPException(status_code=401, detail="Invalid username/password")

    role = u["role"]
    token = make_token(req.username, role)
    return LoginResponse(token=token, role=role, username=req.username)


def _pick_default_catalog_id() -> str:
    if not CATALOG_STORE:
        raise HTTPException(
            status_code=400,
            detail="No catalog uploaded yet. First call POST /catalog/upload with your data_dictionary CSV.",
        )
    return next(reversed(CATALOG_STORE.keys()))


def _mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "app"),
        password=os.getenv("DB_PASSWORD", "app"),
        database=os.getenv("DB_NAME", "safebank"),
    )


# =========================
# Catalog auto-sync helpers
# =========================
def _make_row_with_required_columns(base: Dict[str, Any]) -> Dict[str, Any]:
    """
    REQUIRED_COLUMNS neyse ona göre dict'i doldurur.
    Bilinmeyen kolonlarda default değer basar.
    """
    out: Dict[str, Any] = {}
    for c in REQUIRED_COLUMNS:
        if c in base:
            out[c] = base[c]
        else:
            # defaultlar
            if c in {"pii", "is_primary_key", "is_foreign_key", "is_snapshot_table"}:
                out[c] = False
            else:
                out[c] = ""
    return out


def _fetch_schema_as_rows_from_db() -> List[Dict[str, Any]]:
    """
    INFORMATION_SCHEMA.COLUMNS -> REQUIRED_COLUMNS formatında satırlar üretir
    """
    conn = _mysql_conn()
    try:
        
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
              TABLE_NAME AS table_name,
              COLUMN_NAME AS column_name,
              COLUMN_TYPE AS data_type,
              COLUMN_KEY,
              IS_NULLABLE,
              COLUMN_DEFAULT,
              COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        base = {
            "table_name": r.get("table_name", ""),
            "column_name": r.get("column_name", ""),
            "data_type": r.get("data_type", ""),

            # comment varsa description'a koy
            "description": r.get("COLUMN_COMMENT") or "",
            "pii": False,
            "synonyms": "",

            "is_primary_key": (r.get("COLUMN_KEY") == "PRI"),
            "is_foreign_key": False,
            "references_table": "",
            "references_column": "",

            "semantic_role": "",
            "recommended_aggregation": "",
            "is_snapshot_table": False,
            "snapshot_date_column": "",
            "data_classification": "",
            "example_values": "",
        }
        out_rows.append(_make_row_with_required_columns(base))
    return out_rows


def _index_existing_catalog(existing_catalog: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    build_catalog_from_df tipik olarak bir column_index tutuyor ama garanti değil.
    O yüzden hem column_index varsa onu kullanır, yoksa tables->columns gezip index üretir.
    """
    if not existing_catalog:
        return {}

    idx = existing_catalog.get("column_index")
    if isinstance(idx, dict) and idx:
        # bazen key string olabilir, bazen tuple; normalize etmeye çalışalım
        normalized: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for k, v in idx.items():
            if isinstance(k, tuple) and len(k) == 2:
                normalized[(str(k[0]), str(k[1]))] = v
            elif isinstance(k, str) and "|" in k:
                t, c = k.split("|", 1)
                normalized[(t, c)] = v
        if normalized:
            return normalized

    # Son fallback: existing_catalog["columns"] listesi olabilir
    normalized = {}
    cols_list = existing_catalog.get("columns", [])
    if isinstance(cols_list, list):
        for col in cols_list:
            t = col.get("table_name")
            c = col.get("column_name")
            if t and c:
                normalized[(t, c)] = col

    return normalized


def _merge_schema_rows_with_existing(schema_rows: List[Dict[str, Any]], existing_catalog: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not existing_catalog:
        return schema_rows

    idx = _index_existing_catalog(existing_catalog)

    merged: List[Dict[str, Any]] = []
    for row in schema_rows:
        key = (row.get("table_name", ""), row.get("column_name", ""))
        old = idx.get(key)

        if old:
            # manuel alanları koru
            for keep_field in [
                "description",
                "pii",
                "synonyms",
                "is_foreign_key",
                "references_table",
                "references_column",
                "semantic_role",
                "recommended_aggregation",
                "is_snapshot_table",
                "snapshot_date_column",
                "data_classification",
                "example_values",
            ]:
                if keep_field in old and old.get(keep_field) not in (None, ""):
                    row[keep_field] = old.get(keep_field)

        merged.append(_make_row_with_required_columns(row))
    return merged


def _build_write_prompt(user_message: str, relevant_cols: List[Dict[str, Any]]) -> str:
    cols_text = "\n".join(
        [f"- {c['table_name']}.{c['column_name']} ({c.get('data_type','')})" for c in relevant_cols[:40]]
    )

    return f"""
You are a secure database admin agent for a bank.
Convert the Turkish instruction into SAFE MySQL write statements.

Return ONLY valid JSON with this exact schema:
{{
  "statements": ["SQL1", "SQL2"]
}}

Rules:
- statements must be a LIST of SINGLE statements (no multi-command in one string).
- Allowed: ALTER TABLE ... ADD COLUMN, UPDATE ... WHERE ...
- NEVER use DROP/TRUNCATE/GRANT/REVOKE.
- UPDATE/DELETE MUST include WHERE.
- Prefer MUST_TUM table.
- If asked to create codes, use:
  CONCAT('PR', SUBSTRING(REPLACE(UUID(),'-',''),1,12))

Relevant columns:
{cols_text}

User instruction (Turkish):
{user_message}
""".strip()


def _extract_json_object(text: str) -> Dict[str, Any]:
    """
    LLM bazen JSON'u ``` içinde veya açıklamayla döndürür.
    Bu fonksiyon ilk { ile son } arasını çekip parse etmeye çalışır.
    """
    t = (text or "").strip()
    t = t.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(t)
    except Exception:
        pass

    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if not m:
        raise ValueError("No JSON object found in LLM output")
    return json.loads(m.group(0))


def _normalize_statements(statements: List[str]) -> List[str]:
    """
    Model bazen ["ALTER ...; UPDATE ...;"] şeklinde tek stringte döndürür.
    Bunu split edip güvenli listeye çevirir.
    """
    out: List[str] = []
    for s in statements:
        if not s:
            continue
        chunk = str(s).strip()
        chunk = chunk.replace("```sql", "").replace("```", "").strip()

        parts = [p.strip() for p in chunk.split(";") if p.strip()]
        for p in parts:
            out.append(p.rstrip(";").strip() + ";")
    return out


# =========================
# API
# =========================
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/catalog/upload", response_model=UploadCatalogResponse)
async def upload_catalog(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported")

    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV read error: {e}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing}. Found: {list(df.columns)}",
        )

    try:
        catalog = build_catalog_from_df(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Catalog build error: {e}")

    catalog_id = str(uuid.uuid4())
    CATALOG_STORE[catalog_id] = catalog

    return UploadCatalogResponse(
        catalog_id=catalog_id,
        tables_count=len(catalog.get("tables", {})),
        columns_count=int(catalog.get("columns_count", 0)),
    )


@app.post("/catalog/sync_from_db", response_model=UploadCatalogResponse)
def sync_catalog_from_db(request: Request):
    """
    Director-only: DB şemasını INFORMATION_SCHEMA'dan okur, data dict'i otomatik günceller.
    Statik CSV derdi biter.
    """
    user = _require_bearer_if_configured(request)
    if user.get("role") != "director":
        raise HTTPException(status_code=403, detail="Forbidden: director role required")

    schema_rows = _fetch_schema_as_rows_from_db()

    existing = None
    if CATALOG_STORE:
        existing = CATALOG_STORE[next(reversed(CATALOG_STORE.keys()))]

    merged_rows = _merge_schema_rows_with_existing(schema_rows, existing)

    df = pd.DataFrame(merged_rows)
    df = df[REQUIRED_COLUMNS]

    try:
        catalog = build_catalog_from_df(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Catalog build error: {e}")

    catalog_id = str(uuid.uuid4())
    CATALOG_STORE[catalog_id] = catalog

    return UploadCatalogResponse(
        catalog_id=catalog_id,
        tables_count=len(catalog.get("tables", {})),
        columns_count=int(catalog.get("columns_count", 0)),
    )


@app.post("/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    catalog = CATALOG_STORE.get(req.catalog_id)
    if not catalog:
        raise HTTPException(status_code=404, detail="catalog_id not found. Upload CSV first.")

    relevant = retrieve_relevant_columns(catalog, req.question, top_k=30)
    relevant = drop_pii_columns(relevant)

    plan = plan_question(req.question, relevant)
    plan = sanitize_plan(plan, relevant)
    used_cols = used_columns_from_plan(plan, catalog, relevant)

    sql = write_sql_mysql(plan)
    sql = to_mysql(sql, source_dialect=None)

    sql = enforce_limit(sql, default_limit=1000)
    validate_sql_readonly(sql)

    risk_flags: List[str] = []
    upper_sql = sql.upper()
    if " MUST_TC" in upper_sql or "MUST_TC," in upper_sql or "MUST_TC\n" in upper_sql:
        risk_flags.append("PII_COLUMN_SELECTED")
    if "LIMIT" not in upper_sql:
        risk_flags.append("NO_LIMIT")

    explanation = explain(plan)

    rows: Optional[List[Dict[str, Any]]] = None
    if req.execute:
        try:
            rows = execute_mysql(sql)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MySQL execute error: {e}")

    return QueryResponse(
        catalog_id=req.catalog_id,
        question=req.question,
        sql=sql,
        explanation=explanation,
        risk_flags=risk_flags,
        used_columns=used_cols,
        rows=rows,
    )


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request):
    """
    Auth required (unless AUTH_DISABLED=1)
    """
    _require_bearer_if_configured(request)

    catalog_id = req.catalog_id or _pick_default_catalog_id()
    qres = await query(QueryRequest(catalog_id=catalog_id, question=req.message, execute=req.execute))

    chartData: Optional[Dict[str, Any]] = None
    if qres.rows and len(qres.rows) >= 2:
        keys = list(qres.rows[0].keys())
        if len(keys) >= 2:
            x_key, y_key = keys[0], keys[1]
            labels: List[str] = []
            data: List[float] = []
            ok = True
            for r in qres.rows:
                labels.append(str(r.get(x_key)))
                try:
                    data.append(float(r.get(y_key)))
                except Exception:
                    ok = False
                    break
            if ok:
                chartData = {
                    "labels": labels,
                    "series": [{"name": y_key, "data": data}],
                }

    text = qres.explanation
    if qres.risk_flags:
        text += "\n\nRisk flags: " + ", ".join(qres.risk_flags)

    return ChatResponse(
        text=text,
        chartData=chartData,
        sql=qres.sql,
        risk_flags=qres.risk_flags,
        rows=qres.rows,
    )


@app.post("/admin/execute_sql", response_model=AdminSQLResponse)
def admin_execute_sql(req: AdminSQLRequest, request: Request):
    """
    Director-only endpoint: CREATE/DELETE/UPDATE/INSERT gibi işlemler burada çalışır.
    Intern bu endpoint'i kullanamaz.
    """
    user = _require_bearer_if_configured(request)
    if user.get("role") != "director":
        raise HTTPException(status_code=403, detail="Forbidden: director role required")

    sql = (req.sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL is empty")

    if ";" in sql[:-1]:
        raise HTTPException(status_code=400, detail="Multiple statements are not allowed")

    try:
        validate_sql_write(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Write guard blocked: {e}")

    conn = _mysql_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        return AdminSQLResponse(ok=True, affected_rows=cur.rowcount)
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/admin/execute_batch", response_model=AdminBatchSQLResponse)
def admin_execute_batch(req: AdminBatchSQLRequest, request: Request):
    """
    Director-only: ALTER + UPDATE gibi ardışık komutlar için.
    Multi-statement tek stringte yasak, ama burada list olarak güvenli şekilde çalıştırıyoruz.
    """
    user = _require_bearer_if_configured(request)
    if user.get("role") != "director":
        raise HTTPException(status_code=403, detail="Forbidden: director role required")

    conn = _mysql_conn()
    results: List[Dict[str, Any]] = []

    try:
        cur = conn.cursor()
        for i, raw_sql in enumerate(req.statements):
            sql = (raw_sql or "").strip()
            if not sql:
                continue

            if ";" in sql[:-1]:
                raise HTTPException(status_code=400, detail=f"Multiple statements not allowed in item {i}")

            try:
                validate_sql_write(sql)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Write guard blocked in item {i}: {e}")

            cur.execute(sql)
            results.append({"index": i, "sql": sql, "affected_rows": cur.rowcount})

        conn.commit()
        return AdminBatchSQLResponse(ok=True, results=results)
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/admin/nlp_execute", response_model=AdminNLPExecuteResponse)
def admin_nlp_execute(req: AdminNLPExecuteRequest, request: Request):
    """
    Director-only: doğal dili alır -> Ollama SQL üretir -> normalize -> write guard -> execute -> catalog sync
    """
    user = _require_bearer_if_configured(request)
    if user.get("role") != "director":
        raise HTTPException(status_code=403, detail="Forbidden: director role required")

    catalog_id = req.catalog_id or _pick_default_catalog_id()
    catalog = CATALOG_STORE.get(catalog_id)
    if not catalog:
        raise HTTPException(status_code=404, detail="catalog_id not found")

    relevant = retrieve_relevant_columns(catalog, req.message, top_k=40)
    relevant = drop_pii_columns(relevant)

    prompt = _build_write_prompt(req.message, relevant)

    # ✅ mümkünse JSON format zorla
    try:
        raw = OLLAMA.generate(prompt, system=None, temperature=0.0, force_json=True)  # type: ignore
    except TypeError:
        # force_json desteklemiyorsa fallback
        raw = OLLAMA.generate(prompt, system=None, temperature=0.0)

    try:
        obj = _extract_json_object(raw)
        statements = obj.get("statements", [])
        if not isinstance(statements, list) or not statements:
            raise ValueError("No statements produced")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM output parse failed: {e}. Raw: {raw[:400]}")

    normalized = _normalize_statements([str(s) for s in statements])
    if not normalized:
        raise HTTPException(status_code=500, detail=f"No valid SQL after normalize. Raw: {raw[:400]}")

    # ✅ guard + execute
    conn = _mysql_conn()
    results: List[Dict[str, Any]] = []
    try:
        cur = conn.cursor()
        for i, sql in enumerate(normalized):
            try:
                validate_sql_write(sql)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Write guard blocked item {i}: {e} SQL={sql}")

            cur.execute(sql)
            results.append({"index": i, "sql": sql, "affected_rows": cur.rowcount})

        if req.dry_run:
            conn.rollback()
            return AdminNLPExecuteResponse(ok=True, catalog_id=catalog_id, statements=normalized, results=None)

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # ✅ işlem sonrası catalog sync (yeni kolon anında görülsün)
    schema_rows = _fetch_schema_as_rows_from_db()
    existing = CATALOG_STORE.get(catalog_id)
    merged_rows = _merge_schema_rows_with_existing(schema_rows, existing)
    df = pd.DataFrame(merged_rows)[REQUIRED_COLUMNS]
    new_catalog = build_catalog_from_df(df)

    new_catalog_id = str(uuid.uuid4())
    CATALOG_STORE[new_catalog_id] = new_catalog

    return AdminNLPExecuteResponse(
        ok=True,
        catalog_id=new_catalog_id,
        statements=normalized,
        results=results,
    )
