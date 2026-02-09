import os
import io
from datetime import datetime

import json
import requests
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, session, flash, send_file, jsonify
from dotenv import load_dotenv

load_dotenv()

# ✅ normalize: sondaki slash'ı temizle
FASTAPI_BASE = (os.getenv("FASTAPI_BASE", "http://127.0.0.1:5001") or "").strip().rstrip("/")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")

# ✅ requests session (reuse connection) + proxy kapatma (lokalde bazen proxy karışıyor)
HTTP = requests.Session()
HTTP.trust_env = False  # environment proxylerini yok say (lokal dev için daha stabil)


def _headers():
    """Bearer token'ı session'dan gönder."""
    h = {}
    token = session.get("token")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _url(path: str) -> str:
    """FASTAPI_BASE + /path birleşimi (double slash olmaz)."""
    path = (path or "").strip()
    if not path.startswith("/"):
        path = "/" + path
    return f"{FASTAPI_BASE}{path}"


def _request_error_message(e: Exception) -> str:
    # requests hata mesajını daha okunur hale getir
    return f"{type(e).__name__}: {e}"


# ----------------
# AUTH (NO SIGNUP)
# ----------------
@app.get("/login")
def login_page():
    return render_template("login.html", fastapi_base=FASTAPI_BASE)


@app.post("/login")
def login_submit():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()

    if not username or not password:
        flash("Kullanıcı adı/şifre boş olamaz.")
        return redirect(url_for("login_page"))

    login_url = _url("/auth/login")

    try:
        r = HTTP.post(
            login_url,
            json={"username": username, "password": password},
            timeout=20
        )
    except requests.exceptions.ConnectionError as e:
        flash(f"Login error: Backend'e bağlanamadım. URL={login_url} | {_request_error_message(e)}")
        return redirect(url_for("login_page"))
    except requests.exceptions.Timeout as e:
        flash(f"Login error: Timeout. URL={login_url} | {_request_error_message(e)}")
        return redirect(url_for("login_page"))
    except requests.exceptions.RequestException as e:
        flash(f"Login error: Request failed. URL={login_url} | {_request_error_message(e)}")
        return redirect(url_for("login_page"))
    except Exception as e:
        flash(f"Login error: Unexpected. URL={login_url} | {_request_error_message(e)}")
        return redirect(url_for("login_page"))

    if r.status_code != 200:
        flash(f"Login başarısız ({r.status_code}): {r.text}")
        return redirect(url_for("login_page"))

    data = r.json()
    session["token"] = data["token"]
    session["role"] = data["role"]
    session["username"] = data["username"]

    # Backend restart olunca RAM catalog sıfırlanır; UI tarafında da sıfırlayalım
    session.pop("catalog_id", None)

    flash(f"✅ Giriş yapıldı: {data['username']} ({data['role']})")
    return redirect(url_for("index"))


@app.get("/logout")
def logout():
    session.clear()
    flash("Çıkış yapıldı.")
    return redirect(url_for("login_page"))


# --------------
# MAIN UI
# --------------
@app.get("/")
def index():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    catalog_id = session.get("catalog_id")
    return render_template(
        "index.html",
        catalog_id=catalog_id,
        fastapi_base=FASTAPI_BASE
    )


@app.post("/upload")
def upload():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    if "file" not in request.files:
        flash("CSV dosyası seçmedin.")
        return redirect(url_for("index"))

    file = request.files["file"]
    if not file.filename or not file.filename.lower().endswith(".csv"):
        flash("Sadece .csv destekleniyor.")
        return redirect(url_for("index"))

    files = {"file": (file.filename, file.stream, "text/csv")}

    try:
        r = HTTP.post(
            _url("/catalog/upload"),
            files=files,
            headers=_headers(),
            timeout=120
        )

        if r.status_code != 200:
            flash(f"Upload hata: {r.status_code} - {r.text}")
            return redirect(url_for("index"))

        data = r.json()
        session["catalog_id"] = data.get("catalog_id")

        flash(f"✅ Yüklendi. tables={data.get('tables_count')} columns={data.get('columns_count')}")
        return redirect(url_for("index"))

    except requests.exceptions.ConnectionError:
        flash(f"Backend'e bağlanamadım: {FASTAPI_BASE} (backend çalışıyor mu?)")
        return redirect(url_for("index"))
    except Exception as e:
        flash(f"Upload exception: {e}")
        return redirect(url_for("index"))


@app.post("/send")
def send():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    msg = (request.form.get("message") or "").strip()
    if not msg:
        flash("Mesaj boş.")
        return redirect(url_for("index"))

    catalog_id = session.get("catalog_id")
    payload = {
        "message": msg,
        "catalog_id": catalog_id,
        "execute": True
    }

    try:
        r = HTTP.post(
            _url("/chat"),
            json=payload,
            headers=_headers(),
            timeout=180
        )

        if r.status_code != 200:
            flash(f"Chat hata: {r.status_code} - {r.text}")
            return redirect(url_for("index"))

        data = r.json()
        session["last_user"] = msg
        session["last_ai"] = data.get("text", "")
        session["last_sql"] = data.get("sql", "")
        session["last_risk"] = ", ".join(data.get("risk_flags") or [])
        session["last_rows"] = data.get("rows")
        chart = data.get("chartData")
        session["last_chart_json"] = json.dumps(chart) if chart else ""
        return redirect(url_for("index"))

    except requests.exceptions.ConnectionError:
        flash(f"Backend'e bağlanamadım: {FASTAPI_BASE} (backend çalışıyor mu?)")
        return redirect(url_for("index"))
    except Exception as e:
        flash(f"Chat exception: {e}")
        return redirect(url_for("index"))


# ✅ NEW: Director NLP Execute Proxy (Browser -> Flask -> FastAPI)
@app.post("/director/nlp_execute")
def director_nlp_execute():
    if not session.get("token"):
        return jsonify({"detail": "Unauthorized: login required"}), 401

    if session.get("role") != "director":
        return jsonify({"detail": "Forbidden: director role required"}), 403

    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"detail": "Invalid JSON body"}), 400

    message = (body.get("message") or "").strip()
    catalog_id = body.get("catalog_id")  # null gelebilir
    dry_run = bool(body.get("dry_run", False))

    if not message:
        return jsonify({"detail": "message is empty"}), 400

    payload = {
        "message": message,
        "catalog_id": catalog_id or session.get("catalog_id"),
        "dry_run": dry_run,
    }

    try:
        r = HTTP.post(
            _url("/admin/nlp_execute"),
            json=payload,
            headers=_headers(),
            timeout=180
        )
    except Exception as e:
        return jsonify({"detail": f"Request failed: {e}"}), 500

    if r.status_code != 200:
        try:
            return jsonify(r.json()), r.status_code
        except Exception:
            return jsonify({"detail": r.text}), r.status_code

    data = r.json()

    if data.get("catalog_id"):
        session["catalog_id"] = data["catalog_id"]

    return jsonify(data), 200


# ✅ NEW: Director Catalog Sync Proxy (Browser -> Flask -> FastAPI)
@app.post("/director/sync_catalog")
def director_sync_catalog():
    if not session.get("token"):
        return jsonify({"detail": "Unauthorized: login required"}), 401

    if session.get("role") != "director":
        return jsonify({"detail": "Forbidden: director role required"}), 403

    try:
        r = HTTP.post(
            _url("/catalog/sync_from_db"),
            headers=_headers(),
            timeout=120
        )
    except Exception as e:
        return jsonify({"detail": f"Request failed: {e}"}), 500

    if r.status_code != 200:
        try:
            return jsonify(r.json()), r.status_code
        except Exception:
            return jsonify({"detail": r.text}), r.status_code

    data = r.json()
    if data.get("catalog_id"):
        session["catalog_id"] = data["catalog_id"]

    return jsonify(data), 200


@app.post("/admin_exec")
def admin_exec():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    if session.get("role") != "director":
        flash("❌ Yetkisiz: director role gerekli.")
        return redirect(url_for("index"))

    sql = (request.form.get("admin_sql") or "").strip()
    if not sql:
        flash("SQL boş.")
        return redirect(url_for("index"))

    try:
        r = HTTP.post(
            _url("/admin/execute_sql"),
            json={"sql": sql},
            headers=_headers(),
            timeout=60
        )

        if r.status_code != 200:
            flash(f"Admin SQL hata: {r.status_code} - {r.text}")
            return redirect(url_for("index"))

        data = r.json()
        session["admin_last_sql"] = sql
        session["admin_last_result"] = f"✅ OK. affected_rows={data.get('affected_rows')}"
        flash("✅ Admin SQL çalıştırıldı.")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Admin SQL exception: {e}")
        return redirect(url_for("index"))


@app.get("/download/xlsx")
def download_xlsx():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    rows = session.get("last_rows")
    if not rows:
        flash("İndirilecek sonuç yok. Önce bir sorgu çalıştır.")
        return redirect(url_for("index"))

    df = pd.DataFrame(rows)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")

        meta = pd.DataFrame([
            {"key": "question", "value": session.get("last_user", "")},
            {"key": "catalog_id", "value": session.get("catalog_id", "")},
            {"key": "role", "value": session.get("role", "")},
            {"key": "username", "value": session.get("username", "")},
            {"key": "sql", "value": session.get("last_sql", "")},
        ])
        meta.to_excel(writer, index=False, sheet_name="Meta")

    output.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"safebank_result_{ts}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.post("/demo/send_mail")
def demo_send_mail():
    if not session.get("token"):
        return jsonify({"detail": "Unauthorized"}), 401

    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"detail": "Invalid JSON"}), 400

    must_no = body.get("must_no")
    message = (body.get("message") or "").strip()

    if not must_no:
        return jsonify({"detail": "must_no required"}), 400
    if not message:
        return jsonify({"detail": "message required"}), 400

    # ✅ DEMO: Gerçek mail yok — sadece log + success dön
    print(f"[DEMO MAIL] MUST_NO={must_no} message={message}")

    return jsonify({"ok": True, "sent": True, "must_no": must_no}), 200


@app.get("/clear")
def clear():
    if not session.get("token"):
        return redirect(url_for("login_page"))

    session.pop("last_user", None)
    session.pop("last_ai", None)
    session.pop("last_sql", None)
    session.pop("last_risk", None)
    session.pop("last_rows", None)
    session.pop("admin_last_sql", None)
    session.pop("admin_last_result", None)
    session.pop("last_chart_json", None)
    return redirect(url_for("index"))


if __name__ == "__main__":
    # UI port: 8000 default
    app.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", "8000")), debug=True)
