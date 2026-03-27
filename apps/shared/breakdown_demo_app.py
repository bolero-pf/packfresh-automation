"""
Breakdown Demo App — standalone Flask server to validate shared breakdown components.

Run from apps/shared/:
    python breakdown_demo_app.py

Loads .env from ../ingestion/.env (same DATABASE_URL and PPT_API_KEY as ingest service).
"""

import os
import sys

# Add shared/ to path (same as other services)
sys.path.insert(0, os.path.dirname(__file__))

# Load .env from ingestion service (has DATABASE_URL, PPT_API_KEY, etc.)
_env_path = os.path.join(os.path.dirname(__file__), "..", "ingestion", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val

from flask import Flask, render_template

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("FLASK_SECRET", "demo-secret-key")

# ─── Database ────────────────────────────────────────────────────────

# Minimal db module shim — reuse the same pattern as other services
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

_DATABASE_URL = os.getenv("DATABASE_URL", "")


@contextmanager
def _get_conn():
    conn = psycopg2.connect(_DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class _DB:
    """Minimal db module interface matching what breakdown_logic expects."""

    def query(self, sql, params=None):
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def query_one(self, sql, params=None):
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def execute(self, sql, params=None):
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount

    def execute_returning(self, sql, params=None):
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return cur.fetchone()


db = _DB()

# ─── PPT Client ──────────────────────────────────────────────────────

ppt = None
try:
    from ppt_client import PPTClient
    ppt_key = os.getenv("PPT_API_KEY", "")
    if ppt_key:
        ppt = PPTClient(ppt_key)
except ImportError:
    pass

# ─── Register shared breakdown blueprint ─────────────────────────────

from breakdown_routes import create_breakdown_blueprint

app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: ppt))


# ─── Demo page ───────────────────────────────────────────────────────

@app.route("/")
def demo():
    return render_template("breakdown_demo.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5099))
    print(f"Breakdown Demo running at http://localhost:{port}")
    print(f"Database: {'connected' if _DATABASE_URL else 'NOT SET (set DATABASE_URL)'}")
    print(f"PPT: {'connected' if ppt else 'NOT SET (set PPT_API_KEY)'}")
    app.run(host="0.0.0.0", port=port, debug=True)
