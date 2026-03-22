"""
admin — admin.pack-fresh.com
Unified login portal + command console for Pack Fresh staff tools.
Role-based access: owner, manager, associate.
"""

import os
import logging
from datetime import datetime, timezone

import bcrypt
from flask import Flask, request, jsonify, redirect, render_template, make_response

import db
from auth import (
    create_token, decode_token, set_auth_cookie, clear_auth_cookie,
    get_current_user, JWT_COOKIE_NAME, require_auth,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32).hex())
db.init_pool()

VALID_ROLES = ("owner", "manager", "associate")

# ═══════════════════════════════════════════════════════════════════════════════
# Auth helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _check_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


# ═══════════════════════════════════════════════════════════════════════════════
# Login / Logout
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/login")
def login_page():
    next_url = request.args.get("next", "/")
    # If already logged in, redirect to dashboard
    token = request.cookies.get(JWT_COOKIE_NAME)
    if token and decode_token(token):
        return redirect(next_url)
    return render_template("login.html", next_url=next_url)


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    user = db.query_one(
        "SELECT * FROM admin_users WHERE email = %s AND is_active = TRUE",
        (email,)
    )
    if not user or not _check_password(password, user["password_hash"]):
        return jsonify({"error": "Invalid email or password"}), 401

    # Update last login
    db.execute(
        "UPDATE admin_users SET last_login_at = %s WHERE id = %s",
        (datetime.now(timezone.utc), str(user["id"]))
    )

    token = create_token(
        user_id=str(user["id"]),
        email=user["email"],
        name=user["name"],
        role=user["role"],
    )

    resp = make_response(jsonify({"ok": True, "name": user["name"], "role": user["role"]}))
    set_auth_cookie(resp, token)
    return resp


@app.route("/api/logout", methods=["POST"])
def api_logout():
    resp = make_response(jsonify({"ok": True}))
    clear_auth_cookie(resp)
    return resp


# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard (requires auth)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def dashboard():
    token = request.cookies.get(JWT_COOKIE_NAME)
    payload = decode_token(token) if token else None
    if not payload:
        return redirect("/login")

    # Refresh token on dashboard visit
    token = create_token(
        user_id=payload["sub"],
        email=payload["email"],
        name=payload["name"],
        role=payload["role"],
    )
    resp = make_response(render_template("dashboard.html", user=payload))
    set_auth_cookie(resp, token)
    return resp


# ═══════════════════════════════════════════════════════════════════════════════
# User Management (owner only)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/users")
def users_page():
    auth_result = require_auth(roles=["owner"])
    if auth_result:
        return auth_result
    return render_template("users.html", user=get_current_user())


@app.route("/api/users")
def list_users():
    auth_result = require_auth(roles=["owner"])
    if auth_result:
        return auth_result
    users = db.query(
        "SELECT id, email, name, role, is_active, created_at, last_login_at FROM admin_users ORDER BY created_at"
    )
    return jsonify({"users": [_ser(u) for u in users]})


@app.route("/api/users", methods=["POST"])
def create_user():
    auth_result = require_auth(roles=["owner"])
    if auth_result:
        return auth_result

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    name = (data.get("name") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "associate").lower()

    if not email or not name or not password:
        return jsonify({"error": "Email, name, and password required"}), 400
    if role not in VALID_ROLES:
        return jsonify({"error": f"Role must be one of: {VALID_ROLES}"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400

    existing = db.query_one("SELECT id FROM admin_users WHERE email = %s", (email,))
    if existing:
        return jsonify({"error": "A user with this email already exists"}), 409

    db.execute("""
        INSERT INTO admin_users (email, name, password_hash, role)
        VALUES (%s, %s, %s, %s)
    """, (email, name, _hash_password(password), role))

    return jsonify({"ok": True})


@app.route("/api/users/<user_id>", methods=["PATCH"])
def update_user(user_id):
    auth_result = require_auth(roles=["owner"])
    if auth_result:
        return auth_result

    data = request.get_json(silent=True) or {}

    updates = []
    params = []
    if "name" in data:
        updates.append("name = %s")
        params.append(data["name"].strip())
    if "role" in data:
        role = data["role"].lower()
        if role not in VALID_ROLES:
            return jsonify({"error": f"Role must be one of: {VALID_ROLES}"}), 400
        updates.append("role = %s")
        params.append(role)
    if "is_active" in data:
        updates.append("is_active = %s")
        params.append(bool(data["is_active"]))
    if "password" in data:
        if len(data["password"]) < 8:
            return jsonify({"error": "Password must be at least 8 characters"}), 400
        updates.append("password_hash = %s")
        params.append(_hash_password(data["password"]))

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    params.append(user_id)
    db.execute(f"UPDATE admin_users SET {', '.join(updates)} WHERE id::text = %s", tuple(params))
    return jsonify({"ok": True})


def _ser(d):
    out = {}
    for k, v in dict(d).items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
