"""
admin — admin.pack-fresh.com
Unified login portal + command console for Pack Fresh staff tools.
Role-based access: owner, manager, associate.
"""

import os
import logging
from datetime import datetime, timezone

import bcrypt
from flask import Blueprint, Flask, request, jsonify, redirect, render_template, make_response
from flask_cors import CORS

import db
from auth import (
    create_token, decode_token, set_auth_cookie, clear_auth_cookie,
    get_current_user, JWT_COOKIE_NAME, require_auth,
    create_override_token, OVERRIDE_TTL_MINUTES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32).hex())

# Cross-origin requests from staff subdomains. /api/verify-pin is the
# load-bearing one — staff services call it from their own subdomains
# (offers., inventory., cards., etc.) to mint manager-override tokens.
# Credentials must be allowed so the pf_auth JWT cookie rides along.
CORS(
    app,
    resources={r"/api/*": {
        "origins": [
            "https://offers.pack-fresh.com",
            "https://ingest.pack-fresh.com",
            "https://inventory.pack-fresh.com",
            "https://cards.pack-fresh.com",
            "https://prices.pack-fresh.com",
            "https://screening.pack-fresh.com",
            "https://vip.pack-fresh.com",
            "https://drops.pack-fresh.com",
            "https://analytics.pack-fresh.com",
            "https://kiosk.pack-fresh.com",
            "https://admin.pack-fresh.com",
        ],
        "supports_credentials": True,
    }},
)

db.init_pool()

# Manager-override PIN column — additive migration so existing deploys
# self-upgrade without running migrate_admin_users.py manually.
try:
    db.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS pin_hash VARCHAR(255)")
except Exception as _e:
    logger.warning(f"pin_hash column migration skipped: {_e}")

# Serve shared static assets (pf_theme.css, pf_ui.js) at /pf-static/
# In Docker: WORKDIR=/app, shared/ is at /app/shared/ (not ../shared/)
_shared_static = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared", "static")
if not os.path.isdir(_shared_static):
    _shared_static = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared", "static")
app.register_blueprint(Blueprint(
    "pf_static", __name__,
    static_folder=_shared_static,
    static_url_path="/pf-static",
))

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


@app.route("/logout", methods=["GET"])
def logout_redirect():
    # Plain-link sign-out target for the shared admin bar. Used by every
    # non-admin service (intake-service, ingestion, etc.) where there's no JS
    # logout helper — a fetch POST to /api/logout would bypass cross-origin
    # cookie clearing anyway, so we do GET → clear → 302.
    resp = make_response(redirect("/login"))
    clear_auth_cookie(resp)
    return resp


@app.route("/api/change-password", methods=["POST"])
def change_password():
    """Any logged-in user can change their own password."""
    token = request.cookies.get(JWT_COOKIE_NAME)
    payload = decode_token(token) if token else None
    if not payload:
        return jsonify({"error": "Not authenticated"}), 401

    data = request.get_json(silent=True) or {}
    current = data.get("current_password", "")
    new_pass = data.get("new_password", "")

    if not current or not new_pass:
        return jsonify({"error": "Current and new password required"}), 400
    if len(new_pass) < 8:
        return jsonify({"error": "New password must be at least 8 characters"}), 400

    user = db.query_one(
        "SELECT * FROM admin_users WHERE id::text = %s AND is_active = TRUE",
        (payload["sub"],)
    )
    if not user or not _check_password(current, user["password_hash"]):
        return jsonify({"error": "Current password is incorrect"}), 401

    db.execute(
        "UPDATE admin_users SET password_hash = %s WHERE id::text = %s",
        (_hash_password(new_pass), payload["sub"])
    )
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# Manager-override PIN
# ═══════════════════════════════════════════════════════════════════════════════

def _valid_pin(pin: str) -> bool:
    return bool(pin) and pin.isdigit() and 4 <= len(pin) <= 8


@app.route("/api/set-my-pin", methods=["POST"])
def set_my_pin():
    """Self-service PIN management for owner/manager. Associates can't set a
    PIN — there's nothing they could authorize. Caller must re-enter their
    password to prevent a hijacked session from setting an attacker PIN.
    Pass new_pin='' (empty) to clear it.
    """
    token = request.cookies.get(JWT_COOKIE_NAME)
    payload = decode_token(token) if token else None
    if not payload:
        return jsonify({"error": "Not authenticated"}), 401
    if payload.get("role") not in ("owner", "manager"):
        return jsonify({"error": "Only managers can set a PIN"}), 403

    data = request.get_json(silent=True) or {}
    current_password = data.get("current_password", "")
    new_pin = (data.get("new_pin") or "").strip()
    clear = bool(data.get("clear"))

    if not current_password:
        return jsonify({"error": "Current password required"}), 400
    if not clear and not _valid_pin(new_pin):
        return jsonify({"error": "PIN must be 4-8 digits"}), 400

    user = db.query_one(
        "SELECT * FROM admin_users WHERE id::text = %s AND is_active = TRUE",
        (payload["sub"],)
    )
    if not user or not _check_password(current_password, user["password_hash"]):
        return jsonify({"error": "Current password is incorrect"}), 401

    if clear:
        db.execute("UPDATE admin_users SET pin_hash = NULL WHERE id::text = %s", (payload["sub"],))
    else:
        db.execute(
            "UPDATE admin_users SET pin_hash = %s WHERE id::text = %s",
            (_hash_password(new_pin), payload["sub"])
        )
    return jsonify({"ok": True, "cleared": clear})


@app.route("/api/verify-pin", methods=["POST"])
def verify_pin():
    """Validate a PIN against any active owner/manager and issue a short-
    lived override token. Used by associate-facing flows where a manager
    walks up to authorize a privileged action without logging out.

    Body: { "pin": "1234", "action": "offer_percentage" }
    Returns the override token alongside which manager approved it so the
    consuming service can audit-log the approval pair.
    """
    data = request.get_json(silent=True) or {}
    pin = (data.get("pin") or "").strip()
    action = (data.get("action") or "manager_override").strip() or "manager_override"

    if not _valid_pin(pin):
        return jsonify({"error": "PIN must be 4-8 digits"}), 400

    candidates = db.query("""
        SELECT id, name, role, pin_hash FROM admin_users
        WHERE role IN ('owner', 'manager')
          AND is_active = TRUE
          AND pin_hash IS NOT NULL
    """)

    matched = None
    for c in candidates:
        try:
            if _check_password(pin, c["pin_hash"]):
                matched = c
                break
        except Exception:
            continue

    if not matched:
        # Generic failure — never reveal whether a PIN exists or which user owns it.
        return jsonify({"error": "Invalid PIN"}), 401

    override = create_override_token(
        str(matched["id"]), matched["name"], matched["role"], action
    )
    return jsonify({
        "ok": True,
        "manager": {
            "id": str(matched["id"]),
            "name": matched["name"],
            "role": matched["role"],
        },
        "override_token": override,
        "expires_in_seconds": OVERRIDE_TTL_MINUTES * 60,
    })


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
    users = db.query("""
        SELECT id, email, name, role, is_active, created_at, last_login_at,
               (pin_hash IS NOT NULL) AS has_pin
        FROM admin_users
        ORDER BY created_at
    """)
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
    if "pin" in data:
        # "" or null clears the PIN; any other value must be 4–8 digits.
        pin_val = (data.get("pin") or "").strip()
        if pin_val == "":
            updates.append("pin_hash = NULL")
        else:
            if not _valid_pin(pin_val):
                return jsonify({"error": "PIN must be 4-8 digits"}), 400
            updates.append("pin_hash = %s")
            params.append(_hash_password(pin_val))

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
