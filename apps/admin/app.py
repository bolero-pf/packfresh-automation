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
            "https://rules.pack-fresh.com",
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

# Staff Inventory "on display" flag — kept in its own table so cache
# rebuilds of inventory_product_cache don't wipe the floor's working
# state. Audit table preserves who/when across toggles.
try:
    db.execute("""
        CREATE TABLE IF NOT EXISTS inventory_on_display (
            shopify_variant_id BIGINT PRIMARY KEY,
            on_display         BOOLEAN NOT NULL DEFAULT FALSE,
            toggled_by         VARCHAR(200),
            toggled_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS inventory_on_display_audit (
            id                 BIGSERIAL PRIMARY KEY,
            shopify_variant_id BIGINT NOT NULL,
            on_display         BOOLEAN NOT NULL,
            toggled_by         VARCHAR(200),
            toggled_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_on_display_audit_variant
        ON inventory_on_display_audit (shopify_variant_id, toggled_at DESC)
    """)
except Exception as _e:
    logger.warning(f"inventory_on_display migration skipped: {_e}")

# Staff Inventory saved print views. Filters/sort serialized as JSONB so
# the schema doesn't need to change every time we add a new filter axis.
try:
    db.execute("""
        CREATE TABLE IF NOT EXISTS staff_inventory_saved_views (
            id          BIGSERIAL PRIMARY KEY,
            name        VARCHAR(200) NOT NULL,
            filters     JSONB NOT NULL,
            sort        VARCHAR(50) NOT NULL DEFAULT 'name',
            created_by  VARCHAR(200),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_by  VARCHAR(200),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_staff_views_name_lower
        ON staff_inventory_saved_views (LOWER(name))
    """)
    # show_price added 2026-05-25: lets a saved view print as a "book of
    # barcodes" with prices suppressed. Stock levels are never printed —
    # they go stale faster than the sheet lasts.
    db.execute("""
        ALTER TABLE staff_inventory_saved_views
        ADD COLUMN IF NOT EXISTS show_price BOOLEAN NOT NULL DEFAULT TRUE
    """)
except Exception as _e:
    logger.warning(f"staff_inventory_saved_views migration skipped: {_e}")

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
    # Untriaged "sell to us" submissions — surfaced as a badge on the tile so
    # staff notice new leads without an email/Slack ping. Guarded so a DB hiccup
    # or missing table never takes down the console.
    new_submissions = 0
    try:
        row = db.query_one("SELECT COUNT(*) AS n FROM sell_submissions WHERE status = 'new'")
        new_submissions = (row or {}).get("n", 0) or 0
    except Exception:
        new_submissions = 0

    resp = make_response(render_template(
        "dashboard.html", user=payload, new_submissions=new_submissions))
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


# ═══════════════════════════════════════════════════════════════════════════════
# Staff Inventory (read-only)
# ═══════════════════════════════════════════════════════════════════════════════
# Reads inventory_product_cache (refreshed by the inventory service). Open to
# all staff — read-only, no edit endpoints. Front-of-house lookup tool.

# Form-factor chips for floor lookup. "Booster Packs" excludes sleeved,
# blister, booster-bundle, and buildbattle variants so the chip shows
# loose packs only — those are the ones staff actually grab.
# Era chips are auto-discovered (any tag ending with " era"); specific
# expansions stay reachable via the search box.
STAFF_INVENTORY_TYPE_CHIPS = [
    {"value": "booster_pack", "label": "Booster Packs",
     "include": ["booster pack"], "exclude": ["sleeved", "blister", "booster bundle", "buildbattle"]},
    {"value": "etb",          "label": "ETBs",
     "include": ["etb"],          "exclude": []},
    {"value": "booster_box",  "label": "Booster Boxes",
     "include": ["booster box"],  "exclude": []},
    {"value": "slab",         "label": "Slabs",
     "include": ["slab"],         "exclude": []},
]
_TYPE_CHIP_BY_VALUE = {c["value"]: c for c in STAFF_INVENTORY_TYPE_CHIPS}


def _tags_like_clause(tag: str) -> tuple[str, str]:
    """Build a (sql, param) pair that matches a single tag inside the
    comma-separated `tags` column. Mirrors the pattern used by
    inventory/routes/inventory.py."""
    return (
        "(',' || LOWER(REPLACE(COALESCE(tags, ''), ', ', ',')) || ',') LIKE %s",
        f"%,{tag.lower()},%",
    )


@app.route("/staff-inventory")
def staff_inventory_page():
    auth_result = require_auth()
    if auth_result:
        return auth_result
    return render_template("staff_inventory.html", user=get_current_user())


# Sort options shared by the items API and the print page. Keys are what
# the client sends; values are SQL ORDER BY clauses (without the keyword).
STAFF_INVENTORY_SORTS = {
    "name":       "c.title",
    "set":        ("(SELECT MIN(TRIM(t)) FROM unnest(string_to_array(LOWER(COALESCE(c.tags,'')), ',')) t "
                   "WHERE TRIM(t) LIKE '%% era') NULLS LAST, c.title"),
    "price_asc":  "c.shopify_price ASC NULLS LAST, c.title",
    "price_desc": "c.shopify_price DESC NULLS LAST, c.title",
    "qty_desc":   "c.shopify_qty DESC NULLS LAST, c.title",
}
STAFF_INVENTORY_DEFAULT_SORT = "set"


def _query_staff_items(filters: dict, sort: str = STAFF_INVENTORY_DEFAULT_SORT,
                       limit: int = 500) -> list[dict]:
    """Build + run the staff-inventory query for both the API and the print
    page. `filters` mirrors what the page sends:
        {q, types[], sets[], on_display: bool, in_stock: bool}
    """
    q = (filters.get("q") or "").strip()
    sets = list(filters.get("sets") or [])
    types = [t for t in (filters.get("types") or []) if t in _TYPE_CHIP_BY_VALUE]
    in_stock_only = bool(filters.get("in_stock", True))
    on_display_only = bool(filters.get("on_display"))

    where = ["UPPER(COALESCE(c.status,'')) = 'ACTIVE'"]
    params: list = []

    if q:
        # Explicit search: surface OOS too so staff know an item exists but
        # isn't on hand. Search across title, sku, and barcode.
        where.append("(LOWER(c.title) LIKE %s OR LOWER(COALESCE(c.sku,'')) LIKE %s OR LOWER(COALESCE(c.barcode,'')) LIKE %s)")
        like = f"%{q.lower()}%"
        params.extend([like, like, like])
    elif in_stock_only and not on_display_only:
        # On-display items are tracked manually — don't hide a flagged
        # variant just because Shopify qty briefly dropped to 0.
        where.append("COALESCE(c.shopify_qty, 0) > 0")

    for s in sets:
        sql, p = _tags_like_clause(s)
        where.append(sql.replace("tags", "c.tags"))
        params.append(p)

    # Type chips: OR across selected types, AND across each type's
    # include/exclude rules.
    if types:
        clauses = []
        for tv in types:
            chip = _TYPE_CHIP_BY_VALUE[tv]
            sub = []
            for inc in chip["include"]:
                sql, p = _tags_like_clause(inc)
                sub.append(sql.replace("tags", "c.tags"))
                params.append(p)
            for exc in chip["exclude"]:
                sql, p = _tags_like_clause(exc)
                sub.append("NOT " + sql.replace("tags", "c.tags"))
                params.append(p)
            clauses.append("(" + " AND ".join(sub) + ")")
        where.append("(" + " OR ".join(clauses) + ")")

    if on_display_only:
        where.append("COALESCE(d.on_display, FALSE) = TRUE")

    order_by = STAFF_INVENTORY_SORTS.get(sort) or STAFF_INVENTORY_SORTS[STAFF_INVENTORY_DEFAULT_SORT]

    rows = db.query(f"""
        SELECT c.shopify_variant_id, c.shopify_product_id, c.title, c.sku, c.barcode,
               c.shopify_price, c.shopify_qty, c.image_url, c.tags,
               COALESCE(d.on_display, FALSE) AS on_display
        FROM inventory_product_cache c
        LEFT JOIN inventory_on_display d
          ON d.shopify_variant_id = c.shopify_variant_id
        WHERE {' AND '.join(where)}
        ORDER BY {order_by}
        LIMIT {int(limit)}
    """, tuple(params))

    items = []
    for r in rows:
        items.append({
            "shopify_variant_id": str(r["shopify_variant_id"]),
            "shopify_product_id": str(r["shopify_product_id"]),
            "title": r["title"],
            "sku": r["sku"],
            "barcode": r["barcode"],
            "price": float(r["shopify_price"]) if r["shopify_price"] is not None else None,
            "qty": int(r["shopify_qty"] or 0),
            "image_url": r["image_url"],
            "tags": [t.strip() for t in (r["tags"] or "").split(",") if t.strip()],
            "on_display": bool(r["on_display"]),
        })
    return items


def _filters_from_request_args(args) -> dict:
    return {
        "q": (args.get("q") or "").strip(),
        "sets": [s for s in args.getlist("set") if s],
        "types": [t for t in args.getlist("type") if t],
        "in_stock": args.get("in_stock", "1") == "1",
        "on_display": args.get("on_display") == "1",
    }


@app.route("/api/staff-inventory/items")
def staff_inventory_items():
    auth_result = require_auth()
    if auth_result:
        return auth_result

    filters = _filters_from_request_args(request.args)
    sort = request.args.get("sort") or "name"
    items = _query_staff_items(filters, sort=sort)
    return jsonify({"items": items, "truncated": len(items) >= 500})


@app.route("/api/staff-inventory/toggle-display", methods=["POST"])
def staff_inventory_toggle_display():
    """Flip the on_display flag for a variant. One-tap, no confirmation.
    Open to all staff; every flip recorded in inventory_on_display_audit."""
    auth_result = require_auth()
    if auth_result:
        return auth_result

    data = request.get_json(silent=True) or {}
    try:
        variant_id = int(data.get("shopify_variant_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "shopify_variant_id required"}), 400

    # Confirm the variant actually exists before recording state for it,
    # otherwise stale clients could pollute the table.
    exists = db.query_one(
        "SELECT 1 FROM inventory_product_cache WHERE shopify_variant_id = %s",
        (variant_id,)
    )
    if not exists:
        return jsonify({"error": "Unknown variant"}), 404

    cur = db.query_one(
        "SELECT on_display FROM inventory_on_display WHERE shopify_variant_id = %s",
        (variant_id,)
    )
    new_state = not (cur and cur["on_display"])

    user = get_current_user() or {}
    actor = user.get("name") or user.get("email") or "unknown"

    db.execute("""
        INSERT INTO inventory_on_display (shopify_variant_id, on_display, toggled_by, toggled_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (shopify_variant_id) DO UPDATE
        SET on_display = EXCLUDED.on_display,
            toggled_by = EXCLUDED.toggled_by,
            toggled_at = EXCLUDED.toggled_at
    """, (variant_id, new_state, actor))

    db.execute("""
        INSERT INTO inventory_on_display_audit (shopify_variant_id, on_display, toggled_by)
        VALUES (%s, %s, %s)
    """, (variant_id, new_state, actor))

    return jsonify({"ok": True, "on_display": new_state})


@app.route("/api/staff-inventory/facets")
def staff_inventory_facets():
    """Return chip-filter options.

    Types: curated form-factor chips (booster packs, ETBs, booster boxes,
    slabs) — definitions live in STAFF_INVENTORY_TYPE_CHIPS.
    Sets: auto-discovered era tags (anything ending in " era").
    Everything else stays reachable via the text search."""
    auth_result = require_auth()
    if auth_result:
        return auth_result

    rows = db.query("""
        SELECT LOWER(TRIM(unnest(string_to_array(COALESCE(tags, ''), ',')))) AS tag,
               COUNT(*) AS n
        FROM inventory_product_cache
        WHERE UPPER(COALESCE(status,'')) = 'ACTIVE' AND COALESCE(tags, '') <> ''
        GROUP BY tag
        HAVING COUNT(*) >= 3
        ORDER BY n DESC, tag
    """)
    sets = [
        {"value": (r["tag"] or "").strip(), "count": r["n"]}
        for r in rows
        if (r["tag"] or "").strip().endswith(" era")
    ]
    types = [{"value": c["value"], "label": c["label"]}
             for c in STAFF_INVENTORY_TYPE_CHIPS]
    return jsonify({"types": types, "sets": sets})


# ─── Saved print views ────────────────────────────────────────────────────────
import json as _json


def _serialize_view(row) -> dict:
    f = row["filters"]
    if isinstance(f, str):
        try: f = _json.loads(f)
        except Exception: f = {}
    return {
        "id": row["id"],
        "name": row["name"],
        "filters": f or {},
        "sort": row["sort"],
        "show_price": bool(row.get("show_price", True)),
        "created_by": row.get("created_by"),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_by": row.get("updated_by"),
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def _normalize_view_payload(data: dict):
    """Returns (name, filters, sort, show_price) or (None, None, None, None)."""
    name = (data.get("name") or "").strip()
    if not name or len(name) > 200:
        return None, None, None, None
    filters = data.get("filters") or {}
    if not isinstance(filters, dict):
        return None, None, None, None
    # Whitelist filter keys so noise from the client doesn't end up in the JSONB.
    clean = {
        "q": (filters.get("q") or "").strip(),
        "types": [str(t) for t in (filters.get("types") or []) if t in _TYPE_CHIP_BY_VALUE],
        "sets": [str(s) for s in (filters.get("sets") or []) if s],
        "on_display": bool(filters.get("on_display")),
        "in_stock": bool(filters.get("in_stock", True)),
    }
    sort = data.get("sort") or STAFF_INVENTORY_DEFAULT_SORT
    if sort not in STAFF_INVENTORY_SORTS:
        sort = STAFF_INVENTORY_DEFAULT_SORT
    show_price = bool(data.get("show_price", True))
    return name, clean, sort, show_price


@app.route("/api/staff-inventory/views")
def list_saved_views():
    auth_result = require_auth()
    if auth_result:
        return auth_result
    rows = db.query("""
        SELECT id, name, filters, sort, show_price,
               created_by, created_at, updated_by, updated_at
        FROM staff_inventory_saved_views
        ORDER BY LOWER(name)
    """)
    return jsonify({"views": [_serialize_view(r) for r in rows]})


@app.route("/api/staff-inventory/views", methods=["POST"])
def create_saved_view():
    auth_result = require_auth()
    if auth_result:
        return auth_result
    data = request.get_json(silent=True) or {}
    name, filters, sort, show_price = _normalize_view_payload(data)
    if not name:
        return jsonify({"error": "name + filters required"}), 400

    user = get_current_user() or {}
    actor = user.get("name") or user.get("email") or "unknown"
    try:
        row = db.query_one("""
            INSERT INTO staff_inventory_saved_views
                   (name, filters, sort, show_price, created_by, updated_by)
            VALUES (%s, %s::jsonb, %s, %s, %s, %s)
            RETURNING id, name, filters, sort, show_price,
                      created_by, created_at, updated_by, updated_at
        """, (name, _json.dumps(filters), sort, show_price, actor, actor))
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" in msg or "unique" in msg:
            return jsonify({"error": "A view with that name already exists"}), 409
        raise
    return jsonify({"view": _serialize_view(row)})


@app.route("/api/staff-inventory/views/<int:view_id>", methods=["PATCH"])
def update_saved_view(view_id):
    auth_result = require_auth()
    if auth_result:
        return auth_result
    data = request.get_json(silent=True) or {}
    name, filters, sort, show_price = _normalize_view_payload(data)
    if not name:
        return jsonify({"error": "name + filters required"}), 400

    user = get_current_user() or {}
    actor = user.get("name") or user.get("email") or "unknown"
    try:
        row = db.query_one("""
            UPDATE staff_inventory_saved_views
            SET name       = %s,
                filters    = %s::jsonb,
                sort       = %s,
                show_price = %s,
                updated_by = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING id, name, filters, sort, show_price,
                      created_by, created_at, updated_by, updated_at
        """, (name, _json.dumps(filters), sort, show_price, actor, view_id))
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" in msg or "unique" in msg:
            return jsonify({"error": "A view with that name already exists"}), 409
        raise
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"view": _serialize_view(row)})


@app.route("/api/staff-inventory/views/<int:view_id>", methods=["DELETE"])
def delete_saved_view(view_id):
    auth_result = require_auth()
    if auth_result:
        return auth_result
    db.execute("DELETE FROM staff_inventory_saved_views WHERE id = %s", (view_id,))
    return jsonify({"ok": True})


# ─── Print page ──────────────────────────────────────────────────────────────

@app.route("/staff-inventory/print")
def staff_inventory_print():
    """Print-friendly page. Accepts either ?view=<id> to pull a saved view,
    or the same query params as the main page for an ad-hoc print."""
    auth_result = require_auth()
    if auth_result:
        return auth_result

    view_id = request.args.get("view", type=int)
    view = None
    show_price_default = True
    if view_id:
        row = db.query_one("""
            SELECT id, name, filters, sort, show_price,
                   created_by, created_at, updated_by, updated_at
            FROM staff_inventory_saved_views WHERE id = %s
        """, (view_id,))
        if not row:
            return "View not found", 404
        view = _serialize_view(row)
        filters = view["filters"]
        sort = view["sort"]
        title = view["name"]
        show_price_default = view["show_price"]
    else:
        filters = _filters_from_request_args(request.args)
        sort = request.args.get("sort") or STAFF_INVENTORY_DEFAULT_SORT
        title = (request.args.get("title") or "").strip() or "Staff Inventory"

    # URL param wins over the saved-view default so the print toolbar can
    # flip prices on/off without modifying the view itself.
    sp_param = request.args.get("show_price")
    if sp_param is not None:
        show_price = sp_param == "1"
    else:
        show_price = show_price_default

    items = _query_staff_items(filters, sort=sort, limit=500)
    printed_at = datetime.now(timezone.utc).astimezone().strftime("%a %b %d %Y · %I:%M %p")
    return render_template(
        "staff_inventory_print.html",
        title=title, items=items, printed_at=printed_at,
        view=view, item_count=len(items), show_price=show_price,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
