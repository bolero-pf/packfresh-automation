"""TCG Store - Intake Service (offers.pack-fresh.com).

Thin Flask factory that wires up shared services and registers the
five blueprint modules that hold the real route logic:
  blueprints/sessions_bp.py — session CRUD, uploads, workflow transitions
  blueprints/items_bp.py    — per-item CRUD, state, mark-graded
  blueprints/pricing_bp.py  — offer percentages, refresh-prices, overrides
  blueprints/lookup_bp.py   — /api/lookup/* and /api/search/*
  blueprints/admin_bp.py    — mappings, cache, shopify, barcode, store search
"""
import os
import logging
from decimal import Decimal

from flask import Flask, render_template, jsonify
from flask_cors import CORS

import db
from price_provider import create_price_provider
from shopify_client import ShopifyClient
from cache_manager import CacheManager

# ──────────────────────────────────────────────────────────────────────
# App + JSON provider
# ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

# Flask 3 serializes Decimal to a string ("518.83"), which breaks every
# frontend `.toFixed()` call on a price. Coerce Decimal → float once.
from flask.json.provider import DefaultJSONProvider as _DefaultJSONProvider
class _DecimalJSONProvider(_DefaultJSONProvider):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)
app.json = _DecimalJSONProvider(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# ──────────────────────────────────────────────────────────────────────
# Service handles
# ──────────────────────────────────────────────────────────────────────
try:
    pricing = create_price_provider(db=db)
    app.logger.info(f"Price provider initialized (mode={pricing.mode})")
except Exception as e:
    pricing = None
    app.logger.warning(f"Price provider init failed — price lookups unavailable: {e}")

SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
shopify: ShopifyClient | None = None
if SHOPIFY_TOKEN and SHOPIFY_STORE:
    shopify = ShopifyClient(SHOPIFY_TOKEN, SHOPIFY_STORE)
    app.logger.info(f"Shopify client initialized for {SHOPIFY_STORE}")
else:
    app.logger.warning("SHOPIFY_TOKEN/SHOPIFY_STORE not set — store lookups unavailable")

cache_mgr = CacheManager(db, shopify, table_prefix="inventory_", cache_all_products=True)

INGEST_INTERNAL_URL = os.getenv("INGEST_INTERNAL_URL", "").rstrip("/")

# ──────────────────────────────────────────────────────────────────────
# Auth + shared blueprints
# ──────────────────────────────────────────────────────────────────────
# Blanket JWT cookie auth on everything except /health, /ping, /pf-static.
# Manager-only actions are gated per-route via the manager-override token
# mechanism (see helpers._validate_offer_caps).
from auth import register_auth_hooks
register_auth_hooks(app)

from breakdown_routes import create_breakdown_blueprint
app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: pricing))

# ──────────────────────────────────────────────────────────────────────
# Intake blueprints
# ──────────────────────────────────────────────────────────────────────
from blueprints import sessions_bp, items_bp, pricing_bp, lookup_bp, admin_bp

_common = dict(
    _pricing=pricing,
    _shopify=shopify,
    _cache_mgr=cache_mgr,
    _ingest_url=INGEST_INTERNAL_URL,
    _shopify_store=SHOPIFY_STORE or "",
)
sessions_bp.configure(**_common, _logger=app.logger)
pricing_bp.configure(**_common, _logger=app.logger)
items_bp.configure(**_common, _logger=app.logger)
lookup_bp.configure(**_common, _logger=app.logger)
admin_bp.configure(**_common, _logger=app.logger)

app.register_blueprint(sessions_bp.bp)
app.register_blueprint(pricing_bp.bp)
app.register_blueprint(items_bp.bp)
app.register_blueprint(lookup_bp.bp)
app.register_blueprint(admin_bp.bp)


# ──────────────────────────────────────────────────────────────────────
# DB pool init + dashboard + health
# ──────────────────────────────────────────────────────────────────────
@app.before_request
def ensure_db():
    try:
        db.get_pool()
    except RuntimeError:
        db.init_pool()


@app.teardown_appcontext
def close_db(exception):
    pass  # pool persists across requests


@app.route("/")
def index():
    return render_template("intake_dashboard.html")


@app.route("/health")
def health():
    try:
        db.query("SELECT 1")
        provider_status = "configured" if pricing else "not configured"
        return jsonify({"status": "healthy", "database": "connected", "provider": provider_status})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500
