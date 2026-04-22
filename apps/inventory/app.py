"""
inventory.pack-fresh.com — Standalone Inventory Management Service

Env vars:
    DATABASE_URL            — Postgres (shared with intake/ingestion)
    SHOPIFY_TOKEN           — Shopify Admin API access token
    SHOPIFY_STORE           — Store domain (e.g. my-store.myshopify.com)
    SHOPIFY_STORE_HANDLE    — Short handle for admin links (e.g. "pack-fresh")
    INVENTORY_USER          — Basic auth username
    INVENTORY_PASS          — Basic auth password
    LOCATION_ID             — Shopify location ID for inventory adjustments
    PPT_API_KEY             — PokemonPriceTracker API key
    PF_DRY_RUN              — "1" to prevent Shopify writes (default: 0)
    REMOVE_BG_API_KEY       — Optional: remove.bg key for image processing
    SECRET_KEY              — Flask session secret
"""

import os
import secrets
import logging
from flask import Flask, redirect

import db
from shopify_client import ShopifyClient
from cache_manager import CacheManager
from price_provider import create_price_provider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# ─── Singletons (lazy-initialised on first request) ───────────────────────────

shopify_client: ShopifyClient = None
cache_manager:  CacheManager  = None
ppt_client = None  # PriceProvider instance (Scrydex-first with PPT fallback)


from auth import register_auth_hooks
register_auth_hooks(app, roles=["manager", "owner"])

@app.before_request
def _lazy_init():
    global shopify_client, cache_manager, ppt_client

    if shopify_client is None:
        token = os.getenv("SHOPIFY_TOKEN")
        store = os.getenv("SHOPIFY_STORE")
        if token and store:
            shopify_client = ShopifyClient(token=token, store=store)
        else:
            logger.warning("SHOPIFY_TOKEN or SHOPIFY_STORE not set — Shopify features disabled")

    if cache_manager is None and shopify_client is not None:
        cache_manager = CacheManager(
            db, shopify_client,
            table_prefix="inventory_",
            cache_all_products=True,   # cache slabs + accessories too
        )

    if ppt_client is None:
        try:
            ppt_client = create_price_provider(db=db)
        except Exception as e:
            logger.warning(f"Price provider init failed — pricing features disabled: {e}")

    # Ensure AI enrichment table exists (idempotent)
    if not getattr(_lazy_init, '_ai_table_ok', False):
        try:
            from routes.ai_enrichment import _ensure_table
            _ensure_table()
            _lazy_init._ai_table_ok = True
        except Exception:
            pass


# ─── Blueprints ────────────────────────────────────────────────────────────────

from routes.inventory import bp as inventory_bp  # noqa: E402
from routes.breakdown import bp as breakdown_bp  # noqa: E402
from routes.ai_enrichment import bp as ai_bp  # noqa: E402
from routes.bulk_edit import bp as bulk_edit_bp  # noqa: E402
app.register_blueprint(inventory_bp)
app.register_blueprint(breakdown_bp)
app.register_blueprint(ai_bp)
app.register_blueprint(bulk_edit_bp)

# Shared breakdown-cache blueprint (replaces cache CRUD, search, store-prices in breakdown.py)
from breakdown_routes import create_breakdown_blueprint
app.register_blueprint(
    create_breakdown_blueprint(db, ppt_getter=lambda: ppt_client,
                               url_prefix="/inventory/breakdown/api/cache",
                               name="bd_cache"),
)


# ─── Root ──────────────────────────────────────────────────────────────────────

@app.route("/inventory/api/items")
def api_items():
    """Lightweight item list for breakdown search. Includes drafts, excludes damaged/slabs."""
    from flask import jsonify, request as req
    q = req.args.get("q", "").lower()
    rows = db.query("""
        SELECT c.title AS name, c.shopify_qty, c.shopify_price, c.tcgplayer_id,
               c.shopify_variant_id, c.inventory_item_id, c.status
        FROM inventory_product_cache c
        WHERE c.is_damaged = FALSE
          AND c.tcgplayer_id IS NOT NULL
          AND LOWER(COALESCE(c.tags, '')) NOT LIKE '%slab%'
          AND LOWER(COALESCE(c.tags, '')) NOT LIKE '%graded%'
          AND c.status IN ('ACTIVE', 'DRAFT')
        ORDER BY c.shopify_qty DESC, c.title
    """)
    items = [dict(r) for r in rows]
    if q:
        items = [i for i in items if q in (i["name"] or "").lower()]
    return jsonify({"items": items[:50]})

@app.route("/")
def root():
    return redirect("/inventory")


@app.route("/health")
def health():
    return {"status": "ok", "service": "inventory"}, 200


# ─── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db.init_pool()
    app.run(debug=True, port=5002)
