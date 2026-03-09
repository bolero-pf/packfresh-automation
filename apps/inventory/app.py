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
from ppt_client import PPTClient

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
ppt_client:     PPTClient     = None


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
        key = os.getenv("PPT_API_KEY")
        if key:
            ppt_client = PPTClient(api_key=key)
        else:
            logger.warning("PPT_API_KEY not set — PPT features disabled")


# ─── Blueprints ────────────────────────────────────────────────────────────────

from routes.inventory import bp as inventory_bp  # noqa: E402
app.register_blueprint(inventory_bp)


# ─── Root ──────────────────────────────────────────────────────────────────────

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
