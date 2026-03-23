"""
analytics — analytics.pack-fresh.com (or internal)
SKU sell-through analytics: daily order ingestion + velocity metrics.

Triggered daily via Shopify Flow webhook to /run.
Also exposes /api/analytics for batch lookups from other services.
"""

import os
import logging
import threading
from flask import Flask, request, jsonify

import db
from webhook_verify import verify_flow_signature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


@app.route("/")
def index():
    return jsonify({"service": "analytics", "status": "ok"})


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "analytics"})


@app.route("/run", methods=["POST"])
def run_analytics():
    """
    Trigger the daily analytics pipeline.
    Called by Shopify Flow or manually.
    Runs in background thread, returns immediately.
    """
    # Verify webhook secret if present (optional — also allow manual triggers)
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    if flow_secret and secret and secret != flow_secret:
        return jsonify({"error": "Invalid secret"}), 401

    def _run():
        try:
            from compute import run_full_pipeline
            result = run_full_pipeline()
            logger.info(f"Analytics pipeline complete: {result}")
        except Exception as e:
            logger.exception(f"Analytics pipeline failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True})


@app.route("/run/backfill", methods=["POST"])
def run_backfill():
    """Force a full 90-day backfill (slower, use sparingly)."""
    def _run():
        try:
            from compute import ingest_orders, recompute_analytics
            ingest_orders(full_backfill=True)
            recompute_analytics()
            logger.info("Full backfill complete")
        except Exception as e:
            logger.exception(f"Backfill failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True, "mode": "backfill"})


@app.route("/api/analytics", methods=["POST"])
def batch_analytics():
    """
    Batch lookup SKU analytics by shopify_variant_ids or tcgplayer_ids.
    Body: { "variant_ids": [123, 456] } or { "tcgplayer_ids": [789, 101] }
    """
    data = request.get_json(silent=True) or {}

    variant_ids = data.get("variant_ids")
    tcgplayer_ids = data.get("tcgplayer_ids")

    if variant_ids:
        ph = ",".join(["%s"] * len(variant_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE shopify_variant_id IN ({ph})",
            tuple(int(v) for v in variant_ids)
        )
    elif tcgplayer_ids:
        ph = ",".join(["%s"] * len(tcgplayer_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE tcgplayer_id IN ({ph})",
            tuple(int(t) for t in tcgplayer_ids)
        )
    else:
        return jsonify({"error": "Provide variant_ids or tcgplayer_ids"}), 400

    result = {}
    for r in rows:
        key = r["tcgplayer_id"] or r["shopify_variant_id"]
        result[key] = _ser(r)

    return jsonify({"analytics": result})


@app.route("/api/analytics/summary")
def analytics_summary():
    """Quick stats for the admin dashboard."""
    stats = db.query_one("""
        SELECT
            COUNT(*) AS total_skus,
            COUNT(*) FILTER (WHERE units_sold_90d > 0) AS active_skus,
            AVG(velocity_score) FILTER (WHERE units_sold_90d > 0) AS avg_velocity,
            MAX(computed_at) AS last_computed
        FROM sku_analytics
    """)
    return jsonify(_ser(stats) if stats else {})


def _ser(d):
    out = {}
    for k, v in dict(d).items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            out[k] = float(v)
        else:
            out[k] = v
    return out


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
