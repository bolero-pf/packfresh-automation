"""
analytics — analytics.pack-fresh.com (or internal)
SKU sell-through analytics: daily order ingestion + velocity metrics.

Triggered daily via Shopify Flow webhook to /run.
Also exposes /api/analytics for batch lookups from other services.
"""

import os
import logging
import threading
from flask import Flask, request, jsonify, render_template_string, g, Response

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner"], public_prefixes=('/static', '/api/'),
                    skip_jwt_prefixes=('/run',))


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


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
    # Allow authenticated owners OR valid Flow secret
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user and (not flow_secret or secret != flow_secret):
        return jsonify({"error": "Unauthorized"}), 401

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
    """Force a full backfill — 90d orders + 365d customers (slower, use sparingly)."""
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user and (not flow_secret or secret != flow_secret):
        return jsonify({"error": "Unauthorized"}), 401

    def _run():
        try:
            from compute import ingest_orders, recompute_analytics, snapshot_inventory
            from price_history import snapshot_scrydex_prices
            from taxonomy import classify_taxonomy
            from customers import sync_customer_orders, recompute_customer_summaries, backfill_daily_summaries
            from margins import compute_realized_margins

            snapshot_scrydex_prices()
            snapshot_inventory()
            ingest_orders(full_backfill=True)
            recompute_analytics()
            classify_taxonomy()
            sync_customer_orders(full_backfill=True)
            recompute_customer_summaries()
            backfill_daily_summaries(days=365)
            compute_realized_margins()
            logger.info("Full backfill complete")
        except Exception as e:
            logger.exception(f"Backfill failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True, "mode": "backfill"})


@app.route("/run/migrate", methods=["POST"])
def run_migrate():
    """Run the v2 migration script to create new analytics tables."""
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        import subprocess
        import sys
        result = subprocess.run(
            [sys.executable, "migrate_analytics_v2.py"],
            capture_output=True, text=True, timeout=30
        )
        return jsonify({
            "ok": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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


@app.route("/api/browse")
def browse_analytics():
    """Browse SKU analytics with search, sort, pagination."""
    q = (request.args.get("q") or "").strip()
    sort = request.args.get("sort", "velocity_desc")
    page = max(1, int(request.args.get("page", 1)))
    limit = 50
    offset = (page - 1) * limit
    show = request.args.get("show", "all")  # all, selling, stale

    filters = []
    params = []

    if q:
        filters.append("title ILIKE %s")
        params.append(f"%{q}%")
    if show == "selling":
        filters.append("units_sold_90d > 0")
    elif show == "stale":
        filters.append("(units_sold_90d = 0 OR units_sold_90d IS NULL)")

    where = "WHERE " + " AND ".join(filters) if filters else ""

    sort_map = {
        "velocity_desc": "velocity_score DESC NULLS LAST",
        "velocity_asc": "velocity_score ASC NULLS LAST",
        "sold_desc": "units_sold_90d DESC NULLS LAST",
        "sold_asc": "units_sold_90d ASC NULLS LAST",
        "price_desc": "current_price DESC NULLS LAST",
        "price_asc": "current_price ASC NULLS LAST",
        "days_asc": "avg_days_to_sell ASC NULLS LAST",
        "days_desc": "avg_days_to_sell DESC NULLS LAST",
        "title_asc": "title ASC",
    }
    order = sort_map.get(sort, "velocity_score DESC NULLS LAST")

    count_row = db.query_one(f"SELECT COUNT(*) AS total FROM sku_analytics {where}", tuple(params))
    total = count_row["total"] if count_row else 0

    rows = db.query(f"""
        SELECT * FROM sku_analytics {where}
        ORDER BY {order}
        LIMIT %s OFFSET %s
    """, tuple(params) + (limit, offset))

    return jsonify({
        "items": [_ser(r) for r in rows],
        "total": total,
        "page": page,
        "pages": max(1, (total + limit - 1) // limit),
    })


@app.route("/api/status")
def pipeline_status():
    """Show pipeline run status."""
    meta_rows = db.query("SELECT * FROM analytics_meta ORDER BY key")
    daily_count = db.query_one("SELECT COUNT(*) AS c FROM sku_daily_sales")
    analytics_count = db.query_one("SELECT COUNT(*) AS c FROM sku_analytics")
    active = db.query_one("SELECT COUNT(*) AS c FROM sku_analytics WHERE units_sold_90d > 0")
    return jsonify({
        "meta": {r["key"]: {"value": r["value"], "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None} for r in meta_rows},
        "daily_sales_records": daily_count["c"] if daily_count else 0,
        "analytics_skus": analytics_count["c"] if analytics_count else 0,
        "active_skus": active["c"] if active else 0,
    })


# ── Inventory Flow ───────────────────────────────────────────────────────────
# Reconciles to the inventory page. Two inventory streams:
#   • Sealed / store catalog → inventory_product_cache (shopify_qty * shopify_price),
#     the SAME source the inventory page sums. Velocity is LEFT-joined from
#     sku_analytics, which is built FROM sku_daily_sales — so a SKU only has a
#     velocity row once it has sold. Never-sold deadstock has NO row, which is
#     exactly why we base on the cache and treat a missing row as 0 sales.
#   • Raw singles → raw_cards in STORED/DISPLAY (on hand / available to buy).
#     BARCODED is bulk that just sits, excluded. Velocity from SOLD + removal_date.
# Jobs: (A) buying/breakdown — capital not moving; (B) reorder-from-distro.
# All sealed value/stale metrics gate on shopify_qty > 0.

_RAW_ONHAND = "state IN ('STORED','DISPLAY')"  # available to buy; excludes BARCODED bulk

# Damaged variants share a tcgplayer_id with their non-damaged twin and are a
# separate disposition — they must NEVER appear as their own line or pollute
# velocity/dead analytics. Authoritative across every cache-based view here.
_NOT_DAMAGED = "COALESCE(c.is_damaged, FALSE) = FALSE"

# Dead capital only counts items whose CURRENT stock has actually sat without
# selling. "In stock since" = start of the current continuous in-stock streak =
# the day after the most recent qty=0 snapshot (or the first snapshot if never 0).
# Using MIN(snapshot_date) was wrong: an item that sold out and was restocked
# looked old when its current lot just arrived (e.g. a UPC restocked 5 days ago
# read as "in stock 76 days").
DEAD_MIN_AGE_DAYS = 45
_INV_FIRST_SEEN = ("(SELECT shopify_variant_id, "
                   "COALESCE(MAX(snapshot_date) FILTER (WHERE qty=0) + 1, MIN(snapshot_date)) AS first_seen "
                   "FROM sku_daily_inventory GROUP BY 1)")

# Era is a Pokémon concept. For non-Pokémon, "no era" is correct — label by game
# instead of dumping into "(unclassified)". Only Pokémon with a missing era is a
# real gap, surfaced as "(needs era)".
_ERA_LABEL = ("COALESCE(t.era, CASE t.ip "
              "WHEN 'pokemon' THEN '(needs era)' "
              "WHEN 'mtg' THEN 'Magic' WHEN 'onepiece' THEN 'One Piece' "
              "WHEN 'lorcana' THEN 'Lorcana' WHEN 'other' THEN 'Other / misc' "
              "ELSE INITCAP(COALESCE(t.ip,'?')) END)")

# Whitelisted group-by dimensions (column on product_taxonomy t).
_FLOW_DIMS = {
    "product_type": "t.product_type",
    "ip":           "t.ip",
    "form_factor":  "t.form_factor",
    "set_name":     "t.set_name",
    "era":          _ERA_LABEL,
}

# Whitelisted raw-card group-by dimensions (column on raw_cards).
_RAW_DIMS = {
    "game":      "game",
    "set_name":  "set_name",
    "condition": "condition",
    "rarity":    "rarity",
}


@app.route("/api/inventory/flow")
def inventory_flow():
    """Combined KPI strip (sealed + raw) + sealed group-by roll-up with velocity bands."""
    dim = request.args.get("dim", "product_type")
    col = _FLOW_DIMS.get(dim, "t.product_type")

    # Sealed — based on the cache (the inventory page's source), velocity LEFT-joined.
    # Dead capital = in stock, no 90d sales, AND in stock >= DEAD_MIN_AGE_DAYS (so
    # freshly-stocked items that simply haven't sold yet don't get counted).
    sealed = db.query_one(f"""
        WITH inv AS {_INV_FIRST_SEEN}
        SELECT
          COUNT(*) FILTER (WHERE c.shopify_qty > 0)                                 AS in_stock_skus,
          COALESCE(SUM(GREATEST(c.shopify_qty,0)),0)                                AS units,
          COALESCE(SUM(GREATEST(c.shopify_qty,0)*COALESCE(c.shopify_price,0)),0)     AS inv_value,
          -- Matches the Job A list predicate exactly so the KPI == what you can open.
          COALESCE(SUM(CASE WHEN c.shopify_qty>0 AND c.status='ACTIVE'
                            AND COALESCE(s.units_sold_90d,0)=0 AND iv.first_seen <= CURRENT_DATE - %s
                            THEN c.shopify_qty*COALESCE(c.shopify_price,0) END),0)   AS dead_value,
          COUNT(*) FILTER (WHERE c.shopify_qty>0 AND c.status='ACTIVE'
                            AND COALESCE(s.units_sold_90d,0)=0 AND iv.first_seen <= CURRENT_DATE - %s) AS dead_skus,
          COALESCE(SUM(s.units_sold_90d),0)                                         AS sold_90d
        FROM inventory_product_cache c
        LEFT JOIN sku_analytics s USING (shopify_variant_id)
        LEFT JOIN inv iv USING (shopify_variant_id)
        WHERE {_NOT_DAMAGED}
    """, (DEAD_MIN_AGE_DAYS, DEAD_MIN_AGE_DAYS)) or {}

    raw = db.query_one(f"""
        SELECT
          COALESCE(SUM(current_price) FILTER (WHERE {_RAW_ONHAND}),0)               AS raw_value,
          COUNT(*) FILTER (WHERE {_RAW_ONHAND})                                     AS raw_cnt,
          COUNT(*) FILTER (WHERE state='SOLD' AND removal_date >= CURRENT_DATE-90)  AS raw_sold_90d
        FROM raw_cards
    """) or {}

    sv = float(sealed.get("inv_value") or 0)
    rv = float(raw.get("raw_value") or 0)
    kpi = {
        "total_value":   sv + rv,
        "sealed_value":  sv,
        "raw_value":     rv,
        "in_stock_skus": int(sealed.get("in_stock_skus") or 0),
        "sealed_units":  int(sealed.get("units") or 0),
        "raw_cnt":       int(raw.get("raw_cnt") or 0),
        "dead_value":    float(sealed.get("dead_value") or 0),
        "dead_skus":     int(sealed.get("dead_skus") or 0),
        "sold_90d":      int(sealed.get("sold_90d") or 0),
        "raw_sold_90d":  int(raw.get("raw_sold_90d") or 0),
    }

    rows = db.query(f"""
        WITH inv AS {_INV_FIRST_SEEN}
        SELECT
          COALESCE({col}, '(unclassified)')                              AS grp,
          COUNT(*) FILTER (WHERE c.shopify_qty>0)                        AS skus,
          COALESCE(SUM(GREATEST(c.shopify_qty,0)),0)                     AS units,
          COALESCE(SUM(GREATEST(c.shopify_qty,0)*COALESCE(c.shopify_price,0)),0) AS inv_value,
          COALESCE(SUM(s.units_sold_90d),0)                             AS sold_90d,
          COALESCE(SUM(s.units_sold_30d),0)                             AS sold_30d,
          COALESCE(SUM(s.units_sold_7d),0)                              AS sold_7d,
          -- Velocity bands use the TRUE daily rate (1/avg_days_to_sell), which is
          -- first-seen + OOS adjusted — NOT units/90, which understates recent fast movers.
          COALESCE(SUM(CASE WHEN c.shopify_qty>0 AND s.units_sold_90d>0 AND s.avg_days_to_sell > 0
                            AND s.avg_days_to_sell <= 3.333
                            THEN c.shopify_qty*COALESCE(c.shopify_price,0) END),0) AS val_fast,
          COALESCE(SUM(CASE WHEN c.shopify_qty>0 AND s.units_sold_90d>0
                            AND s.avg_days_to_sell > 3.333 AND s.avg_days_to_sell <= 10
                            THEN c.shopify_qty*COALESCE(c.shopify_price,0) END),0) AS val_med,
          COALESCE(SUM(CASE WHEN c.shopify_qty>0 AND s.units_sold_90d>0 AND s.avg_days_to_sell > 10
                            THEN c.shopify_qty*COALESCE(c.shopify_price,0) END),0) AS val_slow,
          -- "Dead $" uses the SAME actionable definition as the dead list/KPI:
          -- active, zero 90d sales, current stock sat >= DEAD_MIN_AGE_DAYS.
          COALESCE(SUM(CASE WHEN c.shopify_qty>0 AND c.status='ACTIVE'
                            AND COALESCE(s.units_sold_90d,0)=0
                            AND iv.first_seen <= CURRENT_DATE - {DEAD_MIN_AGE_DAYS}
                            THEN c.shopify_qty*COALESCE(c.shopify_price,0) END),0) AS val_dead
        FROM inventory_product_cache c
        JOIN product_taxonomy t USING (shopify_variant_id)
        LEFT JOIN sku_analytics s USING (shopify_variant_id)
        LEFT JOIN inv iv USING (shopify_variant_id)
        WHERE {_NOT_DAMAGED}
        GROUP BY 1
        HAVING SUM(GREATEST(c.shopify_qty,0)) > 0 OR SUM(s.units_sold_90d) > 0
        ORDER BY inv_value DESC NULLS LAST
    """)

    return jsonify({"kpi": kpi, "groups": [_ser(r) for r in rows]})


@app.route("/api/inventory/raw")
def inventory_raw():
    """Raw-singles roll-up: on-hand value/count, velocity from SOLD history, avg age."""
    dim = request.args.get("dim", "game")
    col = _RAW_DIMS.get(dim, "game")

    rows = db.query(f"""
        SELECT
          COALESCE({col}::text, '(none)')                                          AS grp,
          COUNT(*) FILTER (WHERE {_RAW_ONHAND})                                     AS cnt,
          COALESCE(SUM(current_price) FILTER (WHERE {_RAW_ONHAND}),0)               AS value,
          COUNT(*) FILTER (WHERE state='SOLD' AND removal_date >= CURRENT_DATE-90)  AS sold_90d,
          COUNT(*) FILTER (WHERE state='SOLD' AND removal_date >= CURRENT_DATE-30)  AS sold_30d,
          COUNT(*) FILTER (WHERE state='SOLD' AND removal_date >= CURRENT_DATE-7)   AS sold_7d,
          ROUND(AVG(CURRENT_DATE - stored_at::date) FILTER (WHERE {_RAW_ONHAND}))   AS avg_age
        FROM raw_cards
        GROUP BY 1
        HAVING COUNT(*) FILTER (WHERE {_RAW_ONHAND}) > 0
            OR COUNT(*) FILTER (WHERE state='SOLD' AND removal_date >= CURRENT_DATE-90) > 0
        ORDER BY value DESC NULLS LAST
    """)
    return jsonify({"groups": [_ser(r) for r in rows]})


@app.route("/api/inventory/raw-aging")
def inventory_raw_aging():
    """Raw dead capital: on-hand singles held longest without selling. Reprice / bundle / move."""
    game = (request.args.get("game") or "").strip()
    where = [_RAW_ONHAND, "COALESCE(current_price,0) >= 1"]
    params = []
    if game:
        where.append("game = %s")
        params.append(game)

    rows = db.query(f"""
        SELECT card_name, set_name, condition, game, current_price,
               (CURRENT_DATE - stored_at::date) AS age_days
        FROM raw_cards
        WHERE {' AND '.join(where)}
        ORDER BY age_days DESC, current_price DESC
        LIMIT 60
    """, tuple(params))
    return jsonify({"items": [_ser(r) for r in rows]})


def _dead_where(ptype):
    """Job A predicate: genuinely not moving = in stock, active, zero sales in 90d, and
    its CURRENT stock has sat continuously >= DEAD_MIN_AGE_DAYS. Deliberately does NOT
    extrapolate "months of supply" from a handful of sales — that mislabels slow movers
    you intentionally stock (a UPC that sold 1 in 90d is not dead capital)."""
    where = [
        "c.shopify_qty > 0",
        "c.status = 'ACTIVE'",
        _NOT_DAMAGED,
        "COALESCE(s.units_sold_90d,0) = 0",
        f"iv.first_seen <= CURRENT_DATE - {DEAD_MIN_AGE_DAYS}",
    ]
    params = []
    if ptype:
        where.append("t.product_type = %s")
        params.append(ptype)
    return " AND ".join(where), params


# Per-unit break-down value (best breakdown variant's component market) keyed by
# tcgplayer_id. Aggregated so a duplicate recipe can't multiply dead-list rows.
_BD_JOIN = ("LEFT JOIN (SELECT tcgplayer_id, MAX(best_variant_market) AS bd "
            "FROM sealed_breakdown_cache GROUP BY 1) b ON b.tcgplayer_id = c.tcgplayer_id")

# Cost basis: prefer the item's own cache unit_cost; fall back to the COGS the
# intake system computed for that product (intake_items.unit_cost_basis, avg by
# tcgplayer_id). Older stock predating cost tracking still gets an estimate this
# way — lifts dead-sealed cost coverage from ~42% to ~74%.
_INTAKE_COST = ("(SELECT tcgplayer_id, AVG(unit_cost_basis) AS ic "
                "FROM intake_items WHERE unit_cost_basis > 0 GROUP BY 1)")

# Component velocity per parent: do the singles/packs this breaks into actually
# move? Picks the best variant (highest component market) per parent, then sums
# its components' 90d sales — catalog packs via sku_analytics, singles via sold
# raw_cards — weighted by quantity_per_parent. Both keyed on component tcgplayer_id.
_PARTS_VEL = (
    "(SELECT bv.ptcg AS tcgplayer_id, "
    "        SUM(comp.quantity_per_parent*(COALESCE(sa.u,0)+COALESCE(rc.s,0))) AS parts_90d "
    " FROM (SELECT DISTINCT ON (ca.tcgplayer_id) ca.tcgplayer_id AS ptcg, v.id AS vid "
    "         FROM sealed_breakdown_cache ca JOIN sealed_breakdown_variants v ON v.breakdown_id = ca.id "
    "         ORDER BY ca.tcgplayer_id, v.total_component_market DESC NULLS LAST) bv "
    " JOIN sealed_breakdown_components comp ON comp.variant_id = bv.vid "
    " LEFT JOIN (SELECT tcgplayer_id, SUM(units_sold_90d) u FROM sku_analytics GROUP BY 1) sa "
    "        ON sa.tcgplayer_id = comp.tcgplayer_id "
    " LEFT JOIN (SELECT tcgplayer_id, count(*) s FROM raw_cards "
    "            WHERE state='SOLD' AND removal_date >= CURRENT_DATE-90 GROUP BY 1) rc "
    "        ON rc.tcgplayer_id = comp.tcgplayer_id "
    " GROUP BY 1)"
)

_DEAD_SELECT = f"""
    SELECT c.title, c.sku, c.tcgplayer_id, t.product_type, t.ip, t.form_factor,
           t.era, t.set_name,
           c.shopify_qty AS current_qty, c.shopify_price AS current_price,
           COALESCE(NULLIF(c.unit_cost,0), ic.ic) AS cost,
           (c.unit_cost IS NULL OR c.unit_cost = 0) AS cost_is_estimate,
           COALESCE(s.units_sold_90d,0) AS units_sold_90d,
           s.total_sold_all_time,
           (c.shopify_qty * COALESCE(c.shopify_price,0)) AS tied_value,
           (CURRENT_DATE - iv.first_seen) AS age_days,
           b.bd AS breakdown_val,
           pv.parts_90d AS parts_90d
    FROM inventory_product_cache c
    JOIN product_taxonomy t USING (shopify_variant_id)
    LEFT JOIN sku_analytics s USING (shopify_variant_id)
    LEFT JOIN inv iv USING (shopify_variant_id)
    LEFT JOIN ic ON ic.tcgplayer_id = c.tcgplayer_id
    LEFT JOIN pv ON pv.tcgplayer_id = c.tcgplayer_id
    {_BD_JOIN}
"""

# Group-by dimensions for slicing the dead-capital set.
_DEAD_DIMS = {
    "era":          _ERA_LABEL,
    "set_name":     "t.set_name",
    "product_type": "t.product_type",
    "ip":           "t.ip",
}

_VINTAGE_ERAS = {"vintage", "xy", "sm", "swsh"}


def _dead_action(r):
    """Suggested action for a stuck item — a starting point, not gospel.
    For sealed with a breakdown recipe this is a LIQUIDITY call: breaking into
    faster-moving parts to free trapped cash. Judged against COST basis (recover
    what you paid), not the inflated sealed price that's keeping it dead. A small
    loss vs cost can still be worth it for the velocity."""
    era = (r.get("era") or "").lower()
    ptype = r.get("product_type")
    price = float(r.get("current_price") or 0)
    cost = float(r.get("cost") or 0) or None
    bd = r.get("breakdown_val")
    bd = float(bd) if bd not in (None, "") else None
    parts = r.get("parts_90d")
    pnote = f" · parts ~{int(parts)}/90d" if parts else ""
    if ptype == "sealed" and bd is not None:
        if cost:
            if bd >= cost:
                return f"Break down (+${round(bd - cost)}/unit over cost){pnote}"
            if bd >= cost * 0.8:
                return f"Break to free cash (~-${round(cost - bd)}/unit){pnote}"
            return "Markdown / hold"
        # No cost basis recorded — fall back to parts-vs-sealed.
        if bd >= price:
            return f"Break down (parts ≥ sealed){pnote}"
        return "Markdown / bundle"
    if era in _VINTAGE_ERAS or price >= 150:
        return "Reprice / hold (vintage)"
    if price < 15:
        return "Bundle / clearance"
    return "Markdown / reprice"


@app.route("/api/inventory/dead")
def inventory_dead():
    """Job A list + total count/value of capital that is not working."""
    ptype = (request.args.get("ptype") or "").strip()
    wsql, params = _dead_where(ptype)

    agg = db.query_one(f"""
        WITH inv AS {_INV_FIRST_SEEN}
        SELECT COUNT(*) AS n, COALESCE(SUM(c.shopify_qty*COALESCE(c.shopify_price,0)),0) AS v
        FROM inventory_product_cache c
        JOIN product_taxonomy t USING (shopify_variant_id)
        LEFT JOIN sku_analytics s USING (shopify_variant_id)
        LEFT JOIN inv iv USING (shopify_variant_id)
        WHERE {wsql}
    """, tuple(params)) or {}

    rows = db.query(f"""
        WITH inv AS {_INV_FIRST_SEEN}, ic AS {_INTAKE_COST}, pv AS {_PARTS_VEL}
        {_DEAD_SELECT}
        WHERE {wsql}
        ORDER BY tied_value DESC NULLS LAST
        LIMIT 300
    """, tuple(params))
    items = []
    for r in rows:
        d = _ser(r)
        d["action"] = _dead_action(d)
        d["needs_recipe"] = d.get("product_type") == "sealed" and d.get("breakdown_val") in (None, "")
        items.append(d)
    # Items the action recommends breaking (recovers cost or strictly beats sealed).
    brk = [d for d in items if (d.get("action") or "").startswith("Break")]
    brk_recover = sum(float(d.get("breakdown_val") or 0) * (d.get("current_qty") or 0) for d in brk)
    # Sealed with no recipe at all — can't be assessed; candidates to build a recipe.
    no_recipe = [d for d in items if d.get("needs_recipe")]
    no_recipe_value = sum(float(d.get("tied_value") or 0) for d in no_recipe)
    return jsonify({
        "items": items,
        "total": int(agg.get("n") or 0),
        "total_value": float(agg.get("v") or 0),
        "breakdown_count": len(brk),
        "breakdown_recover": round(brk_recover),
        "no_recipe_count": len(no_recipe),
        "no_recipe_value": round(no_recipe_value),
    })


def _needs_era_rows():
    """Pokémon products (active) whose authoritative custom.era metafield is unset —
    grouped to product level (era is a product metafield). In-stock first."""
    return db.query(f"""
        SELECT c.shopify_product_id,
               MAX(c.title) AS title,
               MAX(c.handle) AS handle,
               COALESCE(SUM(GREATEST(c.shopify_qty,0)),0) AS qty,
               round(SUM(GREATEST(c.shopify_qty,0)*COALESCE(c.shopify_price,0))::numeric,2) AS value
        FROM inventory_product_cache c
        JOIN product_taxonomy t USING (shopify_variant_id)
        WHERE c.status='ACTIVE' AND t.ip='pokemon' AND c.era IS NULL AND {_NOT_DAMAGED}
        GROUP BY c.shopify_product_id
        ORDER BY (SUM(GREATEST(c.shopify_qty,0)) > 0) DESC, value DESC NULLS LAST
    """)


@app.route("/api/inventory/needs-era")
def inventory_needs_era():
    store = os.getenv("SHOPIFY_STORE", "")
    rows = _needs_era_rows()
    items = []
    for r in rows:
        d = _ser(r)
        pid = r["shopify_product_id"]
        d["admin_url"] = f"https://{store}/admin/products/{pid}" if store else None
        items.append(d)
    in_stock = sum(1 for d in items if (d.get("qty") or 0) > 0)
    return jsonify({"items": items, "total": len(items), "in_stock": in_stock})


@app.route("/api/inventory/needs-era.csv")
def inventory_needs_era_csv():
    import io, csv
    store = os.getenv("SHOPIFY_STORE", "")
    rows = _needs_era_rows()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "Product ID", "Qty In Stock", "Inventory Value", "Admin URL"])
    for r in rows:
        pid = r["shopify_product_id"]
        w.writerow([r.get("title"), pid, r.get("qty"), r.get("value"),
                    f"https://{store}/admin/products/{pid}" if store else ""])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=needs_era.csv"})


@app.route("/api/inventory/dead-by")
def inventory_dead_by():
    """Slice the dead-capital set by a dimension (era / set / type / game) — where it sits."""
    dim = request.args.get("dim", "era")
    col = _DEAD_DIMS.get(dim, "t.era")
    ptype = (request.args.get("ptype") or "").strip()
    wsql, params = _dead_where(ptype)
    rows = db.query(f"""
        WITH inv AS {_INV_FIRST_SEEN}
        SELECT COALESCE({col}::text, '(unclassified)') AS grp,
               COUNT(*) AS skus,
               COALESCE(SUM(c.shopify_qty*COALESCE(c.shopify_price,0)),0) AS value
        FROM inventory_product_cache c
        JOIN product_taxonomy t USING (shopify_variant_id)
        LEFT JOIN sku_analytics s USING (shopify_variant_id)
        LEFT JOIN inv iv USING (shopify_variant_id)
        WHERE {wsql}
        GROUP BY 1
        ORDER BY value DESC NULLS LAST
    """, tuple(params))
    return jsonify({"groups": [_ser(r) for r in rows]})


@app.route("/api/inventory/dead.csv")
def inventory_dead_csv():
    """Full dead-capital worklist as CSV — for a spreadsheet repricing/breakdown pass."""
    import io, csv
    ptype = (request.args.get("ptype") or "").strip()
    wsql, params = _dead_where(ptype)
    rows = db.query(f"""
        WITH inv AS {_INV_FIRST_SEEN}, ic AS {_INTAKE_COST}, pv AS {_PARTS_VEL}
        {_DEAD_SELECT}
        WHERE {wsql}
        ORDER BY tied_value DESC NULLS LAST
    """, tuple(params))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "SKU", "Type", "Game", "Era", "Set", "Qty", "Cost",
                "Cost Estimated?", "Sealed Price", "Breakdown Value", "Parts Sold 90d",
                "No Recipe?", "Capital Tied", "Days In Stock", "Sold 90d",
                "Total Sold All-Time", "Suggested Action"])
    for r in rows:
        no_recipe = r.get("product_type") == "sealed" and r.get("breakdown_val") in (None, "")
        w.writerow([
            r.get("title"), r.get("sku"), r.get("product_type"), r.get("ip"),
            r.get("era"), r.get("set_name"),
            r.get("current_qty"), r.get("cost"),
            "yes" if r.get("cost_is_estimate") else "", r.get("current_price"),
            r.get("breakdown_val"), r.get("parts_90d"),
            "yes" if no_recipe else "",
            round(float(r.get("tied_value") or 0), 2),
            r.get("age_days"), r.get("units_sold_90d"), r.get("total_sold_all_time"),
            _dead_action(r),
        ])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=dead_capital.csv"})


@app.route("/api/inventory/restock")
def inventory_restock():
    """Job B — reorder signals: in stock with under ~30 days left at the current rate,
    plus out-of-stock non-singles that are still selling (sealed / board games / supplies)."""
    ptype = (request.args.get("ptype") or "").strip()
    params = []
    # Running low = in stock, selling, with <=30 days of stock left at the TRUE rate
    # (qty * avg_days_to_sell). Plus out-of-stock non-singles still selling.
    cond = ("(c.shopify_qty > 0 AND s.units_sold_90d > 0 AND s.avg_days_to_sell > 0 "
            "     AND c.shopify_qty * s.avg_days_to_sell <= 30) "
            "OR (c.shopify_qty = 0 AND COALESCE(s.units_sold_30d,0) > 0 AND t.product_type <> 'card')")
    where = [f"({cond})", _NOT_DAMAGED]
    if ptype:
        where.append("t.product_type = %s")
        params.append(ptype)

    rows = db.query(f"""
        SELECT c.title, c.tcgplayer_id, t.product_type, t.ip,
               c.shopify_qty AS current_qty, c.shopify_price AS current_price,
               COALESCE(s.units_sold_90d,0) AS units_sold_90d,
               COALESCE(s.units_sold_30d,0) AS units_sold_30d,
               COALESCE(s.units_sold_7d,0) AS units_sold_7d,
               CASE WHEN s.units_sold_90d > 0 AND s.avg_days_to_sell > 0
                    THEN c.shopify_qty * s.avg_days_to_sell END AS days_inv
        FROM inventory_product_cache c
        JOIN product_taxonomy t USING (shopify_variant_id)
        LEFT JOIN sku_analytics s USING (shopify_variant_id)
        WHERE {' AND '.join(where)}
        ORDER BY (c.shopify_qty = 0) DESC, days_inv ASC NULLS LAST
        LIMIT 60
    """, tuple(params))
    return jsonify({"items": [_ser(r) for r in rows]})


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


# ═══════════════════════════════════════════════════════════════════════════════
# Sales dashboard — live off source tables (customer_orders / realized_margin /
# raw_cards / intake_sessions). Deliberately NOT reading daily_business_summary:
# that pre-aggregate froze each day at its morning slice (the May/June collapse),
# and customer_orders is only ~10k rows so live roll-up is cheap and always correct.
# ═══════════════════════════════════════════════════════════════════════════════
from datetime import date as _date, timedelta as _timedelta

# In-store = picked up over the counter (walk-in POS or hold pickup); shipped = carrier.
# delivery_method comes from Shopify fulfillmentOrders.deliveryMethod.methodType and is
# authoritative — the pos/online `channel` mislabels in-store RETAIL orders as 'online'.
_INSTORE_DM = "delivery_method IN ('RETAIL','PICK_UP')"
_SHIPPED_DM = "delivery_method = 'SHIPPING'"

_MIX_LABELS = {
    "sealed": "Sealed", "card": "Singles / Slabs", "accessory": "Supplies",
    "board_game": "Board Games", "card_game": "Card Games", "puzzle": "Puzzles",
    "raw_single": "Raw Singles", "pokemon": "Pokémon", "magic": "Magic",
    "onepiece": "One Piece", "lorcana": "Lorcana", "yugioh": "Yu-Gi-Oh",
    "other": "Other",
}


# Granularity adapts to the window so every X tick is meaningful (no weekly buckets
# on a 7-day window). day ≤ 31d, week ≤ 100d, else month — applied to ALL sales series.
def _granularity(days):
    if days <= 31:
        return "day"
    if days <= 100:
        return "week"
    return "month"


def _bucket_start(d, gran):
    if gran == "day":
        return d
    if gran == "week":
        return d - _timedelta(days=d.weekday())  # Monday, matches date_trunc('week')
    return d.replace(day=1)


def _next_bucket(d, gran):
    if gran == "day":
        return d + _timedelta(days=1)
    if gran == "week":
        return d + _timedelta(days=7)
    return (d.replace(day=1) + _timedelta(days=32)).replace(day=1)


def _bucket_seq(start, end, gran):
    """All bucket-start dates from start..end so zero-activity buckets still render."""
    out, cur = [], _bucket_start(start, gran)
    while cur <= end:
        out.append(cur)
        cur = _next_bucket(cur, gran)
    return out


def _margin_by_bucket(start, end, gran):
    """Combined gross margin $ per bucket: catalog (realized_margin) + raw singles."""
    mmap = {}
    for r in db.query(f"""SELECT date_trunc('{gran}', sale_date)::date::text b, COALESCE(SUM(gross_margin),0) m
                          FROM realized_margin WHERE sale_date BETWEEN %s AND %s GROUP BY 1""", (start, end)):
        mmap[r["b"]] = mmap.get(r["b"], 0) + float(r["m"])
    for r in db.query(f"""SELECT date_trunc('{gran}', removal_date)::date::text b,
                                 COALESCE(SUM(sale_price-cost_basis) FILTER (WHERE cost_basis IS NOT NULL),0) m
                          FROM raw_cards WHERE state='SOLD' AND removal_date::date BETWEEN %s AND %s GROUP BY 1""", (start, end)):
        mmap[r["b"]] = mmap.get(r["b"], 0) + float(r["m"])
    return mmap


def _sales_series(start, end, gran):
    """Per-bucket orders / net / units / in-store / shipped / margin for [start,end]."""
    rows = db.query(f"""
        SELECT date_trunc('{gran}', order_date)::date::text b, COUNT(*) orders,
               COALESCE(SUM(net_amount),0) net, COALESCE(SUM(item_count),0) units,
               COALESCE(SUM(net_amount) FILTER (WHERE {_INSTORE_DM}),0) instore,
               COALESCE(SUM(net_amount) FILTER (WHERE {_SHIPPED_DM}),0) shipped,
               COALESCE(SUM(net_amount) FILTER (WHERE delivery_method IS NULL),0) unattr
        FROM customer_orders WHERE order_date BETWEEN %s AND %s GROUP BY 1
    """, (start, end))
    smap = {r["b"]: r for r in rows}
    mmap = _margin_by_bucket(start, end, gran)
    out = []
    for b in _bucket_seq(start, end, gran):
        ds = b.isoformat()
        r = smap.get(ds)
        out.append({
            "d": ds,
            "orders": int(r["orders"]) if r else 0,
            "net": round(float(r["net"]), 2) if r else 0,
            "units": int(r["units"]) if r else 0,
            "instore": round(float(r["instore"]), 2) if r else 0,
            "shipped": round(float(r["shipped"]), 2) if r else 0,
            "unattr": round(float(r["unattr"]), 2) if r else 0,
            "margin": round(mmap.get(ds, 0), 2),
        })
    return out


def _margin_estimate(start, end):
    """Margin over the FULL window.
    - PROVEN = real margin where we have captured cost (cost-known, COGS-tracking era).
    - EST. TOTAL = proven + a category-rate projection of EVERY other dollar of
      merchandise revenue in the window — pre-COGS-era sales, no-cost stock, and
      suspect-cost rows alike. So "90 days" of est. margin really covers 90 days; the
      cost_coverage % says how much is proven vs estimated. Conservative: older stock
      runs higher-margin than the recent category rate we project at.
    Catalog revenue is the catalog merchandise base (sku_daily_sales × taxonomy, full
    window); raw singles come from raw_cards (cost_basis frozen at intake)."""
    # Per-category proven margin + cost-known revenue → category rates (tracked era)
    prov = db.query("""
        SELECT COALESCE(t.product_type,'other') pt,
               COALESCE(SUM(rm.gross_margin),0) margin,
               COALESCE(SUM(rm.revenue) FILTER (WHERE rm.gross_margin IS NOT NULL),0) costed_rev
        FROM realized_margin rm LEFT JOIN product_taxonomy t USING (shopify_variant_id)
        WHERE rm.sale_date BETWEEN %s AND %s GROUP BY 1
    """, (start, end))
    rate, costed_by = {}, {}
    cat_margin = cat_costed = 0.0
    for r in prov:
        m, cr = float(r["margin"]), float(r["costed_rev"])
        cat_margin += m
        cat_costed += cr
        costed_by[r["pt"]] = cr
        if cr > 0:
            rate[r["pt"]] = m / cr
    blended = (cat_margin / cat_costed) if cat_costed > 0 else 0.0
    # Total catalog merchandise revenue by category (full window, catalog only)
    cat_total = cat_est = 0.0
    for r in db.query("""SELECT t.product_type pt, COALESCE(SUM(s.revenue),0) rev
                         FROM sku_daily_sales s JOIN product_taxonomy t USING (shopify_variant_id)
                         WHERE s.sale_date BETWEEN %s AND %s GROUP BY 1""", (start, end)):
        pt = r["pt"] or "other"
        trev = float(r["rev"])
        cat_total += trev
        cat_est += rate.get(pt, blended) * max(0.0, trev - costed_by.get(pt, 0.0))
    cat_est += cat_margin  # proven portion
    # Raw singles (cost_basis frozen at intake; mostly cost-known)
    raw = db.query_one("""SELECT COALESCE(SUM(sale_price-cost_basis) FILTER (WHERE cost_basis IS NOT NULL),0) margin,
                                 COALESCE(SUM(sale_price) FILTER (WHERE cost_basis IS NOT NULL),0) costed_rev,
                                 COALESCE(SUM(sale_price),0) total_rev
                          FROM raw_cards WHERE state='SOLD' AND removal_date::date BETWEEN %s AND %s""", (start, end)) or {}
    raw_margin = float(raw.get("margin") or 0)
    raw_costed = float(raw.get("costed_rev") or 0)
    raw_total = float(raw.get("total_rev") or 0)
    raw_rate = (raw_margin / raw_costed) if raw_costed > 0 else blended
    raw_est = raw_margin + raw_rate * max(0.0, raw_total - raw_costed)

    proven = cat_margin + raw_margin
    proven_costed = cat_costed + raw_costed
    est = cat_est + raw_est
    total_merch = cat_total + raw_total
    return {
        "margin": round(proven, 2),
        "margin_pct": round(proven / proven_costed * 100, 1) if proven_costed > 0 else 0,
        "est_margin": round(est, 2),
        "est_margin_pct": round(est / total_merch * 100, 1) if total_merch > 0 else 0,
        "cost_coverage": round(proven_costed / total_merch * 100, 0) if total_merch > 0 else 0,
    }


def _kpi_block(start, end):
    o = db.query_one(f"""
        SELECT COUNT(*) orders, COALESCE(SUM(net_amount),0) net, COALESCE(SUM(item_count),0) units,
               COALESCE(AVG(net_amount),0) aov,
               COUNT(DISTINCT customer_id) FILTER (WHERE customer_id<>0) custs,
               COALESCE(SUM(net_amount) FILTER (WHERE {_INSTORE_DM}),0) instore,
               COALESCE(SUM(net_amount) FILTER (WHERE {_SHIPPED_DM}),0) shipped
        FROM customer_orders WHERE order_date BETWEEN %s AND %s""", (start, end)) or {}
    return {
        "orders": int(o.get("orders") or 0), "net": round(float(o.get("net") or 0), 2),
        "units": int(o.get("units") or 0), "aov": round(float(o.get("aov") or 0), 2),
        "custs": int(o.get("custs") or 0),
        "instore": round(float(o.get("instore") or 0), 2),
        "shipped": round(float(o.get("shipped") or 0), 2),
        **_margin_estimate(start, end),
    }


@app.route("/api/sales/overview")
def sales_overview():
    days = max(1, min(int(request.args.get("days", 90)), 730))
    gran = _granularity(days)
    today = _date.today()
    start = today - _timedelta(days=days - 1)
    prev_end = start - _timedelta(days=1)
    prev_start = prev_end - _timedelta(days=days - 1)
    return jsonify({
        "days": days, "granularity": gran,
        "kpi": _kpi_block(start, today),
        "prev": _kpi_block(prev_start, prev_end),
        "series": _sales_series(start, today, gran),
    })


@app.route("/api/sales/buysell")
def sales_buysell():
    """Intake spend (by ingested_at) vs net sales per bucket — the cash-conversion view."""
    days = max(7, min(int(request.args.get("days", 180)), 730))
    gran = _granularity(days)
    today = _date.today()
    start = today - _timedelta(days=days - 1)
    sell = db.query(f"""SELECT date_trunc('{gran}',order_date)::date::text b, COALESCE(SUM(net_amount),0) v
                        FROM customer_orders WHERE order_date >= %s GROUP BY 1""", (start,))
    # Buy = what we PAID (total_offer_amount), attributed to ingested_at, only sessions
    # that actually got ingested. partially_ingested counted too (bulk still clearing).
    buy = db.query(f"""SELECT date_trunc('{gran}',ingested_at::date)::date::text b,
                              COALESCE(SUM(total_offer_amount),0) v
                       FROM intake_sessions
                       WHERE status IN ('ingested','partially_ingested')
                         AND ingested_at IS NOT NULL AND ingested_at::date >= %s
                       GROUP BY 1""", (start,))
    sm = {r["b"]: float(r["v"]) for r in sell}
    bm = {r["b"]: float(r["v"]) for r in buy}
    series = [{"period": b.isoformat(), "sell": round(sm.get(b.isoformat(), 0), 2),
               "buy": round(bm.get(b.isoformat(), 0), 2)} for b in _bucket_seq(start, today, gran)]
    partial = db.query_one("""SELECT COUNT(*) c, COALESCE(SUM(total_offer_amount),0) v
                              FROM intake_sessions
                              WHERE status='partially_ingested' AND ingested_at::date >= %s""", (start,)) or {}
    return jsonify({
        "granularity": gran, "series": series,
        "partial": {"cnt": int(partial.get("c") or 0), "value": round(float(partial.get("v") or 0), 2)},
    })


@app.route("/api/sales/mix")
def sales_mix():
    """Stacked revenue per bucket by stream or game (catalog + raw singles), + totals pivot."""
    days = max(7, min(int(request.args.get("days", 90)), 730))
    gran = _granularity(days)
    dim = request.args.get("dim", "stream")
    today = _date.today()
    start = today - _timedelta(days=days - 1)
    if dim == "ip":
        cat_expr = "LOWER(COALESCE(t.ip,'other'))"
        raw_cat = "LOWER(COALESCE(game,'other'))"
    else:  # stream
        cat_expr = "COALESCE(t.product_type,'other')"
        raw_cat = "'raw_single'"
    catalog = db.query(f"""SELECT date_trunc('{gran}',s.sale_date)::date::text b, {cat_expr} cat,
                          SUM(s.revenue) rev, SUM(s.units_sold) units
                          FROM sku_daily_sales s LEFT JOIN product_taxonomy t USING (shopify_variant_id)
                          WHERE s.sale_date >= %s GROUP BY 1,2""", (start,))
    raw = db.query(f"""SELECT date_trunc('{gran}',removal_date)::date::text b, {raw_cat} cat,
                      SUM(sale_price) rev, COUNT(*) units
                      FROM raw_cards WHERE state='SOLD' AND removal_date::date >= %s GROUP BY 1,2""", (start,))
    # Catalog taxonomy says 'mtg'; raw_cards.game says 'magic' — same game, merge them.
    ip_norm = {"mtg": "magic"} if dim == "ip" else {}
    per, totals = {}, {}
    for r in list(catalog) + list(raw):
        b = r["b"]
        cat = r["cat"] or "other"
        cat = ip_norm.get(cat, cat)
        rev = float(r["rev"] or 0)
        units = int(r["units"] or 0)
        per.setdefault(b, {})
        per[b][cat] = per[b].get(cat, 0) + rev
        t = totals.setdefault(cat, {"rev": 0, "units": 0})
        t["rev"] += rev
        t["units"] += units
    ranked = sorted(totals, key=lambda c: -totals[c]["rev"])
    keep = ranked[:8]
    periods = [b.isoformat() for b in _bucket_seq(start, today, gran)]
    series = []
    for p in periods:
        vals = {}
        for cat, rev in per.get(p, {}).items():
            k = cat if cat in keep else "other"
            vals[k] = vals.get(k, 0) + rev
        series.append({"period": p, "vals": {k: round(v, 2) for k, v in vals.items()}})
    cats = keep if "other" in keep or not [c for c in ranked if c not in keep] else keep + ["other"]
    totlist = [{"grp": _MIX_LABELS.get(c, c.title()), "key": c,
                "rev": round(totals[c]["rev"], 2), "units": totals[c]["units"]} for c in ranked]
    return jsonify({
        "granularity": gran, "periods": periods, "cats": cats,
        "labels": {c: _MIX_LABELS.get(c, c.title()) for c in cats},
        "series": series, "totals": totlist,
    })


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Analytics · Pack Fresh</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📈</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header h1 { font-size:1.3rem; }
.main { max-width:1100px; margin:0 auto; padding:20px; }
.stats { display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 18px; min-width:140px; }
.controls { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; align-items:center; }
.controls input, .controls select { height:38px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 12px; font-size:0.85rem; font-family:inherit; outline:none; }
.controls input:focus { border-color:var(--accent); }
.controls input { flex:1; min-width:200px; }
th { cursor:pointer; }
th:hover { color:var(--text); }
.vel-fast { color:var(--green); font-weight:700; }
.vel-med { color:var(--amber); font-weight:600; }
.vel-slow { color:var(--red); }
.vel-none { color:var(--dim); }
.pg { display:flex; gap:4px; justify-content:center; margin-top:16px; }
.pg button { height:32px; min-width:32px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); cursor:pointer; font-size:0.8rem; }
.pg button.active { background:var(--accent); border-color:var(--accent); color:#fff; }
.pg button:disabled { opacity:0.3; }
.empty { text-align:center; padding:40px; color:var(--dim); }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:40px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
/* tabs */
.tabs { display:flex; gap:2px; }
.tab { height:34px; padding:0 16px; background:transparent; border:1px solid var(--border); border-radius:8px; color:var(--dim); cursor:pointer; font:inherit; font-size:0.85rem; font-weight:600; }
.tab.active { background:var(--accent); border-color:var(--accent); color:#fff; }
/* inventory flow */
.section-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin:22px 0 10px; flex-wrap:wrap; }
.section-head h2 { font-size:1rem; }
.section-head select { height:34px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 10px; font:inherit; font-size:0.82rem; outline:none; }
.legend { display:flex; gap:16px; font-size:0.74rem; color:var(--dim); margin-bottom:6px; }
.legend i.sw { display:inline-block; width:11px; height:11px; border-radius:2px; margin-right:5px; vertical-align:-1px; }
.bar { display:flex; height:14px; border-radius:3px; overflow:hidden; background:var(--s2); min-width:40px; }
.bar > span { display:block; height:100%; }
.flow-cols { display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:10px; }
@media (max-width:860px) { .flow-cols { grid-template-columns:1fr; } }
.hint { font-size:0.76rem; color:var(--dim); margin:0 0 10px; }
.list-summary { display:flex; align-items:center; gap:10px; justify-content:space-between; font-size:0.8rem; margin-bottom:8px; }
.list-summary b { font-size:0.95rem; }
.scroll-list { max-height:520px; overflow-y:auto; padding-right:4px; }
.lst { display:flex; flex-direction:column; gap:6px; }
.row { display:flex; align-items:center; gap:10px; padding:8px 10px; background:var(--surface); border:1px solid var(--border); border-radius:8px; }
.row .nm { flex:1; min-width:0; font-size:0.82rem; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.row .nm small { display:block; font-weight:400; color:var(--dim); }
.row .mv { text-align:right; font-size:0.78rem; white-space:nowrap; }
.row .mv b { font-size:0.9rem; }
.act { display:inline-block; padding:1px 7px; border-radius:10px; background:var(--s2); border:1px solid var(--border); color:var(--text); font-size:0.72rem; }
.subtabs { display:flex; gap:4px; margin:4px 0 18px; border-bottom:1px solid var(--border); flex-wrap:wrap; }
.subtab { height:36px; padding:0 14px; background:transparent; border:none; border-bottom:2px solid transparent; color:var(--dim); cursor:pointer; font:inherit; font-size:0.86rem; font-weight:600; margin-bottom:-1px; }
.subtab.active { color:var(--text); border-bottom-color:var(--accent); }
.subtab:hover { color:var(--text); }
/* charts */
.card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:16px 18px; margin-bottom:18px; }
.card h3 { font-size:0.92rem; margin:0 0 2px; }
.card .sub-note { font-size:0.74rem; color:var(--dim); margin:0 0 12px; }
.chart-wrap { position:relative; width:100%; }
.chart-wrap svg { display:block; width:100%; height:auto; }
.chart-grid line { stroke:var(--border); stroke-width:1; }
.chart-axis { fill:var(--dim); font-size:11px; font-family:'DM Mono',monospace; }
.chart-line { fill:none; stroke-width:2; }
.chart-ma { fill:none; stroke-width:1.5; stroke-dasharray:4 3; opacity:0.9; }
.chart-tip { position:absolute; pointer-events:none; background:var(--s2); border:1px solid var(--border); border-radius:8px; padding:8px 10px; font-size:0.74rem; box-shadow:0 4px 14px rgba(0,0,0,0.35); z-index:5; min-width:130px; transform:translate(-50%,-105%); display:none; white-space:nowrap; }
.chart-tip b { display:block; margin-bottom:4px; font-size:0.78rem; }
.chart-tip .tr { display:flex; justify-content:space-between; gap:14px; }
.chart-tip .sw { display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:5px; vertical-align:0; }
.chart-guide { stroke:var(--dim); stroke-width:1; stroke-dasharray:3 3; opacity:0; }
.legend2 { display:flex; gap:14px; flex-wrap:wrap; font-size:0.76rem; color:var(--dim); margin-top:10px; }
.legend2 span { display:inline-flex; align-items:center; }
.legend2 i { width:11px; height:11px; border-radius:3px; margin-right:6px; }
.kpi-delta { font-size:0.72rem; margin-top:2px; }
.kpi-up { color:var(--green); } .kpi-down { color:var(--red); }
.win-sel { display:flex; gap:4px; }
.win-sel button { height:32px; padding:0 12px; background:var(--s2); border:1px solid var(--border); border-radius:7px; color:var(--dim); cursor:pointer; font:inherit; font-size:0.8rem; font-weight:600; }
.win-sel button.active { background:var(--accent); border-color:var(--accent); color:#fff; }
</style>
</head>
<body>
<div class="header">
  <h1>Analytics</h1>
  <div class="tabs">
    <button class="tab active" data-tab="skus" onclick="switchTab('skus')">SKUs</button>
    <button class="tab" data-tab="flow" onclick="switchTab('flow')">Inventory Flow</button>
    <button class="tab" data-tab="sales" onclick="switchTab('sales')">Sales</button>
  </div>
  <button class="btn btn-secondary btn-sm" onclick="runPipeline()" id="run-btn">▶ Run Now</button>
  <button class="btn btn-secondary btn-sm" onclick="runBackfill()" id="bf-btn">↻ Full Backfill</button>
  <span id="status-label" style="font-size:0.78rem;color:var(--dim);margin-left:auto;"></span>
</div>

<div class="main" id="tab-skus">
  <div class="stats" id="stats"><div class="spinner"></div></div>

  <div class="controls">
    <input type="text" id="q" placeholder="Search by product name..." oninput="debounce()">
    <select id="show-filter" onchange="doSearch()">
      <option value="all">All SKUs</option>
      <option value="selling">Has Sales</option>
      <option value="stale">No Sales (90d)</option>
    </select>
    <select id="sort-select" onchange="doSearch()">
      <option value="velocity_asc">Fastest Selling (fewest days left)</option>
      <option value="sold_desc">Units Sold (high)</option>
      <option value="sold_asc">Units Sold (low)</option>
      <option value="days_asc">Avg Days to Sell (fast)</option>
      <option value="price_desc">Price (high)</option>
      <option value="title_asc">Name A-Z</option>
    </select>
  </div>

  <div id="results"><div class="spinner"></div></div>
  <div class="pg" id="pagination"></div>
</div>

<div class="main" id="tab-flow" style="display:none;">
  <div class="subtabs">
    <button class="subtab active" data-sub="overview" onclick="switchSub('overview')">Overview</button>
    <button class="subtab" data-sub="dead" onclick="switchSub('dead')">Dead Capital</button>
    <button class="subtab" data-sub="reorder" onclick="switchSub('reorder')">Reorder</button>
    <button class="subtab" data-sub="raw" onclick="switchSub('raw')">Raw Singles</button>
    <button class="subtab" data-sub="needsera" onclick="switchSub('needsera')">Needs Era</button>
  </div>

  <div class="sub" id="sub-overview">
    <div class="stats" id="flow-kpis"><div class="spinner"></div></div>
    <div class="section-head">
      <h2>Where capital sits vs. where it sells</h2>
      <select id="flow-dim" onchange="loadFlow()">
        <option value="product_type">Group by Product Type</option>
        <option value="ip">Group by Game / IP</option>
        <option value="form_factor">Group by Form Factor</option>
        <option value="set_name">Group by Set</option>
        <option value="era">Group by Era</option>
      </select>
    </div>
    <div class="legend">
      <span><i class="sw" style="background:var(--green)"></i>Fast</span>
      <span><i class="sw" style="background:var(--amber)"></i>Medium</span>
      <span><i class="sw" style="background:#d98a4b"></i>Slow</span>
      <span><i class="sw" style="background:var(--dim)"></i>No sales</span>
    </div>
    <div id="flow-rollup"><div class="spinner"></div></div>
  </div>

  <div class="sub" id="sub-dead" style="display:none;">
    <div class="section-head">
      <h2>🟡 Dead capital — where it sits</h2>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <select id="dead-ptype" onchange="reloadDead()">
          <option value="">All types</option>
          <option value="sealed">Sealed</option>
          <option value="card">Cards / slabs</option>
          <option value="board_game">Board games</option>
          <option value="card_game">Card games</option>
          <option value="puzzle">Puzzles</option>
          <option value="accessory">Supplies</option>
        </select>
        <select id="dead-dim" onchange="loadDeadBy()">
          <option value="era">Break down by Era</option>
          <option value="set_name">by Set</option>
          <option value="product_type">by Type</option>
          <option value="ip">by Game</option>
        </select>
      </div>
    </div>
    <p class="hint">Current stock sat 45+ days with zero sales in 90 days — genuinely not moving. (Restocks reset the clock, so freshly-bought stock isn't counted.)</p>
    <div id="dead-summary" class="list-summary"></div>
    <div id="dead-by"><div class="spinner"></div></div>
    <div id="dead-list" class="scroll-list"><div class="spinner"></div></div>
  </div>

  <div class="sub" id="sub-reorder" style="display:none;">
    <div class="section-head"><h2>🔵 Reorder from distro</h2>
      <select id="restock-ptype" onchange="loadRestock()">
        <option value="">All types</option>
        <option value="sealed">Sealed</option>
        <option value="board_game">Board games</option>
        <option value="card_game">Card games</option>
        <option value="puzzle">Puzzles</option>
        <option value="accessory">Supplies</option>
      </select>
    </div>
    <p class="hint">Under ~30 days of stock left at the current rate (out-of-stock non-singles shown first). Reorder soon.</p>
    <div id="restock-list"><div class="spinner"></div></div>
  </div>

  <div class="sub" id="sub-raw" style="display:none;">
    <div class="section-head">
      <h2>Raw singles — on hand (stored + display)</h2>
      <select id="raw-dim" onchange="loadRaw()">
        <option value="game">Group by Game</option>
        <option value="set_name">Group by Set</option>
        <option value="condition">Group by Condition</option>
        <option value="rarity">Group by Rarity</option>
      </select>
    </div>
    <div id="raw-rollup"><div class="spinner"></div></div>
    <div class="section-head">
      <h2>⏳ Aged raw singles</h2>
      <select id="raw-aging-game" onchange="loadRawAging()">
        <option value="">All games</option>
        <option value="pokemon">Pokemon</option>
        <option value="magic">Magic</option>
        <option value="onepiece">One Piece</option>
      </select>
    </div>
    <p class="hint">On-hand singles held longest without selling — oldest first. Candidates to reprice, bundle, or move.</p>
    <div id="raw-aging-list"><div class="spinner"></div></div>
  </div>

  <div class="sub" id="sub-needsera" style="display:none;">
    <div class="section-head">
      <h2>⚠ Pokémon missing era metafield</h2>
      <a class="btn btn-secondary btn-sm" href="/api/inventory/needs-era.csv">⬇ Export CSV</a>
    </div>
    <p class="hint" id="needs-era-hint">Active Pokémon products with no custom.era metafield — click a title to set it in Shopify.</p>
    <div id="needs-era-list" class="scroll-list"><div class="spinner"></div></div>
  </div>
</div>

<div class="main" id="tab-sales" style="display:none;">
  <div class="section-head" style="margin-top:0;">
    <div class="subtabs" style="margin:0;border:none;">
      <button class="subtab active" data-ssub="overview" onclick="switchSales('overview')">Overview</button>
      <button class="subtab" data-ssub="channel" onclick="switchSales('channel')">Channel</button>
      <button class="subtab" data-ssub="buysell" onclick="switchSales('buysell')">Buy vs Sell</button>
      <button class="subtab" data-ssub="mix" onclick="switchSales('mix')">Product Mix</button>
    </div>
    <div class="win-sel" id="sales-win">
      <button data-d="7" onclick="setSalesWin(7)">7d</button>
      <button data-d="30" onclick="setSalesWin(30)">30d</button>
      <button data-d="90" class="active" onclick="setSalesWin(90)">90d</button>
      <button data-d="365" onclick="setSalesWin(365)">1yr</button>
    </div>
  </div>

  <div class="ssub" id="ssub-overview">
    <div class="stats" id="sales-kpis"><div class="spinner"></div></div>
    <div class="card">
      <h3>Net revenue</h3>
      <p class="sub-note">Daily net sales with a 7-day moving average. In-store + shipped combined.</p>
      <div class="chart-wrap" id="chart-rev"></div>
    </div>
    <div class="card">
      <h3>Orders &amp; average order value</h3>
      <p class="sub-note">Order count (bars) with AOV trend (line).</p>
      <div class="chart-wrap" id="chart-orders"></div>
    </div>
  </div>

  <div class="ssub" id="ssub-channel" style="display:none;">
    <div class="stats" id="channel-kpis"><div class="spinner"></div></div>
    <div class="card">
      <h3>In-store vs shipped — revenue</h3>
      <p class="sub-note">Stacked daily revenue by delivery method (Shopify deliveryMethod — RETAIL/PICK_UP = in-store, SHIPPING = shipped). The pos/online channel field mislabels in-store, so this is the real split.</p>
      <div class="chart-wrap" id="chart-channel"></div>
    </div>
  </div>

  <div class="ssub" id="ssub-buysell" style="display:none;">
    <div class="stats" id="buysell-kpis"><div class="spinner"></div></div>
    <div class="card">
      <h3>Buy vs Sell — weekly capital flow</h3>
      <p class="sub-note">What we paid to intake (by ingested date, offer amount) vs net sales. Buy is attributed when stock is <b>ingested</b>, not offered — partially-ingested lots (bulk still clearing) are flagged below.</p>
      <div class="chart-wrap" id="chart-buysell"></div>
      <div class="legend2" id="buysell-legend"></div>
      <p class="hint" id="buysell-partial" style="margin-top:10px;"></p>
    </div>
  </div>

  <div class="ssub" id="ssub-mix" style="display:none;">
    <div class="section-head">
      <h2>Product mix over time</h2>
      <select id="mix-dim" onchange="loadMix()">
        <option value="stream">By stream (sealed / singles / raw …)</option>
        <option value="ip">By game (Pokémon / Magic …)</option>
      </select>
    </div>
    <div class="card">
      <p class="sub-note">Weekly revenue, stacked by category. Raw singles are categorized from the card table by barcode; catalog from taxonomy.</p>
      <div class="chart-wrap" id="chart-mix"></div>
      <div class="legend2" id="mix-legend"></div>
    </div>
    <div class="card">
      <h3>Totals for the window</h3>
      <div id="mix-totals"><div class="spinner"></div></div>
    </div>
  </div>
</div>

<script>
let _page = 1, _timer = null;

function debounce() { clearTimeout(_timer); _timer = setTimeout(() => doSearch(), 400); }

async function doSearch(page) {
  _page = page || 1;
  const q = document.getElementById('q').value.trim();
  const show = document.getElementById('show-filter').value;
  const sort = document.getElementById('sort-select').value;
  const el = document.getElementById('results');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch(`/api/browse?q=${encodeURIComponent(q)}&show=${show}&sort=${sort}&page=${_page}`);
    const d = await r.json();
    renderTable(d.items, d.total, d.page, d.pages);
  } catch(e) { el.innerHTML = `<div class="empty">${e.message}</div>`; }
}

function velBadge(doi, units, qty) {
  if (!units || units === 0) return '<span class="badge badge-dim">No Sales</span>';
  const daily = (units / 90).toFixed(1);
  let label, cls;
  if (daily >= 5) { label = 'Very Fast'; cls = 'badge-green'; }
  else if (daily >= 1) { label = 'Fast'; cls = 'badge-green'; }
  else if (daily >= 0.3) { label = 'Medium'; cls = 'badge-amber'; }
  else if (daily >= 0.1) { label = 'Slow'; cls = 'badge-red'; }
  else { label = 'Very Slow'; cls = 'badge-red'; }
  const stockStr = qty === 0 ? ' · <span style="color:var(--red);">OOS</span>' : doi < 9999 ? ' · ' + Math.round(doi) + 'd stock' : '';
  return '<span class="badge ' + cls + '">' + label + '</span> <small style="color:var(--dim);">' + daily + '/day' + stockStr + '</small>';
}

function renderTable(items, total, page, pages) {
  const el = document.getElementById('results');
  if (!items.length) { el.innerHTML = '<div class="empty">No data yet. Run the backfill first.</div>'; return; }
  el.innerHTML = `
    <div style="font-size:0.78rem;color:var(--dim);margin-bottom:8px;">${total} SKUs</div>
    <div style="overflow-x:auto;"><table>
      <thead><tr>
        <th>Product</th><th>Velocity</th><th>Sold 90d</th><th>Sold 30d</th><th>Sold 7d</th>
        <th>Avg Days</th><th>Qty</th><th>Price</th><th>Avg Sale</th><th>Trend</th><th>OOS Days</th>
      </tr></thead>
      <tbody>${items.map(i => {
        const trend = i.price_trend_pct || 0;
        const trendColor = trend > 5 ? 'var(--green)' : trend < -5 ? 'var(--red)' : 'var(--dim)';
        const trendStr = (trend >= 0 ? '+' : '') + trend.toFixed(1) + '%';
        const days = i.avg_days_to_sell ? i.avg_days_to_sell.toFixed(1) + 'd' : '—';
        return '<tr>' +
          '<td><strong>' + (i.title||'—') + '</strong>' +
            (i.tcgplayer_id ? '<br><small style="color:var(--dim)">TCG#' + i.tcgplayer_id + '</small>' : '') +
          '</td>' +
          '<td>' + velBadge(i.velocity_score, i.units_sold_90d, i.current_qty) + '</td>' +
          '<td style="font-weight:600;">' + (i.units_sold_90d||0) + '</td>' +
          '<td>' + (i.units_sold_30d||0) + '</td>' +
          '<td>' + (i.units_sold_7d||0) + '</td>' +
          '<td>' + days + '</td>' +
          '<td style="color:' + (i.current_qty > 0 ? 'var(--green)' : 'var(--red)') + ';font-weight:600;">' + (i.current_qty||0) + '</td>' +
          '<td>$' + (i.current_price||0).toFixed(2) + '</td>' +
          '<td>$' + (i.avg_sale_price||0).toFixed(2) + '</td>' +
          '<td style="color:' + trendColor + '">' + trendStr + '</td>' +
          '<td>' + (i.out_of_stock_days||0) + '</td>' +
        '</tr>';
      }).join('')}</tbody>
    </table></div>`;
  renderPagination(page, pages);
}

function renderPagination(page, pages) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.innerHTML = ''; return; }
  let h = '<button ' + (page<=1?'disabled':'') + ' onclick="doSearch(' + (page-1) + ')">←</button>';
  for (let p = Math.max(1,page-2); p <= Math.min(pages,page+2); p++) {
    h += '<button class="' + (p===page?'active':'') + '" onclick="doSearch(' + p + ')">' + p + '</button>';
  }
  h += '<button ' + (page>=pages?'disabled':'') + ' onclick="doSearch(' + (page+1) + ')">→</button>';
  el.innerHTML = h;
}

async function loadStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="stat-label">Total SKUs</div><div class="stat-val">${d.analytics_skus}</div></div>
      <div class="stat"><div class="stat-label">Active (has sales)</div><div class="stat-val" style="color:var(--green);">${d.active_skus}</div></div>
      <div class="stat"><div class="stat-label">Daily Records</div><div class="stat-val">${d.daily_sales_records}</div></div>
      <div class="stat"><div class="stat-label">Last Run</div><div class="stat-val" style="font-size:0.85rem;">${d.meta?.last_order_ingest?.value ? new Date(d.meta.last_order_ingest.value).toLocaleString() : 'Never'}</div></div>
    `;
    document.getElementById('status-label').textContent = d.analytics_skus > 0 ? '' : 'No data — run backfill first';
  } catch(e) {}
}

async function runPipeline() {
  const btn = document.getElementById('run-btn');
  btn.disabled = true; btn.textContent = 'Running...';
  try { await fetch('/run', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
  catch(e) {}
  btn.textContent = 'Started!';
  setTimeout(() => { btn.disabled = false; btn.textContent = '▶ Run Now'; loadStatus(); doSearch(); }, 5000);
}

async function runBackfill() {
  const btn = document.getElementById('bf-btn');
  btn.disabled = true; btn.textContent = 'Backfilling...';
  try { await fetch('/run/backfill', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
  catch(e) {}
  btn.textContent = 'Started!';
  setTimeout(() => { btn.disabled = false; btn.textContent = '↻ Full Backfill'; loadStatus(); doSearch(); }, 15000);
}

// ── Inventory Flow ──────────────────────────────────────────────
let _flowLoaded = false;

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.getElementById('tab-skus').style.display = name === 'skus' ? '' : 'none';
  document.getElementById('tab-flow').style.display = name === 'flow' ? '' : 'none';
  document.getElementById('tab-sales').style.display = name === 'sales' ? '' : 'none';
  if (name === 'flow' && !_flowLoaded) { _flowLoaded = true; switchSub('overview'); }
  if (name === 'sales' && !_salesLoaded) { _salesLoaded = true; switchSales('overview'); }
}

const _subLoaded = {};
const _SUBS = ['overview', 'dead', 'reorder', 'raw', 'needsera'];
function switchSub(name) {
  document.querySelectorAll('.subtab').forEach(t => t.classList.toggle('active', t.dataset.sub === name));
  _SUBS.forEach(s => { document.getElementById('sub-' + s).style.display = s === name ? '' : 'none'; });
  if (_subLoaded[name]) return;
  _subLoaded[name] = true;
  if (name === 'overview') loadFlow();
  else if (name === 'dead') reloadDead();
  else if (name === 'reorder') loadRestock();
  else if (name === 'raw') { loadRaw(); loadRawAging(); }
  else if (name === 'needsera') loadNeedsEra();
}

async function loadNeedsEra() {
  const el = document.getElementById('needs-era-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/needs-era');
    const d = await r.json();
    document.getElementById('needs-era-hint').textContent =
      d.total + ' Pokémon products missing era (' + d.in_stock + ' in stock) — click a title to set custom.era in Shopify.';
    if (!d.items.length) { el.innerHTML = '<div class="empty">Every Pokémon product has an era. 🎉</div>'; return; }
    el.innerHTML = '<div class="lst">' + d.items.map(i => {
      const name = i.admin_url
        ? '<a href="' + i.admin_url + '" target="_blank" rel="noopener" style="color:var(--accent)">' + (i.title||'—') + '</a>'
        : (i.title||'—');
      return '<div class="row"><div class="nm">' + name + '<small>qty ' + (i.qty||0) + '</small></div>' +
        '<div class="mv"><b>' + fmtMoney(i.value) + '</b></div></div>';
    }).join('') + '</div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function reloadDead() { loadDead(); loadDeadBy(); }

async function loadDeadBy() {
  const dim = document.getElementById('dead-dim').value;
  const ptype = document.getElementById('dead-ptype').value;
  const el = document.getElementById('dead-by');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/dead-by?dim=' + dim + '&ptype=' + encodeURIComponent(ptype));
    const d = await r.json();
    if (!d.groups.length) { el.innerHTML = ''; return; }
    const maxVal = Math.max(...d.groups.map(g => g.value || 0), 1);
    const total = d.groups.reduce((a, g) => a + (g.value || 0), 0) || 1;
    el.innerHTML = '<div style="overflow-x:auto;margin-bottom:14px;"><table>' +
      '<thead><tr><th style="text-align:left;">Where it sits</th><th>Tied up</th><th></th><th>% </th><th>Items</th></tr></thead><tbody>' +
      d.groups.map(g => {
        const v = g.value || 0;
        const bar = '<div class="bar" style="width:' + Math.max(v/maxVal*100,2) + '%;"><span style="width:100%;background:var(--red);"></span></div>';
        return '<tr><td style="text-align:left;font-weight:600;">' + g.grp + '</td>' +
          '<td style="font-weight:600;">' + fmtMoney(v) + '</td>' +
          '<td style="min-width:140px;">' + bar + '</td>' +
          '<td>' + (v/total*100).toFixed(0) + '%</td>' +
          '<td>' + (g.skus||0) + '</td></tr>';
      }).join('') + '</tbody></table></div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function fmtMoney(n) { return '$' + Math.round(n || 0).toLocaleString(); }

async function loadFlow() {
  const dim = document.getElementById('flow-dim').value;
  const roll = document.getElementById('flow-rollup');
  roll.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/flow?dim=' + dim);
    const d = await r.json();
    renderKpis(d.kpi);
    renderRollup(d.groups);
  } catch(e) { roll.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function renderKpis(k) {
  const su = k.sealed_units || 0, sold = k.sold_90d || 0;
  const sellThrough = (su + sold) > 0 ? (sold / (su + sold) * 100) : 0;
  document.getElementById('flow-kpis').innerHTML = `
    <div class="stat"><div class="stat-label">Total Inventory</div><div class="stat-val">${fmtMoney(k.total_value)}</div></div>
    <div class="stat"><div class="stat-label">Sealed / Catalog</div><div class="stat-val">${fmtMoney(k.sealed_value)}</div><div class="stat-label">${(k.in_stock_skus||0).toLocaleString()} SKUs</div></div>
    <div class="stat"><div class="stat-label">Raw Singles</div><div class="stat-val">${fmtMoney(k.raw_value)}</div><div class="stat-label">${(k.raw_cnt||0).toLocaleString()} on hand</div></div>
    <div class="stat"><div class="stat-label">Dead Capital</div><div class="stat-val" style="color:var(--red);">${fmtMoney(k.dead_value)}</div><div class="stat-label">${k.dead_skus||0} SKUs, no 90d sales</div></div>
    <div class="stat"><div class="stat-label">Sealed Sell-Through 90d</div><div class="stat-val" style="color:var(--green);">${sellThrough.toFixed(0)}%</div></div>
    <div class="stat"><div class="stat-label">Raw Sold 90d</div><div class="stat-val" style="color:var(--green);">${(k.raw_sold_90d||0).toLocaleString()}</div></div>`;
}

function renderRollup(groups) {
  const el = document.getElementById('flow-rollup');
  if (!groups || !groups.length) { el.innerHTML = '<div class="empty">No data.</div>'; return; }
  const maxVal = Math.max(...groups.map(g => g.inv_value || 0), 1);
  const totalVal = groups.reduce((a, g) => a + (g.inv_value || 0), 0) || 1;
  el.innerHTML = `<div style="overflow-x:auto;"><table>
    <thead><tr>
      <th style="text-align:left;">Group</th><th>Capital (by velocity)</th><th>Value</th><th>% of $</th>
      <th>Sold 90d</th><th>30d</th><th>7d</th><th>Sell-Thru</th><th>Dead $</th>
    </tr></thead><tbody>${groups.map(g => {
      const v = g.inv_value || 0;
      const seg = (x) => v > 0 ? (x / v * 100) : 0;
      const barW = (v / maxVal * 100);
      const st = (g.units + g.sold_90d) > 0 ? (g.sold_90d / (g.units + g.sold_90d) * 100) : 0;
      const bar = '<div class="bar" style="width:' + Math.max(barW, 2) + '%;">' +
        '<span style="width:' + seg(g.val_fast) + '%;background:var(--green);"></span>' +
        '<span style="width:' + seg(g.val_med) + '%;background:var(--amber);"></span>' +
        '<span style="width:' + seg(g.val_slow) + '%;background:#d98a4b;"></span>' +
        '<span style="width:' + seg(g.val_dead) + '%;background:var(--dim);"></span></div>';
      return '<tr>' +
        '<td style="text-align:left;font-weight:600;">' + g.grp + '</td>' +
        '<td style="min-width:200px;">' + bar + '</td>' +
        '<td style="font-weight:600;">' + fmtMoney(v) + '</td>' +
        '<td>' + (v / totalVal * 100).toFixed(0) + '%</td>' +
        '<td style="font-weight:600;">' + (g.sold_90d||0) + '</td>' +
        '<td>' + (g.sold_30d||0) + '</td>' +
        '<td>' + (g.sold_7d||0) + '</td>' +
        '<td>' + st.toFixed(0) + '%</td>' +
        '<td style="color:' + (g.val_dead > 0 ? 'var(--red)' : 'var(--dim)') + ';">' + fmtMoney(g.val_dead) + '</td>' +
      '</tr>';
    }).join('')}</tbody></table></div>`;
}

async function loadDead() {
  const ptype = document.getElementById('dead-ptype').value;
  const el = document.getElementById('dead-list');
  const sum = document.getElementById('dead-summary');
  el.innerHTML = '<div class="spinner"></div>'; sum.innerHTML = '';
  try {
    const r = await fetch('/api/inventory/dead?ptype=' + encodeURIComponent(ptype));
    const d = await r.json();
    const bd = d.breakdown_count
      ? ' · break down <b>' + d.breakdown_count + '</b> to recover <b style="color:var(--green)">' + fmtMoney(d.breakdown_recover) + '</b> cash'
      : ' · <span style="color:var(--dim)">none worth breaking down — markdown/bundle play</span>';
    const nr = d.no_recipe_count
      ? ' · <b style="color:var(--amber)">' + d.no_recipe_count + '</b> sealed have no recipe (' + fmtMoney(d.no_recipe_value) + ' — add to assess)'
      : '';
    sum.innerHTML = '<span><b>' + d.total + '</b> items · <b>' + fmtMoney(d.total_value) + '</b> tied up' + bd + nr + '</span>' +
      '<a class="btn btn-secondary btn-sm" href="/api/inventory/dead.csv?ptype=' + encodeURIComponent(ptype) + '">⬇ Export CSV</a>';
    if (!d.items.length) { el.innerHTML = '<div class="empty">Nothing stale here.</div>'; return; }
    const shown = d.items.length < d.total ? ' <small style="color:var(--dim)">(showing top ' + d.items.length + ' of ' + d.total + ' — Export CSV for all)</small>' : '';
    el.innerHTML = '<div class="lst">' + d.items.map(i => {
      const tied = (i.current_qty || 0) * (i.current_price || 0);
      const ctx = [i.era || i.product_type, i.set_name].filter(Boolean).join(' · ');
      const sub = 'qty ' + (i.current_qty||0) + (i.age_days != null ? ' · ' + i.age_days + 'd in stock' : '') +
        (i.total_sold_all_time ? '' : ' · never sold');
      const nums = [];
      if (i.cost) nums.push('cost ' + fmtMoney(i.cost) + (i.cost_is_estimate ? '~' : ''));
      nums.push('sealed ' + fmtMoney(i.current_price));
      if (i.breakdown_val != null) nums.push('parts ' + fmtMoney(i.breakdown_val) +
        (i.parts_90d ? ' (~' + Math.round(i.parts_90d) + ' sold/90d)' : ''));
      const cmp = (i.cost || i.breakdown_val != null)
        ? '<br><small style="color:var(--dim)">' + nums.join(' · ') + '</small>' : '';
      const recipeTag = i.needs_recipe
        ? ' <span class="act" style="border-color:var(--amber);color:var(--amber)">no recipe</span>' : '';
      return '<div class="row"><div class="nm">' + (i.title || '—') +
        '<small>' + ctx + ' — ' + sub + '</small></div>' +
        '<div class="mv"><b>' + fmtMoney(tied) + '</b><br>' +
        '<small><span class="act">' + (i.action || '') + '</span>' + recipeTag + '</small>' + cmp + '</div></div>';
    }).join('') + '</div>' + shown;
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

async function loadRestock() {
  const ptype = document.getElementById('restock-ptype').value;
  const el = document.getElementById('restock-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/restock?ptype=' + encodeURIComponent(ptype));
    const d = await r.json();
    if (!d.items.length) { el.innerHTML = '<div class="empty">Nothing running low.</div>'; return; }
    el.innerHTML = '<div class="lst">' + d.items.map(i => {
      const out = (i.current_qty || 0) === 0;
      const lead = out ? '<b style="color:var(--red);">OUT</b>'
                       : '<b>' + Math.round(i.days_inv) + 'd</b> left';
      return '<div class="row"><div class="nm">' + (i.title || '—') +
        '<small>' + (i.product_type || '?') + ' · qty ' + (i.current_qty||0) + '</small></div>' +
        '<div class="mv">' + lead + '<br><small>' + (i.units_sold_30d||0) + ' · 30d / ' + (i.units_sold_7d||0) + ' · 7d</small></div></div>';
    }).join('') + '</div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

async function loadRaw() {
  const dim = document.getElementById('raw-dim').value;
  const el = document.getElementById('raw-rollup');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/raw?dim=' + dim);
    const d = await r.json();
    if (!d.groups.length) { el.innerHTML = '<div class="empty">No raw inventory.</div>'; return; }
    const maxVal = Math.max(...d.groups.map(g => g.value || 0), 1);
    el.innerHTML = '<div style="overflow-x:auto;"><table>' +
      '<thead><tr><th style="text-align:left;">Group</th><th>On-hand Value</th><th>On hand</th>' +
      '<th>Sold 90d</th><th>30d</th><th>7d</th><th>Avg Age</th></tr></thead><tbody>' +
      d.groups.map(g => {
        const v = g.value || 0;
        const bar = '<div class="bar" style="width:' + Math.max(v/maxVal*100, 2) + '%;">' +
          '<span style="width:100%;background:var(--accent);"></span></div>';
        return '<tr>' +
          '<td style="text-align:left;font-weight:600;">' + g.grp + '</td>' +
          '<td style="min-width:160px;"><div style="display:flex;align-items:center;gap:8px;">' + bar +
            '<span style="font-weight:600;white-space:nowrap;">' + fmtMoney(v) + '</span></div></td>' +
          '<td>' + (g.cnt||0) + '</td>' +
          '<td style="font-weight:600;">' + (g.sold_90d||0) + '</td>' +
          '<td>' + (g.sold_30d||0) + '</td>' +
          '<td>' + (g.sold_7d||0) + '</td>' +
          '<td>' + (g.avg_age != null ? g.avg_age + 'd' : '—') + '</td>' +
        '</tr>';
      }).join('') + '</tbody></table></div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

async function loadRawAging() {
  const game = document.getElementById('raw-aging-game').value;
  const el = document.getElementById('raw-aging-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/raw-aging?game=' + encodeURIComponent(game));
    const d = await r.json();
    if (!d.items.length) { el.innerHTML = '<div class="empty">Nothing aged.</div>'; return; }
    el.innerHTML = '<div class="lst">' + d.items.map(i =>
      '<div class="row"><div class="nm">' + (i.card_name || '—') +
        '<small>' + (i.set_name || '?') + ' · ' + (i.condition || '?') + '</small></div>' +
        '<div class="mv"><b>' + Math.round(i.age_days) + 'd</b> held<br><small>' + fmtMoney(i.current_price) + '</small></div></div>'
    ).join('') + '</div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

// ── Sales dashboard ─────────────────────────────────────────────
let _salesLoaded = false, _salesWin = 90, _salesSub = 'overview', _salesData = null;
const _SSUBS = ['overview', 'channel', 'buysell', 'mix'];
const PALETTE = ['#5b8def','#34c759','#ff9f0a','#ff6b6b','#bf5af2','#32ade6','#ffd60a','#8e8e93','#d98a4b'];
const _charts = {};

function fmtK(n){ n = Math.round(n||0); const a = Math.abs(n); if (a >= 1000) return '$'+(n/1000).toFixed(a>=10000?0:1)+'k'; return '$'+n; }
function fmtDate(iso, gran){ if(!iso) return ''; const p = iso.split('-'); const mon = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][(+p[1])-1]; if(gran==='month') return mon+" '"+p[0].slice(2); return mon+' '+(+p[2]); }
function movingAvg(arr,w){ const out=[]; for(let i=0;i<arr.length;i++){ let s=0,c=0; for(let j=Math.max(0,i-w+1);j<=i;j++){ s+=arr[j]||0; c++; } out.push(c?s/c:0);} return out; }
function legendHtml(items){ return items.map(i=>'<span><i style="background:'+i.color+'"></i>'+i.label+'</span>').join(''); }
function niceMax(v){ if(v<=0) return 1; const p=Math.pow(10,Math.floor(Math.log10(v))); const f=v/p; let nf; if(f<=1)nf=1;else if(f<=2)nf=2;else if(f<=2.5)nf=2.5;else if(f<=5)nf=5;else nf=10; return nf*p; }
function pctDelta(c,p){ if(!p) return null; return (c-p)/p*100; }
function deltaHtml(c,p){ const d=pctDelta(c,p); if(d===null||!isFinite(d)) return ''; const up=d>=0; return '<div class="kpi-delta '+(up?'kpi-up':'kpi-down')+'">'+(up?'▲':'▼')+' '+Math.abs(d).toFixed(0)+'% vs prev</div>'; }

function switchSales(name){
  _salesSub = name;
  document.querySelectorAll('[data-ssub]').forEach(t => t.classList.toggle('active', t.dataset.ssub === name));
  _SSUBS.forEach(s => document.getElementById('ssub-'+s).style.display = s === name ? '' : 'none');
  loadSalesSub(name);
}
function loadSalesSub(name){
  if (name==='overview') loadSalesOverview();
  else if (name==='channel') loadChannel();
  else if (name==='buysell') loadBuySell();
  else if (name==='mix') loadMix();
}
function setSalesWin(d){
  _salesWin = d; _salesData = null;
  document.querySelectorAll('#sales-win button').forEach(b => b.classList.toggle('active', +b.dataset.d === d));
  loadSalesSub(_salesSub);
}

// Generic SVG chart: series = [{key,label,color,type:'area'|'line'|'ma'|'bar',data:[],axis,money}]
function buildChart(wrapId, periods, series, opts){
  opts = opts || {};
  const wrap = document.getElementById(wrapId);
  const n = periods.length;
  if (!n){ wrap.innerHTML = '<div class="empty">No sales in this window.</div>'; return; }
  const hasRight = series.some(s => s.axis === 'right');
  const W = 1000, H = opts.h || 300, padL = 54, padR = hasRight ? 56 : 16, padT = 14, padB = 30;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const xAt = i => n <= 1 ? padL + plotW/2 : padL + (i/(n-1))*plotW;
  const areas = series.filter(s => s.type === 'area');
  const bars = series.filter(s => s.type === 'bar');
  const stackTop = new Array(n).fill(0);
  areas.forEach(a => { for(let i=0;i<n;i++) stackTop[i] += (+a.data[i]||0); });
  let leftMax = 0, rightMax = 0;
  for(let i=0;i<n;i++) leftMax = Math.max(leftMax, stackTop[i]);
  series.forEach(s => { if(s.type==='area') return; const t = s.axis==='right'?'r':'l'; s.data.forEach(v => { const val=+v||0; if(t==='r') rightMax=Math.max(rightMax,val); else leftMax=Math.max(leftMax,val); }); });
  leftMax = niceMax(leftMax); rightMax = niceMax(rightMax);
  const yL = v => padT + plotH - (v/leftMax)*plotH;
  const yR = v => padT + plotH - (v/rightMax)*plotH;

  let grid = '', body = '', steps = 4;
  for(let s=0;s<=steps;s++){
    const val = leftMax*s/steps, y = yL(val);
    grid += '<line x1="'+padL+'" y1="'+y+'" x2="'+(W-padR)+'" y2="'+y+'"/>';
    grid += '<text class="chart-axis" x="'+(padL-6)+'" y="'+(y+3)+'" text-anchor="end">'+(opts.money?fmtK(val):Math.round(val))+'</text>';
    if (hasRight){ const ry = yR(rightMax*s/steps); body += '<text class="chart-axis" x="'+(W-padR+6)+'" y="'+(ry+3)+'" text-anchor="start">'+(opts.rightMoney?fmtK(rightMax*s/steps):Math.round(rightMax*s/steps))+'</text>'; }
  }
  // bars (behind)
  const bw = Math.max(1, (plotW/Math.max(n,1))*0.55);
  bars.forEach(b => { for(let i=0;i<n;i++){ const v=+b.data[i]||0; if(v<=0) continue; const y=yL(v); body += '<rect x="'+(xAt(i)-bw/2)+'" y="'+y+'" width="'+bw+'" height="'+(padT+plotH-y)+'" fill="'+b.color+'" opacity="0.5" rx="1"/>'; } });
  // stacked areas
  const bottom = new Array(n).fill(0);
  areas.forEach(a => {
    const top=[], bot=[];
    for(let i=0;i<n;i++){ const v=+a.data[i]||0; bot[i]=bottom[i]; bottom[i]+=v; top[i]=bottom[i]; }
    let pts=''; for(let i=0;i<n;i++) pts += xAt(i)+','+yL(top[i])+' ';
    for(let i=n-1;i>=0;i--) pts += xAt(i)+','+yL(bot[i])+' ';
    body += '<polygon points="'+pts.trim()+'" fill="'+a.color+'" opacity="0.30"/>';
    let lp=''; for(let i=0;i<n;i++) lp += (i?'L':'M')+xAt(i)+' '+yL(top[i])+' ';
    body += '<path d="'+lp+'" class="chart-line" stroke="'+a.color+'"/>';
  });
  // lines + ma
  series.filter(s => s.type==='line'||s.type==='ma').forEach(l => {
    const yf = l.axis==='right'?yR:yL; let lp='', started=false;
    for(let i=0;i<n;i++){ const v=+l.data[i]; if(v==null||isNaN(v)) continue; lp += (started?'L':'M')+xAt(i)+' '+yf(v)+' '; started=true; }
    body += '<path d="'+lp+'" class="'+(l.type==='ma'?'chart-ma':'chart-line')+'" stroke="'+l.color+'"/>';
  });
  // x labels
  const want = Math.min(8, n);
  for(let k=0;k<want;k++){ const i = want<=1?0:Math.round(k/(want-1)*(n-1)); body += '<text class="chart-axis" x="'+xAt(i)+'" y="'+(H-8)+'" text-anchor="middle">'+fmtDate(periods[i], opts.gran)+'</text>'; }
  // guide + hit area
  body += '<line class="chart-guide" id="'+wrapId+'-guide" x1="0" y1="'+padT+'" x2="0" y2="'+(padT+plotH)+'"/>';
  body += '<rect x="'+padL+'" y="'+padT+'" width="'+plotW+'" height="'+plotH+'" fill="transparent"/>';

  wrap.innerHTML = '<svg viewBox="0 0 '+W+' '+H+'"><g class="chart-grid">'+grid+'</g>'+body+'</svg>';
  const tip = document.createElement('div'); tip.className='chart-tip'; tip.id=wrapId+'-tip'; wrap.appendChild(tip);
  _charts[wrapId] = {periods, series, gran:opts.gran, geom:{W,padL,padT,plotW,plotH,n}};
  attachHover(wrapId);
}

function attachHover(wrapId){
  const wrap = document.getElementById(wrapId);
  const svg = wrap.querySelector('svg');
  const guide = document.getElementById(wrapId+'-guide');
  const tip = document.getElementById(wrapId+'-tip');
  const c = _charts[wrapId], G = c.geom;
  svg.addEventListener('mousemove', ev => {
    const rect = svg.getBoundingClientRect();
    const vbx = (ev.clientX-rect.left)/rect.width*G.W;
    let i = G.n<=1?0:Math.round((vbx-G.padL)/G.plotW*(G.n-1));
    i = Math.max(0, Math.min(G.n-1, i));
    const x = G.n<=1?G.padL+G.plotW/2:G.padL+(i/(G.n-1))*G.plotW;
    guide.setAttribute('x1',x); guide.setAttribute('x2',x); guide.style.opacity=0.7;
    let rows='';
    c.series.forEach(s => { if(s.noTip) return; const v=s.data[i]; rows += '<div class="tr"><span><i class="sw" style="background:'+s.color+'"></i>'+s.label+'</span><b>'+(s.money===false?Math.round(v||0).toLocaleString():fmtMoney(v))+'</b></div>'; });
    tip.innerHTML = '<b>'+(c.gran==='week'?'Week of ':'')+fmtDate(c.periods[i], c.gran)+'</b>'+rows;
    tip.style.display='block';
    tip.style.left = (x/G.W*rect.width)+'px';
    tip.style.top = (ev.clientY-rect.top-10)+'px';
  });
  svg.addEventListener('mouseleave', () => { tip.style.display='none'; guide.style.opacity=0; });
}

async function loadSalesOverview(){
  document.getElementById('sales-kpis').innerHTML = '<div class="spinner"></div>';
  try {
    const d = await (await fetch('/api/sales/overview?days='+_salesWin)).json();
    _salesData = d;
    const k = d.kpi, p = d.prev;
    const cards = [
      {label:'Net revenue', val:fmtMoney(k.net), cur:k.net, prev:p.net},
      {label:'Orders', val:k.orders.toLocaleString(), cur:k.orders, prev:p.orders},
      {label:'Avg order value', val:fmtMoney(k.aov), cur:k.aov, prev:p.aov},
      {label:'Gross margin (proven)', val:fmtMoney(k.margin), sub:k.margin_pct+'% · cost known on '+k.cost_coverage+'% of window sales', color:'var(--green)', cur:k.margin, prev:p.margin},
      {label:'Est. total margin', val:'≥ '+fmtMoney(k.est_margin), sub:k.est_margin_pct+'% · pre-COGS + no-cost projected at category rates — conservative floor', color:'var(--green)', cur:k.est_margin, prev:p.est_margin},
      {label:'Units sold', val:k.units.toLocaleString(), cur:k.units, prev:p.units},
      {label:'Customers', val:k.custs.toLocaleString(), cur:k.custs, prev:p.custs},
    ];
    document.getElementById('sales-kpis').innerHTML = cards.map(c =>
      '<div class="stat"><div class="stat-label">'+c.label+'</div><div class="stat-val" style="'+(c.color?'color:'+c.color:'')+'">'+c.val+'</div>'+
      (c.sub?'<div class="stat-label">'+c.sub+'</div>':'')+deltaHtml(c.cur,c.prev)+'</div>').join('');
    const periods = d.series.map(s=>s.d), net = d.series.map(s=>s.net);
    const revSeries = [{key:'net', label:'Net revenue', color:'var(--accent)', type:'area', data:net}];
    if (d.granularity === 'day') revSeries.push({key:'ma', label:'7-day avg', color:'var(--green)', type:'ma', data:movingAvg(net,7)});
    buildChart('chart-rev', periods, revSeries, {money:true, gran:d.granularity});
    buildChart('chart-orders', periods, [
      {key:'orders', label:'Orders', color:'var(--accent)', type:'bar', data:d.series.map(s=>s.orders), money:false},
      {key:'aov', label:'AOV', color:'var(--amber)', type:'line', axis:'right', data:d.series.map(s=> s.orders? s.net/s.orders : 0)},
    ], {money:false, rightMoney:true, gran:d.granularity});
  } catch(e){ document.getElementById('sales-kpis').innerHTML = '<div class="empty">'+e.message+'</div>'; }
}

async function loadChannel(){
  if (!_salesData) await loadSalesOverview();
  const d = _salesData; if (!d) return;
  const k = d.kpi, tot = (k.instore+k.shipped)||1;
  document.getElementById('channel-kpis').innerHTML = [
    {label:'In-store revenue', val:fmtMoney(k.instore), sub:(k.instore/tot*100).toFixed(0)+'% of attributed', color:'#34c759'},
    {label:'Shipped revenue', val:fmtMoney(k.shipped), sub:(k.shipped/tot*100).toFixed(0)+'% of attributed', color:'#5b8def'},
    {label:'Total net revenue', val:fmtMoney(k.net)},
  ].map(c => '<div class="stat"><div class="stat-label">'+c.label+'</div><div class="stat-val" style="'+(c.color?'color:'+c.color:'')+'">'+c.val+'</div>'+(c.sub?'<div class="stat-label">'+c.sub+'</div>':'')+'</div>').join('');
  const periods = d.series.map(s=>s.d);
  buildChart('chart-channel', periods, [
    {key:'instore', label:'In-store', color:'#34c759', type:'area', data:d.series.map(s=>s.instore)},
    {key:'shipped', label:'Shipped', color:'#5b8def', type:'area', data:d.series.map(s=>s.shipped)},
  ], {money:true, gran:d.granularity});
}

async function loadBuySell(){
  document.getElementById('buysell-kpis').innerHTML = '<div class="spinner"></div>';
  try {
    const d = await (await fetch('/api/sales/buysell?days='+_salesWin)).json();
    const periods = d.series.map(s=>s.period);
    buildChart('chart-buysell', periods, [
      {key:'buy', label:'Buy (ingested cost)', color:'#ff9f0a', type:'area', data:d.series.map(s=>s.buy)},
      {key:'sell', label:'Sell (net sales)', color:'#34c759', type:'line', data:d.series.map(s=>s.sell)},
    ], {money:true, gran:d.granularity});
    document.getElementById('buysell-legend').innerHTML = legendHtml([{label:'Buy — what we paid to intake (ingested)',color:'#ff9f0a'},{label:'Sell — net sales',color:'#34c759'}]);
    const tb = d.series.reduce((a,s)=>a+s.buy,0), ts = d.series.reduce((a,s)=>a+s.sell,0);
    document.getElementById('buysell-kpis').innerHTML = [
      {label:'Intake spend (ingested)', val:fmtMoney(tb), color:'#ff9f0a'},
      {label:'Net sales', val:fmtMoney(ts), color:'var(--green)'},
      {label:'Sell : Buy', val:(tb>0?(ts/tb).toFixed(2):'—')+'×'},
    ].map(c => '<div class="stat"><div class="stat-label">'+c.label+'</div><div class="stat-val" style="'+(c.color?'color:'+c.color:'')+'">'+c.val+'</div></div>').join('');
    const pp = d.partial;
    document.getElementById('buysell-partial').textContent = pp.cnt
      ? '⚠ '+pp.cnt+' partially-ingested lot(s) ('+fmtMoney(pp.value)+' offer) counted at ingest date — bulk may still be clearing, so recent buy can be slightly overstated.'
      : '';
  } catch(e){ document.getElementById('buysell-kpis').innerHTML = '<div class="empty">'+e.message+'</div>'; }
}

async function loadMix(){
  const dim = document.getElementById('mix-dim').value;
  document.getElementById('mix-totals').innerHTML = '<div class="spinner"></div>';
  try {
    const d = await (await fetch('/api/sales/mix?days='+_salesWin+'&dim='+dim)).json();
    const colors = {}; d.cats.forEach((c,i)=> colors[c] = PALETTE[i%PALETTE.length]);
    const rowFor = {}; d.series.forEach(s => rowFor[s.period] = s.vals);
    const series = d.cats.map(c => ({ key:c, label:d.labels[c]||c, color:colors[c], type:'area',
      data:d.periods.map(p => (rowFor[p] && rowFor[p][c]) || 0) }));
    buildChart('chart-mix', d.periods, series, {money:true, gran:d.granularity});
    document.getElementById('mix-legend').innerHTML = legendHtml(d.cats.map(c => ({label:d.labels[c]||c, color:colors[c]})));
    const tot = d.totals, max = Math.max(...tot.map(t=>t.rev), 1), sum = tot.reduce((a,t)=>a+t.rev,0)||1;
    document.getElementById('mix-totals').innerHTML = '<div style="overflow-x:auto;"><table>' +
      '<thead><tr><th style="text-align:left;">Category</th><th>Revenue</th><th></th><th>%</th><th>Units</th></tr></thead><tbody>' +
      tot.map(t => '<tr><td style="text-align:left;font-weight:600;">'+t.grp+'</td>' +
        '<td style="font-weight:600;">'+fmtMoney(t.rev)+'</td>' +
        '<td style="min-width:120px;"><div class="bar" style="width:'+Math.max(t.rev/max*100,2)+'%;"><span style="width:100%;background:var(--accent);"></span></div></td>' +
        '<td>'+(t.rev/sum*100).toFixed(0)+'%</td><td>'+t.units.toLocaleString()+'</td></tr>').join('') +
      '</tbody></table></div>';
  } catch(e){ document.getElementById('mix-totals').innerHTML = '<div class="empty">'+e.message+'</div>'; }
}

loadStatus();
doSearch();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
