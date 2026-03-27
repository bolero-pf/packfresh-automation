"""
routes/inventory.py

Fixes:
  1. Timestamp formatted to US/Central with human-readable format.
     Sync badge polls /api/status every 30s and updates without reload.
  2. Tag pills auto-filter on click (onchange="this.form.submit()").
  3. After push, updated cells refreshed in-place from server response.
  4. Enter-key push: stays in place, updates cells, shows green toast.
  5. physical_count persists in inventory_overrides (Postgres) — survives
     redeploys and refreshes. Notes same.
  6. Tag filtering uses exact comma-separated tag match, not substring.
  7. Added blister, sleeved, international, mtg to curated tags.
  8. Export CSV passes current filters through query string.
  9. Clicking search result fills TCG ID and auto-triggers preview.
"""

import os
import logging
import datetime
from functools import wraps

import db
from flask import Blueprint, request, redirect, flash, Response, jsonify, g

logger = logging.getLogger(__name__)

bp = Blueprint("inventory", __name__, url_prefix="/inventory")

# ─── Config ────────────────────────────────────────────────────────────────────

INVENTORY_USER       = os.getenv("INVENTORY_USER", "admin")
INVENTORY_PASS       = os.getenv("INVENTORY_PASS", "secret")
LOCATION_ID          = os.getenv("LOCATION_ID")
DRY_RUN              = os.getenv("PF_DRY_RUN", "0") == "1"
SHOPIFY_STORE_HANDLE = os.getenv("SHOPIFY_STORE_HANDLE", "")

# ─── Auth ──────────────────────────────────────────────────────────────────────

def _check_auth(u, p):
    return u == INVENTORY_USER and p == INVENTORY_PASS

def _authenticate():
    return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Inventory"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, 'user', None):
            return f(*args, **kwargs)  # JWT already validated
        auth = request.authorization
        if not auth or not _check_auth(auth.username, auth.password):
            return _authenticate()
        return f(*args, **kwargs)
    return decorated

# ─── Singletons ────────────────────────────────────────────────────────────────

def _get_shopify_client():
    import app as _app
    return _app.shopify_client

def _get_cache_manager():
    import app as _app
    return _app.cache_manager

def _get_ppt_client():
    import app as _app
    return _app.ppt_client

# ─── Timestamp ─────────────────────────────────────────────────────────────────

def _format_sync_time(ts) -> str:
    """Convert UTC DB timestamp to US/Central, human-readable."""
    if not ts:
        return "Never"
    try:
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        month = ts.month
        is_dst = 3 <= month <= 11
        offset = datetime.timedelta(hours=-5 if is_dst else -6)
        tz_label = "CDT" if is_dst else "CST"
        local = ts + offset
        return local.strftime(f"%-m/%-d/%y %-I:%M %p {tz_label}")
    except Exception:
        return str(ts)[:19]

def _get_last_sync_str() -> str:
    try:
        row = db.query_one("SELECT last_refreshed_at FROM inventory_cache_meta LIMIT 1")
        if row and row.get("last_refreshed_at"):
            return _format_sync_time(row["last_refreshed_at"])
        return "Never"
    except Exception:
        return "Unknown"

# ─── Data helpers ──────────────────────────────────────────────────────────────

def _load_inventory() -> list[dict]:
    rows = db.query("""
        SELECT
            c.shopify_product_id,
            c.shopify_variant_id,
            c.title                                         AS name,
            c.shopify_qty,
            c.shopify_price,
            ROUND(c.shopify_qty * c.shopify_price, 2)      AS shopify_value,
            c.tags                                          AS shopify_tags,
            LOWER(c.status)                                 AS shopify_status,
            c.inventory_item_id                             AS shopify_inventory_id,
            c.tcgplayer_id,
            COALESCE(c.committed, 0)                        AS committed,
            COALESCE(o.physical_count, 0)                   AS physical_count,
            COALESCE(o.notes, '')                           AS notes
        ,sbc.best_variant_market                          AS bd_value
        ,sbc.variant_count                                 AS bd_variant_count
        FROM inventory_product_cache c
        LEFT JOIN inventory_overrides o ON o.shopify_variant_id = c.shopify_variant_id
        LEFT JOIN sealed_breakdown_cache sbc ON sbc.tcgplayer_id = c.tcgplayer_id
        ORDER BY c.title
    """)
    return [dict(r) for r in rows]

def _save_override(variant_id: int, physical_count=None, notes=None):
    if physical_count is None and notes is None:
        return
    db.execute("""
        INSERT INTO inventory_overrides (shopify_variant_id, physical_count, notes, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (shopify_variant_id) DO UPDATE SET
            physical_count = COALESCE(EXCLUDED.physical_count, inventory_overrides.physical_count),
            notes          = COALESCE(EXCLUDED.notes, inventory_overrides.notes),
            updated_at     = CURRENT_TIMESTAMP
    """, (variant_id, physical_count, notes))

def _update_shopify_price(variant_id: int, price: float) -> bool:
    sc = _get_shopify_client()
    if sc is None:
        return False
    if DRY_RUN:
        logger.info(f"[DRY_RUN] price variant_id={variant_id} → {price:.2f}")
        return True
    try:
        sc.update_variant_price(variant_id, price)
        db.execute("UPDATE inventory_product_cache SET shopify_price=%s WHERE shopify_variant_id=%s",
                   (price, variant_id))
        _get_cache_manager().record_tool_push()
        return True
    except Exception as e:
        logger.error(f"Price update failed variant {variant_id}: {e}")
        return False

def _update_shopify_qty(inventory_item_id: int, variant_id: int, new_qty: int) -> bool:
    sc = _get_shopify_client()
    if sc is None or not LOCATION_ID:
        return False
    if DRY_RUN:
        logger.info(f"[DRY_RUN] qty inv_item={inventory_item_id} → {new_qty}")
        return True
    try:
        sc.set_inventory_level(inventory_item_id, int(LOCATION_ID), new_qty)
        db.execute("UPDATE inventory_product_cache SET shopify_qty=%s WHERE shopify_variant_id=%s",
                   (new_qty, variant_id))
        _get_cache_manager().record_tool_push()
        return True
    except Exception as e:
        logger.error(f"Qty update failed inv_item {inventory_item_id}: {e}")
        return False

# ─── Tags ──────────────────────────────────────────────────────────────────────

CURATED_TAGS = [
    "sealed", "slab", "collection box", "tin", "etb", "pcetb",
    "booster box", "booster pack", "blister", "sleeved", "international", "mtg",
]

def _row_has_tag(row, tag: str) -> bool:
    """Exact match against Shopify's comma-separated tags — no substring matching."""
    tags_raw = row.get("shopify_tags") or ""
    tags = {t.strip().lower() for t in tags_raw.split(",")}
    return tag.lower() in tags

def _apply_filters(rows, *, q=None, in_stock=False, tag_any=None, status="all", qty_mismatch=False):
    if status == "published":
        rows = [r for r in rows if r.get("shopify_status") != "draft"]
    elif status == "draft":
        rows = [r for r in rows if r.get("shopify_status") == "draft"]
    if q:
        rows = [r for r in rows if q in (r.get("name") or "").lower()]
    if in_stock:
        rows = [r for r in rows if (r.get("shopify_qty") or 0) > 0]
    if tag_any:
        rows = [r for r in rows if all(_row_has_tag(r, t) for t in tag_any)]
    if qty_mismatch:
        rows = [r for r in rows if r.get("physical_count", 0) != (r.get("shopify_qty") or 0)]
    return rows

def _sort_rows(rows, sort_col, sort_dir):
    SORTABLE = {"name", "shopify_qty", "shopify_price", "shopify_value", "physical_count", "notes", "committed", "breakdown"}
    if sort_col not in SORTABLE:
        return rows
    actual_col = "bd_value" if sort_col == "breakdown" else sort_col
    def key(r):
        v = r.get(actual_col)
        if v is None: return (1, 0, "")
        try:
            return (0, float(v), "")
        except (TypeError, ValueError):
            return (0, 0, str(v).lower())
    return sorted(rows, key=key, reverse=(sort_dir == "desc"))

# ─── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/sync")
@requires_auth
def sync_now():
    cm = _get_cache_manager()
    if cm:
        cm.ensure_tables()
        cm.invalidate("manual_sync")
        flash("🔁 Sync triggered — refreshing in background.", "success")
    else:
        flash("⚠ Shopify not configured.", "warning")
    return redirect("/inventory")


@bp.route("/api/status")
@requires_auth
def api_status():
    """Polled to update sync timestamp without page reload."""
    cm = _get_cache_manager()
    in_progress = cm._refresh_in_progress if cm else False
    return jsonify({"last_sync": _get_last_sync_str(), "refresh_in_progress": in_progress})


@bp.route("/api/cache/record-push", methods=["POST"])
def api_record_push():
    """
    Called by ingestion (and any other tool) after pushing changes to Shopify.
    Records a tool push timestamp to suppress the product_updated staleness
    signal for TOOL_PUSH_COOLDOWN_MINUTES, preventing thrashing.
    No auth required — internal service-to-service call only.
    """
    cm = _get_cache_manager()
    if cm:
        cm.record_tool_push()
    return jsonify({"ok": True})


@bp.route("/api/cache/invalidate", methods=["POST"])
def api_cache_invalidate():
    """Force cache refresh — called by price_updater after pushing new prices."""
    cm = _get_cache_manager()
    if cm:
        reason = (request.get_json(silent=True) or {}).get("reason", "external")
        cm.invalidate(reason)
    return jsonify({"ok": True})


@bp.route("/api/push", methods=["POST"])
@requires_auth
def api_push():
    """
    JSON push endpoint — called by Enter key, no page reload.
    Body: {"changes": [{"variant_id", "inventory_item_id", "shopify_qty"?, "shopify_price"?}]}
    Returns: {"pushed", "failed", "label", "rows": {variant_id: {shopify_qty, shopify_price}}}
    """
    data    = request.get_json() or {}
    changes = data.get("changes", [])
    pushed  = failed = 0
    updated_rows = {}

    for ch in changes:
        vid    = ch.get("variant_id")
        inv_id = ch.get("inventory_item_id")
        price  = ch.get("shopify_price")
        qty    = ch.get("shopify_qty")

        if price is not None and vid:
            ok = _update_shopify_price(int(vid), float(price))
            pushed += ok; failed += not ok

        if qty is not None and vid:
            ok = _update_shopify_qty(int(inv_id), int(vid), int(qty)) if inv_id else False
            pushed += ok; failed += not ok

        if vid:
            row = db.query_one(
                "SELECT shopify_qty, shopify_price FROM inventory_product_cache WHERE shopify_variant_id=%s",
                (int(vid),)
            )
            if row:
                updated_rows[str(vid)] = {
                    "shopify_qty":   row["shopify_qty"],
                    "shopify_price": float(row["shopify_price"] or 0),
                }

    cm = _get_cache_manager()
    if cm and pushed > 0 and not DRY_RUN:
        cm.record_tool_push()

    return jsonify({
        "pushed": pushed,
        "failed": failed,
        "label":  "DRY RUN" if DRY_RUN else "LIVE",
        "rows":   updated_rows,
    })


@bp.route("/export.csv")
@requires_auth
def export_csv():
    """Export respects current filters (fix #8)."""
    rows     = _load_inventory()
    q        = (request.args.get("q") or "").strip().lower()
    in_stock = request.args.get("in_stock") == "1"
    tag_any  = [t.lower() for t in request.args.getlist("tag")]
    status   = request.args.get("status", "all")
    qty_mm   = request.args.get("qty_mismatch") == "1"
    rows     = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                               status=status, qty_mismatch=qty_mm)
    import csv, io
    out  = io.StringIO()
    cols = ["name", "shopify_qty", "shopify_price", "shopify_value",
            "physical_count", "shopify_tags", "notes",
            "shopify_variant_id", "shopify_inventory_id"]
    writer = csv.DictWriter(out, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    fname = f"inventory-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')}.csv"
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@bp.route("/", methods=["GET", "POST"])
@requires_auth
def index():
    cm = _get_cache_manager()
    if cm:
        cm.ensure_tables()
        cm.check_and_refresh_if_stale()

    # ── Save (POST) ── local-only fields: physical_count, notes ──────────────
    if request.method == "POST" and request.form.get("save") == "1":
        dirty_keys = set((request.form.get("dirty_keys") or "").split(","))
        updates    = request.form.to_dict(flat=True)
        q          = (request.args.get("q") or "").strip().lower()
        in_stock   = request.args.get("in_stock") == "1"
        tag_any    = [t.lower() for t in request.args.getlist("tag")]
        status     = request.args.get("status", "all")
        qty_mm     = request.args.get("qty_mismatch") == "1"
        sort_col   = request.args.get("sort")
        sort_dir   = request.args.get("dir", "asc")
        limit      = int(request.args.get("limit", 400))
        rows       = _load_inventory()
        filtered   = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                                    status=status, qty_mismatch=qty_mm)
        page       = _sort_rows(filtered, sort_col, sort_dir)[:limit]

        for i, row in enumerate(page):
            variant_id = row.get("shopify_variant_id")
            new_phys   = None
            new_notes  = None
            for col in ("physical_count", "notes"):
                key = f"cell_{i}_{col}"
                if dirty_keys and key not in dirty_keys: continue
                if key not in updates: continue
                raw = (updates[key] or "").strip()
                if not raw: continue
                if col == "physical_count":
                    try: new_phys = int(raw)
                    except Exception: pass
                else:
                    new_notes = raw
            if (new_phys is not None or new_notes is not None) and variant_id:
                _save_override(variant_id, physical_count=new_phys, notes=new_notes)

        flash("💾 Saved.", "success")
        return redirect(request.full_path or "/inventory")

    # ── GET ───────────────────────────────────────────────────────────────────
    rows       = _load_inventory()
    q          = (request.args.get("q") or "").strip().lower()
    in_stock   = request.args.get("in_stock") == "1"
    tag_any    = [t.lower() for t in request.args.getlist("tag")]
    status     = request.args.get("status", "all")
    qty_mm     = request.args.get("qty_mismatch") == "1"
    sort_col   = request.args.get("sort")
    sort_dir   = request.args.get("dir", "asc")
    limit      = int(request.args.get("limit", 400))
    total_rows = len(rows)
    filtered   = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                                 status=status, qty_mismatch=qty_mm)
    filtered   = _sort_rows(filtered, sort_col, sort_dir)
    page       = filtered[:limit]

    totals = {
        "count":          len(filtered),
        "shopify_qty":    sum((r.get("shopify_qty") or 0) for r in filtered),
        "physical_count": sum((r.get("physical_count") or 0) for r in filtered),
        "shopify_value":  sum((r.get("shopify_qty") or 0) * (r.get("shopify_price") or 0) for r in filtered),
    }
    meta = {
        "last_sync":    _get_last_sync_str(),
        "mode_label":   "DRY RUN" if DRY_RUN else "LIVE",
        "totals":       totals,
        "query_string": request.query_string.decode("utf-8"),
    }
    filters = {
        "q": q, "in_stock": in_stock, "tag_options": CURATED_TAGS,
        "selected_tags": tag_any, "status": status,
        "sort": sort_col, "dir": sort_dir, "qty_mismatch": qty_mm,
    }
    return _render_inventory(rows=page, total_rows=total_rows,
                             filters=filters, meta=meta, limit=limit)


@bp.route("/push_prices", methods=["POST"])
@requires_auth
def push_prices():
    rows   = _load_inventory()
    pushed = failed = 0
    for row in rows:
        vid   = row.get("shopify_variant_id")
        price = row.get("shopify_price")
        if vid and price is not None:
            ok = _update_shopify_price(int(vid), float(price))
            pushed += ok; failed += not ok
    label = "DRY RUN" if DRY_RUN else "LIVE"
    flash(f"💸 {label}: pushed prices for {pushed} variant(s){' (some failed)' if failed else ''}.",
          "success" if not failed else "warning")
    return redirect("/inventory")


@bp.route("/zero_physical", methods=["POST"])
@requires_auth
def zero_physical():
    """Reset all physical counts to zero for a fresh inventory session."""
    db.execute("UPDATE inventory_overrides SET physical_count = 0, updated_at = CURRENT_TIMESTAMP")
    flash("🔄 All physical counts zeroed out.", "success")
    return redirect("/inventory")


# ─── PPT / Listing creation ────────────────────────────────────────────────────

@bp.route("/add")
@requires_auth
def add_item_page():
    return _render_add_page()

@bp.route("/api/ppt/sealed/<int:tcgplayer_id>")
@requires_auth
def ppt_sealed_lookup(tcgplayer_id):
    ppt = _get_ppt_client()
    if ppt is None: return jsonify({"error": "PPT not configured"}), 503
    item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not item: return jsonify({"error": f"No PPT product for {tcgplayer_id}"}), 404
    return jsonify(item)

@bp.route("/api/ppt/search")
@requires_auth
def ppt_search():
    q = (request.args.get("q") or "").strip()
    if not q: return jsonify([])
    ppt = _get_ppt_client()
    if ppt is None: return jsonify({"error": "PPT not configured"}), 503
    return jsonify(ppt.search_sealed_products(q, limit=8))

@bp.route("/api/enrich/preview", methods=["POST"])
@requires_auth
def enrich_preview():
    import product_enrichment as enrichment
    data     = request.get_json() or {}
    name     = data.get("product_name", "")
    set_name = data.get("set_name", "")
    return jsonify({
        "product_name": name, "set_name": set_name,
        "tags":      enrichment.infer_tags(name, set_name),
        "era":       enrichment.infer_era(name, set_name),
        "weight_oz": enrichment.infer_weight_oz(name),
    })

@bp.route("/api/enrich/create-listing", methods=["POST"])
@requires_auth
def create_listing():
    import product_enrichment as enrichment
    data         = request.get_json() or {}
    tcgplayer_id = data.get("tcgplayer_id")
    quantity     = int(data.get("quantity", 0))
    if not tcgplayer_id: return jsonify({"error": "tcgplayer_id required"}), 400
    ppt = _get_ppt_client()
    if ppt is None: return jsonify({"error": "PPT not configured"}), 503
    ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not ppt_item: return jsonify({"error": f"No PPT product for {tcgplayer_id}"}), 404
    price = data.get("price") or ppt_item.get("unopenedPrice") or ppt_item.get("marketPrice") or 0
    if not price: return jsonify({"error": "No price available"}), 400
    if DRY_RUN: return jsonify({"dry_run": True, "product_name": ppt_item.get("name"), "price": price}), 200
    summary = enrichment.create_draft_listing(ppt_item, price=float(price), quantity=quantity)
    cm = _get_cache_manager()
    if cm: cm.record_tool_push()
    return jsonify(summary)

@bp.route("/api/stub/create", methods=["POST"])
@requires_auth
def create_stub():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    qty  = int(data.get("qty", 0))
    if not name: return jsonify({"error": "name required"}), 400
    sc = _get_shopify_client()
    if sc is None: return jsonify({"error": "Shopify not configured"}), 503
    if DRY_RUN: return jsonify({"dry_run": True, "name": name}), 200
    try:
        result      = sc.create_draft_product_stub(name=name)
        product_id  = result["product_id"]
        variant_id  = result["variant_id"]
        inv_item_id = result["inventory_item_id"]
        if qty > 0 and LOCATION_ID and inv_item_id:
            _update_shopify_qty(int(inv_item_id), int(variant_id), qty)
        cm = _get_cache_manager()
        if cm: cm.record_tool_push()
        return jsonify({"product_id": product_id, "variant_id": variant_id, "title": name})
    except Exception as e:
        logger.error(f"Stub create failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Renderers ────────────────────────────────────────────────────────────────

def _build_sort_qs(col, next_dir):
    from urllib.parse import parse_qs, urlencode
    qs = parse_qs(request.query_string.decode("utf-8"), keep_blank_values=True)
    qs.pop("sort", None); qs.pop("dir", None)
    qs["sort"] = [col]; qs["dir"] = [next_dir]
    return "?" + urlencode(qs, doseq=True)


def _render_inventory(rows, total_rows, filters, meta, limit):
    import html as _html

    q             = filters.get("q", "")
    in_stock      = filters.get("in_stock", False)
    tag_options   = filters.get("tag_options", [])
    selected_tags = {t.lower() for t in filters.get("selected_tags", [])}
    status        = filters.get("status", "all")
    sort_col      = filters.get("sort")
    sort_dir      = filters.get("dir", "asc")
    qty_mismatch  = filters.get("qty_mismatch", False)
    last_sync     = meta.get("last_sync", "Never")
    mode_label    = meta.get("mode_label", "LIVE")
    totals        = meta.get("totals", {})
    query_string  = meta.get("query_string", "")
    qs            = f"?{query_string}" if query_string else ""

    def sort_link(col):
        if col == sort_col:
            nd    = "desc" if sort_dir == "asc" else "asc"
            arrow = " ▲" if sort_dir == "asc" else " ▼"
        else:
            nd = "asc"; arrow = ""
        return _build_sort_qs(col, nd), arrow

    def disp(col):
        return {"name": "Name", "shopify_qty": "Shopify Qty", "shopify_price": "Price",
                "shopify_value": "Value", "physical_count": "Physical",
                "adjust_delta": "Adjust Δ", "committed": "Committed", "notes": "Notes",
                "breakdown": "Breakdown"}.get(col, col)

    SHOW_COLS = ["physical_count", "name", "shopify_qty", "committed", "adjust_delta",
                 "shopify_price", "shopify_value", "breakdown", "notes"]
    EDITABLE  = {"shopify_qty", "shopify_price", "physical_count", "notes", "adjust_delta"}

    from flask import get_flashed_messages
    flash_html = ""
    for cat, msg in get_flashed_messages(with_categories=True):
        color = {"success": "#2dd4a0", "warning": "#f5a623", "danger": "#f05252"}.get(cat, "#dfa260")
        flash_html += (f'<div class="pf-flash" style="border-left-color:{color};color:{color};">'
                       f'{_html.escape(msg)}</div>')

    totals_html = (
        f'<span class="badge">Items: {totals.get("count", 0)}</span>'
        f'<span class="badge">Shopify Qty: {totals.get("shopify_qty", 0)}</span>'
        f'<span class="badge">Physical: {totals.get("physical_count", 0)}</span>'
        f'<span class="badge">Value: ${totals.get("shopify_value", 0):,.2f}</span>'
    )

    # Status radio chips
    def sc(v): return "checked" if status == v else ""
    status_chips = f"""
    <label class="chip"><input type="radio" name="status" value="all" {sc('all')} onchange="this.form.submit()"><span>All</span></label>
    <label class="chip"><input type="radio" name="status" value="published" {sc('published')} onchange="this.form.submit()"><span>Published</span></label>
    <label class="chip"><input type="radio" name="status" value="draft" {sc('draft')} onchange="this.form.submit()"><span>Drafts</span></label>"""

    # Tag checkbox chips — auto-submit on click (fix #2)
    tag_chips = "".join(
        f'<label class="chip"><input type="checkbox" name="tag" value="{_html.escape(t)}" '
        f'{"checked" if t in selected_tags else ""} onchange="this.form.submit()">'
        f'<span>{_html.escape(t)}</span></label>'
        for t in tag_options
    )

    # thead
    thead_cells = ["<th></th>"]
    for col in SHOW_COLS:
        href, arrow = sort_link(col)
        thead_cells.append(f'<th><a href="{href}" class="sort-link">{_html.escape(disp(col))}{arrow}</a></th>')
    thead = "<thead><tr>" + "".join(thead_cells) + "</tr></thead>"

    # tbody
    body_rows = []
    for i, row in enumerate(rows):
        vid     = row.get("shopify_variant_id", "")
        iid     = row.get("shopify_inventory_id", "")
        tds     = [f'<td><input type="checkbox"></td>']
        for col in SHOW_COLS:
            val = row.get(col, "")
            val = "" if val is None else val
            vs  = _html.escape(str(val))
            if col in EDITABLE:
                da = (f'data-orig="{vs}" data-col="{col}" '
                      f'data-vid="{vid}" data-iid="{iid}"')
                if col == "shopify_price":
                    cell = f'<input name="cell_{i}_shopify_price" value="{vs}" {da} inputmode="decimal" class="pf-inp" style="max-width:110px">'
                elif col in ("shopify_qty", "physical_count"):
                    cell = (f'<div class="qty-wrap">'
                            f'<button type="button" class="qty-btn" data-delta="-1" data-target="cell_{i}_{col}">−</button>'
                            f'<input name="cell_{i}_{col}" value="{vs}" {da} inputmode="numeric" class="pf-inp" style="max-width:72px">'
                            f'<button type="button" class="qty-btn" data-delta="1" data-target="cell_{i}_{col}">+</button>'
                            f'</div>')
                elif col == "adjust_delta":
                    cell = (f'<input name="cell_{i}_adjust_delta" value="" {da} '
                            f'inputmode="numeric" class="pf-inp" placeholder="±" style="max-width:64px">')
                else:
                    cell = f'<input name="cell_{i}_{col}" value="{vs}" {da} class="pf-inp">'
                tds.append(f"<td>{cell}</td>")
            elif col == "breakdown":
                bd_val = row.get("bd_value")
                tcg_id = row.get("tcgplayer_id")
                store_price = float(row.get("shopify_price") or 0)
                store_qty = int(row.get("shopify_qty") or 0)
                name_esc = _html.escape(str(row.get("name", ""))).replace("'", "\\'")
                if bd_val and tcg_id:
                    bd_f = float(bd_val)
                    delta_pct = ((bd_f - store_price) / store_price * 100) if store_price > 0 else 0
                    delta_cls = "color:#2dd4a0" if delta_pct >= 0 else "color:#f05252" if delta_pct < -10 else "color:#f5a623"
                    tds.append(
                        f'<td style="white-space:nowrap;">'
                        f'<span style="{delta_cls};font-weight:600;" '
                        f'title="BD Value: ${bd_f:.2f} ({delta_pct:+.1f}%)">'
                        f'${bd_f:.2f}</span> '
                        f'<button type="button" class="qty-btn" title="Edit recipe" '
                        f'onclick="openBdRecipe({tcg_id},\'{name_esc}\',{store_price},{store_qty})" '
                        f'style="font-size:10px;padding:1px 5px;">📋</button> '
                        f'<button type="button" class="qty-btn" title="Break down" '
                        f'onclick="openBdExecute({tcg_id},\'{name_esc}\',{store_price},{store_qty},{vid},{iid})" '
                        f'style="font-size:10px;padding:1px 5px;background:#2dd4a0;color:#000;">▶</button>'
                        f'</td>')
                elif tcg_id:
                    tds.append(
                        f'<td>'
                        f'<button type="button" class="qty-btn" '
                        f'onclick="openBdRecipe({tcg_id},\'{name_esc}\',{store_price},{store_qty})" '
                        f'title="Create breakdown recipe" '
                        f'style="font-size:11px;padding:2px 8px;">+ Recipe</button>'
                        f'</td>')
                else:
                    tds.append('<td style="color:var(--muted);">—</td>')
            else:
                tds.append(f"<td>{vs}</td>")
        body_rows.append(f'<tr data-vid="{vid}">' + "".join(tds) + "</tr>")

    tbody = "<tbody>" + "".join(body_rows) + "</tbody>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pack Fresh · Inventory</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
a{{color:inherit;text-decoration:none;}}
body{{padding:14px 18px;}}
.pf-flash{{border-left:3px solid;padding:9px 14px;margin-bottom:8px;border-radius:6px;background:rgba(0,0,0,.3);font-size:13px;}}
/* topbar */
.topbar{{display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap;}}
.badge{{padding:3px 9px;border-radius:999px;font-size:12px;background:var(--surface);border:1px solid var(--border);}}
.actions{{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto;}}
.btn{{height:34px;padding:0 13px;border-radius:9px;font-size:13px;border:1px solid var(--border);background:transparent;color:var(--text);white-space:nowrap;}}
.btn:hover{{border-color:var(--accent);color:var(--accent);}}
.btn-primary{{background:var(--accent);color:#1b1b1b;border-color:var(--accent);}}
.btn-primary:hover{{filter:brightness(1.08);}}
.btn-green{{background:var(--green);color:#0d1117;border-color:var(--green);}}
/* toolbar */
.toolbar{{display:flex;flex-wrap:wrap;gap:8px 10px;align-items:center;margin-bottom:8px;}}
.search{{flex:1 1 240px;max-width:520px;}}
.search input{{width:100%;height:38px;font-size:13px;border-radius:9px;background:var(--surface);color:var(--text);border:1px solid var(--border);padding:7px 11px;font-family:inherit;}}
.search input:focus{{outline:none;border-color:var(--accent);}}
/* chips */
.chips{{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:10px;}}
.chip{{display:inline-flex;align-items:center;cursor:pointer;}}
.chip input{{display:none;}}
.chip span{{display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;background:var(--surface);border:1px solid var(--border);font-size:12px;transition:all .15s;}}
.chip:has(input:checked) span{{background:var(--accent);color:#1b1b1b;border-color:var(--accent);}}
/* table */
.tbl-wrap{{overflow-x:auto;}}
thead th{{position:sticky;top:0;z-index:2;padding:8px 9px;white-space:nowrap;}}
.sort-link:hover{{color:var(--accent);}}
tbody tr:nth-child(even){{background:rgba(255,255,255,.025);}}
tbody tr:hover{{background:rgba(255,255,255,.06);}}
td{{padding:6px 9px;vertical-align:middle;}}
/* inputs */
.pf-inp{{background:var(--surface-2);color:var(--text);border:1px solid var(--border);border-radius:6px;height:30px;font-size:13px;padding:3px 7px;font-family:inherit;}}
.pf-inp:focus{{outline:none;border-color:var(--accent);}}
.pf-inp.dirty{{outline:2px solid var(--amber);}}
.qty-wrap{{display:flex;align-items:center;gap:3px;}}
.qty-btn{{min-width:26px;height:26px;border-radius:6px;background:transparent;color:var(--text);border:1px solid var(--border);cursor:pointer;font-size:13px;line-height:1;}}
.qty-btn:hover{{border-color:var(--accent);color:var(--accent);}}
/* toast (legacy — used by inline JS) */
#toast{{position:fixed;bottom:22px;right:22px;background:#1e2535;border:1px solid var(--green);color:var(--green);padding:11px 18px;border-radius:9px;font-size:13px;font-weight:600;z-index:9999;transform:translateY(70px);opacity:0;transition:transform .22s ease,opacity .22s ease;pointer-events:none;}}
#toast.show{{transform:translateY(0);opacity:1;}}
#toast.err{{border-color:var(--red);color:var(--red);}}
</style>
<link rel="stylesheet" href="/inventory/breakdown/api/cache/bd-static/breakdown_modal.css">
</head>
<body>
<script src="/inventory/breakdown/api/cache/bd-static/breakdown_modal.js"></script>
<div id="toast"></div>
{flash_html}

<div class="topbar">
  <strong style="font-size:.95rem;">🗂 Inventory</strong>
  <small style="color:var(--muted);">({total_rows} total · {len(rows)} shown)</small>
  {totals_html}
  <span class="badge" id="sync-badge">🔄 {_html.escape(last_sync)}</span>
  <span class="badge" style="border-color:var(--accent);">{_html.escape(mode_label)}</span>
  <div class="actions">
    <a class="btn" href="/inventory/sync">🔁 Sync</a>
    <a class="btn" href="/inventory/breakdown/">🔓 Breakdown</a>
    <a class="btn btn-green" href="/inventory/add">➕ Add</a>
    <a class="btn" href="/inventory/export.csv{qs}">📤 Export</a>
    <form method="post" action="/inventory/zero_physical" style="display:inline"
          onsubmit="return confirm('Zero out ALL physical counts? This cannot be undone.')">
      <button class="btn" style="color:var(--red);">🔄 Zero Physical</button>
    </form>
  </div>
</div>

<form id="filter-form" method="get">
  <div class="toolbar">
    <div class="search">
      <input name="q" value="{_html.escape(q)}" placeholder="Search by name…" autocomplete="off">
    </div>
    <label style="display:flex;align-items:center;gap:5px;font-size:13px;cursor:pointer;">
      <input type="checkbox" name="in_stock" value="1" {'checked' if in_stock else ''} onchange="this.form.submit()" style="width:15px;height:15px;accent-color:var(--accent);">In stock
    </label>
    <label style="display:flex;align-items:center;gap:5px;font-size:13px;cursor:pointer;">
      <input type="checkbox" name="qty_mismatch" value="1" {'checked' if qty_mismatch else ''} onchange="this.form.submit()" style="width:15px;height:15px;accent-color:var(--accent);">Qty mismatch
    </label>
    <input type="hidden" name="limit" value="{limit}">
    <input type="hidden" name="sort" value="{sort_col or ''}">
    <input type="hidden" name="dir" value="{sort_dir or 'asc'}">
    <button class="btn">Search</button>
  </div>
  <div class="chips">
    {status_chips}
    <span style="width:1px;background:var(--border);margin:2px 4px;"></span>
    {tag_chips}
  </div>
</form>

<form id="main-form" method="post">
  <input type="hidden" name="save" value="1">
  <input type="hidden" id="dirty-keys" name="dirty_keys" value="">
  <div class="tbl-wrap">
    <table>
      {thead}
      {tbody}
    </table>
  </div>
  <button id="save-btn" class="btn btn-primary" style="margin-top:10px;" type="button">💾 Save</button>
</form>

<script>
(function(){{
  const dirty = new Map();

  function toast(msg, isErr){{
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = isErr ? 'err' : '';
    void t.offsetHeight;
    t.classList.add('show');
    clearTimeout(t._t);
    t._t = setTimeout(()=>t.classList.remove('show'), 3200);
  }}

  // Track dirty cells
  document.querySelectorAll('#main-form .pf-inp').forEach(inp=>{{
    inp.addEventListener('input', ()=>{{
      const key = inp.name;
      if(!key) return;
      if(inp.value !== (inp.dataset.orig||'')){{
        inp.classList.add('dirty');
        dirty.set(key, inp);
      }} else {{
        inp.classList.remove('dirty');
        dirty.delete(key);
      }}
    }});
    inp.addEventListener('keydown', e=>{{
      if(e.key==='Enter'){{ e.preventDefault(); doPush(); }}
    }});
  }});

  // +/- buttons
  document.querySelectorAll('.qty-btn').forEach(btn=>{{
    btn.addEventListener('click', ()=>{{
      const inp = document.querySelector(`[name="${{btn.dataset.target}}"]`);
      if(!inp) return;
      inp.value = String((parseInt(inp.value||'0',10)||0) + parseInt(btn.dataset.delta));
      inp.dispatchEvent(new Event('input',{{bubbles:true}}));
      inp.focus(); inp.select();
    }});
  }});

  // Save button — saves physical_count + notes to DB via form POST
  document.getElementById('save-btn').addEventListener('click', ()=>{{
    document.getElementById('dirty-keys').value = Array.from(dirty.keys()).join(',');
    document.getElementById('main-form').submit();
  }});

  // Enter → push to Shopify via JSON, update cells in-place (fixes #3 #4)
  async function doPush(){{
    if(!dirty.size) return;

    const byVariant = {{}};
    for(const [key, inp] of dirty){{
      const vid = inp.dataset.vid;
      const iid = inp.dataset.iid;
      const col = inp.dataset.col;
      if(!vid) continue;
      if(!byVariant[vid]) byVariant[vid] = {{variant_id:+vid, inventory_item_id:iid?+iid:null}};
      if(col==='shopify_price') byVariant[vid].shopify_price = parseFloat(inp.value)||0;
      else if(col==='shopify_qty') byVariant[vid].shopify_qty = parseInt(inp.value,10)||0;
      else if(col==='adjust_delta'){{
        const row = inp.closest('tr');
        const base = parseInt(row?.querySelector('[data-col="shopify_qty"]')?.dataset.orig||'0',10)||0;
        const delta = parseInt(inp.value,10)||0;
        if(delta!==0) byVariant[vid].shopify_qty = base + delta;
      }}
    }}

    const shopifyChanges = Object.values(byVariant).filter(c=>
      c.shopify_price!=null || c.shopify_qty!=null
    );

    // Also collect local-only dirty fields (physical_count, notes)
    const localKeys = Array.from(dirty.keys()).filter(k=>{{
      const inp = dirty.get(k);
      return inp && (inp.dataset.col==='physical_count' || inp.dataset.col==='notes');
    }});

    if(shopifyChanges.length>0){{
      try{{
        const resp = await fetch('/inventory/api/push',{{
          method:'POST', headers:{{'Content-Type':'application/json'}},
          body: JSON.stringify({{changes:shopifyChanges}})
        }});
        const data = await resp.json();

        // Update cells in-place with fresh server values
        for(const [vid, vals] of Object.entries(data.rows||{{}})){{
          const row = document.querySelector(`tr[data-vid="${{vid}}"]`);
          if(!row) continue;
          for(const [col, val] of Object.entries(vals)){{
            const inp = row.querySelector(`[data-col="${{col}}"]`);
            if(!inp) continue;
            inp.value = val;
            inp.dataset.orig = val;
            inp.classList.remove('dirty');
            dirty.delete(inp.name);
          }}
          const delta = row.querySelector('[data-col="adjust_delta"]');
          if(delta){{ delta.value=''; delta.dataset.orig=''; delta.classList.remove('dirty'); dirty.delete(delta.name); }}
        }}

        const label = data.failed>0
          ? `⚠ ${{data.pushed}} pushed, ${{data.failed}} failed`
          : `✓ ${{data.label}}: ${{data.pushed}} update${{data.pushed===1?'':'s'}} pushed`;
        toast(label, data.failed>0);
      }}catch(err){{
        toast('Push failed: '+err.message, true);
        return;
      }}
    }}

    // Persist local-only fields silently
    if(localKeys.length>0){{
      document.getElementById('dirty-keys').value = localKeys.join(',');
      const fd = new FormData(document.getElementById('main-form'));
      fd.set('dirty_keys', localKeys.join(','));
      await fetch(window.location.href, {{method:'POST', body:fd}});
      localKeys.forEach(k=>{{
        const inp = dirty.get(k);
        if(inp){{ inp.classList.remove('dirty'); dirty.delete(k); }}
      }});
    }}
  }}

  // Adaptive poll: 3s while refresh in progress, 30s otherwise
  // Reloads page when last_sync changes (data is fresh)
  const _initSync = document.getElementById('sync-badge').textContent.replace('🔄 ','').trim();
  const _syncBadge = document.getElementById('sync-badge');
  let _pollTimer = null;
  async function _pollStatus() {{
    try {{
      const d = await (await fetch('/inventory/api/status')).json();
      if (d.refresh_in_progress) {{
        _syncBadge.innerHTML = '⟳ Refreshing...';
        _syncBadge.style.color = 'var(--accent, #4f7df9)';
        _pollTimer = setTimeout(_pollStatus, 3000);
      }} else if (d.last_sync && d.last_sync !== _initSync) {{
        window.location.reload();
      }} else {{
        _syncBadge.innerHTML = '🔄 ' + (d.last_sync || _initSync);
        _syncBadge.style.color = '';
        _pollTimer = setTimeout(_pollStatus, 30000);
      }}
    }} catch(_) {{
      _pollTimer = setTimeout(_pollStatus, 30000);
    }}
  }}
  _pollTimer = setTimeout(_pollStatus, 5000);
}})();

// ── Breakdown shared modal ──────────────────────────────────────
var _bdLastVariantId = null, _bdLastInvItemId = null, _bdLastTcgId = null;

function _bdExecuteHandler(variantId, qty, components) {{
  return fetch('/inventory/breakdown/api/execute', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{
      parent_variant_id: _bdLastVariantId,
      parent_inventory_item_id: _bdLastInvItemId,
      parent_tcgplayer_id: _bdLastTcgId,
      qty_to_break: qty,
      variant_id: variantId,
    }})
  }}).then(function(r) {{ return r.json().then(function(d) {{
    if (!r.ok) throw new Error(d.error || 'Execute failed');
    return d;
  }}); }});
}}

function openBdRecipe(tcgId, name, storePrice, storeQty) {{
  if (typeof openBreakdownModal === 'undefined') {{
    window.open('/inventory/breakdown/?bd_tcg=' + tcgId + '&bd_action=recipe', 'breakdown', 'width=1100,height=800,scrollbars=yes');
    return;
  }}
  openBreakdownModal({{
    tcgplayerId: tcgId,
    productName: name,
    parentStore: storePrice || null,
    parentQty: storeQty || 1,
    apiBase: '/inventory/breakdown/api/cache',
    priceMode: 'best',
    onExecute: null,
    onSave: function() {{ window.location.reload(); }},
    showQtySelector: false,
  }});
}}

function openBdExecute(tcgId, name, storePrice, storeQty, variantId, invItemId) {{
  if (typeof openBreakdownModal === 'undefined') {{
    window.open('/inventory/breakdown/?bd_tcg=' + tcgId + '&bd_action=execute', 'breakdown', 'width=1100,height=800,scrollbars=yes');
    return;
  }}
  _bdLastTcgId = tcgId;
  _bdLastVariantId = variantId || null;
  _bdLastInvItemId = invItemId || null;
  openBreakdownModal({{
    tcgplayerId: tcgId,
    productName: name,
    parentStore: storePrice || null,
    parentQty: storeQty || 1,
    apiBase: '/inventory/breakdown/api/cache',
    priceMode: 'best',
    onExecute: _bdExecuteHandler,
    onSave: function() {{ window.location.reload(); }},
    showQtySelector: true,
  }});
}}
</script>
</body>
</html>"""


def _render_add_page():
    import html as _html
    sh = _html.escape(SHOPIFY_STORE_HANDLE)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Add Item · Pack Fresh</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;height:50px;display:flex;align-items:center;gap:12px;}}
header .logo{{font-weight:700;}} header .logo span{{color:var(--green);}}
header .sub{{color:var(--dim);font-size:.83rem;}}
header a{{margin-left:auto;color:var(--dim);font-size:.8rem;text-decoration:none;}}
header a:hover{{color:var(--text);}}
.container{{max-width:860px;margin:0 auto;padding:28px 20px;}}
.card h2{{font-size:.95rem;font-weight:600;margin-bottom:4px;}}
.card p{{color:var(--dim);font-size:.82rem;margin-bottom:14px;}}
.lbl{{font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);margin-bottom:6px;}}
.row{{display:flex;gap:7px;align-items:flex-end;margin-bottom:9px;}}
.row input{{flex:1;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:9px 11px;font-size:.93rem;font-family:inherit;height:40px;}}
.row input:focus{{outline:none;border-color:var(--accent);}}
.btn{{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:0 16px;height:40px;font-size:.88rem;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap;}}
.btn:hover{{filter:brightness(1.08);}} .btn:disabled{{opacity:.5;cursor:not-allowed;}}
.btn-green{{background:var(--green);color:#000;}}
hr{{border:none;border-top:1px solid var(--border);margin:16px 0;}}
.spinner{{display:inline-block;width:12px;height:12px;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:4px;}}
@keyframes spin{{to{{transform:rotate(360deg);}}}}
.alert{{background:var(--red-bg);border:1px solid var(--red);border-radius:8px;padding:10px 14px;color:var(--red);font-size:.85rem;margin-bottom:10px;}}
/* search dropdown */
.search-wrap{{flex:1;position:relative;}}
.dd{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;margin-top:3px;max-height:240px;overflow-y:auto;display:none;position:absolute;width:100%;z-index:20;box-shadow:0 8px 24px rgba(0,0,0,.4);}}
.dd-item{{padding:9px 12px;cursor:pointer;font-size:.86rem;border-bottom:1px solid var(--border);}}
.dd-item:last-child{{border-bottom:none;}}
.dd-item:hover{{background:rgba(255,255,255,.07);}}
.dd-item .price{{float:right;color:var(--green);font-weight:600;}}
.dd-item .set{{color:var(--dim);font-size:.75rem;display:block;margin-top:2px;}}
/* result card */
#result{{display:none;}}
.rc{{background:var(--surface);border:1px solid var(--border);border-radius:11px;overflow:hidden;}}
.rc-header{{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;gap:12px;align-items:flex-start;}}
.rc-header h2{{font-size:1rem;font-weight:700;line-height:1.3;}}
.rc-grid{{display:grid;grid-template-columns:1fr 1fr;}}
.rc-sec{{padding:14px 18px;border-bottom:1px solid var(--border);}}
.rc-sec:nth-child(odd){{border-right:1px solid var(--border);}}
.tags{{display:flex;flex-wrap:wrap;gap:4px;}}
.tag{{background:var(--surface2);border:1px solid var(--border);border-radius:18px;padding:2px 8px;font-size:.74rem;color:var(--dim);}}
.tag.hi{{background:var(--green-bg);border-color:var(--green);color:var(--green);}}
.meta-t{{width:100%;font-size:.81rem;border-collapse:collapse;}}
.meta-t td:first-child{{color:var(--dim);padding-right:10px;padding-bottom:3px;}}
.img-sec{{grid-column:1/-1;padding:14px 18px;border-bottom:1px solid var(--border);display:flex;gap:10px;}}
.img-box{{flex:1;text-align:center;}}
.img-frame{{background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:6px;display:inline-flex;align-items:center;justify-content:center;width:100%;min-height:90px;}}
.img-frame img{{max-width:100%;max-height:180px;border-radius:3px;}}
.img-ph{{color:var(--dim);font-size:.77rem;padding:14px;}}
.action-bar{{padding:14px 18px;display:flex;gap:7px;align-items:center;background:var(--surface2);flex-wrap:wrap;}}
.action-note{{color:var(--dim);font-size:.75rem;margin-left:auto;}}
@media(max-width:580px){{.rc-grid{{grid-template-columns:1fr;}}.rc-sec:nth-child(odd){{border-right:none;}}.img-sec{{flex-direction:column;}}}}
</style>
</head>
<body>
<header>
  <div class="logo">Pack<span>Fresh</span></div>
  <div class="sub">Add Item</div>
  <a href="/inventory">← Back to Inventory</a>
</header>
<div class="container">
  <div id="error-box" style="display:none;"></div>

  <div class="card">
    <h2>Add via TCGPlayer ID or Name Search</h2>
    <p>Search by product name to find it on PPT, or enter a TCGPlayer ID directly.</p>

    <div class="lbl">Search by name</div>
    <div class="row">
      <div class="search-wrap">
        <input type="text" id="name-search" placeholder="e.g. Prismatic Evolutions ETB…" oninput="onSearch()" onkeydown="if(event.key==='Enter')doNameSearch()" autocomplete="off">
        <div class="dd" id="dd"></div>
      </div>
      <button class="btn" onclick="doNameSearch()">Search</button>
    </div>
    <hr>
    <div class="lbl">Or enter TCGPlayer ID directly</div>
    <div class="row">
      <input type="number" id="tcg-input" placeholder="e.g. 593457" onkeydown="if(event.key==='Enter')doPreview()">
      <button class="btn" id="prev-btn" onclick="doPreview()">Preview</button>
    </div>
  </div>

  <div id="result"></div>

  <div class="card" style="margin-top:24px;">
    <h2>Stub Listing (no TCGPlayer ID)</h2>
    <p>For slabs, accessories, or anything not on PPT.</p>
    <div class="row">
      <input type="text" id="stub-name" placeholder="Listing name e.g. Charizard PSA 10 Slab">
      <input type="number" id="stub-qty" placeholder="Qty" style="max-width:72px;flex:0 0 72px;">
      <button class="btn btn-ghost" id="stub-btn" onclick="createStub()">Create Stub</button>
    </div>
    <div id="stub-result" style="display:none;margin-top:8px;"></div>
  </div>
</div>

<script>
const STORE_HANDLE = "{sh}";
let _ppt = null, _timer = null;

function onSearch(){{
  clearTimeout(_timer);
  const q = document.getElementById('name-search').value.trim();
  const dd = document.getElementById('dd');
  if(q.length < 3){{ dd.style.display='none'; return; }}
  _timer = setTimeout(()=>runSearch(q), 600);
}}

function doNameSearch(){{
  clearTimeout(_timer);
  const q = document.getElementById('name-search').value.trim();
  if(q.length < 2){{ return; }}
  runSearch(q);
}}

async function runSearch(q){{
  const dd = document.getElementById('dd');
  dd.innerHTML='<div class="dd-item" style="color:var(--dim);">Searching…</div>';
  dd.style.display='block';
  try{{
    const r   = await fetch('/inventory/api/ppt/search?q='+encodeURIComponent(q));
    const items = await r.json();
    if(!Array.isArray(items)||!items.length){{
      dd.innerHTML='<div class="dd-item" style="color:var(--dim);">No results</div>';
      return;
    }}
    dd.innerHTML = items.map(it=>{{
      const id  = it.tcgPlayerId || it.tcgplayer_id || '';
      const p   = it.unopenedPrice || it.prices?.market || 0;
      if (!id) return '';
      const safeId = String(id).replace(/[^0-9]/g, '');
      if (!safeId) return '';
      return `<div class="dd-item" onpointerdown="event.preventDefault();selectItem('${{safeId}}')">
        ${{esc(it.name)}}
        <span class="price">$${{(+p).toFixed(2)}}</span>
        <span class="set">${{esc(it.setName||'')}}</span>
      </div>`;
    }}).join('');
  }}catch(e){{
    dd.innerHTML='<div class="dd-item" style="color:var(--red);">Search error</div>';
  }}
}}

function selectItem(tcgId){{
  const dd = document.getElementById('dd');
  dd.style.display='none';
  document.getElementById('name-search').value='';
  document.getElementById('tcg-input').value = tcgId;
  doPreview();
}}

document.addEventListener('pointerdown', e=>{{
  const wrap = document.querySelector('.search-wrap');
  if(wrap && !wrap.contains(e.target))
    document.getElementById('dd').style.display='none';
}});

async function doPreview(){{
  const tcgId = document.getElementById('tcg-input').value.trim();
  if(!tcgId){{ document.getElementById('tcg-input').focus(); return; }}
  const btn = document.getElementById('prev-btn');
  btn.disabled=true; btn.innerHTML='<span class="spinner"></span>Loading…';
  hideErr(); document.getElementById('result').style.display='none';
  try{{
    const pr = await fetch('/inventory/api/ppt/sealed/'+encodeURIComponent(tcgId));
    if(!pr.ok){{ const d=await pr.json(); throw new Error(d.error||'PPT lookup failed'); }}
    const ppt = await pr.json();
    _ppt = ppt;
    const xr = await fetch('/inventory/api/enrich/preview',{{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{product_name:ppt.name||'',set_name:ppt.setName||'',tcgplayer_id:tcgId}})
    }});
    renderResult(ppt, await xr.json());
  }}catch(e){{ showErr(e.message); }}
  finally{{ btn.disabled=false; btn.innerHTML='Preview'; }}
}}

function renderResult(ppt, prev){{
  const price = ppt.unopenedPrice||ppt.prices?.market||ppt.marketPrice||0;
  const img   = ppt.imageCdnUrl800||ppt.imageCdnUrl400||ppt.imageCdnUrl||'';
  const era   = prev.era||null;
  const w     = prev.weight_oz||'—';
  const tags  = prev.tags||[];
  const hiSet = new Set(['booster box','booster pack','etb','pcetb','blister','sleeved','tin','collection box','sealed','pokemon']);
  const hiT   = tags.filter(t=>hiSet.has(t));
  const otT   = tags.filter(t=>!hiSet.has(t));
  const chs   = ['Online Store','Shop','Point of Sale','Inbox','Facebook & Instagram'];
  const el    = document.getElementById('result');
  el.innerHTML=`<div class="rc">
    <div class="rc-header">
      <div><h2>${{esc(ppt.name)}}</h2><div style="color:var(--dim);font-size:.82rem;margin-top:2px;">${{esc(ppt.setName||'—')}}</div></div>
      <div style="text-align:right;flex-shrink:0;">
        <div style="font-size:.68rem;color:var(--dim);text-transform:uppercase;letter-spacing:.07em;margin-bottom:2px;">Market Price</div>
        <div style="font-size:1.3rem;font-weight:700;color:var(--green);">$${{(+price).toFixed(2)}}</div>
      </div>
    </div>
    ${{!era?`<div style="padding:8px 18px;background:var(--amber-bg);border-bottom:1px solid var(--amber);color:var(--amber);font-size:.82rem;">⚠ <strong>Era unknown.</strong> Listing will include a NEEDS REVIEW note.</div>`:''}}
    <div class="img-sec">
      <div class="img-box">
        <div class="lbl">Source Image</div>
        <div class="img-frame">${{img?`<img src="${{esc(img)}}" alt="">`:'<div class="img-ph">No image</div>'}}</div>
      </div>
      <div class="img-box">
        <div class="lbl">After Processing</div>
        <div class="img-frame"><div class="img-ph">🖼 Generated on create</div></div>
      </div>
    </div>
    <div class="rc-grid">
      <div class="rc-sec"><div class="lbl">Tags</div><div class="tags">${{hiT.map(t=>`<span class="tag hi">${{esc(t)}}</span>`).join('')}}${{otT.map(t=>`<span class="tag">${{esc(t)}}</span>`).join('')}}</div></div>
      <div class="rc-sec"><div class="lbl">Weight</div><div style="font-weight:700;">${{w}} oz</div><div style="font-size:.78rem;color:var(--dim);margin-top:3px;">${{((+w)/16).toFixed(2)}} lb</div></div>
      <div class="rc-sec"><div class="lbl">Metafields</div><table class="meta-t"><tr><td>era</td><td>${{era?esc(era):`<span style="color:var(--amber);">⚠ unknown</span>`}}</td></tr><tr><td>TCGPlayer ID</td><td>${{esc(String(ppt.tcgPlayerId||document.getElementById('tcg-input').value))}}</td></tr></table></div>
      <div class="rc-sec"><div class="lbl">Channels</div><div class="tags">${{chs.map(c=>`<span class="tag hi">✓ ${{esc(c)}}</span>`).join('')}}</div></div>
    </div>
    <div class="action-bar">
      <button class="btn btn-green" onclick="createListing()">✦ Create Draft Listing</button>
      <button class="btn btn-ghost" onclick="doPreview()">↺ Refresh</button>
      <span class="action-note">Creates as DRAFT</span>
    </div>
  </div>`;
  el.style.display='block';
}}

async function createListing(){{
  if(!_ppt) return;
  const tcgId = document.getElementById('tcg-input').value.trim();
  const price = _ppt.unopenedPrice||_ppt.prices?.market||_ppt.marketPrice||0;
  const btn   = event.target;
  btn.disabled=true; btn.innerHTML='<span class="spinner"></span>Creating…';
  try{{
    const resp = await fetch('/inventory/api/enrich/create-listing',{{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{tcgplayer_id:parseInt(tcgId),price:parseFloat(price)||0}})
    }});
    const data = await resp.json();
    if(!resp.ok) throw new Error(data.error||'Create failed');
    const shopUrl = STORE_HANDLE
      ? `https://admin.shopify.com/store/${{STORE_HANDLE}}/products/${{data.product_id}}`
      : `https://admin.shopify.com/products/${{data.product_id}}`;
    btn.closest('.action-bar').innerHTML=`
      <span style="color:var(--green);font-weight:600;">✓ Draft created!</span>
      ${{data.product_id?`<a href="${{shopUrl}}" target="_blank" style="color:var(--accent);font-size:.83rem;">Open in Shopify ↗</a>`:''}}
      ${{data.errors?.length?`<span style="color:var(--amber);font-size:.78rem;">⚠ ${{data.errors.length}} step(s) had errors</span>`:'<span style="color:var(--dim);font-size:.78rem;">All steps OK</span>'}}`;
  }}catch(e){{ btn.disabled=false; btn.innerHTML='✦ Create Draft Listing'; showErr(e.message); }}
}}

async function createStub(){{
  const name = document.getElementById('stub-name').value.trim();
  const qty  = parseInt(document.getElementById('stub-qty').value||'0',10)||0;
  if(!name){{ document.getElementById('stub-name').focus(); return; }}
  const btn=document.getElementById('stub-btn'), res=document.getElementById('stub-result');
  btn.disabled=true; btn.innerHTML='<span class="spinner"></span>Creating…';
  res.style.display='none';
  try{{
    const resp=await fetch('/inventory/api/stub/create',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{name,qty}})}});
    const data=await resp.json();
    if(!resp.ok) throw new Error(data.error||'Failed');
    const shopUrl = STORE_HANDLE
      ? `https://admin.shopify.com/store/${{STORE_HANDLE}}/products/${{data.product_id}}`
      : `https://admin.shopify.com/products/${{data.product_id}}`;
    res.innerHTML=`<div style="background:var(--green-bg);border:1px solid var(--green);border-radius:8px;padding:10px 14px;color:var(--green);font-size:.86rem;">✓ Stub created: <strong>${{esc(name)}}</strong>${{data.product_id?` — <a href="${{shopUrl}}" target="_blank" style="color:var(--accent);">Open in Shopify ↗</a>`:''}}</div>`;
    res.style.display='block';
    document.getElementById('stub-name').value='';
    document.getElementById('stub-qty').value='';
  }}catch(e){{res.innerHTML=`<div class="alert">${{esc(e.message)}}</div>`;res.style.display='block';}}
  finally{{btn.disabled=false;btn.innerHTML='Create Stub';}}
}}

function showErr(m){{ const b=document.getElementById('error-box'); b.innerHTML=`<div class="alert">${{esc(m)}}</div>`; b.style.display='block'; b.scrollIntoView({{behavior:'smooth'}}); }}
function hideErr(){{ document.getElementById('error-box').style.display='none'; }}
</script>
</body>
</html>"""
