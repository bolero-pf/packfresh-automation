"""
routes/inventory.py

All inventory routes: listing, save, push-to-shopify, sync,
PPT-backed listing creation, and stub listing creation.
"""

import os
import logging
import datetime
from functools import wraps

import db
from flask import (
    Blueprint, request, redirect, flash, Response, jsonify, render_template_string
)

logger = logging.getLogger(__name__)

bp = Blueprint("inventory", __name__, url_prefix="/inventory")

# ─── Config ────────────────────────────────────────────────────────────────────

INVENTORY_USER = os.getenv("INVENTORY_USER", "admin")
INVENTORY_PASS = os.getenv("INVENTORY_PASS", "secret")
LOCATION_ID    = os.getenv("LOCATION_ID")
DRY_RUN        = os.getenv("PF_DRY_RUN", "0") == "1"
SHOPIFY_STORE_HANDLE = os.getenv("SHOPIFY_STORE_HANDLE", "")

# ─── Auth ──────────────────────────────────────────────────────────────────────

def _check_auth(u, p):
    return u == INVENTORY_USER and p == INVENTORY_PASS

def _authenticate():
    return Response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Inventory"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not _check_auth(auth.username, auth.password):
            return _authenticate()
        return f(*args, **kwargs)
    return decorated

# ─── Helpers ───────────────────────────────────────────────────────────────────

def _get_shopify_client():
    """Retrieve the ShopifyClient singleton from the app context."""
    import app as _app
    return _app.shopify_client

def _get_cache_manager():
    import app as _app
    return _app.cache_manager

def _get_ppt_client():
    import app as _app
    return _app.ppt_client

def _get_last_sync_str() -> str:
    try:
        row = db.query_one("SELECT last_refreshed_at FROM inventory_cache_meta LIMIT 1")
        if row and row.get("last_refreshed_at"):
            return str(row["last_refreshed_at"])[:19]
        return "Never"
    except Exception:
        return "Unknown"

def _load_inventory() -> list[dict]:
    """
    Load the full inventory from inventory_product_cache + inventory_overrides.
    Returns list of dicts with all display columns.
    """
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
            COALESCE(o.physical_count, 0)                   AS physical_count,
            COALESCE(o.notes, '')                           AS notes
        FROM inventory_product_cache c
        LEFT JOIN inventory_overrides o ON o.shopify_variant_id = c.shopify_variant_id
        ORDER BY c.title
    """)
    return [dict(r) for r in rows]

def _save_override(variant_id: int, physical_count=None, notes=None):
    """Upsert physical_count / notes for a variant."""
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
        logger.info(f"[DRY_RUN] Would set price variant_id={variant_id} → {price:.2f}")
        return True
    try:
        sc.update_variant_price(variant_id, price)
        return True
    except Exception as e:
        logger.error(f"Price update failed for variant {variant_id}: {e}")
        return False

def _update_shopify_qty(inventory_item_id: int, new_qty: int) -> bool:
    sc = _get_shopify_client()
    loc_id = LOCATION_ID
    if sc is None or not loc_id:
        return False
    if DRY_RUN:
        logger.info(f"[DRY_RUN] Would set qty inventory_item_id={inventory_item_id} @ loc={loc_id} → {new_qty}")
        return True
    try:
        sc.set_inventory_level(inventory_item_id, int(loc_id), new_qty)
        return True
    except Exception as e:
        logger.error(f"Qty update failed for inv_item {inventory_item_id}: {e}")
        return False

# ─── Tag options (curated) ─────────────────────────────────────────────────────

CURATED_TAGS = [
    "sealed", "slab", "collection box", "tin", "etb", "pcetb",
    "booster box", "booster pack",
]

# ─── Routes ────────────────────────────────────────────────────────────────────

@bp.route("/sync")
@requires_auth
def sync_now():
    cm = _get_cache_manager()
    if cm:
        cm.ensure_tables()
        cm.invalidate("manual_sync")
        flash("🔁 Sync triggered — cache is refreshing in the background.", "success")
    else:
        flash("⚠ Shopify not configured.", "warning")
    return redirect("/inventory")


@bp.route("/export.csv")
@requires_auth
def export_csv():
    cm = _get_cache_manager()
    if cm:
        cm.check_and_refresh_if_stale()

    rows = _load_inventory()

    q          = (request.args.get("q") or "").strip().lower()
    in_stock   = request.args.get("in_stock") == "1"
    tag_any    = [t.lower() for t in request.args.getlist("tag")]
    status     = request.args.get("status", "all")
    qty_mm     = request.args.get("qty_mismatch") == "1"

    rows = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                          status=status, qty_mismatch=qty_mm)

    import csv, io
    out = io.StringIO()
    cols = ["name", "shopify_qty", "shopify_price", "shopify_value",
            "physical_count", "shopify_tags", "notes",
            "shopify_variant_id", "shopify_inventory_id"]
    writer = csv.DictWriter(out, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    filename = f"inventory-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')}.csv"
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


def _apply_filters(rows, *, q=None, in_stock=False, tag_any=None, status="all", qty_mismatch=False):
    if status != "all":
        rows = [r for r in rows if (r["shopify_status"] != "draft") == (status == "published")
                or (status == "draft" and r["shopify_status"] == "draft")]
    if q:
        rows = [r for r in rows if q in (r.get("name") or "").lower()]
    if in_stock:
        rows = [r for r in rows if (r.get("shopify_qty") or 0) > 0]
    if tag_any:
        def has_all(r):
            tags_str = (r.get("shopify_tags") or "").lower()
            return all(t in tags_str for t in tag_any)
        rows = [r for r in rows if has_all(r)]
    if qty_mismatch:
        rows = [r for r in rows if r.get("physical_count", 0) != (r.get("shopify_qty") or 0)]
    return rows


@bp.route("/", methods=["GET", "POST"])
@requires_auth
def index():
    cm = _get_cache_manager()
    if cm:
        cm.ensure_tables()
        cm.check_and_refresh_if_stale()

    # ── SAVE (POST) ─────────────────────────────────────────────────────────
    if request.method == "POST" and request.form.get("save") == "1":
        dirty_keys = set((request.form.get("dirty_keys") or "").split(","))
        mode = request.form.get("mode", "save")
        updates = request.form.to_dict(flat=True)

        rows = _load_inventory()

        # Build index: row_index → row
        limit = int(request.args.get("limit", 400))
        q         = (request.args.get("q") or "").strip().lower()
        in_stock  = request.args.get("in_stock") == "1"
        tag_any   = [t.lower() for t in request.args.getlist("tag")]
        status    = request.args.get("status", "all")
        qty_mm    = request.args.get("qty_mismatch") == "1"
        sort_col  = request.args.get("sort")
        sort_dir  = request.args.get("dir", "asc")

        filtered = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                                  status=status, qty_mismatch=qty_mm)
        filtered = _sort_rows(filtered, sort_col, sort_dir)
        page = filtered[:limit]

        changed_rows = []

        for i, row in enumerate(page):
            variant_id  = row.get("shopify_variant_id")
            inv_item_id = row.get("shopify_inventory_id")
            new_price   = None
            new_qty     = None
            new_phys    = None
            new_notes   = None
            pending_delta = None

            for col in ("shopify_qty", "shopify_price", "physical_count", "notes", "adjust_delta"):
                key = f"cell_{i}_{col}"
                if dirty_keys and key not in dirty_keys:
                    continue
                if key not in updates:
                    continue
                raw = (updates[key] or "").strip()
                if raw == "":
                    continue

                if col == "adjust_delta":
                    try:
                        pending_delta = int(raw)
                    except Exception:
                        pass
                elif col == "shopify_qty":
                    try:
                        new_qty = int(raw)
                    except Exception:
                        pass
                elif col == "shopify_price":
                    try:
                        new_price = round(float(raw), 2)
                    except Exception:
                        pass
                elif col == "physical_count":
                    try:
                        new_phys = int(raw)
                    except Exception:
                        pass
                elif col == "notes":
                    new_notes = raw

            if pending_delta is not None and variant_id:
                # Fetch current qty from DB
                cur = db.query_one(
                    "SELECT shopify_qty FROM inventory_product_cache WHERE shopify_variant_id = %s",
                    (variant_id,)
                )
                base = (cur or {}).get("shopify_qty") or 0
                new_qty = base + pending_delta

            # Persist physical_count + notes overrides
            if new_phys is not None or new_notes is not None:
                if variant_id:
                    _save_override(variant_id, physical_count=new_phys, notes=new_notes)

            if new_qty is not None or new_price is not None:
                changed_rows.append({
                    "variant_id": variant_id,
                    "shopify_inventory_id": inv_item_id,
                    "shopify_qty": new_qty,
                    "shopify_price": new_price,
                })

        # Push to Shopify if Enter-key mode
        if mode == "push" and changed_rows:
            pushed = failed = 0
            for ch in changed_rows:
                if ch.get("shopify_price") is not None and ch.get("variant_id"):
                    ok = _update_shopify_price(int(ch["variant_id"]), ch["shopify_price"])
                    pushed += ok; failed += not ok

                if ch.get("shopify_qty") is not None and ch.get("shopify_inventory_id"):
                    ok = _update_shopify_qty(int(ch["shopify_inventory_id"]), ch["shopify_qty"])
                    pushed += ok; failed += not ok

            label = "DRY RUN" if DRY_RUN else "LIVE"
            flash(f"🚀 {label}: pushed {pushed} update(s){' with errors' if failed else ''}.",
                  "success" if not failed else "warning")
            # Invalidate cache so the next load reflects Shopify's new values
            if cm and not DRY_RUN:
                cm.invalidate("push_from_inventory")
        else:
            flash("💾 Saved locally.", "success")

        return redirect(request.full_path or "/inventory")

    # ── GET (render) ─────────────────────────────────────────────────────────
    rows = _load_inventory()

    q         = (request.args.get("q") or "").strip().lower()
    in_stock  = request.args.get("in_stock") == "1"
    tag_any   = [t.lower() for t in request.args.getlist("tag")]
    status    = request.args.get("status", "all")
    qty_mm    = request.args.get("qty_mismatch") == "1"
    sort_col  = request.args.get("sort")
    sort_dir  = request.args.get("dir", "asc")
    limit     = int(request.args.get("limit", 400))

    total_rows = len(rows)
    filtered   = _apply_filters(rows, q=q, in_stock=in_stock, tag_any=tag_any,
                                 status=status, qty_mismatch=qty_mm)
    filtered   = _sort_rows(filtered, sort_col, sort_dir)
    page       = filtered[:limit]

    totals = {
        "count":         len(filtered),
        "shopify_qty":   sum((r.get("shopify_qty") or 0) for r in filtered),
        "physical_count": sum((r.get("physical_count") or 0) for r in filtered),
        "shopify_value": sum(
            (r.get("shopify_qty") or 0) * (r.get("shopify_price") or 0) for r in filtered
        ),
    }

    meta = {
        "last_sync":  _get_last_sync_str(),
        "mode_label": "DRY RUN" if DRY_RUN else "LIVE",
        "totals":     totals,
        "query_string": request.query_string.decode("utf-8"),
    }
    filters = {
        "q":             q,
        "in_stock":      in_stock,
        "tag_options":   CURATED_TAGS,
        "selected_tags": tag_any,
        "status":        status,
        "sort":          sort_col,
        "dir":           sort_dir,
        "qty_mismatch":  qty_mm,
    }

    return _render_inventory(
        rows=page,
        total_rows=total_rows,
        filters=filters,
        meta=meta,
        limit=limit,
    )


def _sort_rows(rows, sort_col, sort_dir):
    SORTABLE = {"name", "shopify_qty", "shopify_price", "shopify_value", "physical_count", "notes"}
    if sort_col not in SORTABLE:
        return rows

    def key(r):
        v = r.get(sort_col)
        if v is None:
            return (1, 0, "")
        if isinstance(v, (int, float)):
            return (0, v, "")
        return (0, 0, str(v).lower())

    return sorted(rows, key=key, reverse=(sort_dir == "desc"))


@bp.route("/push_prices", methods=["POST"])
@requires_auth
def push_prices():
    rows = _load_inventory()
    pushed = failed = 0
    for row in rows:
        vid = row.get("shopify_variant_id")
        price = row.get("shopify_price")
        if vid and price is not None:
            ok = _update_shopify_price(int(vid), float(price))
            pushed += ok; failed += not ok
    label = "DRY RUN" if DRY_RUN else "LIVE"
    flash(f"💸 {label}: pushed prices for {pushed} variant(s){' (some failed)' if failed else ''}.",
          "success" if not failed else "warning")
    return redirect("/inventory")


# ─── PPT / Listing creation routes ────────────────────────────────────────────

@bp.route("/add")
@requires_auth
def add_item_page():
    """Render the 'Add New Item' page (enrich_preview style)."""
    return _render_add_page()


@bp.route("/api/ppt/sealed/<int:tcgplayer_id>")
@requires_auth
def ppt_sealed_lookup(tcgplayer_id):
    ppt = _get_ppt_client()
    if ppt is None:
        return jsonify({"error": "PPT not configured"}), 503
    name = request.args.get("product_name", "")
    item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id, product_name=name)
    if not item:
        return jsonify({"error": f"No PPT product found for TCGPlayer ID {tcgplayer_id}"}), 404
    return jsonify(item)


@bp.route("/api/ppt/search")
@requires_auth
def ppt_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify([])
    ppt = _get_ppt_client()
    if ppt is None:
        return jsonify({"error": "PPT not configured"}), 503
    results = ppt.search_sealed_products(q, limit=8)
    return jsonify(results)


@bp.route("/api/enrich/preview", methods=["POST"])
@requires_auth
def enrich_preview():
    import product_enrichment as enrichment
    data = request.get_json() or {}
    name = data.get("product_name", "")
    set_name = data.get("set_name", "")
    return jsonify({
        "product_name": name,
        "set_name": set_name,
        "tags":       enrichment.infer_tags(name, set_name),
        "era":        enrichment.infer_era(name, set_name),
        "weight_oz":  enrichment.infer_weight_oz(name),
    })


@bp.route("/api/enrich/create-listing", methods=["POST"])
@requires_auth
def create_listing():
    """
    Create a new Shopify listing from a TCGPlayer ID (full enrichment).
    Body: { "tcgplayer_id": 12345, "price": 29.99, "quantity": 0 }
    """
    import product_enrichment as enrichment
    data = request.get_json() or {}
    tcgplayer_id = data.get("tcgplayer_id")
    quantity     = int(data.get("quantity", 0))

    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    ppt = _get_ppt_client()
    if ppt is None:
        return jsonify({"error": "PPT not configured"}), 503

    ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id, product_name=data.get("product_name"))
    if not ppt_item:
        return jsonify({"error": f"No PPT product for TCGPlayer ID {tcgplayer_id}"}), 404

    price = data.get("price") or ppt_item.get("unopenedPrice") or ppt_item.get("marketPrice") or 0
    if not price:
        return jsonify({"error": "No price available — provide price or ensure PPT has market data"}), 400

    if DRY_RUN:
        return jsonify({"dry_run": True, "product_name": ppt_item.get("name"), "price": price}), 200

    summary = enrichment.create_draft_listing(ppt_item, price=float(price), quantity=quantity)

    # Invalidate cache so the new listing appears on next load
    cm = _get_cache_manager()
    if cm:
        cm.invalidate("new_listing_created")

    return jsonify(summary)


@bp.route("/api/stub/create", methods=["POST"])
@requires_auth
def create_stub():
    """
    Create a minimal stub listing: name + qty only (no TCGPlayer ID, no enrichment).
    Body: { "name": "...", "qty": 0 }
    """
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    qty  = int(data.get("qty", 0))

    if not name:
        return jsonify({"error": "name required"}), 400

    sc = _get_shopify_client()
    if sc is None:
        return jsonify({"error": "Shopify not configured"}), 503

    if DRY_RUN:
        return jsonify({"dry_run": True, "name": name}), 200

    try:
        result = sc.create_draft_product_stub(name=name)
        product_id = result["product_id"]
        variant_id = result["variant_id"]
        inv_item_id = result["inventory_item_id"]

        if qty > 0 and LOCATION_ID and inv_item_id:
            _update_shopify_qty(int(inv_item_id), qty)

        cm = _get_cache_manager()
        if cm:
            cm.invalidate("stub_created")

        return jsonify({"product_id": product_id, "variant_id": variant_id, "title": name})
    except Exception as e:
        logger.error(f"Stub create failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Renderer ─────────────────────────────────────────────────────────────────

def _build_sort_qs(col, next_dir):
    from urllib.parse import parse_qs, urlencode
    qs = parse_qs(request.query_string.decode("utf-8"), keep_blank_values=True)
    qs.pop("sort", None); qs.pop("dir", None)
    qs["sort"] = [col]; qs["dir"] = [next_dir]
    return "?" + urlencode(qs, doseq=True)


def _render_inventory(rows, total_rows, filters, meta, limit):
    import html as _html

    q            = filters.get("q", "")
    in_stock     = filters.get("in_stock", False)
    tag_options  = filters.get("tag_options", [])
    selected_tags = set(t.lower() for t in filters.get("selected_tags", []))
    status       = filters.get("status", "all")
    sort_col     = filters.get("sort")
    sort_dir     = filters.get("dir", "asc")
    qty_mismatch = filters.get("qty_mismatch", False)
    last_sync    = meta.get("last_sync", "Never")
    mode_label   = meta.get("mode_label", "LIVE")
    totals       = meta.get("totals", {})
    query_string = meta.get("query_string", "")

    qs = f"?{query_string}" if query_string else ""

    def disp(col):
        return {
            "name":           "Name",
            "shopify_qty":    "Shopify Qty",
            "shopify_price":  "Shopify Price",
            "shopify_value":  "Shopify Value",
            "physical_count": "Physical Count",
            "adjust_delta":   "Adjust Δ",
            "notes":          "Notes",
        }.get(col, col.replace("_", " ").title())

    def sort_link(col):
        if col == sort_col:
            nd = "desc" if sort_dir == "asc" else "asc"
            arrow = " ▲" if sort_dir == "asc" else " ▼"
        else:
            nd = "asc"; arrow = ""
        return _build_sort_qs(col, nd), arrow

    SHOW_COLS = ["physical_count", "name", "shopify_qty", "adjust_delta",
                 "shopify_price", "shopify_value", "notes"]
    EDITABLE  = {"shopify_qty", "shopify_price", "physical_count", "notes", "adjust_delta"}

    # ── flashed messages
    from flask import get_flashed_messages
    flash_html = ""
    for cat, msg in get_flashed_messages(with_categories=True):
        color = {"success": "#2dd4a0", "warning": "#f5a623", "danger": "#f05252"}.get(cat, "#dfa260")
        flash_html += f'<div style="background:rgba(0,0,0,.4);border-left:3px solid {color};padding:10px 16px;margin-bottom:8px;border-radius:6px;color:{color};font-size:14px;">{_html.escape(msg)}</div>'

    # ── totals badges
    totals_html = (
        f'<span class="pf-badge">Items: {totals.get("count",0)}</span>'
        f'<span class="pf-badge">Shopify Qty: {totals.get("shopify_qty",0)}</span>'
        f'<span class="pf-badge">Physical: {totals.get("physical_count",0)}</span>'
        f'<span class="pf-badge">Value: ${totals.get("shopify_value",0):,.2f}</span>'
    )

    # ── status chips
    def sc_checked(v): return "checked" if status == v else ""
    status_chips = f"""
    <div class="pf-chips pf-chips-status mb-2">
      <label class="pf-chip"><input type="radio" name="status" value="all" {sc_checked('all')} onchange="this.form.submit()"><span>All</span></label>
      <label class="pf-chip"><input type="radio" name="status" value="published" {sc_checked('published')} onchange="this.form.submit()"><span>Published</span></label>
      <label class="pf-chip"><input type="radio" name="status" value="draft" {sc_checked('draft')} onchange="this.form.submit()"><span>Drafts</span></label>
    </div>"""

    # ── tag chips
    tag_chips_html = "".join(
        f'<label class="pf-chip"><input type="checkbox" name="tag" value="{_html.escape(t)}" {"checked" if t in selected_tags else ""}>{_html.escape(t)}</label>'
        for t in tag_options
    )

    qty_checked = "checked" if qty_mismatch else ""

    # ── thead
    thead_cells = ["<th></th>"]
    for col in SHOW_COLS:
        href, arrow = sort_link(col)
        thead_cells.append(f'<th><a href="{href}" class="pf-sort">{_html.escape(disp(col))}{arrow}</a></th>')
    thead_html = "<thead><tr>" + "".join(thead_cells) + "</tr></thead>"

    # ── tbody
    body_rows = []
    for i, row in enumerate(rows):
        variant_id = row.get("shopify_variant_id")
        tds = [f'<td><input type="checkbox" name="merge_ids" value="{i}"></td>']
        for col in SHOW_COLS:
            val = row.get(col, "")
            if val is None: val = ""
            vs = _html.escape(str(val))
            if col in EDITABLE:
                if col == "shopify_price":
                    cell = f'<input name="cell_{i}_shopify_price" value="{vs}" data-orig="{vs}" inputmode="decimal" class="pf-input" style="max-width:120px">'
                elif col in ("shopify_qty", "physical_count"):
                    cell = (
                        f'<div class="pf-qty-wrap">'
                        f'<button type="button" class="pf-qty-btn pf-dec" data-target="cell_{i}_{col}">−</button>'
                        f'<input name="cell_{i}_{col}" value="{vs}" data-orig="{vs}" inputmode="numeric" class="pf-input" style="max-width:90px">'
                        f'<button type="button" class="pf-qty-btn pf-inc" data-target="cell_{i}_{col}">+</button>'
                        f'</div>'
                    )
                elif col == "adjust_delta":
                    cell = f'<input name="cell_{i}_adjust_delta" value="" data-orig="" inputmode="numeric" class="pf-input" placeholder="±qty" style="max-width:90px">'
                else:
                    cell = f'<input name="cell_{i}_{col}" value="{vs}" data-orig="{vs}" class="pf-input">'
                tds.append(f"<td>{cell}</td>")
            else:
                # shopify_value: coloured by sign
                if col == "shopify_value":
                    tds.append(f"<td>{vs}</td>")
                else:
                    tds.append(f"<td class='text-truncate'>{vs}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")

    tbody_html = "<tbody>" + "".join(body_rows) + "</tbody>"

    page_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pack Fresh · Inventory</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
a{{color:inherit;text-decoration:none;}}
body{{padding:16px 20px;}}

/* Topbar */
.pf-topbar{{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:0 0 16px;flex-wrap:wrap;}}
.pf-title{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}}
.pf-badge{{padding:5px 10px;border-radius:999px;font-size:12px;border:1px solid var(--border);background:var(--surface);color:var(--text);}}
.pf-actions{{display:flex;gap:8px;flex-wrap:wrap;}}
.pf-btn{{display:inline-flex;align-items:center;justify-content:center;gap:6px;height:38px;padding:0 14px;border-radius:10px;font-size:13px;white-space:nowrap;cursor:pointer;border:1px solid rgba(255,255,255,.22);background:transparent;color:var(--text);font-family:inherit;}}
.pf-btn:hover{{border-color:var(--accent);color:var(--accent);}}
.pf-btn-primary{{background:var(--accent);color:#1b1b1b;border-color:var(--accent);}}
.pf-btn-primary:hover{{filter:brightness(1.05);}}
.pf-btn-green{{background:var(--green);color:#0d1117;border-color:var(--green);}}

/* Toolbar */
.pf-toolbar{{display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px;margin:0 0 12px;}}
.pf-search{{flex:1 1 320px;max-width:700px;min-width:220px;}}
.pf-search input{{width:100%;height:42px;font-size:15px;border-radius:10px;background:var(--surface);color:var(--text);border:1px solid var(--border);padding:9px 13px;font-family:inherit;}}
.pf-search input:focus{{outline:none;border-color:var(--accent);}}
.form-check-input{{width:18px;height:18px;accent-color:var(--accent);}}

/* Chips */
.pf-chips{{display:flex;flex-wrap:wrap;gap:7px;}}
.pf-chip{{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:999px;background:var(--surface);border:1px solid var(--border);font-size:13px;cursor:pointer;}}
.pf-chip input[type="radio"]{{display:none;}}
.pf-chip input[type="radio"]:checked+span,.pf-chip input[type="checkbox"]:checked+.label-text{{border-color:var(--accent);color:var(--accent);}}
.pf-chips-status .pf-chip{{padding:0;border:none;background:none;}}
.pf-chips-status .pf-chip span{{display:inline-flex;align-items:center;padding:6px 12px;border-radius:999px;background:var(--surface);border:1px solid var(--border);}}
.pf-chips-status input[type="radio"]:checked+span{{background:var(--accent);color:#1b1b1b;border-color:var(--accent);}}

/* Table */
.pf-sort{{color:var(--text);text-decoration:none;}}
.pf-sort:hover{{color:var(--accent);}}
.table{{width:100%;border-collapse:collapse;font-size:13px;color:var(--text);}}
.table thead th{{background:var(--surface);position:sticky;top:0;z-index:2;border-bottom:1px solid var(--border);padding:10px 11px;text-align:left;}}
.table tbody tr:nth-child(even){{background:rgba(255,255,255,.04);}}
.table tbody tr:hover{{background:rgba(255,255,255,.09);}}
.table td{{padding:8px 11px;vertical-align:middle;}}
.text-truncate{{max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}

/* Inputs */
.pf-input{{background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:8px;height:34px;font-size:13px;padding:5px 9px;font-family:inherit;}}
.pf-input:focus{{outline:none;border-color:var(--accent);}}
.is-warning{{outline:2px solid var(--accent);}}

/* Qty cluster */
.pf-qty-wrap{{display:flex;align-items:center;gap:5px;}}
.pf-qty-btn{{min-width:30px;height:30px;border-radius:8px;background:transparent;color:var(--text);border:1px solid var(--border);cursor:pointer;font-family:inherit;font-size:14px;}}
.pf-qty-btn:hover{{border-color:var(--accent);color:var(--accent);}}
</style>
</head>
<body>

{flash_html}

<div class="pf-topbar">
  <div class="pf-title">
    <h3 style="margin:0;font-size:1.1rem;">🗂️ Inventory <small style="font-size:.7em;color:var(--pf-muted);">({total_rows} variants · showing {len(rows)})</small></h3>
    {totals_html}
    <span class="pf-badge">Last sync: {_html.escape(last_sync)}</span>
    <span class="pf-badge" style="border-color:var(--pf-accent);">{_html.escape(mode_label)}</span>
  </div>
  <div class="pf-actions">
    <a class="pf-btn" href="/inventory/sync">🔁 Sync Shopify</a>
    <a class="pf-btn pf-btn-green" href="/inventory/add">➕ Add Item</a>
    <a class="pf-btn" href="/inventory/export.csv{qs}">📤 Export CSV</a>
    <form method="post" action="/inventory/push_prices" style="display:inline">
      <button class="pf-btn pf-btn-primary">💸 Push Prices</button>
    </form>
  </div>
</div>

<!-- Filter bar -->
<form id="pf-filter" method="get" class="pf-toolbar">
  <div class="pf-search">
    <input name="q" value="{_html.escape(q)}" placeholder="Search by name…">
  </div>
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;">
    <input class="form-check-input" type="checkbox" name="in_stock" value="1" {'checked' if in_stock else ''}>
    In stock only
  </label>
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;">
    <input class="form-check-input" type="checkbox" name="qty_mismatch" value="1" {qty_checked} onchange="this.form.submit()">
    Qty mismatch
  </label>
  <input type="hidden" name="limit" value="{_html.escape(str(limit))}">
  <button class="pf-btn">Filter</button>
  <div style="flex-basis:100%;height:0;"></div>
  {status_chips}
  <div class="pf-chips">{tag_chips_html}</div>
</form>

<!-- Edit form -->
<form id="pf-form" method="post">
  <input type="hidden" name="save" value="1">
  <input type="hidden" id="dirty_keys" name="dirty_keys" value="">
  <input type="hidden" id="mode" name="mode" value="save">
  <div style="overflow-x:auto;">
    <table class="table">
      {thead_html}
      {tbody_html}
    </table>
  </div>
  <button id="pf-save" class="pf-btn pf-btn-primary" style="margin-top:12px;">💾 Save</button>
</form>

<script>
(function(){{
  const editForm   = document.getElementById('pf-form');
  const filterForm = document.getElementById('pf-filter');
  const dirty = new Set();
  let lastEditedInput = null;

  function markDirty(inp){{
    const orig = inp.getAttribute('data-orig');
    const key  = inp.name;
    if(orig === null || !key) return;
    if(inp.value !== orig){{ dirty.add(key); inp.classList.add('is-warning'); }}
    else {{ dirty.delete(key); inp.classList.remove('is-warning'); }}
  }}

  document.querySelectorAll('#pf-form .pf-input').forEach(function(inp){{
    inp.addEventListener('focus', ()=>{{ lastEditedInput=inp; }});
    inp.addEventListener('input', ()=>{{ lastEditedInput=inp; markDirty(inp); }});
    inp.addEventListener('change', ()=>{{ lastEditedInput=inp; markDirty(inp); }});
    inp.addEventListener('keydown', function(e){{
      if(e.key==='Enter'){{
        e.preventDefault();
        document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
        document.getElementById('mode').value = 'push';
        editForm.submit();
      }}
    }});
  }});

  const saveBtn = document.getElementById('pf-save');
  if(saveBtn){{ saveBtn.addEventListener('click', function(){{
    document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
    document.getElementById('mode').value = 'save';
  }}); }}

  function bump(targetName, delta){{
    const inp = document.querySelector('#pf-form input[name="'+targetName+'"]');
    if(!inp) return;
    const v = parseInt(inp.value||'0',10)||0;
    inp.value = String(v+delta);
    lastEditedInput = inp;
    inp.dispatchEvent(new Event('input',{{bubbles:true}}));
    inp.focus(); inp.select();
  }}
  document.querySelectorAll('#pf-form .pf-inc').forEach(function(btn){{
    btn.addEventListener('click', function(){{ bump(btn.getAttribute('data-target'),+1); btn.blur(); }});
  }});
  document.querySelectorAll('#pf-form .pf-dec').forEach(function(btn){{
    btn.addEventListener('click', function(){{ bump(btn.getAttribute('data-target'),-1); btn.blur(); }});
  }});

  document.addEventListener('keydown', function(e){{
    if(e.key!=='Enter') return;
    const el = document.activeElement;
    const inEdit   = editForm   && editForm.contains(el);
    const inFilter = filterForm && filterForm.contains(el);
    if(inFilter) return;
    if(inEdit && el.classList && !el.classList.contains('pf-input')){{ e.preventDefault(); return; }}
    if(!inFilter && !inEdit && lastEditedInput){{
      e.preventDefault();
      document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
      document.getElementById('mode').value = 'push';
      editForm.submit();
    }}
  }});
}})();
</script>
</body>
</html>"""
    return page_html


def _render_add_page():
    import html as _html
    store_handle = _html.escape(SHOPIFY_STORE_HANDLE)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Add Item · Pack Fresh Inventory</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;height:52px;display:flex;align-items:center;gap:12px;}}
header .logo{{font-weight:700;font-size:1rem;}} header .logo span{{color:var(--green);}}
header .sub{{color:var(--dim);font-size:.85rem;}}
header a{{margin-left:auto;color:var(--dim);font-size:.8rem;text-decoration:none;}}
header a:hover{{color:var(--text);}}
.container{{max-width:900px;margin:0 auto;padding:32px 24px;}}
.card{{padding:28px;margin-bottom:24px;}}
.card h2{{font-size:1rem;font-weight:600;margin-bottom:6px;}}
.card p{{color:var(--dim);font-size:.85rem;margin-bottom:18px;}}
.section-title{{font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);margin-bottom:10px;}}
.row{{display:flex;gap:10px;align-items:flex-end;margin-bottom:10px;}}
.row input{{flex:1;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 14px;font-size:1rem;font-family:inherit;}}
.row input:focus{{outline:none;border-color:var(--accent);}}
.btn{{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:10px 20px;font-size:.9rem;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap;}}
.btn:hover{{background:var(--accent-hover);}} .btn:disabled{{opacity:.5;cursor:not-allowed;}}
.btn-green{{background:var(--green);color:#000;}} .btn-green:hover{{background:#25bd8e;}}
.divider{{border:none;border-top:1px solid var(--border);margin:20px 0;}}
.spinner{{display:inline-block;width:14px;height:14px;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:6px;}}
@keyframes spin{{to{{transform:rotate(360deg);}}}}
.alert{{background:var(--red-bg);border:1px solid var(--red);border-radius:8px;padding:12px 16px;color:var(--red);font-size:.88rem;margin-bottom:14px;}}
.alert-amber{{background:var(--amber-bg);border-color:var(--amber);color:var(--amber);}}
/* Result card */
#result{{display:none;}}
.result-card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;}}
.result-header{{padding:20px 24px 16px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;justify-content:space-between;gap:16px;}}
.result-header h2{{font-size:1.1rem;font-weight:700;line-height:1.3;}}
.result-header .set{{color:var(--dim);font-size:.85rem;margin-top:3px;}}
.result-grid{{display:grid;grid-template-columns:1fr 1fr;}}
.result-section{{padding:18px 22px;border-bottom:1px solid var(--border);}}
.result-section:nth-child(odd){{border-right:1px solid var(--border);}}
.tags{{display:flex;flex-wrap:wrap;gap:6px;}}
.tag{{background:var(--surface2);border:1px solid var(--border);border-radius:20px;padding:3px 10px;font-size:.78rem;color:var(--dim);}}
.tag.hi{{background:var(--green-bg);border-color:var(--green);color:var(--green);}}
.meta-table{{width:100%;font-size:.84rem;border-collapse:collapse;}}
.meta-table td:first-child{{color:var(--dim);padding-right:14px;padding-bottom:5px;white-space:nowrap;}}
.image-section{{grid-column:1/-1;padding:18px 22px;border-bottom:1px solid var(--border);display:flex;gap:14px;}}
.image-orig,.image-proc{{flex:1;text-align:center;}}
.image-frame{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:8px;display:inline-flex;align-items:center;justify-content:center;width:100%;min-height:120px;}}
.image-frame img{{max-width:100%;max-height:220px;border-radius:4px;}}
.image-ph{{color:var(--dim);font-size:.8rem;padding:18px;}}
.action-bar{{padding:18px 22px;display:flex;gap:10px;align-items:center;background:var(--surface2);flex-wrap:wrap;}}
.action-note{{color:var(--dim);font-size:.78rem;margin-left:auto;}}
/* Search results dropdown */
.search-results{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;margin-top:4px;max-height:260px;overflow-y:auto;display:none;}}
.search-result-item{{padding:10px 14px;cursor:pointer;font-size:.88rem;border-bottom:1px solid var(--border);}}
.search-result-item:last-child{{border-bottom:none;}}
.search-result-item:hover{{background:rgba(255,255,255,.07);}}
.search-result-item .price{{float:right;color:var(--green);font-weight:600;}}
.search-result-item .set{{color:var(--dim);font-size:.78rem;display:block;margin-top:2px;}}
@media(max-width:600px){{.result-grid{{grid-template-columns:1fr;}} .result-section:nth-child(odd){{border-right:none;}} .image-section{{flex-direction:column;}} .row{{flex-direction:column;}}}}
</style>
</head>
<body>
<header>
  <div class="logo">Pack<span>Fresh</span></div>
  <div class="sub">Add Inventory Item</div>
  <a href="/inventory">← Inventory</a>
</header>
<div class="container">

  <div id="error-box" style="display:none;"></div>

  <!-- ── Tab 1: TCGPlayer ID / PPT Search ── -->
  <div class="card">
    <h2>Add via TCGPlayer ID or PPT Search</h2>
    <p>Enter a TCGPlayer ID directly, or search by product name to find it. Creates a fully enriched Shopify draft listing with tags, image, metafields, and channels.</p>

    <div class="section-title">Search by name</div>
    <div class="row" style="position:relative;">
      <div style="flex:1;position:relative;">
        <input type="text" id="name-search" placeholder="e.g. Prismatic Evolutions ETB…" oninput="onNameSearch()" autocomplete="off">
        <div class="search-results" id="search-results"></div>
      </div>
    </div>

    <hr class="divider">

    <div class="section-title">Or enter TCGPlayer ID directly</div>
    <div class="row">
      <input type="number" id="tcg-input" placeholder="e.g. 593457" onkeydown="if(event.key==='Enter') doPreview()">
      <button class="btn" onclick="doPreview()" id="preview-btn">Preview</button>
    </div>
  </div>

  <!-- ── Result preview ── -->
  <div id="result"></div>

  <!-- ── Tab 2: Stub listing (no TCG ID) ── -->
  <div class="card" style="margin-top:32px;">
    <h2>Create Stub Listing (no TCGPlayer ID)</h2>
    <p>Use this for slabs, accessories, or items you can't find on PPT. Creates a minimal Shopify draft with just a name and optional quantity.</p>
    <div class="row">
      <input type="text" id="stub-name" placeholder="Listing name e.g. 2024 Charizard PSA 10 Slab">
      <input type="number" id="stub-qty" placeholder="Qty" style="max-width:90px;flex:0 0 90px;">
      <button class="btn btn-ghost" onclick="createStub()" id="stub-btn">Create Stub</button>
    </div>
    <div id="stub-result" style="margin-top:10px;display:none;"></div>
  </div>

</div>
<script>
const STORE_HANDLE = "{store_handle}";
let _currentPptItem = null;
let _searchTimer = null;

// ── Name search ──────────────────────────────────────────────────────────────
function onNameSearch(){{
  clearTimeout(_searchTimer);
  const q = document.getElementById('name-search').value.trim();
  const dd = document.getElementById('search-results');
  if(q.length < 3){{ dd.style.display='none'; return; }}
  _searchTimer = setTimeout(()=>runSearch(q), 350);
}}

async function runSearch(q){{
  const dd = document.getElementById('search-results');
  dd.innerHTML = '<div class="search-result-item" style="color:var(--dim);">Searching…</div>';
  dd.style.display = 'block';
  try{{
    const r = await fetch('/inventory/api/ppt/search?q='+encodeURIComponent(q));
    const items = await r.json();
    if(!items.length){{
      dd.innerHTML = '<div class="search-result-item" style="color:var(--dim);">No results — try a TCGPlayer ID below</div>';
      return;
    }}
    dd.innerHTML = items.map(it=>{{
      const price = it.unopenedPrice || it.prices?.market || 0;
      return `<div class="search-result-item" onclick="selectSearchResult(${{JSON.stringify(it.tcgPlayerId||it.tcgplayer_id)}})">
        ${{esc(it.name)}}
        <span class="price">$${{(+price).toFixed(2)}}</span>
        <span class="set">${{esc(it.setName||'')}}</span>
      </div>`;
    }}).join('');
  }}catch(e){{
    dd.innerHTML = '<div class="search-result-item" style="color:var(--red);">Search failed</div>';
  }}
}}

function selectSearchResult(tcgId){{
  document.getElementById('tcg-input').value = tcgId;
  document.getElementById('search-results').style.display = 'none';
  document.getElementById('name-search').value = '';
  doPreview();
}}

document.addEventListener('click', e=>{{
  if(!document.getElementById('name-search').contains(e.target))
    document.getElementById('search-results').style.display='none';
}});

// ── Preview ──────────────────────────────────────────────────────────────────
async function doPreview(){{
  const tcgId = document.getElementById('tcg-input').value.trim();
  if(!tcgId){{ document.getElementById('tcg-input').focus(); return; }}

  const btn = document.getElementById('preview-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Loading…';
  hideError();
  document.getElementById('result').style.display = 'none';

  try{{
    const [pptResp, previewResp] = await Promise.all([
      fetch('/inventory/api/ppt/sealed/'+encodeURIComponent(tcgId)),
      fetch('/inventory/api/enrich/preview', {{
        method:'POST', headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{product_name:'', set_name:'', tcgplayer_id: tcgId}})
      }})
    ]);

    if(!pptResp.ok){{
      const d = await pptResp.json();
      throw new Error(d.error || 'PPT lookup failed ('+pptResp.status+')');
    }}
    const ppt = await pptResp.json();
    _currentPptItem = ppt;

    // Re-fetch preview with actual product name
    const previewResp2 = await fetch('/inventory/api/enrich/preview', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{product_name: ppt.name||'', set_name: ppt.setName||'', tcgplayer_id: tcgId}})
    }});
    const preview = await previewResp2.json();
    renderResult(ppt, preview);
  }}catch(err){{
    showError(err.message);
  }}finally{{
    btn.disabled=false; btn.innerHTML='Preview';
  }}
}}

function renderResult(ppt, preview){{
  const price = ppt.unopenedPrice || ppt.prices?.market || ppt.marketPrice || 0;
  const imageUrl = ppt.imageCdnUrl800 || ppt.imageCdnUrl400 || ppt.imageCdnUrl || '';
  const era = preview.era || null;
  const weight = preview.weight_oz || '—';
  const tags = preview.tags || [];
  const typeSet = new Set(['booster box','booster pack','etb','pcetb','blister','sleeved','tin','display','collection box','buildbattle','ultra premium collection','sealed','pokemon']);
  const hiTags = tags.filter(t=>typeSet.has(t));
  const otherTags = tags.filter(t=>!typeSet.has(t));
  const channels = ['Online Store','Shop','Point of Sale','Inbox','Facebook & Instagram'];

  const resultEl = document.getElementById('result');
  resultEl.innerHTML = `
    <div class="result-card">
      <div class="result-header">
        <div>
          <h2>${{esc(ppt.name)}}</h2>
          <div class="set">${{esc(ppt.setName||'—')}}</div>
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-size:.7rem;color:var(--dim);text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px;">Market Price</div>
          <div style="font-size:1.4rem;font-weight:700;color:var(--green);">$${{(+price).toFixed(2)}}</div>
        </div>
      </div>

      ${{!era?`<div style="padding:10px 22px;background:var(--amber-bg);border-bottom:1px solid var(--amber);color:var(--amber);font-size:.84rem;">⚠ <strong>Era could not be determined.</strong> Listing will include a NEEDS review note.</div>`:''}}

      <div class="image-section">
        <div class="image-orig">
          <div class="section-title">Source Image (TCGPlayer)</div>
          <div class="image-frame">
            ${{imageUrl?`<img src="${{esc(imageUrl)}}" alt="${{esc(ppt.name)}}">`:`<div class="image-ph">No image available</div>`}}
          </div>
        </div>
        <div class="image-proc">
          <div class="section-title">After Processing (remove.bg + matte)</div>
          <div class="image-frame"><div class="image-ph" style="text-align:center;">🖼<br>2000×2000 transparent PNG<br><span style="font-size:.74rem;">Generated on listing creation</span></div></div>
        </div>
      </div>

      <div class="result-grid">
        <div class="result-section">
          <div class="section-title">Tags</div>
          <div class="tags">
            ${{hiTags.map(t=>`<span class="tag hi">${{esc(t)}}</span>`).join('')}}
            ${{otherTags.map(t=>`<span class="tag">${{esc(t)}}</span>`).join('')}}
          </div>
        </div>
        <div class="result-section">
          <div class="section-title">Weight</div>
          <div style="font-size:1.1rem;font-weight:700;">${{weight}} <span style="font-size:.8rem;color:var(--dim);">oz</span></div>
          <div style="margin-top:6px;font-size:.8rem;color:var(--dim);">${{((+weight)/16).toFixed(2)}} lb</div>
        </div>
        <div class="result-section">
          <div class="section-title">Product Details</div>
          <table class="meta-table">
            <tr><td>Status</td><td style="color:var(--amber);">DRAFT</td></tr>
            <tr><td>Type</td><td>Pokemon</td></tr>
            <tr><td>Vendor</td><td>Pack Fresh</td></tr>
            <tr><td>Template</td><td>product.cro-alt</td></tr>
            <tr><td>Category</td><td>Gaming Cards</td></tr>
          </table>
        </div>
        <div class="result-section">
          <div class="section-title">Metafields</div>
          <table class="meta-table">
            <tr><td>era</td><td>${{era?esc(era):`<span style="color:var(--amber);">⚠ unknown</span>`}}</td></tr>
            <tr><td>TCGPlayer ID</td><td>${{esc(String(ppt.tcgPlayerId||document.getElementById('tcg-input').value))}}</td></tr>
          </table>
        </div>
        <div class="result-section" style="grid-column:1/-1;border-right:none;">
          <div class="section-title">Published Channels</div>
          <div class="tags">
            ${{channels.map(c=>`<span class="tag hi">✓ ${{esc(c)}}</span>`).join('')}}
          </div>
        </div>
      </div>

      <div class="action-bar">
        <button class="btn btn-green" onclick="createListing()">✦ Create Draft Listing</button>
        <button class="btn btn-ghost" onclick="doPreview()">↺ Refresh</button>
        <span class="action-note">Creates as DRAFT · won't appear in store until published</span>
      </div>
    </div>`;
  resultEl.style.display = 'block';
}}

async function createListing(){{
  if(!_currentPptItem) return;
  const tcgId  = document.getElementById('tcg-input').value.trim();
  const price  = _currentPptItem.unopenedPrice || _currentPptItem.prices?.market || _currentPptItem.marketPrice || 0;
  const btn    = event.target;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Creating…';

  try{{
    const resp = await fetch('/inventory/api/enrich/create-listing', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{ tcgplayer_id: parseInt(tcgId), price: parseFloat(price)||0 }})
    }});
    const data = await resp.json();
    if(!resp.ok) throw new Error(data.error||'Create failed');

    const pid  = data.product_id;
    const shopUrl = STORE_HANDLE
      ? `https://admin.shopify.com/store/${{STORE_HANDLE}}/products/${{pid}}`
      : `https://admin.shopify.com/products/${{pid}}`;

    btn.closest('.action-bar').innerHTML = `
      <span style="color:var(--green);font-weight:600;">✓ Draft created!</span>
      <a href="${{shopUrl}}" target="_blank" style="color:var(--accent);font-size:.85rem;">Open in Shopify ↗</a>
      ${{data.errors?.length
        ? `<span style="color:var(--amber);font-size:.8rem;">⚠ ${{data.errors.length}} enrichment step(s) had errors</span>`
        : '<span style="color:var(--dim);font-size:.8rem;">All enrichment steps succeeded</span>'}}
    `;
  }}catch(err){{
    btn.disabled=false; btn.innerHTML='✦ Create Draft Listing';
    showError(err.message);
  }}
}}

// ── Stub ─────────────────────────────────────────────────────────────────────
async function createStub(){{
  const name = document.getElementById('stub-name').value.trim();
  const qty  = parseInt(document.getElementById('stub-qty').value||'0',10)||0;
  if(!name){{ document.getElementById('stub-name').focus(); return; }}

  const btn = document.getElementById('stub-btn');
  btn.disabled=true; btn.innerHTML='<span class="spinner"></span>Creating…';
  const resultEl = document.getElementById('stub-result');
  resultEl.style.display='none';

  try{{
    const resp = await fetch('/inventory/api/stub/create', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{ name, qty }})
    }});
    const data = await resp.json();
    if(!resp.ok) throw new Error(data.error||'Stub create failed');

    const pid = data.product_id;
    const shopUrl = STORE_HANDLE
      ? `https://admin.shopify.com/store/${{STORE_HANDLE}}/products/${{pid}}`
      : `https://admin.shopify.com/products/${{pid}}`;

    resultEl.innerHTML = `<div style="background:var(--green-bg);border:1px solid var(--green);border-radius:8px;padding:12px 16px;color:var(--green);font-size:.88rem;">
      ✓ Stub draft created: <strong>${{esc(name)}}</strong>
      ${{pid?`— <a href="${{shopUrl}}" target="_blank" style="color:var(--accent);">Open in Shopify ↗</a>`:''}}
    </div>`;
    resultEl.style.display='block';
    document.getElementById('stub-name').value='';
    document.getElementById('stub-qty').value='';
  }}catch(err){{
    resultEl.innerHTML = `<div class="alert">${{esc(err.message)}}</div>`;
    resultEl.style.display='block';
  }}finally{{
    btn.disabled=false; btn.innerHTML='Create Stub';
  }}
}}

// ── Utils ────────────────────────────────────────────────────────────────────
function showError(msg){{
  const b=document.getElementById('error-box');
  b.innerHTML=`<div class="alert">${{esc(msg)}}</div>`;
  b.style.display='block';
  b.scrollIntoView({{behavior:'smooth'}});
}}
function hideError(){{ document.getElementById('error-box').style.display='none'; }}
</script>
</body>
</html>"""
