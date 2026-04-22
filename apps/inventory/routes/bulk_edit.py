"""
routes/bulk_edit.py — Mass-edit Shopify product/variant attributes.

Filter with the same controls as inventory (q / tags / status / in-stock),
select rows, apply one or more bulk operations:
  - Add tags / Remove tags
  - Set weight (oz)
  - Set vendor / product type / status
  - Set price / compare-at price

Each change is logged to bulk_edit_log with before/after values so operations
can be read off and reversed manually if needed.
"""

import os
import uuid
import html as _html
import logging
import datetime
from functools import wraps
from urllib.parse import urlencode

from flask import Blueprint, request, jsonify, g

import db
import requests

logger = logging.getLogger(__name__)

bp = Blueprint("bulk_edit", __name__, url_prefix="/inventory/bulk-edit")

DRY_RUN = os.getenv("PF_DRY_RUN", "0") == "1"
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = "2025-10"
MAX_EXEC_ITEMS = 500   # keep synchronous execute under ~60s


# ─── Auth ──────────────────────────────────────────────────────────────────────

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, "user", None):
            return f(*args, **kwargs)
        from flask import Response
        return Response("Unauthorized", 401)
    return decorated


# ─── DB ────────────────────────────────────────────────────────────────────────

def _ensure_table():
    db.execute("""
        CREATE TABLE IF NOT EXISTS bulk_edit_log (
            id              SERIAL PRIMARY KEY,
            batch_id        UUID NOT NULL,
            shopify_product_id BIGINT,
            shopify_variant_id BIGINT,
            title           TEXT,
            field           TEXT NOT NULL,
            old_value       TEXT,
            new_value       TEXT,
            status          TEXT NOT NULL,
            error           TEXT,
            user_email      TEXT,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS bulk_edit_log_batch_idx ON bulk_edit_log(batch_id)")
    db.execute("CREATE INDEX IF NOT EXISTS bulk_edit_log_created_idx ON bulk_edit_log(created_at DESC)")


# ─── Filter + load ─────────────────────────────────────────────────────────────

# Mirror the curated list on the inventory page.
CURATED_TAGS = [
    "sealed", "slab", "collection box", "tin", "etb", "pcetb",
    "booster box", "booster pack", "blister", "sleeved", "international", "mtg",
]


def _load_filtered(*, q=None, tag_any=None, status="all", in_stock=False):
    """Server-side filtering. Returns list of dicts, minimal columns."""
    where = []
    params = []

    if status == "published":
        where.append("LOWER(COALESCE(status,'')) != 'draft'")
    elif status == "draft":
        where.append("LOWER(COALESCE(status,'')) = 'draft'")

    if q:
        where.append("LOWER(COALESCE(title,'')) LIKE %s")
        params.append(f"%{q.lower()}%")

    if in_stock:
        where.append("COALESCE(shopify_qty, 0) > 0")

    # Tag filtering: require ALL selected tags (same semantics as inventory).
    # Tags are stored as comma-separated — we use LIKE with bracketed commas.
    if tag_any:
        for t in tag_any:
            where.append(
                "(',' || LOWER(REPLACE(COALESCE(tags,''), ', ', ',')) || ',') LIKE %s"
            )
            params.append(f"%,{t.lower()},%")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT shopify_product_id, shopify_variant_id, title,
               tags, status, shopify_qty, shopify_price
        FROM inventory_product_cache
        {where_sql}
        ORDER BY title
    """
    rows = db.query(sql, tuple(params) if params else None)
    return [dict(r) for r in rows]


# ─── Shopify helpers ───────────────────────────────────────────────────────────

def _shopify_headers():
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    }


def _gql(query: str, variables: dict = None) -> dict:
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(url, headers=_shopify_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {body['errors']}")
    data = body.get("data", {})
    # Surface userErrors from first mutation (if any)
    for v in data.values():
        if isinstance(v, dict):
            ue = v.get("userErrors") or []
            if ue:
                raise RuntimeError(f"userErrors: {ue}")
    return data


def _rest(method: str, path: str, **kwargs) -> dict:
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}{path}"
    resp = requests.request(method, url, headers=_shopify_headers(), timeout=30, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _product_gid(pid) -> str:
    return f"gid://shopify/Product/{pid}"


def _variant_gid(vid) -> str:
    return f"gid://shopify/ProductVariant/{vid}"


def _apply_tags_add(product_id, current_tags: str, new_tags: list[str]) -> tuple[str, str]:
    existing = {t.strip() for t in (current_tags or "").split(",") if t.strip()}
    merged = sorted(existing | {t.strip() for t in new_tags if t.strip()}, key=str.lower)
    new_value = ", ".join(merged)
    if DRY_RUN:
        return current_tags or "", new_value
    _gql("""
        mutation tagsAdd($id: ID!, $tags: [String!]!) {
          tagsAdd(id: $id, tags: $tags) { userErrors { field message } }
        }
    """, {"id": _product_gid(product_id), "tags": new_tags})
    db.execute(
        "UPDATE inventory_product_cache SET tags=%s WHERE shopify_product_id=%s",
        (new_value, product_id),
    )
    return current_tags or "", new_value


def _apply_tags_remove(product_id, current_tags: str, tags_to_remove: list[str]) -> tuple[str, str]:
    remove_lower = {t.strip().lower() for t in tags_to_remove if t.strip()}
    existing = [t.strip() for t in (current_tags or "").split(",") if t.strip()]
    remaining = [t for t in existing if t.lower() not in remove_lower]
    new_value = ", ".join(remaining)
    if DRY_RUN:
        return current_tags or "", new_value
    _gql("""
        mutation tagsRemove($id: ID!, $tags: [String!]!) {
          tagsRemove(id: $id, tags: $tags) { userErrors { field message } }
        }
    """, {"id": _product_gid(product_id), "tags": tags_to_remove})
    db.execute(
        "UPDATE inventory_product_cache SET tags=%s WHERE shopify_product_id=%s",
        (new_value, product_id),
    )
    return current_tags or "", new_value


def _apply_product_field(product_id, field: str, old_value, new_value) -> tuple[str, str]:
    """Set vendor / productType / status / descriptionHtml via productUpdate."""
    if DRY_RUN:
        return str(old_value or ""), str(new_value or "")
    gql_field = {
        "vendor": "vendor",
        "product_type": "productType",
        "status": "status",
    }.get(field)
    if not gql_field:
        raise ValueError(f"Unknown product field: {field}")

    variables = {"input": {"id": _product_gid(product_id), gql_field: new_value}}
    # Status must be uppercased enum
    if gql_field == "status":
        variables["input"]["status"] = str(new_value).upper()

    _gql("""
        mutation productUpdate($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id }
            userErrors { field message }
          }
        }
    """, variables)

    if field == "status":
        db.execute(
            "UPDATE inventory_product_cache SET status=%s WHERE shopify_product_id=%s",
            (str(new_value).upper(), product_id),
        )
    return str(old_value or ""), str(new_value or "")


def _apply_variant_price(product_id, variant_id, field: str, old_value, new_value) -> tuple[str, str]:
    """Set price or compareAtPrice via productVariantsBulkUpdate."""
    if DRY_RUN:
        return str(old_value or ""), str(new_value or "")
    variant_input = {"id": _variant_gid(variant_id)}
    if field == "price":
        variant_input["price"] = f"{float(new_value):.2f}"
    elif field == "compare_at_price":
        variant_input["compareAtPrice"] = f"{float(new_value):.2f}" if new_value else None
    else:
        raise ValueError(f"Unknown variant price field: {field}")

    _gql("""
        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id }
            userErrors { field message }
          }
        }
    """, {"productId": _product_gid(product_id), "variants": [variant_input]})

    if field == "price":
        db.execute(
            "UPDATE inventory_product_cache SET shopify_price=%s WHERE shopify_variant_id=%s",
            (float(new_value), variant_id),
        )
    return str(old_value or ""), str(new_value or "")


def _apply_variant_weight(variant_id, old_value, new_weight_oz: float) -> tuple[str, str]:
    """Set variant weight via REST (matches product_enrichment pattern)."""
    if DRY_RUN:
        return str(old_value or ""), f"{new_weight_oz:.2f} oz"
    _rest("PUT", f"/variants/{variant_id}.json", json={
        "variant": {"id": int(variant_id), "weight": float(new_weight_oz), "weight_unit": "oz"}
    })
    return str(old_value or ""), f"{new_weight_oz:.2f} oz"


# ─── Core operation dispatcher ─────────────────────────────────────────────────

def _dispatch(op: dict, row: dict) -> tuple[str, str, str]:
    """
    Apply one operation to one row.
    Returns (field_label, old_value_str, new_value_str).
    Raises on failure.
    """
    kind = op["kind"]
    pid = row["shopify_product_id"]
    vid = row["shopify_variant_id"]

    if kind == "tags_add":
        tags = [t.strip() for t in op.get("tags", []) if t.strip()]
        old, new = _apply_tags_add(pid, row.get("tags") or "", tags)
        return "tags", old, new

    if kind == "tags_remove":
        tags = [t.strip() for t in op.get("tags", []) if t.strip()]
        old, new = _apply_tags_remove(pid, row.get("tags") or "", tags)
        return "tags", old, new

    if kind == "weight_oz":
        old, new = _apply_variant_weight(vid, row.get("weight_oz"), float(op["value"]))
        return "weight_oz", old, new

    if kind in ("vendor", "product_type", "status"):
        old = row.get(kind) if kind != "status" else (row.get("status") or "").lower()
        old, new = _apply_product_field(pid, kind, old, op["value"])
        return kind, old, new

    if kind == "price":
        old, new = _apply_variant_price(pid, vid, "price", row.get("shopify_price"), op["value"])
        return "price", old, new

    if kind == "compare_at_price":
        old, new = _apply_variant_price(pid, vid, "compare_at_price", None, op["value"])
        return "compare_at_price", old, new

    raise ValueError(f"Unknown op kind: {kind}")


def _preview_row(op: dict, row: dict) -> dict:
    """Compute what would change for a single row without writing."""
    kind = op["kind"]
    current = row.get("tags") or ""

    if kind == "tags_add":
        add = [t.strip() for t in op.get("tags", []) if t.strip()]
        existing = {t.strip() for t in current.split(",") if t.strip()}
        merged = sorted(existing | set(add), key=str.lower)
        return {"field": "tags", "old": current, "new": ", ".join(merged),
                "noop": set(merged) == existing}

    if kind == "tags_remove":
        remove_lower = {t.strip().lower() for t in op.get("tags", []) if t.strip()}
        existing = [t.strip() for t in current.split(",") if t.strip()]
        remaining = [t for t in existing if t.lower() not in remove_lower]
        return {"field": "tags", "old": current, "new": ", ".join(remaining),
                "noop": len(remaining) == len(existing)}

    if kind == "weight_oz":
        return {"field": "weight_oz", "old": "—", "new": f"{float(op['value']):.2f} oz", "noop": False}

    if kind in ("vendor", "product_type"):
        return {"field": kind, "old": "—", "new": str(op["value"]), "noop": False}

    if kind == "status":
        old = (row.get("status") or "").lower()
        new = str(op["value"]).lower()
        return {"field": "status", "old": old, "new": new, "noop": old == new}

    if kind == "price":
        old = row.get("shopify_price")
        return {"field": "price", "old": f"{float(old):.2f}" if old is not None else "—",
                "new": f"{float(op['value']):.2f}",
                "noop": old is not None and float(old) == float(op["value"])}

    if kind == "compare_at_price":
        return {"field": "compare_at_price", "old": "—",
                "new": f"{float(op['value']):.2f}", "noop": False}

    return {"field": kind, "old": "?", "new": "?", "noop": True}


# ─── Routes: JSON API ──────────────────────────────────────────────────────────

@bp.route("/api/filter")
@requires_auth
def api_filter():
    _ensure_table()
    q = (request.args.get("q") or "").strip()
    tag_any = [t for t in request.args.getlist("tag") if t]
    status = request.args.get("status", "all")
    in_stock = request.args.get("in_stock") == "1"
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    try:
        per_page = max(1, min(500, int(request.args.get("per_page", 100))))
    except ValueError:
        per_page = 100

    rows = _load_filtered(q=q, tag_any=tag_any, status=status, in_stock=in_stock)
    total = len(rows)
    start = (page - 1) * per_page
    page_rows = rows[start:start + per_page]

    # Also return all matching product_ids / variant_ids so "select all matching"
    # works without the client needing to paginate through everything.
    all_ids = [
        {"product_id": r["shopify_product_id"], "variant_id": r["shopify_variant_id"]}
        for r in rows
    ]

    return jsonify({
        "total": total,
        "page": page,
        "per_page": per_page,
        "rows": [{
            "product_id": r["shopify_product_id"],
            "variant_id": r["shopify_variant_id"],
            "title": r["title"],
            "tags": r.get("tags") or "",
            "status": (r.get("status") or "").lower(),
            "price": float(r["shopify_price"]) if r.get("shopify_price") is not None else None,
            "qty": r.get("shopify_qty") or 0,
        } for r in page_rows],
        "all_ids": all_ids,
    })


@bp.route("/api/preview", methods=["POST"])
@requires_auth
def api_preview():
    data = request.get_json() or {}
    variant_ids = [int(v) for v in data.get("variant_ids", []) if v]
    ops = data.get("ops", [])
    if not variant_ids:
        return jsonify({"error": "variant_ids required"}), 400
    if not ops:
        return jsonify({"error": "at least one op required"}), 400

    # Load selected rows
    placeholders = ",".join(["%s"] * len(variant_ids))
    rows = db.query(
        f"SELECT shopify_product_id, shopify_variant_id, title, tags, status, shopify_qty, shopify_price "
        f"FROM inventory_product_cache WHERE shopify_variant_id IN ({placeholders})",
        tuple(variant_ids),
    )
    rows = [dict(r) for r in rows]

    diffs = []
    for r in rows:
        per_row = []
        for op in ops:
            try:
                per_row.append(_preview_row(op, r))
            except Exception as e:
                per_row.append({"field": op.get("kind"), "old": "?", "new": "?",
                                "noop": True, "error": str(e)})
        diffs.append({
            "variant_id": r["shopify_variant_id"],
            "title": r["title"],
            "changes": per_row,
        })

    return jsonify({"count": len(rows), "diffs": diffs})


@bp.route("/api/execute", methods=["POST"])
@requires_auth
def api_execute():
    _ensure_table()
    data = request.get_json() or {}
    variant_ids = [int(v) for v in data.get("variant_ids", []) if v]
    ops = data.get("ops", [])

    if not variant_ids:
        return jsonify({"error": "variant_ids required"}), 400
    if len(variant_ids) > MAX_EXEC_ITEMS:
        return jsonify({"error": f"Max {MAX_EXEC_ITEMS} items per batch"}), 400
    if not ops:
        return jsonify({"error": "at least one op required"}), 400

    placeholders = ",".join(["%s"] * len(variant_ids))
    rows = db.query(
        f"SELECT shopify_product_id, shopify_variant_id, title, tags, status, shopify_qty, shopify_price "
        f"FROM inventory_product_cache WHERE shopify_variant_id IN ({placeholders})",
        tuple(variant_ids),
    )
    rows = [dict(r) for r in rows]

    batch_id = str(uuid.uuid4())
    user = getattr(g, "user", {}) or {}
    email = user.get("email", "")

    ok = 0
    failed = 0
    errors = []

    for r in rows:
        for op in ops:
            try:
                field, old_v, new_v = _dispatch(op, r)
                db.execute("""
                    INSERT INTO bulk_edit_log
                    (batch_id, shopify_product_id, shopify_variant_id, title,
                     field, old_value, new_value, status, user_email)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'ok',%s)
                """, (batch_id, r["shopify_product_id"], r["shopify_variant_id"],
                      r["title"], field, old_v, new_v, email))
                ok += 1
            except Exception as e:
                err = str(e)[:500]
                db.execute("""
                    INSERT INTO bulk_edit_log
                    (batch_id, shopify_product_id, shopify_variant_id, title,
                     field, old_value, new_value, status, error, user_email)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'failed',%s,%s)
                """, (batch_id, r["shopify_product_id"], r["shopify_variant_id"],
                      r["title"], op.get("kind"), None, None, err, email))
                failed += 1
                errors.append({"variant_id": r["shopify_variant_id"],
                               "title": r["title"], "error": err})
                logger.exception(f"Bulk edit failed variant={r['shopify_variant_id']} op={op}")

    return jsonify({
        "batch_id": batch_id,
        "ok": ok,
        "failed": failed,
        "errors": errors[:20],
        "dry_run": DRY_RUN,
    })


@bp.route("/api/log")
@requires_auth
def api_log():
    _ensure_table()
    batch = request.args.get("batch_id")
    if batch:
        rows = db.query("""
            SELECT batch_id, shopify_product_id, shopify_variant_id, title,
                   field, old_value, new_value, status, error, user_email, created_at
            FROM bulk_edit_log
            WHERE batch_id = %s
            ORDER BY id
        """, (batch,))
    else:
        rows = db.query("""
            SELECT batch_id,
                   COUNT(*) AS rows,
                   COUNT(*) FILTER (WHERE status='ok') AS ok,
                   COUNT(*) FILTER (WHERE status='failed') AS failed,
                   MIN(user_email) AS user_email,
                   MAX(created_at) AS created_at,
                   STRING_AGG(DISTINCT field, ', ') AS fields
            FROM bulk_edit_log
            GROUP BY batch_id
            ORDER BY created_at DESC
            LIMIT 50
        """)
    return jsonify([dict(r) for r in rows])


# ─── Routes: page ──────────────────────────────────────────────────────────────

@bp.route("/", methods=["GET"])
@requires_auth
def page():
    _ensure_table()
    return _render_page()


def _render_page() -> str:
    tags_options_html = "".join(
        f'<label class="chip"><input type="checkbox" class="tag-chip" value="{_html.escape(t)}">'
        f'<span>{_html.escape(t)}</span></label>'
        for t in CURATED_TAGS
    )
    max_items = MAX_EXEC_ITEMS

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Bulk Edit · Inventory</title>
<style>
:root {{
  --bg:#0c1015; --surface:#151b24; --s2:#1b2230; --border:#2a3346;
  --text:#e9eef7; --dim:#8b94a8; --green:#2dd4a0; --red:#f05252;
  --amber:#f5a623; --accent:#dfa260; --accent2:#6ba6d9;
}}
* {{ box-sizing: border-box; }}
body {{ background: var(--bg); color: var(--text); font-family: -apple-system, system-ui, sans-serif;
       font-size: 13.5px; margin: 0; padding: 18px; }}
h1 {{ font-size: 18px; margin: 0 0 4px; }}
.sub {{ color: var(--dim); font-size: 12px; margin-bottom: 16px; }}
.row {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 10px; }}
input, select, textarea {{ background: var(--surface); color: var(--text);
       border: 1px solid var(--border); border-radius: 6px; padding: 7px 10px;
       font-family: inherit; font-size: 13px; }}
input:focus, select:focus {{ outline: none; border-color: var(--accent); }}
.btn {{ background: var(--surface); color: var(--text); border: 1px solid var(--border);
        border-radius: 6px; padding: 7px 12px; cursor: pointer; font-size: 13px;
        font-family: inherit; }}
.btn:hover {{ background: var(--s2); }}
.btn-primary {{ background: var(--accent); color: #000; border-color: var(--accent); font-weight: 600; }}
.btn-primary:hover {{ filter: brightness(1.08); }}
.btn-danger {{ background: var(--red); color: #fff; border-color: var(--red); }}
.btn-sm {{ padding: 4px 9px; font-size: 12px; }}
.chip {{ display: inline-flex; align-items: center; gap: 5px; padding: 4px 10px;
         border: 1px solid var(--border); border-radius: 999px; cursor: pointer;
         background: var(--surface); font-size: 12px; user-select: none; }}
.chip input {{ margin: 0; accent-color: var(--accent); }}
.chip:has(input:checked) {{ background: var(--s2); border-color: var(--accent); color: var(--accent); }}
.card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
         padding: 12px; margin-bottom: 12px; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 6px 8px; border-bottom: 1px solid var(--border); text-align: left;
         vertical-align: top; font-size: 12.5px; }}
th {{ font-size: 11px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.06em; }}
tbody tr:hover {{ background: var(--s2); }}
.title-cell {{ max-width: 480px; }}
.tag-list {{ color: var(--dim); font-size: 11px; }}
.status-badge {{ display: inline-block; padding: 1px 7px; border-radius: 3px;
         font-size: 10.5px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; }}
.s-active {{ background: rgba(45,212,160,0.12); color: var(--green); }}
.s-draft {{ background: rgba(245,166,35,0.12); color: var(--amber); }}
.s-archived {{ background: rgba(139,148,168,0.12); color: var(--dim); }}
.pill {{ display: inline-block; padding: 2px 8px; background: var(--s2); border-radius: 10px;
         font-size: 11px; color: var(--dim); }}
.actions-bar {{ position: sticky; bottom: 0; background: var(--surface); border-top: 2px solid var(--accent);
        padding: 12px; border-radius: 8px 8px 0 0; margin-top: 16px; }}
.op-row {{ display: flex; gap: 8px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }}
.op-row select {{ min-width: 170px; }}
.op-row input[type=text], .op-row input[type=number] {{ min-width: 240px; }}
.op-remove {{ color: var(--red); background: none; border: none; cursor: pointer; font-size: 16px; padding: 0 6px; }}
.diff-table {{ font-size: 12px; }}
.diff-old {{ color: var(--dim); text-decoration: line-through; }}
.diff-new {{ color: var(--green); font-weight: 600; }}
.diff-noop {{ color: var(--dim); font-style: italic; }}
.modal-overlay {{ position: fixed; inset: 0; background: rgba(0,0,0,0.7); display: flex;
         align-items: flex-start; justify-content: center; padding: 40px 20px;
         z-index: 1000; overflow: auto; }}
.modal {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
         padding: 20px; max-width: 1100px; width: 100%; }}
.modal h2 {{ margin: 0 0 12px; font-size: 16px; }}
.modal-actions {{ display: flex; gap: 8px; justify-content: flex-end; margin-top: 14px; }}
.toast {{ position: fixed; top: 20px; right: 20px; padding: 10px 16px;
         border-radius: 6px; z-index: 2000; font-weight: 600; font-size: 13px; }}
.toast.green {{ background: var(--green); color: #000; }}
.toast.red {{ background: var(--red); color: #fff; }}
.toast.amber {{ background: var(--amber); color: #000; }}
.spinner {{ display: inline-block; width: 14px; height: 14px; border: 2px solid var(--border);
         border-top-color: var(--accent); border-radius: 50%; animation: spin 0.8s linear infinite;
         vertical-align: middle; }}
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
.nav {{ display: flex; gap: 14px; margin-bottom: 14px; font-size: 12px; }}
.nav a {{ color: var(--dim); text-decoration: none; }}
.nav a:hover {{ color: var(--accent); }}
</style></head>
<body>
<div class="nav">
  <a href="/inventory">← Inventory</a>
  <a href="/inventory/bulk-edit">Bulk Edit</a>
</div>

<h1>Bulk Edit</h1>
<div class="sub">Filter, select, and apply operations across many products at once.</div>

<div class="card">
  <div class="row">
    <input id="q" type="text" placeholder="Search by title…" style="min-width:320px; flex:1;">
    <select id="status">
      <option value="all">All statuses</option>
      <option value="published">Published</option>
      <option value="draft">Drafts</option>
    </select>
    <label class="chip"><input id="in_stock" type="checkbox"><span>In stock only</span></label>
    <button class="btn btn-primary" onclick="loadRows()">Apply Filter</button>
  </div>
  <div class="row" style="margin-top:6px">{tags_options_html}</div>
</div>

<div class="card">
  <div class="row" style="justify-content:space-between">
    <div>
      <span id="summary" class="pill">0 items</span>
      <span id="selected-pill" class="pill" style="margin-left:6px;">0 selected</span>
    </div>
    <div>
      <button class="btn btn-sm" onclick="selectAllMatching()">Select all matching</button>
      <button class="btn btn-sm" onclick="clearSelection()">Clear selection</button>
    </div>
  </div>
  <div style="overflow-x:auto; margin-top:10px;">
    <table>
      <thead>
        <tr>
          <th style="width:30px;"><input type="checkbox" id="select-page" onclick="toggleSelectPage(this)"></th>
          <th>Title</th>
          <th>Status</th>
          <th>Price</th>
          <th>Qty</th>
          <th>Tags</th>
        </tr>
      </thead>
      <tbody id="rows-body"></tbody>
    </table>
  </div>
  <div class="row" style="justify-content:flex-end; margin-top:10px;">
    <button id="prev-btn" class="btn btn-sm" onclick="changePage(-1)">← Prev</button>
    <span id="page-info" class="pill">—</span>
    <button id="next-btn" class="btn btn-sm" onclick="changePage(1)">Next →</button>
  </div>
</div>

<div class="actions-bar">
  <div style="font-weight:600; margin-bottom:8px;">Operations to apply</div>
  <div id="ops-list"></div>
  <div class="row">
    <button class="btn btn-sm" onclick="addOp()">+ Add operation</button>
    <div style="flex:1"></div>
    <button class="btn" onclick="showPreview()">Preview</button>
    <button class="btn btn-primary" onclick="executeOps()">Execute on selected</button>
  </div>
  <div id="op-err" style="color: var(--red); font-size: 12px; margin-top: 6px;"></div>
</div>

<div id="modal-mount"></div>

<script>
const state = {{
  page: 1,
  per_page: 100,
  rows: [],
  total: 0,
  allIds: [],
  selected: new Set(),  // variant_ids
  ops: [],              // [{{kind, ...}}]
}};

const OP_KINDS = [
  {{ v:"tags_add", label:"Add tags" }},
  {{ v:"tags_remove", label:"Remove tags" }},
  {{ v:"weight_oz", label:"Set weight (oz)" }},
  {{ v:"vendor", label:"Set vendor" }},
  {{ v:"product_type", label:"Set product type" }},
  {{ v:"status", label:"Set status" }},
  {{ v:"price", label:"Set price" }},
  {{ v:"compare_at_price", label:"Set compare-at price" }},
];

function esc(s) {{
  return String(s == null ? "" : s).replace(/[&<>\"']/g,
    c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}}[c]));
}}

function getFilter() {{
  const q = document.getElementById('q').value.trim();
  const status = document.getElementById('status').value;
  const in_stock = document.getElementById('in_stock').checked ? '1' : '0';
  const tags = Array.from(document.querySelectorAll('.tag-chip:checked')).map(i => i.value);
  const p = new URLSearchParams();
  if (q) p.set('q', q);
  if (status !== 'all') p.set('status', status);
  if (in_stock === '1') p.set('in_stock', '1');
  tags.forEach(t => p.append('tag', t));
  p.set('page', state.page);
  p.set('per_page', state.per_page);
  return p;
}}

async function loadRows() {{
  const p = getFilter();
  const r = await fetch('/inventory/bulk-edit/api/filter?' + p.toString());
  const d = await r.json();
  state.rows = d.rows || [];
  state.total = d.total || 0;
  state.allIds = d.all_ids || [];
  renderRows();
}}

function renderRows() {{
  document.getElementById('summary').textContent = state.total + ' items';
  const body = document.getElementById('rows-body');
  if (!state.rows.length) {{
    body.innerHTML = '<tr><td colspan="6" style="color:var(--dim); text-align:center; padding:24px;">No items match.</td></tr>';
  }} else {{
    body.innerHTML = state.rows.map(r => {{
      const checked = state.selected.has(r.variant_id) ? 'checked' : '';
      const statusCls = {{ 'active':'s-active','draft':'s-draft','archived':'s-archived' }}[r.status] || '';
      const price = r.price != null ? '$' + r.price.toFixed(2) : '—';
      return `<tr>
        <td><input type="checkbox" data-vid="${{r.variant_id}}" ${{checked}} onclick="toggleRow(${{r.variant_id}}, this.checked)"></td>
        <td class="title-cell">${{esc(r.title)}}</td>
        <td><span class="status-badge ${{statusCls}}">${{esc(r.status || '')}}</span></td>
        <td>${{price}}</td>
        <td>${{r.qty}}</td>
        <td class="tag-list">${{esc(r.tags)}}</td>
      </tr>`;
    }}).join('');
  }}
  const pageCount = Math.max(1, Math.ceil(state.total / state.per_page));
  document.getElementById('page-info').textContent = `Page ${{state.page}} / ${{pageCount}}`;
  document.getElementById('prev-btn').disabled = state.page <= 1;
  document.getElementById('next-btn').disabled = state.page >= pageCount;
  updateSelectedPill();
}}

function toggleRow(vid, checked) {{
  if (checked) state.selected.add(vid); else state.selected.delete(vid);
  updateSelectedPill();
}}

function toggleSelectPage(cb) {{
  for (const r of state.rows) {{
    if (cb.checked) state.selected.add(r.variant_id);
    else state.selected.delete(r.variant_id);
  }}
  renderRows();
}}

function selectAllMatching() {{
  for (const i of state.allIds) state.selected.add(i.variant_id);
  renderRows();
}}

function clearSelection() {{
  state.selected.clear();
  renderRows();
}}

function updateSelectedPill() {{
  document.getElementById('selected-pill').textContent = state.selected.size + ' selected';
}}

function changePage(delta) {{
  state.page = Math.max(1, state.page + delta);
  loadRows();
}}

// ─── Operations UI ───────────────────────────────────────────────────────────

function addOp() {{
  state.ops.push({{ kind: 'tags_add', tags: [], value: '' }});
  renderOps();
}}

function removeOp(idx) {{
  state.ops.splice(idx, 1);
  renderOps();
}}

function updateOp(idx, field, value) {{
  state.ops[idx][field] = value;
  if (field === 'kind') renderOps();
}}

function renderOps() {{
  const list = document.getElementById('ops-list');
  if (!state.ops.length) {{
    list.innerHTML = '<div style="color:var(--dim); font-size:12px; padding:4px 0;">No operations added yet. Click "+ Add operation".</div>';
    return;
  }}
  list.innerHTML = state.ops.map((op, i) => {{
    const kindSel = `<select onchange="updateOp(${{i}}, 'kind', this.value)">` +
      OP_KINDS.map(k => `<option value="${{k.v}}" ${{k.v===op.kind?'selected':''}}>${{k.label}}</option>`).join('') + '</select>';
    let valueInput = '';
    if (op.kind === 'tags_add' || op.kind === 'tags_remove') {{
      valueInput = `<input type="text" placeholder="tag1, tag2, tag3" value="${{esc((op.tags||[]).join(', '))}}"
        onchange="state.ops[${{i}}].tags = this.value.split(',').map(s=>s.trim()).filter(Boolean)">`;
    }} else if (op.kind === 'weight_oz' || op.kind === 'price' || op.kind === 'compare_at_price') {{
      valueInput = `<input type="number" step="0.01" placeholder="value" value="${{esc(op.value)}}"
        onchange="state.ops[${{i}}].value = this.value">`;
    }} else if (op.kind === 'status') {{
      valueInput = `<select onchange="state.ops[${{i}}].value = this.value">
        <option value="">— select —</option>
        <option value="active" ${{op.value==='active'?'selected':''}}>active</option>
        <option value="draft" ${{op.value==='draft'?'selected':''}}>draft</option>
        <option value="archived" ${{op.value==='archived'?'selected':''}}>archived</option>
      </select>`;
    }} else {{
      valueInput = `<input type="text" placeholder="value" value="${{esc(op.value)}}"
        onchange="state.ops[${{i}}].value = this.value">`;
    }}
    return `<div class="op-row">${{kindSel}} ${{valueInput}}
      <button class="op-remove" onclick="removeOp(${{i}})" title="Remove">✕</button></div>`;
  }}).join('');
}}

// ─── Preview / Execute ───────────────────────────────────────────────────────

function validateBeforeRun() {{
  const err = document.getElementById('op-err');
  err.textContent = '';
  if (!state.selected.size) {{ err.textContent = 'Select at least one item.'; return false; }}
  if (!state.ops.length) {{ err.textContent = 'Add at least one operation.'; return false; }}
  for (const op of state.ops) {{
    if (op.kind === 'tags_add' || op.kind === 'tags_remove') {{
      if (!op.tags || !op.tags.length) {{ err.textContent = 'Tag operation needs at least one tag.'; return false; }}
    }} else if (!op.value && op.value !== 0) {{
      err.textContent = 'All operations need a value.'; return false;
    }}
  }}
  if (state.selected.size > {max_items}) {{
    err.textContent = 'Max {max_items} items per batch. Narrow the filter or split into runs.'; return false;
  }}
  return true;
}}

async function showPreview() {{
  if (!validateBeforeRun()) return;
  const body = {{ variant_ids: Array.from(state.selected), ops: state.ops }};
  const r = await fetch('/inventory/bulk-edit/api/preview',
    {{ method: 'POST', headers: {{'Content-Type':'application/json'}}, body: JSON.stringify(body) }});
  const d = await r.json();
  if (!r.ok) {{ toast(d.error || 'Preview failed', 'red'); return; }}
  renderPreviewModal(d);
}}

function renderPreviewModal(d) {{
  const rows = (d.diffs || []).map(row => {{
    const changes = row.changes.map(c => {{
      if (c.noop) return `<div class="diff-noop">${{esc(c.field)}}: no change</div>`;
      return `<div><span style="color:var(--dim);">${{esc(c.field)}}:</span>
        <span class="diff-old">${{esc(c.old || '(empty)')}}</span>
        → <span class="diff-new">${{esc(c.new || '(empty)')}}</span></div>`;
    }}).join('');
    return `<tr><td>${{esc(row.title)}}</td><td>${{changes}}</td></tr>`;
  }}).join('');

  const html = `<div class="modal-overlay" onclick="if(event.target===this)closeModal()">
    <div class="modal">
      <h2>Preview — ${{d.count}} item(s)</h2>
      <div style="max-height: 60vh; overflow:auto;">
        <table class="diff-table"><thead><tr><th>Item</th><th>Changes</th></tr></thead>
        <tbody>${{rows || '<tr><td colspan=2 style="color:var(--dim);padding:20px;">no changes</td></tr>'}}</tbody></table>
      </div>
      <div class="modal-actions">
        <button class="btn" onclick="closeModal()">Close</button>
        <button class="btn btn-primary" onclick="closeModal(); executeOps()">Looks good — Execute</button>
      </div>
    </div></div>`;
  document.getElementById('modal-mount').innerHTML = html;
}}

function closeModal() {{ document.getElementById('modal-mount').innerHTML = ''; }}

async function executeOps() {{
  if (!validateBeforeRun()) return;
  const n = state.selected.size;
  const opLabels = state.ops.map(o => OP_KINDS.find(k=>k.v===o.kind).label).join(', ');
  if (!confirm(`Apply ${{state.ops.length}} operation(s) [${{opLabels}}] to ${{n}} item(s)?`)) return;

  toast('<span class="spinner"></span> Running…', 'amber', true);
  const body = {{ variant_ids: Array.from(state.selected), ops: state.ops }};
  const r = await fetch('/inventory/bulk-edit/api/execute',
    {{ method: 'POST', headers: {{'Content-Type':'application/json'}}, body: JSON.stringify(body) }});
  const d = await r.json();
  hideToast();
  if (!r.ok) {{ toast(d.error || 'Execute failed', 'red'); return; }}
  const msg = `Done — ${{d.ok}} ok, ${{d.failed}} failed ${{d.dry_run?'[DRY RUN]':''}}`;
  toast(msg, d.failed ? 'amber' : 'green');
  if (d.failed && d.errors && d.errors.length) {{
    console.warn('Bulk edit errors:', d.errors);
  }}
  // Refresh rows so post-edit values are visible
  loadRows();
}}

function toast(html, cls, sticky) {{
  hideToast();
  const t = document.createElement('div');
  t.className = 'toast ' + cls;
  t.innerHTML = html;
  t.id = 'toast-el';
  document.body.appendChild(t);
  if (!sticky) setTimeout(hideToast, 4000);
}}
function hideToast() {{
  const t = document.getElementById('toast-el');
  if (t) t.remove();
}}

// initial
loadRows();
addOp();
</script>
</body></html>"""
