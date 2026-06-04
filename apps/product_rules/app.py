"""
product_rules — rules.pack-fresh.com

Tag-driven product rule engine. Operators tag products in Shopify Admin
(limit-N, limit-N-per-week, preorder-YYYY-MM-DD, ...) and this service
materializes the rules into Shopify metafields that the storefront theme
extension and checkout-validation Function consume.

Phase 1 scope:
  - Webhooks (products/create, products/update, orders/create)
  - Per-tag pre-order messaging overrides
  - Dashboard viewer for rules currently in use
  - /release endpoint to clear expired pre-order tags (used by Phase 4 cron
    and the dashboard's manual release button)
"""

import os
import json
import logging
from datetime import date
from flask import Flask, request, jsonify, render_template_string

import db
from auth import register_auth_hooks, get_current_user

import service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

# Auth: standard JWT for everything except webhooks (HMAC-verified) and
# /release (HMAC-of-Flow-secret OR owner). Both are handled inside their
# routes — register_auth_hooks just needs to skip them on the JWT side.
register_auth_hooks(
    app,
    roles=["owner", "manager"],
    skip_jwt_prefixes=("/webhooks/", "/release"),
)


# ═══════════════════════════════════════════════════════════════════════════════
# Health
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "product_rules"})


# ═══════════════════════════════════════════════════════════════════════════════
# Webhooks (Shopify HMAC-verified)
# ═══════════════════════════════════════════════════════════════════════════════

def _coerce_tags(raw):
    """Shopify product webhooks deliver tags as either a CSV string (REST-
    style payload) or a list (newer GraphQL-style). Handle both."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    return []


def _verified_webhook_payload():
    """
    Read raw body, verify Shopify's HMAC, return the parsed JSON or None.
    Returning None signals the caller should respond 401 — never echo a body
    that didn't pass HMAC, even an empty one.
    """
    raw = request.get_data()
    signature = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not service.verify_shopify_hmac(raw, signature):
        return None
    try:
        return json.loads(raw.decode("utf-8") or "{}")
    except (UnicodeDecodeError, ValueError):
        return None


@app.route("/webhooks/products/create", methods=["POST"])
@app.route("/webhooks/products/update", methods=["POST"])
def webhook_product_changed():
    payload = _verified_webhook_payload()
    if payload is None:
        return jsonify({"error": "unauthorized"}), 401

    product_id = payload.get("id") or payload.get("admin_graphql_api_id")
    if not product_id:
        return jsonify({"error": "no product id"}), 200  # 200 so Shopify doesn't retry forever
    if isinstance(product_id, int) or (isinstance(product_id, str) and product_id.isdigit()):
        product_gid = f"gid://shopify/Product/{product_id}"
    else:
        product_gid = product_id  # already a GID

    tags = _coerce_tags(payload.get("tags"))
    try:
        result = service.sync_product_metafields(product_gid, tags, db)
        logger.info("synced product %s rules=%s", product_gid, {
            "qty_limit": bool(result["qty_limit"]),
            "preorder":  bool(result["preorder"]),
        })
    except Exception as e:
        logger.exception("sync failed for %s: %s", product_gid, e)
        return jsonify({"error": "sync failed"}), 500
    return jsonify({"ok": True})


@app.route("/webhooks/orders/create", methods=["POST"])
def webhook_order_create():
    payload = _verified_webhook_payload()
    if payload is None:
        return jsonify({"error": "unauthorized"}), 401

    customer = payload.get("customer") or {}
    customer_id = customer.get("id")
    if not customer_id:
        # Guest orders skipped — no customer to credit. Plus checkout always
        # produces a customer ID, so this is the rare-edge path.
        return jsonify({"ok": True, "skipped": "no customer"})

    customer_gid = f"gid://shopify/Customer/{customer_id}"
    line_items = []
    for li in payload.get("line_items") or []:
        pid = li.get("product_id")
        qty = li.get("quantity") or 0
        if pid and qty:
            line_items.append({"product_id": str(pid), "qty": int(qty)})

    if not line_items:
        return jsonify({"ok": True, "skipped": "no items"})

    try:
        service.append_customer_purchases(customer_gid, line_items)
    except Exception as e:
        logger.exception("purchase log update failed for %s: %s", customer_gid, e)
        return jsonify({"error": "update failed"}), 500
    return jsonify({"ok": True, "items": len(line_items)})


# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard data
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_tag(tag):
    if service.PREORDER_TAG_RE.match(tag):
        return "preorder"
    if service.LIMIT_TAG_RE.match(tag):
        return "qty_limit"
    return None


@app.route("/api/rules")
def api_rules():
    """
    Aggregate currently-active rule tags from product_rule_state and join
    against preorder_overrides so the dashboard can show override status.
    """
    rows = db.query("""
        SELECT tag, COUNT(*) AS product_count
        FROM (
            SELECT unnest(rule_tags) AS tag FROM product_rule_state
        ) sub
        GROUP BY tag
        ORDER BY tag
    """)

    overrides = {
        r["tag"]: r for r in db.query(
            "SELECT tag, display_name, button_text, pdp_message, cart_message FROM preorder_overrides"
        )
    }

    preorders = []
    limits = []
    for r in rows:
        tag = r["tag"]
        kind = _classify_tag(tag)
        if kind == "preorder":
            m = service.PREORDER_TAG_RE.match(tag)
            street_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
            o = overrides.get(tag) or {}
            preorders.append({
                "tag": tag,
                "street_date": street_date,
                "product_count": int(r["product_count"]),
                "display_name": o.get("display_name") or "",
                "has_override": bool(o.get("button_text") or o.get("pdp_message") or o.get("cart_message") or o.get("display_name")),
            })
        elif kind == "qty_limit":
            m = service.LIMIT_TAG_RE.match(tag)
            if not m:
                continue
            unit, count = service.WINDOW_BY_SUFFIX[(m.group(2) or "").lower() or None]
            limits.append({
                "tag": tag,
                "max_qty": int(m.group(1)),
                "window_unit": unit,
                "window_count": count,
                "product_count": int(r["product_count"]),
            })

    # Sort: preorders by date asc, limits by max_qty then unit
    preorders.sort(key=lambda x: (x["street_date"] or "9999"))
    limits.sort(key=lambda x: (x["max_qty"], x["window_unit"]))

    return jsonify({"preorders": preorders, "qty_limits": limits})


@app.route("/api/preorder-overrides/<tag>", methods=["GET"])
def api_get_override(tag):
    row = db.query_one(
        "SELECT tag, display_name, button_text, pdp_message, cart_message, updated_at "
        "FROM preorder_overrides WHERE tag = %s",
        (tag,),
    )
    if not row:
        return jsonify({"tag": tag, "exists": False})
    return jsonify({"tag": tag, "exists": True, **{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items() if k != "tag"}})


@app.route("/api/preorder-overrides/<tag>", methods=["PUT"])
def api_put_override(tag):
    if not service.PREORDER_TAG_RE.match(tag):
        return jsonify({"error": "tag is not a valid preorder-YYYY-MM-DD"}), 400
    data = request.get_json(silent=True) or {}
    display_name = (data.get("display_name") or "").strip() or None
    button_text  = (data.get("button_text")  or "").strip() or None
    pdp_message  = (data.get("pdp_message")  or "").strip() or None
    cart_message = (data.get("cart_message") or "").strip() or None

    db.execute("""
        INSERT INTO preorder_overrides (tag, display_name, button_text, pdp_message, cart_message)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tag) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            button_text  = EXCLUDED.button_text,
            pdp_message  = EXCLUDED.pdp_message,
            cart_message = EXCLUDED.cart_message,
            updated_at   = NOW()
    """, (tag, display_name, button_text, pdp_message, cart_message))

    # Re-sync every product currently carrying this tag so the new messages
    # propagate to metafields immediately — without this an edit looks like a
    # no-op until the next time someone touches the product.
    products = db.query(
        "SELECT shopify_product_id FROM product_rule_state WHERE %s = ANY(rule_tags)",
        (tag,),
    )
    resynced = 0
    for p in products:
        gid = f"gid://shopify/Product/{p['shopify_product_id']}"
        try:
            prod = service.get_product(gid) or {}
            service.sync_product_metafields(gid, prod.get("tags") or [], db)
            resynced += 1
        except Exception as e:
            logger.warning("resync %s failed: %s", gid, e)

    return jsonify({"ok": True, "resynced": resynced})


@app.route("/api/preorder-overrides/<tag>", methods=["DELETE"])
def api_delete_override(tag):
    db.execute("DELETE FROM preorder_overrides WHERE tag = %s", (tag,))
    # Re-sync products so the defaults take over immediately.
    products = db.query(
        "SELECT shopify_product_id FROM product_rule_state WHERE %s = ANY(rule_tags)",
        (tag,),
    )
    for p in products:
        gid = f"gid://shopify/Product/{p['shopify_product_id']}"
        try:
            prod = service.get_product(gid) or {}
            service.sync_product_metafields(gid, prod.get("tags") or [], db)
        except Exception as e:
            logger.warning("resync %s failed: %s", gid, e)
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# Release (cron + manual)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/release", methods=["POST"])
def release_preorders():
    """
    Clear all preorder-YYYY-MM-DD tags whose date has passed.
    Auth: owner JWT OR matching VIP_FLOW_SECRET (Shopify Flow trigger).
    """
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    secret = request.headers.get("X-Flow-Secret", "")
    user = get_current_user()
    if not user and (not flow_secret or secret != flow_secret):
        return jsonify({"error": "Unauthorized"}), 401

    today = date.today().isoformat()
    # All currently-tracked preorder tags
    rows = db.query("""
        SELECT DISTINCT tag FROM (
            SELECT unnest(rule_tags) AS tag FROM product_rule_state
        ) sub
        WHERE tag ~ '^preorder-[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    """)
    expired = []
    for r in rows:
        tag = r["tag"]
        m = service.PREORDER_TAG_RE.match(tag)
        if not m:
            continue
        street_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if street_date <= today:
            expired.append(tag)

    if not expired:
        return jsonify({"ok": True, "released": 0, "tags": []})

    released = {}
    for tag in expired:
        try:
            touched = service.release_preorder_tag(tag, db)
            released[tag] = len(touched)
        except Exception as e:
            logger.exception("release %s failed: %s", tag, e)
            released[tag] = {"error": str(e)}

    return jsonify({"ok": True, "released": sum(v for v in released.values() if isinstance(v, int)), "by_tag": released})


# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard HTML
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rules · Pack Fresh</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x1F4DC;</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header h1 { font-size:1.3rem; }
.main { max-width:1000px; margin:0 auto; padding:20px; }
.section-title { font-size:0.78rem; text-transform:uppercase; letter-spacing:0.08em; color:var(--text-dim); margin:24px 0 10px; }
.rule-card { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 16px; margin-bottom:8px; display:flex; align-items:center; gap:14px; }
.rule-tag { font-family:'DM Mono',monospace; font-size:0.84rem; color:var(--accent); }
.rule-meta { color:var(--text-dim); font-size:0.82rem; }
.rule-actions { margin-left:auto; display:flex; gap:8px; }
.empty { color:var(--text-dim); padding:30px; text-align:center; }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:20px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
.modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.6); display:none; align-items:center; justify-content:center; z-index:10000; }
.modal-overlay.active { display:flex; }
.modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:24px; max-width:520px; width:90%; }
.modal h3 { margin:0 0 10px; }
.modal .form-group { margin-bottom:12px; }
.modal .form-label { font-size:0.72rem; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.06em; display:block; margin-bottom:4px; }
.modal .form-input { width:100%; height:38px; background:var(--surface-2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 12px; font-size:0.88rem; font-family:inherit; outline:none; box-sizing:border-box; }
.modal textarea.form-input { height:auto; min-height:60px; padding:10px 12px; resize:vertical; }
.modal .form-input:focus { border-color:var(--accent); }
.modal .btn-row { display:flex; gap:8px; margin-top:14px; justify-content:flex-end; }
.preview { font-size:0.78rem; color:var(--text-dim); margin-top:4px; }
.badge-override { background:rgba(79,125,249,0.15); color:var(--accent); padding:2px 8px; border-radius:6px; font-size:0.72rem; }
</style>
</head>
<body>
<div class="header">
  <h1>&#x1F4DC; Product Rules</h1>
  <button class="btn btn-green btn-sm" onclick="releaseNow()" style="margin-left:auto;">&#x1F680; Release Now</button>
</div>

<div class="main">
  <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:18px;font-size:0.84rem;color:var(--text-dim);line-height:1.55;">
    Tag products in Shopify Admin to apply rules. The webhook syncs metafields automatically.
    <div style="margin-top:8px;">
      <span class="rule-tag">limit-N</span> &nbsp;<span class="rule-tag">limit-N-per-day</span> &nbsp;<span class="rule-tag">limit-N-per-week</span> &nbsp;<span class="rule-tag">limit-N-per-month</span> &nbsp;<span class="rule-tag">limit-N-all-time</span>
    </div>
    <div style="margin-top:4px;"><span class="rule-tag">preorder-YYYY-MM-DD</span></div>
  </div>

  <div class="section-title">Pre-Order Tags</div>
  <div id="preorders"><div class="spinner"></div></div>

  <div class="section-title">Quantity Limit Tags</div>
  <div id="limits"><div class="spinner"></div></div>
</div>

<div class="modal-overlay" id="override-modal">
  <div class="modal">
    <h3 id="override-title">Customize Pre-Order Messaging</h3>
    <div class="form-group">
      <span class="form-label">Display Name (internal label)</span>
      <input class="form-input" id="ovr-display" placeholder="e.g. MTG Final Fantasy">
    </div>
    <div class="form-group">
      <span class="form-label">Button Text</span>
      <input class="form-input" id="ovr-button" placeholder="Pre-Order Now">
    </div>
    <div class="form-group">
      <span class="form-label">PDP Message</span>
      <textarea class="form-input" id="ovr-pdp" placeholder="Releases June 24, 2026"></textarea>
    </div>
    <div class="form-group">
      <span class="form-label">Cart Message</span>
      <textarea class="form-input" id="ovr-cart" placeholder="Pre-order &mdash; releases June 24, 2026"></textarea>
    </div>
    <div class="btn-row">
      <button class="btn btn-secondary btn-sm" onclick="closeOverride()">Cancel</button>
      <button class="btn btn-red btn-sm" id="ovr-clear" onclick="clearOverride()" style="display:none;">Clear Override</button>
      <button class="btn btn-primary btn-sm" onclick="saveOverride()">Save</button>
    </div>
  </div>
</div>

<script>
let _editingTag = null;

async function loadRules() {
  try {
    const r = await fetch('/api/rules');
    const d = await r.json();
    renderPreorders(d.preorders || []);
    renderLimits(d.qty_limits || []);
  } catch (e) {
    document.getElementById('preorders').innerHTML = '<div class="empty">' + e.message + '</div>';
    document.getElementById('limits').innerHTML = '';
  }
}

function fmtDate(iso) {
  if (!iso) return '';
  const [y, m, d] = iso.split('-').map(Number);
  const dt = new Date(y, m - 1, d);
  return dt.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' });
}

function renderPreorders(list) {
  const el = document.getElementById('preorders');
  if (!list.length) { el.innerHTML = '<div class="empty">No pre-order tags in use.</div>'; return; }
  const today = new Date().toISOString().slice(0, 10);
  el.innerHTML = list.map(p => {
    const expired = p.street_date && p.street_date <= today;
    const meta = `<div class="rule-meta">${fmtDate(p.street_date)} &middot; ${p.product_count} product${p.product_count === 1 ? '' : 's'}${expired ? ' &middot; <span style="color:var(--amber);">past street date</span>' : ''}</div>`;
    const badge = p.has_override ? '<span class="badge-override">custom copy</span>' : '';
    const name = p.display_name ? `<div style="font-weight:600;">${escapeHtml(p.display_name)}</div>` : '';
    return `<div class="rule-card">
      <div style="flex:1;">
        ${name}
        <div class="rule-tag">${escapeHtml(p.tag)}</div>
        ${meta}
      </div>
      ${badge}
      <div class="rule-actions">
        <button class="btn btn-sm btn-secondary" onclick="editOverride('${escapeHtml(p.tag)}')">Edit Copy</button>
      </div>
    </div>`;
  }).join('');
}

function renderLimits(list) {
  const el = document.getElementById('limits');
  if (!list.length) { el.innerHTML = '<div class="empty">No quantity-limit tags in use.</div>'; return; }
  const unitLabel = { order: 'per order', day: 'per day', week: 'per week', month: 'per month', all_time: 'all time' };
  el.innerHTML = list.map(l => `<div class="rule-card">
    <div style="flex:1;">
      <div class="rule-tag">${escapeHtml(l.tag)}</div>
      <div class="rule-meta">Max ${l.max_qty} ${unitLabel[l.window_unit] || l.window_unit} &middot; per customer &middot; ${l.product_count} product${l.product_count === 1 ? '' : 's'}</div>
    </div>
  </div>`).join('');
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

async function editOverride(tag) {
  _editingTag = tag;
  document.getElementById('override-title').textContent = 'Customize: ' + tag;
  // Reset
  ['ovr-display','ovr-button','ovr-pdp','ovr-cart'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('ovr-clear').style.display = 'none';
  try {
    const r = await fetch('/api/preorder-overrides/' + encodeURIComponent(tag));
    const d = await r.json();
    if (d.exists) {
      document.getElementById('ovr-display').value = d.display_name || '';
      document.getElementById('ovr-button').value = d.button_text || '';
      document.getElementById('ovr-pdp').value = d.pdp_message || '';
      document.getElementById('ovr-cart').value = d.cart_message || '';
      document.getElementById('ovr-clear').style.display = '';
    }
  } catch (e) { /* show empty form */ }
  document.getElementById('override-modal').classList.add('active');
}

function closeOverride() {
  document.getElementById('override-modal').classList.remove('active');
  _editingTag = null;
}

async function saveOverride() {
  if (!_editingTag) return;
  const body = {
    display_name: document.getElementById('ovr-display').value,
    button_text:  document.getElementById('ovr-button').value,
    pdp_message:  document.getElementById('ovr-pdp').value,
    cart_message: document.getElementById('ovr-cart').value,
  };
  try {
    const r = await fetch('/api/preorder-overrides/' + encodeURIComponent(_editingTag), {
      method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Save failed'); return; }
    toast('Saved &mdash; resynced ' + (d.resynced || 0) + ' product(s)', 'green');
    closeOverride();
    loadRules();
  } catch (e) { alert(e.message); }
}

async function clearOverride() {
  if (!_editingTag) return;
  if (!confirm('Remove custom copy for ' + _editingTag + ' and fall back to defaults?')) return;
  try {
    const r = await fetch('/api/preorder-overrides/' + encodeURIComponent(_editingTag), { method: 'DELETE' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Clear failed'); return; }
    toast('Override cleared', 'green');
    closeOverride();
    loadRules();
  } catch (e) { alert(e.message); }
}

async function releaseNow() {
  if (!confirm('Clear all pre-order tags whose street date is today or earlier?')) return;
  try {
    const r = await fetch('/release', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Release failed'); return; }
    toast('Released ' + (d.released || 0) + ' product-tag pair(s)', 'green');
    loadRules();
  } catch (e) { alert(e.message); }
}

loadRules();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
