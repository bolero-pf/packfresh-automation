"""
screening — screening.pack-fresh.com
Order screening + review console: verification queue, combine shipping queue.
"""

import os
import logging
from flask import Flask, request, jsonify, render_template_string

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

from routes import bp as screening_bp
app.register_blueprint(screening_bp)

import db
db.init_pool()
db.execute("""
    CREATE TABLE IF NOT EXISTS customer_notes (
        id SERIAL PRIMARY KEY,
        customer_email TEXT NOT NULL,
        customer_name TEXT,
        note_type TEXT NOT NULL DEFAULT 'note',
        note_text TEXT NOT NULL,
        active BOOLEAN NOT NULL DEFAULT true,
        created_by TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
""")
db.execute("CREATE INDEX IF NOT EXISTS idx_customer_notes_email ON customer_notes(customer_email)")

from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner", "manager"], public_prefixes=('/screening/',))


@app.route("/")
def index():
    store = os.environ.get("SHOPIFY_STORE", "").replace(".myshopify.com", "")
    return render_template_string(CONSOLE_HTML, shopify_store=store)


@app.route("/api/held-orders")
def api_held_orders():
    """Fetch all orders with hold-for-review tag."""
    from shopify_graphql import shopify_gql, gid_numeric

    data = shopify_gql("""
        query($first:Int!, $q:String!) {
          orders(first:$first, query:$q, sortKey:CREATED_AT, reverse:true) {
            edges {
              node {
                id
                name
                createdAt
                tags
                note
                currentTotalPriceSet { shopMoney { amount } }
                displayFulfillmentStatus
                customer {
                  id email firstName lastName
                }
                shippingAddress { firstName lastName address1 city province zip }
                lineItems(first:20) {
                  edges { node { title quantity image { url } } }
                }
              }
            }
          }
        }
    """, {"first": 50, "q": 'tag:"hold-for-review"'})

    verification = []
    combine = []

    for edge in data.get("data", {}).get("orders", {}).get("edges", []):
        o = edge["node"]
        # Skip fulfilled orders — they've already been shipped
        if o.get("displayFulfillmentStatus") in ("FULFILLED", "PARTIALLY_FULFILLED"):
            continue
        tags = [t.lower() for t in (o.get("tags") or [])]
        customer = o.get("customer") or {}
        addr = o.get("shippingAddress") or {}
        items = [{"title": e["node"]["title"], "qty": e["node"]["quantity"],
                  "image": (e["node"].get("image") or {}).get("url", "")}
                 for e in o.get("lineItems", {}).get("edges", [])]
        note = o.get("note") or ""

        order_data = {
            "id": o["id"],
            "numeric_id": gid_numeric(o["id"]),
            "name": o["name"],
            "created_at": o["createdAt"],
            "total": float(o.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0)),
            "fulfillment_status": o.get("displayFulfillmentStatus"),
            "customer_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
            "customer_email": customer.get("email", ""),
            "customer_id": customer.get("id"),
            "shipping_address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('province', '')} {addr.get('zip', '')}",
            "tags": o.get("tags", []),
            "note": note,
            "items": items,
        }

        # Determine type based on tags
        is_verification = any(t in tags for t in [
            "high-value-tier1", "high-value-tier2", "spend-spike-review",
            "fraud-medium", "firsttime5-review", "customer-hold"
        ])
        is_combine = "combine" in note.lower() if note else False

        if is_verification:
            # Determine specific type
            if "high-value-tier2" in tags:
                order_data["check_type"] = "ID + Selfie ($1000+)"
            elif "high-value-tier1" in tags:
                order_data["check_type"] = "ID Verification ($700+)"
            elif "spend-spike-review" in tags:
                order_data["check_type"] = "Spend Spike"
            elif "fraud-medium" in tags:
                order_data["check_type"] = "Medium Fraud"
            elif "firsttime5-review" in tags:
                order_data["check_type"] = "FIRSTTIME5 Abuse"
            elif "customer-hold" in tags:
                order_data["check_type"] = "Customer Hold"
            else:
                order_data["check_type"] = "Review"
            verification.append(order_data)
        elif is_combine:
            order_data["check_type"] = "Combine Shipping"
            combine.append(order_data)
        else:
            order_data["check_type"] = "Other Hold"
            verification.append(order_data)

    # Group combine orders by customer
    combine_groups = {}
    for o in combine:
        key = o["customer_email"] or o["customer_name"]
        if key not in combine_groups:
            combine_groups[key] = {
                "customer_name": o["customer_name"],
                "customer_email": o["customer_email"],
                "shipping_address": o["shipping_address"],
                "orders": [],
                "total_value": 0,
                "all_items": [],
            }
        combine_groups[key]["orders"].append(o)
        combine_groups[key]["total_value"] += o["total"]
        # Consolidate duplicate SKUs in combined packing list
        for item in o["items"]:
            existing = next((a for a in combine_groups[key]["all_items"]
                           if a["title"] == item["title"]), None)
            if existing:
                existing["qty"] += item["qty"]
            else:
                combine_groups[key]["all_items"].append({**item})

    return jsonify({
        "verification": verification,
        "combine_groups": list(combine_groups.values()),
    })


@app.route("/api/release-hold", methods=["POST"])
def api_release_hold():
    """Release a held order: remove tags, release fulfillment holds."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400

    from service import on_order_fulfilled
    result = on_order_fulfilled(order_gid)
    return jsonify({"ok": True, **result})


@app.route("/api/release-and-fulfill", methods=["POST"])
def api_release_and_fulfill():
    """Release holds and create fulfillment with tracking for an order."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    tracking = (data.get("tracking_number") or "").strip()
    company = (data.get("tracking_company") or "USPS").strip()
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400
    if not tracking:
        return jsonify({"error": "tracking_number required"}), 400

    from service import release_and_fulfill, on_order_fulfilled
    result = release_and_fulfill(order_gid, tracking, company)
    if result.get("fulfilled"):
        # Clean up tags
        on_order_fulfilled(order_gid)
    return jsonify(result)


@app.route("/api/cancel-order", methods=["POST"])
def api_cancel_order():
    """Cancel + full refund a held order, then clean up tags."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400

    from shopify_graphql import shopify_gql
    # Cancel with full refund
    try:
        shopify_gql("""
            mutation OrderCancel($orderId:ID!, $reason:OrderCancelReason!, $refund:Boolean!, $restock:Boolean!, $notifyCustomer:Boolean, $staffNote:String) {
              orderCancel(orderId:$orderId, reason:$reason, refund:$refund, restock:$restock, notifyCustomer:$notifyCustomer, staffNote:$staffNote) {
                orderCancelUserErrors { field message code }
              }
            }
        """, {
            "orderId": order_gid,
            "reason": "OTHER",
            "refund": True,
            "restock": True,
            "notifyCustomer": True,
            "staffNote": "Cancelled from screening console",
        })
    except Exception as e:
        return jsonify({"error": f"Cancel failed: {e}"}), 500

    # Clean up tags
    from service import on_order_fulfilled
    try:
        on_order_fulfilled(order_gid)
    except Exception:
        pass

    return jsonify({"ok": True})


@app.route("/api/customer-search")
def api_customer_search():
    """Search Shopify customers by name or email."""
    q = (request.args.get("q") or "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    from shopify_graphql import shopify_gql
    data = shopify_gql("""
        query($q: String!) {
          customers(first: 10, query: $q) {
            edges {
              node {
                id
                firstName
                lastName
                email
                phone
                numberOfOrders
                defaultAddress {
                  address1
                  city
                  province
                  zip
                }
              }
            }
          }
        }
    """, {"q": q})

    results = []
    for edge in data.get("data", {}).get("customers", {}).get("edges", []):
        c = edge["node"]
        addr = c.get("defaultAddress") or {}
        results.append({
            "name": f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
            "email": c.get("email", ""),
            "phone": c.get("phone", ""),
            "orders": c.get("numberOfOrders", 0),
            "address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('province', '')} {addr.get('zip', '')}".strip(", ") if addr.get("address1") else "",
        })
    return jsonify(results)


@app.route("/api/customer-notes")
def api_customer_notes():
    """List active customer notes, optional ?q= search."""
    q = (request.args.get("q") or "").strip().lower()
    if q:
        rows = db.query(
            "SELECT * FROM customer_notes WHERE active = true AND (LOWER(customer_email) LIKE %s OR LOWER(customer_name) LIKE %s) ORDER BY created_at DESC",
            (f"%{q}%", f"%{q}%"),
        )
    else:
        rows = db.query("SELECT * FROM customer_notes WHERE active = true ORDER BY created_at DESC")
    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
        r["updated_at"] = r["updated_at"].isoformat() if r.get("updated_at") else None
    return jsonify(rows)


@app.route("/api/customer-notes", methods=["POST"])
def api_create_customer_note():
    """Create a customer note."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    name = (data.get("name") or "").strip()
    note_type = data.get("type", "note")
    text = (data.get("text") or "").strip()

    if not email or not text:
        return jsonify({"error": "email and text required"}), 400
    if note_type not in ("note", "hold"):
        return jsonify({"error": "type must be 'note' or 'hold'"}), 400

    row = db.execute_returning(
        "INSERT INTO customer_notes (customer_email, customer_name, note_type, note_text) VALUES (%s, %s, %s, %s) RETURNING *",
        (email, name or None, note_type, text),
    )
    if row:
        row["created_at"] = row["created_at"].isoformat() if row.get("created_at") else None
        row["updated_at"] = row["updated_at"].isoformat() if row.get("updated_at") else None
    return jsonify(row), 201


@app.route("/api/customer-notes/<int:note_id>", methods=["DELETE"])
def api_delete_customer_note(note_id):
    """Soft-delete a customer note."""
    affected = db.execute(
        "UPDATE customer_notes SET active = false, updated_at = NOW() WHERE id = %s AND active = true",
        (note_id,),
    )
    if not affected:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


CONSOLE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pack Fresh — Screening Console</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; }
.header h1 { font-size:1.3rem; }
.main { max-width:1000px; margin:0 auto; padding:20px; }
.section-title { font-size:0.75rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.1em; margin:24px 0 12px; display:flex; align-items:center; gap:8px; }
.section-title:first-child { margin-top:0; }
.count-badge { background:var(--red); color:#fff; border-radius:10px; padding:1px 8px; font-size:0.7rem; }
.order-header { display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:8px; }
.order-name { font-weight:700; font-size:1rem; }
.order-meta { font-size:0.8rem; color:var(--dim); line-height:1.5; }
.items-list { font-size:0.8rem; color:var(--dim); margin-top:6px; padding:8px 12px; background:var(--s2); border-radius:6px; }
.btn-green { background:var(--green); color:#000; }
.combine-group { background:var(--surface); border:2px solid var(--accent); border-radius:12px; padding:18px; margin-bottom:14px; }
.combine-header { font-weight:700; font-size:1rem; margin-bottom:4px; }
.combine-orders { display:flex; flex-direction:column; gap:8px; margin:10px 0; }
.combine-order { background:var(--s2); border-radius:8px; padding:10px 14px; }
.tab { background:none; border:none; padding:10px 18px; color:var(--dim); cursor:pointer; font-size:0.88rem; font-weight:500; border-bottom:2px solid transparent; font-family:inherit; }
.tab:hover { color:var(--text); }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.pane { display:none; }
.pane.active { display:block; }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:30px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
.empty { color:var(--dim); text-align:center; padding:30px; }
</style>
</head>
<body>
<div class="header">
  <h1>🛡️ Screening Console</h1>
  <button class="btn btn-secondary btn-sm" onclick="loadOrders()" style="margin-left:auto;">↻ Refresh</button>
</div>

<div class="main">
  <div style="display:flex;gap:2px;margin-bottom:20px;border-bottom:1px solid var(--border);">
    <button class="tab active" id="tab-verify" onclick="switchTab('verify')">🔍 Verification Queue</button>
    <button class="tab" id="tab-combine" onclick="switchTab('combine')">📦 Combine Shipping</button>
    <button class="tab" id="tab-notes" onclick="switchTab('notes')">👤 Customer Notes</button>
  </div>
  <div id="pane-verify" class="pane active"><div class="spinner"></div></div>
  <div id="pane-combine" class="pane"><div class="spinner"></div></div>
  <div id="pane-notes" class="pane"></div>
</div>

<script>
let _data = null;

function switchTab(id) {
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-' + id).classList.add('active');
  document.getElementById('tab-' + id).classList.add('active');
  if (id === 'notes') loadNotes();
  else if (_data) renderAll();
}

async function loadOrders() {
  document.getElementById('pane-verify').innerHTML = '<div class="spinner"></div>';
  document.getElementById('pane-combine').innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/held-orders');
    _data = await r.json();
    renderAll();
  } catch(e) {
    document.getElementById('pane-verify').innerHTML = `<div class="empty">${e.message}</div>`;
  }
}

function renderAll() {
  renderVerification(_data.verification || []);
  renderCombine(_data.combine_groups || []);
  // Update tab badges
  document.getElementById('tab-verify').textContent = '🔍 Verification (' + (_data.verification||[]).length + ')';
  document.getElementById('tab-combine').textContent = '📦 Combine (' + (_data.combine_groups||[]).length + ')';
}

function renderVerification(orders) {
  const el = document.getElementById('pane-verify');
  if (!orders.length) { el.innerHTML = '<div class="empty">✅ No orders awaiting verification</div>'; return; }
  el.innerHTML = orders.map(o => `
    <div class="card">
      <div class="order-header">
        <span class="order-name">${o.name}</span>
        <span class="badge badge-amber">${o.check_type}</span>
        <span style="font-weight:700;">$${o.total.toFixed(2)}</span>
        <div style="margin-left:auto;display:flex;gap:6px;">
          <button class="btn btn-green btn-sm" onclick="releaseHold('${o.id}','${o.name}')">✓ Verify & Release</button>
          <button class="btn btn-sm" style="background:var(--red);color:#fff;" onclick="cancelOrder('${o.id}','${o.name}')">✕ Cancel & Refund</button>
        </div>
      </div>
      <div class="order-meta">
        <strong>${o.customer_name}</strong> · ${o.customer_email}<br>
        ${o.shipping_address}<br>
        ${o.note ? '<em style="color:var(--amber);">' + o.note + '</em>' : ''}
      </div>
      <div class="items-list">${o.items.map(i => i.title + ' ×' + i.qty).join(' · ')}</div>
    </div>
  `).join('');
}

function renderCombine(groups) {
  const el = document.getElementById('pane-combine');
  if (!groups.length) { el.innerHTML = '<div class="empty">✅ No orders to combine</div>'; return; }
  const printAllBtn = groups.length > 1
    ? '<div style="margin-bottom:14px;"><button class="btn btn-secondary btn-sm" onclick="printAllPackingLists()">🖨 Print All Packing Lists (' + groups.length + ')</button></div>'
    : '';
  el.innerHTML = printAllBtn + groups.map(g => `
    <div class="combine-group">
      <div class="combine-header">${g.customer_name} · ${g.orders.length} orders · $${g.total_value.toFixed(2)}</div>
      <div style="font-size:0.8rem;color:var(--dim);">${g.customer_email} · ${g.shipping_address}</div>
      <div class="combine-orders">
        ${g.orders.map(o => `
          <div class="combine-order">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <strong>${o.name}</strong>
              <span>$${o.total.toFixed(2)}</span>
            </div>
            <div style="font-size:0.78rem;color:var(--dim);margin-top:4px;">
              ${o.items.map(i => i.title + ' ×' + i.qty).join(' · ')}
            </div>
            ${((o.note || '').split(/\\n+/).filter(l => l.trim() && !l.includes('Combine Order')).length > 0) ? '<div style="font-size:0.75rem;margin-top:4px;padding:4px 8px;background:rgba(255,170,0,0.08);border-radius:4px;color:var(--amber);">' + (o.note || '').split(/\\n+/).filter(l => l.trim() && !l.includes('Combine Order')).map(l => '⚠ ' + l.trim()).join('<br>') + '</div>' : ''}
          </div>
        `).join('')}
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin:8px 0 4px;">
        <span style="font-size:0.78rem;font-weight:600;">Combined Packing List:</span>
        <button class="btn btn-secondary btn-sm" style="font-size:0.72rem;padding:2px 8px;" onclick="printPackingList(this)">🖨 Print</button>
      </div>
      <div class="items-list packing-list-content">
        ${g.all_items.map(i => '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' + (i.image ? '<img src="' + i.image + '" style="width:40px;height:40px;object-fit:cover;border-radius:4px;">' : '') + '<span><strong>' + i.qty + '×</strong> ' + i.title + '</span></div>').join('')}
      </div>
      <div style="margin-top:10px;display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;">
        <div style="flex:1;min-width:200px;">
          <label style="font-size:0.72rem;color:var(--dim);">Tracking Number</label>
          <input type="text" class="tracking-input" placeholder="Paste tracking #" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
        </div>
        <div style="width:100px;">
          <label style="font-size:0.72rem;color:var(--dim);">Carrier</label>
          <select class="carrier-select" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
            <option value="USPS">USPS</option>
            <option value="UPS">UPS</option>
            <option value="FedEx">FedEx</option>
          </select>
        </div>
        <button class="btn btn-green btn-sm" onclick="releaseAndFulfillGroup(this, ${JSON.stringify(g.orders.map(o=>o.id)).replace(/"/g,'&quot;')})">🚀 Release & Ship</button>
      </div>
      <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
        ${g.orders.map(o => `
          <a href="https://admin.shopify.com/store/{{ shopify_store }}/orders/${o.numeric_id}" target="_blank" class="btn btn-secondary btn-sm">
            ${o.name} → Admin ↗
          </a>
        `).join('')}
      </div>
    </div>
  `).join('');
}

async function cancelOrder(orderId, orderName) {
  if (!confirm('Cancel ' + orderName + ' and issue full refund?')) return;
  try {
    const r = await fetch('/api/cancel-order', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_id: orderId }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Cancelled + refunded: ' + orderName, 'green');
    loadOrders();
  } catch(e) { alert(e.message); }
}

async function releaseHold(orderId, orderName) {
  if (!confirm('Release hold on ' + orderName + '?')) return;
  try {
    const r = await fetch('/api/release-hold', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_id: orderId }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Released: ' + orderName, 'green');
    loadOrders();
  } catch(e) { alert(e.message); }
}

const _slipStyle = '<style>body{font-family:-apple-system,sans-serif;padding:24px;font-size:18px;}'
  + 'h2{margin:0 0 6px;font-size:22px;} .addr{color:#666;font-size:15px;margin-bottom:14px;}'
  + '.orders{color:#666;font-size:14px;margin-bottom:18px;}'
  + '.item-row{display:flex;align-items:center;gap:12px;margin-bottom:10px;}'
  + '.item-row img{width:60px;height:60px;object-fit:cover;border-radius:6px;border:1px solid #ddd;}'
  + '.item-row strong{font-size:20px;} .item-row span{font-size:17px;}'
  + '.slip{margin-bottom:24px;}'
  + '@media print{body{padding:12px;} .slip{page-break-after:always;} .slip:last-child{page-break-after:auto;}}</style>';

function _packingSlipHtml(group) {
  const header = group.querySelector('.combine-header').textContent;
  const addr = group.querySelector('.combine-header').nextElementSibling.textContent;
  const orders = [...group.querySelectorAll('.combine-order')].map(o => o.querySelector('strong').textContent).join(', ');
  const itemEls = group.querySelectorAll('.packing-list-content .item-row, .packing-list-content div[style]');
  let itemsHtml = '';
  itemEls.forEach(el => { itemsHtml += '<div class="item-row">' + el.innerHTML + '</div>'; });
  return '<div class="slip"><h2>' + header + '</h2><div class="addr">' + addr + '</div><div class="orders">Orders: ' + orders + '</div><hr>' + itemsHtml + '</div>';
}

function printPackingList(btn) {
  const group = btn.closest('.combine-group');
  const win = window.open('', '_blank', 'width=500,height=700');
  win.document.write('<html><head><title>Packing List</title>' + _slipStyle + '</head><body>' + _packingSlipHtml(group) + '</body></html>');
  win.document.close();
  win.print();
}

function printAllPackingLists() {
  const groups = document.querySelectorAll('.combine-group');
  const slips = [...groups].map(g => _packingSlipHtml(g)).join('');
  const win = window.open('', '_blank', 'width=500,height=700');
  win.document.write('<html><head><title>All Packing Lists</title>' + _slipStyle + '</head><body>' + slips + '</body></html>');
  win.document.close();
  win.print();
}

async function releaseAndFulfillGroup(btn, orderIds) {
  const container = btn.parentElement;
  const tracking = container.querySelector('.tracking-input').value.trim();
  const company = container.querySelector('.carrier-select').value;
  if (!tracking) { alert('Enter a tracking number'); return; }
  if (!confirm('Release holds and fulfill ' + orderIds.length + ' orders with tracking ' + tracking + '?')) return;
  btn.disabled = true;
  btn.textContent = '⏳ Fulfilling...';
  let ok = 0, errors = [];
  for (const id of orderIds) {
    try {
      const r = await fetch('/api/release-and-fulfill', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ order_id: id, tracking_number: tracking, tracking_company: company }),
      });
      const d = await r.json();
      if (d.fulfilled) ok++;
      else errors.push(d.error || 'Unknown error');
    } catch(e) { errors.push(e.message); }
  }
  btn.disabled = false;
  btn.textContent = '🚀 Release & Ship';
  if (errors.length) {
    toast(ok + ' fulfilled, ' + errors.length + ' failed: ' + errors[0], 'red');
  } else {
    toast('All ' + ok + ' orders fulfilled with tracking', 'green');
  }
  loadOrders();
}

// ── Customer Notes Tab ──
let _selectedCustomer = null;

async function loadNotes(q) {
  const el = document.getElementById('pane-notes');
  const url = q ? '/api/customer-notes?q=' + encodeURIComponent(q) : '/api/customer-notes';
  try {
    const r = await fetch(url);
    const notes = await r.json();
    renderNotes(notes);
  } catch(e) {
    el.innerHTML = '<div class="empty">' + e.message + '</div>';
  }
}

function renderNotes(notes) {
  const el = document.getElementById('pane-notes');
  const form = `
    <div class="card" style="margin-bottom:16px;">
      <div style="font-weight:600;margin-bottom:10px;">Add Customer Note</div>
      <div style="position:relative;margin-bottom:10px;">
        <label style="font-size:0.72rem;color:var(--dim);">Find Customer</label>
        <input id="cn-customer-search" type="text" placeholder="Search by name or email..." autocomplete="off"
          oninput="debounceCustomerSearch()"
          style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
        <div id="cn-search-results" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:10;background:var(--surface);border:1px solid var(--border);border-radius:6px;margin-top:2px;max-height:250px;overflow-y:auto;box-shadow:0 4px 12px rgba(0,0,0,0.3);"></div>
      </div>
      <div id="cn-selected" style="display:none;margin-bottom:10px;padding:10px 14px;background:var(--s2);border-radius:8px;border:1px solid var(--accent);">
      </div>
      <div id="cn-note-form" style="display:none;">
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;">
          <input id="cn-email" type="hidden">
          <input id="cn-name" type="hidden">
          <div style="width:120px;">
            <label style="font-size:0.72rem;color:var(--dim);">Type</label>
            <select id="cn-type" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
              <option value="note">Note</option>
              <option value="hold">Hold</option>
            </select>
          </div>
          <div style="flex:1;min-width:200px;">
            <label style="font-size:0.72rem;color:var(--dim);">Note Text *</label>
            <input id="cn-text" type="text" placeholder="e.g. KEEP ADDRESS AS 123 Main St" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
          </div>
          <button class="btn btn-green btn-sm" onclick="addNote()">+ Add</button>
        </div>
      </div>
    </div>
    <div class="section-title">Active Notes <span class="count-badge">${notes.length}</span></div>
    <div style="margin-bottom:12px;">
      <input id="cn-filter" type="text" placeholder="Filter existing notes..." oninput="debounceNoteFilter()" style="width:100%;max-width:350px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
    </div>
  `;

  if (!notes.length) {
    el.innerHTML = form + '<div class="empty">No customer notes yet</div>';
    return;
  }

  const rows = notes.map(n => `
    <div class="card" style="display:flex;align-items:center;gap:12px;">
      <div style="flex:1;">
        <div style="font-weight:600;">${n.customer_name || '—'} <span style="font-weight:400;color:var(--dim);font-size:0.85rem;">${n.customer_email}</span></div>
        <div style="margin-top:4px;font-size:0.9rem;">${n.note_text}</div>
        <div style="margin-top:4px;font-size:0.72rem;color:var(--dim);">Added ${n.created_at ? new Date(n.created_at).toLocaleDateString() : '—'}</div>
      </div>
      <span class="badge ${n.note_type === 'hold' ? 'badge-red' : 'badge-blue'}" style="white-space:nowrap;">${n.note_type === 'hold' ? '⏸ Hold' : '📋 Note'}</span>
      <button class="btn btn-sm" style="background:var(--red);color:#fff;" onclick="deleteNote(${n.id})">✕</button>
    </div>
  `).join('');

  el.innerHTML = form + rows;
}

let _custSearchTimer;
function debounceCustomerSearch() {
  clearTimeout(_custSearchTimer);
  _custSearchTimer = setTimeout(searchCustomers, 350);
}

async function searchCustomers() {
  const q = document.getElementById('cn-customer-search').value.trim();
  const resultsEl = document.getElementById('cn-search-results');
  if (q.length < 2) { resultsEl.style.display = 'none'; return; }

  try {
    const r = await fetch('/api/customer-search?q=' + encodeURIComponent(q));
    const customers = await r.json();
    if (!customers.length) {
      resultsEl.innerHTML = '<div style="padding:10px;color:var(--dim);font-size:0.85rem;">No customers found</div>';
      resultsEl.style.display = 'block';
      return;
    }
    resultsEl.innerHTML = customers.map((c, i) => `
      <div onclick='selectCustomer(${JSON.stringify(c).replace(/'/g,"&#39;")})' style="padding:10px 14px;cursor:pointer;border-bottom:1px solid var(--border);${i === 0 ? '' : ''}" onmouseover="this.style.background='var(--s2)'" onmouseout="this.style.background='none'">
        <div style="font-weight:600;font-size:0.9rem;">${c.name} <span style="font-weight:400;color:var(--dim);font-size:0.8rem;">${c.orders} order${c.orders===1?'':'s'}</span></div>
        <div style="font-size:0.8rem;color:var(--dim);">${c.email}${c.phone ? ' · ' + c.phone : ''}</div>
        ${c.address ? '<div style="font-size:0.78rem;color:var(--dim);margin-top:2px;">' + c.address + '</div>' : ''}
      </div>
    `).join('');
    resultsEl.style.display = 'block';
  } catch(e) {
    resultsEl.innerHTML = '<div style="padding:10px;color:var(--red);">Search failed</div>';
    resultsEl.style.display = 'block';
  }
}

function selectCustomer(c) {
  _selectedCustomer = c;
  document.getElementById('cn-search-results').style.display = 'none';
  document.getElementById('cn-customer-search').value = '';

  const selEl = document.getElementById('cn-selected');
  selEl.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;">
      <div>
        <div style="font-weight:600;">${c.name}</div>
        <div style="font-size:0.85rem;color:var(--dim);">${c.email}${c.phone ? ' · ' + c.phone : ''}</div>
        ${c.address ? '<div style="font-size:0.82rem;color:var(--dim);margin-top:2px;">' + c.address + '</div>' : ''}
        <div style="font-size:0.75rem;color:var(--dim);margin-top:2px;">${c.orders} order${c.orders===1?'':'s'}</div>
      </div>
      <button class="btn btn-secondary btn-sm" onclick="clearCustomer()">✕</button>
    </div>
  `;
  selEl.style.display = 'block';

  document.getElementById('cn-email').value = c.email;
  document.getElementById('cn-name').value = c.name;
  document.getElementById('cn-note-form').style.display = 'block';
}

function clearCustomer() {
  _selectedCustomer = null;
  document.getElementById('cn-selected').style.display = 'none';
  document.getElementById('cn-note-form').style.display = 'none';
  document.getElementById('cn-email').value = '';
  document.getElementById('cn-name').value = '';
  document.getElementById('cn-text').value = '';
}

let _noteFilterTimer;
function debounceNoteFilter() {
  clearTimeout(_noteFilterTimer);
  _noteFilterTimer = setTimeout(() => {
    const q = document.getElementById('cn-filter').value.trim();
    loadNotes(q || undefined);
  }, 300);
}

async function addNote() {
  const email = document.getElementById('cn-email').value.trim();
  const name = document.getElementById('cn-name').value.trim();
  const type = document.getElementById('cn-type').value;
  const text = document.getElementById('cn-text').value.trim();
  if (!email || !text) { alert('Email and note text are required'); return; }
  try {
    const r = await fetch('/api/customer-notes', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ email, name, type, text }),
    });
    if (!r.ok) { const d = await r.json(); alert(d.error); return; }
    toast('Note added for ' + name, 'green');
    clearCustomer();
    loadNotes();
  } catch(e) { alert(e.message); }
}

async function deleteNote(id) {
  if (!confirm('Remove this customer note?')) return;
  try {
    const r = await fetch('/api/customer-notes/' + id, { method: 'DELETE' });
    if (!r.ok) { alert('Failed to delete'); return; }
    toast('Note removed', 'green');
    loadNotes();
  } catch(e) { alert(e.message); }
}

// Close search results when clicking outside
document.addEventListener('click', (e) => {
  const results = document.getElementById('cn-search-results');
  if (results && !e.target.closest('#cn-customer-search') && !e.target.closest('#cn-search-results')) {
    results.style.display = 'none';
  }
});

loadOrders();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
