"""
vip — vip.pack-fresh.com
VIP tier management + console: customer profiles, tier analysis, Klaviyo sync.
"""

import os
import logging
from flask import Flask, request, jsonify, render_template_string

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

from routes import bp as vip_bp
app.register_blueprint(vip_bp)


@app.before_request
def _check_auth():
    # Skip webhook endpoints (they use flow secret)
    if request.path.startswith('/vip/'):
        return
    if request.path in ('/ping', '/health'):
        return
    try:
        from auth import require_auth
        return require_auth(roles=["owner", "manager"])
    except Exception:
        pass

@app.after_request
def _add_admin_bar(response):
    try:
        from auth import inject_admin_bar, get_current_user
        if get_current_user():
            return inject_admin_bar(response)
    except Exception:
        pass
    return response


@app.route("/")
def index():
    return render_template_string(CONSOLE_HTML)


@app.route("/api/vip/set-tier", methods=["POST"])
def api_set_tier():
    """Manually set a customer's VIP tier + lock window."""
    data = request.get_json(silent=True) or {}
    customer_gid = data.get("customer_id")
    new_tier = data.get("tier", "").upper()
    lock_days = int(data.get("lock_days", 90))

    if not customer_gid or new_tier not in ("VIP0", "VIP1", "VIP2", "VIP3"):
        return jsonify({"error": "customer_id and valid tier (VIP0-VIP3) required"}), 400

    from service import shopify_gql, shopify_metafields_set
    from shopify_graphql import gid_numeric
    import json
    from datetime import date, timedelta

    # Set tier metafield
    lock_start = date.today().isoformat()
    lock_end = (date.today() + timedelta(days=lock_days)).isoformat()
    lock_obj = {"start": lock_start, "end": lock_end, "tier": new_tier}

    try:
        shopify_metafields_set([
            {"ownerId": customer_gid, "namespace": "custom", "key": "loyalty_vip_tier", "value": new_tier, "type": "single_line_text_field"},
            {"ownerId": customer_gid, "namespace": "custom", "key": "loyalty_lock_window", "value": json.dumps(lock_obj), "type": "json"},
        ])
    except Exception as e:
        return jsonify({"error": f"Metafield update failed: {e}"}), 500

    # Update tags — remove old VIP tags, add new one
    try:
        old_tags = ["VIP0", "VIP1", "VIP2", "VIP3", "VIP1-risk", "VIP2-risk", "VIP3-risk", "VIP1-hopeful", "VIP2-hopeful", "VIP3-hopeful"]
        shopify_gql("""
            mutation($id:ID!,$tags:[String!]!) { tagsRemove(id:$id,tags:$tags) { userErrors{message} } }
        """, {"id": customer_gid, "tags": old_tags})
        shopify_gql("""
            mutation($id:ID!,$tags:[String!]!) { tagsAdd(id:$id,tags:$tags) { userErrors{message} } }
        """, {"id": customer_gid, "tags": [new_tier]})
    except Exception as e:
        return jsonify({"error": f"Tag update failed: {e}"}), 500

    return jsonify({"ok": True, "tier": new_tier, "lock": lock_obj})


@app.route("/api/vip/recalculate", methods=["POST"])
def api_recalculate():
    """
    Recalculate what a customer's tier and lock SHOULD be based on actual order history.
    Returns the proposed state without applying it. Call /api/vip/set-tier to apply.
    """
    data = request.get_json(silent=True) or {}
    customer_gid = data.get("customer_id")
    if not customer_gid:
        return jsonify({"error": "customer_id required"}), 400

    if not customer_gid.startswith("gid://"):
        customer_gid = f"gid://shopify/Customer/{customer_gid}"

    from service import compute_rolling_90d_spend, get_customer_state
    from shopify_graphql import shopify_gql, gid_numeric
    from datetime import date, timedelta

    # Get current state
    current_state = get_customer_state(customer_gid)
    current_tier = current_state.get("tier", "VIP0")
    current_lock = current_state.get("lock") or {}

    # Compute fresh rolling spend
    rolling = compute_rolling_90d_spend(customer_gid)

    # Determine tier from current spend
    thresholds = [("VIP3", 2500), ("VIP2", 1250), ("VIP1", 500), ("VIP0", 0)]
    spend_tier = "VIP0"
    for tier, thresh in thresholds:
        if rolling >= thresh:
            spend_tier = tier
            break

    # Check if current lock is still active
    lock_active = False
    lock_end_date = None
    if current_lock.get("end"):
        try:
            lock_end_date = date.fromisoformat(current_lock["end"])
            lock_active = lock_end_date >= date.today()
        except Exception:
            pass

    # Decision logic:
    # 1. If lock is active and current tier is HIGHER than spend tier → keep current tier (lock protects)
    # 2. If lock is active and spend tier is HIGHER → promote (spend exceeds locked tier)
    # 3. If lock expired → use spend tier
    tier_rank = {"VIP0": 0, "VIP1": 1, "VIP2": 2, "VIP3": 3}

    if lock_active:
        locked_tier = current_lock.get("tier", current_tier)
        if tier_rank.get(spend_tier, 0) >= tier_rank.get(locked_tier, 0):
            # Spend qualifies for same or higher tier — propose spend tier with fresh lock
            computed_tier = spend_tier
            proposed_lock = {"start": date.today().isoformat(), "end": (date.today() + timedelta(days=90)).isoformat(), "tier": computed_tier}
            reason = f"Spend qualifies for {spend_tier} (${rolling:.2f}) — refreshing lock"
        else:
            # Lock protects higher tier despite lower spend
            computed_tier = locked_tier
            proposed_lock = current_lock  # keep existing lock
            reason = f"Lock active until {current_lock['end']} — tier protected despite ${rolling:.2f} spend"
    else:
        # No active lock — tier is purely based on spend
        computed_tier = spend_tier
        if computed_tier != "VIP0":
            # Find the most recent qualifying order for lock start
            numeric_id = gid_numeric(customer_gid)
            since = (date.today() - timedelta(days=180)).isoformat()  # look back further
            order_data = shopify_gql("""
                query($first:Int!, $q:String!) {
                  orders(first:$first, query:$q, sortKey:CREATED_AT, reverse:true) {
                    edges { node { createdAt currentTotalPriceSet { shopMoney { amount } } totalRefundedSet { shopMoney { amount } } } }
                  }
                }
            """, {"first": 100, "q": f'customer_id:{numeric_id} financial_status:paid created_at:>="{since}"'})

            # Find the last order that was within the qualification window
            last_order_date = None
            for edge in order_data.get("data", {}).get("orders", {}).get("edges", []):
                o = edge["node"]
                amt = float(o.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
                ref = float(o.get("totalRefundedSet", {}).get("shopMoney", {}).get("amount", 0))
                if max(0, amt - ref) > 0:
                    last_order_date = o["createdAt"][:10]
                    break  # most recent paid order (reverse sorted)

            if last_order_date:
                proposed_lock = {"start": last_order_date, "end": (date.fromisoformat(last_order_date) + timedelta(days=90)).isoformat(), "tier": computed_tier}
            else:
                proposed_lock = None
            reason = f"Lock expired — recalculated from ${rolling:.2f} spend"
        else:
            proposed_lock = None
            reason = f"Below all thresholds (${rolling:.2f})"

    changed = (computed_tier != current_tier or
               round(rolling, 2) != round(float(current_state.get("rolling") or 0), 2) or
               (proposed_lock or {}).get("end") != current_lock.get("end"))

    return jsonify({
        "current": {
            "tier": current_tier,
            "rolling": float(current_state.get("rolling") or 0),
            "lock": current_lock,
        },
        "proposed": {
            "tier": computed_tier,
            "rolling": round(rolling, 2),
            "lock": proposed_lock,
            "reason": reason,
        },
        "changed": changed,
    })


@app.route("/api/vip/customer/<path:customer_gid>")
def api_customer_detail(customer_gid):
    """Full customer detail: state, orders, tier analysis."""
    from service import get_customer_state, compute_rolling_90d_spend
    from shopify_graphql import shopify_gql, gid_numeric
    import json

    # Ensure full GID format
    if not customer_gid.startswith("gid://"):
        customer_gid = f"gid://shopify/Customer/{customer_gid}"

    # Get VIP state
    state = get_customer_state(customer_gid)

    # Get customer details + recent orders
    data = shopify_gql("""
        query($id:ID!) {
          customer(id:$id) {
            id email firstName lastName phone numberOfOrders
            createdAt
            tags
            defaultAddress { address1 city province zip }
            orders(first:20, sortKey:CREATED_AT, reverse:true) {
              edges {
                node {
                  id name createdAt
                  displayFulfillmentStatus
                  displayFinancialStatus
                  currentTotalPriceSet { shopMoney { amount } }
                  totalRefundedSet { shopMoney { amount } }
                }
              }
            }
          }
        }
    """, {"id": customer_gid})

    customer = data.get("data", {}).get("customer", {})
    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    orders = []
    for edge in customer.get("orders", {}).get("edges", []):
        o = edge["node"]
        total = float(o.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
        refunded = float(o.get("totalRefundedSet", {}).get("shopMoney", {}).get("amount", 0))
        orders.append({
            "id": o["id"],
            "name": o["name"],
            "created_at": o["createdAt"],
            "total": total,
            "refunded": refunded,
            "net": round(total - refunded, 2),
            "fulfillment": o.get("displayFulfillmentStatus"),
            "financial": o.get("displayFinancialStatus"),
        })

    # Tier analysis
    tier = state.get("tier", "VIP0")
    rolling = float(state.get("rolling") or 0)
    lock = state.get("lock") or {}
    thresholds = {"VIP0": 0, "VIP1": 500, "VIP2": 1250, "VIP3": 2500}
    tier_order = ["VIP0", "VIP1", "VIP2", "VIP3"]
    idx = tier_order.index(tier) if tier in tier_order else 0
    next_tier = tier_order[idx + 1] if idx < 3 else None

    from datetime import date
    lock_end = lock.get("end")
    lock_days = 0
    if lock_end:
        try:
            lock_days = max(0, (date.fromisoformat(lock_end) - date.today()).days)
        except Exception:
            pass

    addr = customer.get("defaultAddress") or {}

    return jsonify({
        "customer": {
            "id": customer["id"],
            "numeric_id": gid_numeric(customer["id"]),
            "email": customer.get("email"),
            "first_name": customer.get("firstName"),
            "last_name": customer.get("lastName"),
            "name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
            "phone": customer.get("phone"),
            "created_at": customer.get("createdAt"),
            "orders_count": customer.get("numberOfOrders", 0),
            "tags": customer.get("tags", []),
            "address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('province', '')} {addr.get('zip', '')}",
        },
        "vip": {
            "tier": tier,
            "rolling_spend": round(rolling, 2),
            "lock": lock,
            "lock_days_remaining": lock_days,
            "gap_to_next": round(max(0, thresholds.get(next_tier, 0) - rolling), 2) if next_tier else 0,
            "gap_to_maintain": round(max(0, thresholds.get(tier, 0) - rolling), 2),
            "next_tier": next_tier,
            "threshold": thresholds.get(tier, 0),
            "next_threshold": thresholds.get(next_tier, 0) if next_tier else None,
        },
        "orders": orders,
    })


@app.route("/api/vip/customers")
def api_customers():
    """List VIP customers by tier with cursor pagination."""
    tier = request.args.get("tier", "").upper()
    page_size = int(request.args.get("limit", 50))
    cursor = request.args.get("cursor")

    from shopify_graphql import shopify_gql

    if tier and tier in ("VIP1", "VIP2", "VIP3"):
        query_str = f'tag:"{tier}"'
    else:
        query_str = 'tag:"VIP1" OR tag:"VIP2" OR tag:"VIP3"'

    q = request.args.get("q", "").strip()
    if q:
        query_str = f'({query_str}) AND ({q})'

    variables = {"first": page_size, "q": query_str}
    if cursor:
        variables["after"] = cursor

    data = shopify_gql("""
        query($first:Int!, $after:String, $q:String!) {
          customers(first:$first, after:$after, query:$q, sortKey:UPDATED_AT, reverse:true) {
            edges {
              node {
                id
                email
                firstName
                lastName
                numberOfOrders
                tags
                metafields(first:5, keys:["custom.loyalty_vip_tier","custom.loyalty_rolling_spend_90d","custom.loyalty_lock_window"]) {
                  edges { node { key value } }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
    """, variables)

    page_info = data.get("data", {}).get("customers", {}).get("pageInfo", {})

    customers = []
    for edge in data.get("data", {}).get("customers", {}).get("edges", []):
        c = edge["node"]
        # Parse metafields — key might be "loyalty_vip_tier" or "custom.loyalty_vip_tier"
        mf = {}
        for me in (c.get("metafields") or {}).get("edges", []):
            node = me["node"]
            key = node["key"]
            # Strip namespace prefix if present
            if "." in key:
                key = key.split(".", 1)[1]
            mf[key] = node["value"]

        # Also try to determine tier from tags if metafield is missing
        tags = [t.upper() for t in (c.get("tags") or [])]
        tier_from_tags = "VIP0"
        if "VIP3" in tags:
            tier_from_tags = "VIP3"
        elif "VIP2" in tags:
            tier_from_tags = "VIP2"
        elif "VIP1" in tags:
            tier_from_tags = "VIP1"

        tier_val = mf.get("loyalty_vip_tier") or tier_from_tags
        rolling = 0
        try:
            rolling = float(mf.get("loyalty_rolling_spend_90d") or 0)
        except (ValueError, TypeError):
            pass

        lock = {}
        try:
            import json
            raw_lock = mf.get("loyalty_lock_window", "")
            if raw_lock:
                lock = json.loads(raw_lock)
        except Exception:
            pass

        # Compute gaps
        thresholds = {"VIP0": 0, "VIP1": 500, "VIP2": 1250, "VIP3": 2500}
        tier_order = ["VIP0", "VIP1", "VIP2", "VIP3"]
        idx = tier_order.index(tier_val) if tier_val in tier_order else 0
        next_tier = tier_order[idx + 1] if idx < 3 else None
        gap_next = max(0, thresholds.get(next_tier, 0) - rolling) if next_tier else 0
        gap_maintain = max(0, thresholds.get(tier_val, 0) - rolling)

        from shopify_graphql import gid_numeric
        customers.append({
            "id": c["id"],
            "numeric_id": gid_numeric(c["id"]),
            "email": c.get("email"),
            "first_name": c.get("firstName"),
            "last_name": c.get("lastName"),
            "name": f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
            "orders": c.get("numberOfOrders", 0),
            "tags": c.get("tags", []),
            "tier": tier_val,
            "rolling_spend": round(rolling, 2),
            "lock": lock,
            "gap_to_next": round(gap_next, 2),
            "gap_to_maintain": round(gap_maintain, 2),
        })

    return jsonify({
        "customers": customers,
        "has_next": page_info.get("hasNextPage", False),
        "next_cursor": page_info.get("endCursor"),
    })


@app.route("/api/vip/stats")
def api_stats():
    """Aggregate VIP tier stats by counting tagged customers."""
    from shopify_graphql import shopify_gql
    # Fetch a page of all VIP-tagged customers and count by tier from tags
    # More reliable than customersCount which may not support tag queries
    stats = {"VIP1": 0, "VIP2": 0, "VIP3": 0}
    cursor = None
    total_fetched = 0
    while True:
        variables = {"first": 250, "q": 'tag:"VIP1" OR tag:"VIP2" OR tag:"VIP3"'}
        if cursor:
            variables["after"] = cursor
        data = shopify_gql("""
            query($first:Int!, $after:String, $q:String!) {
              customers(first:$first, after:$after, query:$q, sortKey:ID) {
                edges { node { id tags } }
                pageInfo { hasNextPage endCursor }
              }
            }
        """, variables)
        edges = data.get("data", {}).get("customers", {}).get("edges", [])
        for edge in edges:
            tags = [t.upper() for t in (edge["node"].get("tags") or [])]
            # Count highest tier only
            if "VIP3" in tags:
                stats["VIP3"] += 1
            elif "VIP2" in tags:
                stats["VIP2"] += 1
            elif "VIP1" in tags:
                stats["VIP1"] += 1
            total_fetched += 1
        page_info = data.get("data", {}).get("customers", {}).get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if total_fetched > 5000:
            break  # safety cap
    return jsonify({"stats": stats, "total": sum(stats.values())})


CONSOLE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pack Fresh — VIP Console</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<style>
:root { --bg:#0a0c10; --surface:#141720; --s2:#1c2030; --border:#2a2f42; --accent:#4f7df9; --green:#34d058; --amber:#f6ad55; --red:#fc5c5c; --text:#e8eaf0; --dim:#6b7280; --vip1:#e8c547; --vip2:#a78bfa; --vip3:#f59e0b; }
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:'DM Sans',sans-serif; font-size:14px; }
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; }
.header h1 { font-size:1.3rem; }
.main { max-width:1000px; margin:0 auto; padding:20px; }
.stats { display:flex; gap:14px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:16px 20px; flex:1; min-width:160px; cursor:pointer; transition:border-color 0.15s; }
.stat:hover { border-color:var(--accent); }
.stat.active { border-color:var(--accent); border-width:2px; }
.stat-label { font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.08em; }
.stat-val { font-size:1.8rem; font-weight:700; margin-top:4px; }
.tier-name { font-size:0.78rem; font-weight:600; margin-top:2px; }
.controls { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }
.controls input { height:38px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 14px; font-size:0.85rem; font-family:inherit; outline:none; flex:1; min-width:200px; }
.controls input:focus { border-color:var(--accent); }
table { width:100%; border-collapse:collapse; font-size:0.82rem; }
th { text-align:left; color:var(--dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.06em; padding:8px; border-bottom:1px solid var(--border); }
td { padding:10px 8px; border-bottom:1px solid var(--border); }
tr:hover { background:var(--s2); }
.badge { display:inline-block; padding:3px 10px; border-radius:10px; font-size:0.7rem; font-weight:700; }
.badge-vip1 { background:rgba(232,197,71,0.15); color:var(--vip1); }
.badge-vip2 { background:rgba(167,139,250,0.15); color:var(--vip2); }
.badge-vip3 { background:rgba(245,158,11,0.15); color:var(--vip3); }
.progress-bar { height:6px; background:var(--s2); border-radius:3px; overflow:hidden; width:100px; display:inline-block; vertical-align:middle; margin-left:6px; }
.progress-fill { height:100%; border-radius:3px; }
.btn-action { background:none; border:1px solid var(--border); color:var(--dim); width:30px; height:30px; border-radius:6px; cursor:pointer; font-size:0.8rem; display:inline-flex; align-items:center; justify-content:center; }
.btn-action:hover { border-color:var(--accent); color:var(--text); }
.modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.7); z-index:100; align-items:center; justify-content:center; backdrop-filter:blur(4px); }
.modal-overlay.active { display:flex; }
.modal-box { background:var(--surface); border:1px solid var(--border); border-radius:14px; padding:28px; width:400px; max-width:92vw; }
.modal-box h3 { margin-bottom:18px; font-size:1.1rem; }
.modal-field { margin-bottom:14px; }
.modal-field label { display:block; font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:6px; }
.modal-field select, .modal-field input { width:100%; height:42px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 14px; font-size:0.9rem; font-family:inherit; outline:none; }
.modal-field select:focus, .modal-field input:focus { border-color:var(--accent); }
.recalc-row { display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid var(--border); font-size:0.85rem; }
.recalc-label { color:var(--dim); }
.recalc-old { color:var(--dim); text-decoration:line-through; }
.recalc-new { font-weight:700; }
.btn-green { background:var(--green); color:#000; }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:30px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div class="header">
  <h1>⭐ VIP Console</h1>
</div>

<div class="main">
  <div class="stats" id="tier-stats"><div class="spinner"></div></div>

  <div class="controls">
    <input type="text" id="search" placeholder="Search by name or email..." oninput="debounce()">
    <select id="sort-select" onchange="sortAndRender()" style="height:38px;background:var(--s2);border:1.5px solid var(--border);border-radius:8px;color:var(--text);padding:0 12px;font-size:0.85rem;font-family:inherit;">
      <option value="spend_desc">90d Spend (high)</option>
      <option value="spend_asc">90d Spend (low)</option>
      <option value="gap_next_asc">Gap to Next (close)</option>
      <option value="gap_maintain_asc">Gap to Maintain (at risk)</option>
      <option value="orders_desc">Orders (most)</option>
      <option value="lock_asc">Lock Expiry (soonest)</option>
      <option value="name_asc">Name A-Z</option>
    </select>
  </div>

  <div id="list-view">
    <div id="customer-list"><div class="spinner"></div></div>
    <div id="pagination" style="display:flex;gap:8px;justify-content:center;margin-top:16px;"></div>
  </div>

  <div id="detail-view" style="display:none;">
    <button class="btn" style="background:var(--s2);border:1px solid var(--border);color:var(--text);margin-bottom:16px;" onclick="closeDetail()">← Back to List</button>
    <div id="detail-content"><div class="spinner"></div></div>
  </div>

  <!-- Set Tier Modal -->
  <div id="tier-modal" class="modal-overlay">
    <div class="modal-box">
      <h3 id="tier-modal-title">Set Tier</h3>
      <div class="modal-field">
        <label>Tier</label>
        <select id="modal-tier">
          <option value="VIP0">None</option>
          <option value="VIP1">Adventurer</option>
          <option value="VIP2">Guardian</option>
          <option value="VIP3">Champion</option>
        </select>
      </div>
      <div class="modal-field">
        <label>Lock Duration (days)</label>
        <input id="modal-lock-days" type="number" value="90">
      </div>
      <div style="display:flex;gap:10px;margin-top:20px;">
        <button class="btn" style="flex:1;background:var(--s2);border:1px solid var(--border);color:var(--text);" onclick="closeTierModal()">Cancel</button>
        <button class="btn btn-green" style="flex:1;" onclick="submitTierChange()">Apply</button>
      </div>
    </div>
  </div>

  <!-- Recalculate Modal -->
  <div id="recalc-modal" class="modal-overlay">
    <div class="modal-box" style="max-width:480px;">
      <h3>Recalculate Tier & Lock</h3>
      <div id="recalc-content"><div class="spinner"></div></div>
      <div id="recalc-actions" style="display:none;margin-top:20px;">
        <div style="display:flex;gap:10px;">
          <button class="btn" style="flex:1;background:var(--s2);border:1px solid var(--border);color:var(--text);" onclick="closeRecalcModal()">Cancel</button>
          <button class="btn btn-green" style="flex:1;" id="recalc-apply-btn" onclick="applyRecalc()">Accept & Apply</button>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let _tier = '', _timer = null, _allCustomers = [], _cursor = null, _hasNext = false, _modalGid = null;
const TIER_NAMES = {VIP0:'', VIP1:'Adventurer', VIP2:'Guardian', VIP3:'Champion'};
const TIER_THRESH = {VIP0:0, VIP1:500, VIP2:1250, VIP3:2500};

function toast(msg) { /* simple toast */ const d=document.createElement('div'); d.style.cssText='position:fixed;bottom:20px;right:20px;background:var(--green);color:#000;padding:10px 20px;border-radius:8px;font-weight:600;z-index:200;'; d.textContent=msg; document.body.appendChild(d); setTimeout(()=>d.remove(),3000); }

async function loadStats() {
  try {
    const r = await fetch('/api/vip/stats');
    const d = await r.json();
    const s = d.stats || {};
    const total = (s.VIP1||0) + (s.VIP2||0) + (s.VIP3||0);
    document.getElementById('tier-stats').innerHTML = `
      <div class="stat ${_tier===''?'active':''}" onclick="filterTier('')">
        <div class="stat-label">All VIPs</div>
        <div class="stat-val">${total}</div>
      </div>
      <div class="stat ${_tier==='VIP1'?'active':''}" onclick="filterTier('VIP1')">
        <div class="stat-label">Adventurer</div>
        <div class="stat-val" style="color:var(--vip1);">${s.VIP1||0}</div>
      </div>
      <div class="stat ${_tier==='VIP2'?'active':''}" onclick="filterTier('VIP2')">
        <div class="stat-label">Guardian</div>
        <div class="stat-val" style="color:var(--vip2);">${s.VIP2||0}</div>
      </div>
      <div class="stat ${_tier==='VIP3'?'active':''}" onclick="filterTier('VIP3')">
        <div class="stat-label">Champion</div>
        <div class="stat-val" style="color:var(--vip3);">${s.VIP3||0}</div>
      </div>
    `;
  } catch(e) {}
}

function filterTier(t) { _tier = t; loadStats(); loadCustomers(); }
function debounce() { clearTimeout(_timer); _timer = setTimeout(loadCustomers, 400); }

async function loadCustomers(cursor) {
  const el = document.getElementById('customer-list');
  el.innerHTML = '<div class="spinner"></div>';
  const q = document.getElementById('search').value.trim();
  const params = new URLSearchParams({ tier: _tier, limit: 50 });
  if (q) params.set('q', q);
  if (cursor) params.set('cursor', cursor);
  try {
    const r = await fetch('/api/vip/customers?' + params);
    const d = await r.json();
    _allCustomers = d.customers || [];
    _hasNext = d.has_next;
    _cursor = d.next_cursor;
    sortAndRender();
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function sortAndRender() {
  const sort = document.getElementById('sort-select').value;
  let custs = [..._allCustomers];
  if (sort === 'spend_desc') custs.sort((a,b) => b.rolling_spend - a.rolling_spend);
  else if (sort === 'spend_asc') custs.sort((a,b) => a.rolling_spend - b.rolling_spend);
  else if (sort === 'gap_next_asc') custs.sort((a,b) => (a.gap_to_next||9999) - (b.gap_to_next||9999));
  else if (sort === 'gap_maintain_asc') custs.sort((a,b) => (b.gap_to_maintain||0) - (a.gap_to_maintain||0));
  else if (sort === 'orders_desc') custs.sort((a,b) => (b.orders||0) - (a.orders||0));
  else if (sort === 'lock_asc') custs.sort((a,b) => {
    const da = a.lock?.end ? new Date(a.lock.end).getTime() : Infinity;
    const db = b.lock?.end ? new Date(b.lock.end).getTime() : Infinity;
    return da - db;
  });
  else if (sort === 'name_asc') custs.sort((a,b) => (a.name||'').localeCompare(b.name||''));
  renderCustomers(custs);
}

function renderCustomers(custs) {
  const el = document.getElementById('customer-list');
  if (!custs.length) { el.innerHTML = '<div style="color:var(--dim);text-align:center;padding:30px;">No customers found</div>'; return; }
  el.innerHTML = `<table>
    <thead><tr><th>Customer</th><th>Tier</th><th>90d Spend</th><th>Gap to Next</th><th>To Maintain</th><th>Orders</th><th>Lock</th><th></th></tr></thead>
    <tbody>${custs.map(c => {
      const tierCls = c.tier.toLowerCase();
      const lockEnd = c.lock?.end || '';
      const lockDays = lockEnd ? Math.max(0, Math.ceil((new Date(lockEnd) - new Date()) / 86400000)) : 0;
      const lockStr = lockEnd ? lockDays + 'd left' : '—';
      const nextThresh = TIER_THRESH[{VIP0:'VIP1',VIP1:'VIP2',VIP2:'VIP3',VIP3:'VIP3'}[c.tier]] || 0;
      const progress = nextThresh > 0 ? Math.min(1, c.rolling_spend / nextThresh) : 1;
      const pctColor = c.tier === 'VIP3' ? 'var(--vip3)' : c.tier === 'VIP2' ? 'var(--vip2)' : 'var(--vip1)';
      const esc = s => (s||'').replace(/'/g,"\\\\'").replace(/"/g,'&quot;');
      return `<tr style="cursor:pointer;" onclick="openDetail('${c.id}')">
        <td>
          <strong>${c.name || c.email || '—'}</strong>
          <br><small style="color:var(--dim);">${c.email || ''}</small>
        </td>
        <td><span class="badge badge-${tierCls}">${TIER_NAMES[c.tier]||c.tier}</span></td>
        <td>
          $${c.rolling_spend.toFixed(2)}
          <div class="progress-bar"><div class="progress-fill" style="width:${(progress*100).toFixed(0)}%;background:${pctColor};"></div></div>
        </td>
        <td style="color:${c.gap_to_next > 0 ? 'var(--amber)' : 'var(--green)'};">${c.gap_to_next > 0 ? '$'+c.gap_to_next.toFixed(2) : '—'}</td>
        <td style="color:${c.gap_to_maintain > 0 ? 'var(--red)' : 'var(--green)'};">${c.gap_to_maintain > 0 ? '$'+c.gap_to_maintain.toFixed(2) : '✓'}</td>
        <td>${c.orders||0}</td>
        <td style="color:${lockDays > 0 ? 'var(--green)' : 'var(--dim)'};">${lockStr}</td>
        <td style="white-space:nowrap;" onclick="event.stopPropagation();">
          <button class="btn-action" onclick="openTierModal('${c.id}','${esc(c.name)}','${c.tier}')" title="Set tier">✎</button>
          <button class="btn-action" onclick="recalculate('${c.id}','${esc(c.name)}')" title="Recalculate tier & lock">🔄</button>
        </td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
  // Pagination
  const pg = document.getElementById('pagination');
  pg.innerHTML = _hasNext ? '<button class="btn btn-secondary" onclick="loadCustomers(_cursor)">Load More →</button>' : '';
}

// Tier modal
function openTierModal(gid, name, currentTier) {
  _modalGid = gid;
  document.getElementById('tier-modal-title').textContent = 'Set Tier: ' + name;
  document.getElementById('modal-tier').value = currentTier;
  document.getElementById('tier-modal').classList.add('active');
}
function closeTierModal() { document.getElementById('tier-modal').classList.remove('active'); _modalGid = null; }
async function submitTierChange() {
  if (!_modalGid) return;
  const tier = document.getElementById('modal-tier').value;
  const days = parseInt(document.getElementById('modal-lock-days').value) || 90;
  try {
    const r = await fetch('/api/vip/set-tier', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ customer_id: _modalGid, tier, lock_days: days }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast(TIER_NAMES[tier] + ' set with ' + days + ' day lock');
    closeTierModal();
    loadCustomers();
  } catch(e) { alert(e.message); }
}

// Recalculate modal
let _recalcData = null;
function closeRecalcModal() { document.getElementById('recalc-modal').classList.remove('active'); _recalcData = null; }
async function recalculate(gid, name) {
  _modalGid = gid;
  _recalcData = null;
  document.getElementById('recalc-modal').classList.add('active');
  document.getElementById('recalc-content').innerHTML = '<div class="spinner"></div>';
  document.getElementById('recalc-actions').style.display = 'none';
  const id = gid.includes('/') ? gid.split('/').pop() : gid;
  try {
    const r = await fetch('/api/vip/recalculate', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ customer_id: gid }),
    });
    const d = await r.json();
    if (!r.ok) { document.getElementById('recalc-content').innerHTML = '<div style="color:var(--red);">' + (d.error||'Error') + '</div>'; return; }
    _recalcData = d;
    const c = d.current;
    const p = d.proposed;
    const changed = d.changed;
    document.getElementById('recalc-content').innerHTML = `
      <div style="margin-bottom:12px;font-weight:600;">${name}</div>
      <div class="recalc-row">
        <span class="recalc-label">Tier</span>
        <span>${changed && c.tier !== p.tier ? '<span class="recalc-old">' + (TIER_NAMES[c.tier]||c.tier) + '</span> → ' : ''}<span class="recalc-new">${TIER_NAMES[p.tier]||p.tier}</span></span>
      </div>
      <div class="recalc-row">
        <span class="recalc-label">90d Spend</span>
        <span>${changed && c.rolling !== p.rolling ? '<span class="recalc-old">$' + c.rolling.toFixed(2) + '</span> → ' : ''}<span class="recalc-new">$${p.rolling.toFixed(2)}</span></span>
      </div>
      <div class="recalc-row">
        <span class="recalc-label">Lock</span>
        <span>${p.lock ? p.lock.start + ' → ' + p.lock.end : 'No lock'}</span>
      </div>
      ${p.reason ? '<div style="margin-top:12px;font-size:0.82rem;color:var(--dim);font-style:italic;">' + p.reason + '</div>' : ''}
      ${!changed ? '<div style="color:var(--green);margin-top:12px;font-weight:600;">✓ No changes needed — current state is correct.</div>' : '<div style="color:var(--amber);margin-top:12px;font-weight:600;">⚠ Changes detected — review and apply below.</div>'}
    `;
    document.getElementById('recalc-actions').style.display = changed ? '' : 'none';
  } catch(e) { document.getElementById('recalc-content').innerHTML = '<div style="color:var(--red);">' + e.message + '</div>'; }
}
async function applyRecalc() {
  if (!_modalGid || !_recalcData) return;
  const p = _recalcData.proposed;
  const lockDays = p.lock ? Math.max(1, Math.ceil((new Date(p.lock.end) - new Date()) / 86400000)) : 90;
  try {
    const r = await fetch('/api/vip/set-tier', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ customer_id: _modalGid, tier: p.tier, lock_days: lockDays }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Recalculated: ' + (TIER_NAMES[p.tier]||p.tier));
    closeRecalcModal();
    loadCustomers();
  } catch(e) { alert(e.message); }
}

// Detail view
async function openDetail(gid) {
  document.getElementById('list-view').style.display = 'none';
  document.getElementById('detail-view').style.display = '';
  const el = document.getElementById('detail-content');
  el.innerHTML = '<div class="spinner"></div>';
  // Pass numeric ID or full GID
  const id = gid.includes('/') ? gid.split('/').pop() : gid;
  try {
    const r = await fetch('/api/vip/customer/' + id);
    const d = await r.json();
    if (!r.ok) { el.innerHTML = '<div style="color:var(--red);">' + (d.error||'Error') + '</div>'; return; }
    renderDetail(d);
  } catch(e) { el.innerHTML = '<div style="color:var(--red);">' + e.message + '</div>'; }
}

function closeDetail() {
  document.getElementById('detail-view').style.display = 'none';
  document.getElementById('list-view').style.display = '';
}

function renderDetail(d) {
  const c = d.customer;
  const v = d.vip;
  const orders = d.orders || [];
  const el = document.getElementById('detail-content');

  const tierCls = v.tier.toLowerCase();
  const lockStr = v.lock_days_remaining > 0 ? v.lock_days_remaining + ' days left (expires ' + (v.lock?.end||'') + ')' : 'No active lock';
  const nextThresh = v.next_threshold || v.threshold;
  const progress = nextThresh > 0 ? Math.min(1, v.rolling_spend / nextThresh) : 1;
  const pctColor = v.tier === 'VIP3' ? 'var(--vip3)' : v.tier === 'VIP2' ? 'var(--vip2)' : 'var(--vip1)';
  const memberSince = c.created_at ? new Date(c.created_at).toLocaleDateString() : '—';

  let html = `
    <div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:24px;">
      <div style="flex:1;min-width:280px;">
        <div style="font-size:1.4rem;font-weight:700;margin-bottom:4px;">${c.name || c.email}</div>
        <div style="color:var(--dim);font-size:0.85rem;line-height:1.6;">
          ${c.email ? '✉ ' + c.email + '<br>' : ''}
          ${c.phone ? '📱 ' + c.phone + '<br>' : ''}
          ${c.address && c.address !== ', ' ? '📍 ' + c.address + '<br>' : ''}
          Member since ${memberSince} · ${c.orders_count} orders
        </div>
        <div style="margin-top:12px;display:flex;gap:6px;">
          <button class="btn-action" style="width:auto;padding:0 12px;" onclick="openTierModal('${c.id}','${(c.name||'').replace(/'/g,"\\\\'")}','${v.tier}')">✎ Set Tier</button>
          <button class="btn-action" style="width:auto;padding:0 12px;" onclick="recalculate('${c.id}','${(c.name||'').replace(/'/g,"\\\\'")}')">🔄 Recalculate</button>
        </div>
      </div>
      <div style="flex:1;min-width:280px;">
        <div class="card" style="margin:0;">
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
            <span class="badge badge-${tierCls}" style="font-size:0.85rem;padding:6px 14px;">${TIER_NAMES[v.tier]||v.tier}</span>
          </div>
          <div style="font-size:0.82rem;line-height:1.8;">
            <strong>90-Day Spend:</strong> $${v.rolling_spend.toFixed(2)}
            <div class="progress-bar" style="width:200px;"><div class="progress-fill" style="width:${(progress*100).toFixed(0)}%;background:${pctColor};"></div></div>
            <br>
            ${v.next_tier ? '<strong>To ' + v.next_tier + ':</strong> $' + v.gap_to_next.toFixed(2) + ' more<br>' : '<strong>Top tier!</strong><br>'}
            <strong>To maintain ${v.tier}:</strong> ${v.gap_to_maintain > 0 ? '$' + v.gap_to_maintain.toFixed(2) + ' more' : '✓ Qualified'}
            <br>
            <strong>Lock:</strong> ${lockStr}
          </div>
        </div>
      </div>
    </div>

    <div style="font-size:0.75rem;color:var(--dim);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Recent Orders (${orders.length})</div>
    ${orders.length ? '<table><thead><tr><th>Order</th><th>Date</th><th>Total</th><th>Refunded</th><th>Net</th><th>Status</th><th>Fulfillment</th></tr></thead><tbody>' +
      orders.map(o => {
        const dt = new Date(o.created_at).toLocaleDateString();
        const fin = o.financial || '—';
        const ful = o.fulfillment || '—';
        return '<tr>' +
          '<td><strong>' + o.name + '</strong></td>' +
          '<td style="color:var(--dim);">' + dt + '</td>' +
          '<td>$' + o.total.toFixed(2) + '</td>' +
          '<td style="color:' + (o.refunded > 0 ? 'var(--red)' : 'var(--dim)') + ';">$' + o.refunded.toFixed(2) + '</td>' +
          '<td style="font-weight:600;">$' + o.net.toFixed(2) + '</td>' +
          '<td>' + fin + '</td>' +
          '<td>' + ful + '</td>' +
        '</tr>';
      }).join('') +
      '</tbody></table>' : '<div style="color:var(--dim);">No orders found</div>'}

    <div style="margin-top:12px;font-size:0.78rem;color:var(--dim);">
      Tags: ${(c.tags||[]).join(', ') || 'none'}
    </div>
  `;

  el.innerHTML = html;
}

loadStats();
loadCustomers();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
