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


@app.route("/api/vip/customers")
def api_customers():
    """List VIP customers by tier."""
    tier = request.args.get("tier", "").upper()
    page_size = int(request.args.get("limit", 50))

    from shopify_graphql import shopify_gql

    if tier and tier in ("VIP1", "VIP2", "VIP3"):
        query_str = f'tag:"{tier}"'
    else:
        query_str = 'tag:"VIP1" OR tag:"VIP2" OR tag:"VIP3"'

    q = request.args.get("q", "").strip()
    if q:
        query_str = f'({query_str}) AND ({q})'

    data = shopify_gql("""
        query($first:Int!, $q:String!) {
          customers(first:$first, query:$q, sortKey:UPDATED_AT, reverse:true) {
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
          }
        }
    """, {"first": page_size, "q": query_str})

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

    return jsonify({"customers": customers})


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
  </div>

  <div id="customer-list"><div class="spinner"></div></div>
</div>

<script>
let _tier = '', _timer = null;
const TIER_NAMES = {VIP1:'Adventurer', VIP2:'Guardian', VIP3:'Champion'};
const TIER_THRESH = {VIP0:0, VIP1:500, VIP2:1250, VIP3:2500};

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
        <div class="stat-label">VIP1</div>
        <div class="stat-val" style="color:var(--vip1);">${s.VIP1||0}</div>
        <div class="tier-name" style="color:var(--vip1);">Adventurer</div>
      </div>
      <div class="stat ${_tier==='VIP2'?'active':''}" onclick="filterTier('VIP2')">
        <div class="stat-label">VIP2</div>
        <div class="stat-val" style="color:var(--vip2);">${s.VIP2||0}</div>
        <div class="tier-name" style="color:var(--vip2);">Guardian</div>
      </div>
      <div class="stat ${_tier==='VIP3'?'active':''}" onclick="filterTier('VIP3')">
        <div class="stat-label">VIP3</div>
        <div class="stat-val" style="color:var(--vip3);">${s.VIP3||0}</div>
        <div class="tier-name" style="color:var(--vip3);">Champion</div>
      </div>
    `;
  } catch(e) {}
}

function filterTier(t) { _tier = t; loadStats(); loadCustomers(); }
function debounce() { clearTimeout(_timer); _timer = setTimeout(loadCustomers, 400); }

async function loadCustomers() {
  const el = document.getElementById('customer-list');
  el.innerHTML = '<div class="spinner"></div>';
  const q = document.getElementById('search').value.trim();
  const params = new URLSearchParams({ tier: _tier, limit: 50 });
  if (q) params.set('q', q);
  try {
    const r = await fetch('/api/vip/customers?' + params);
    const d = await r.json();
    const custs = d.customers || [];
    if (!custs.length) { el.innerHTML = '<div style="color:var(--dim);text-align:center;padding:30px;">No customers found</div>'; return; }
    el.innerHTML = `<table>
      <thead><tr><th>Customer</th><th>Tier</th><th>90d Spend</th><th>Gap to Next</th><th>To Maintain</th><th>Orders</th><th>Lock</th></tr></thead>
      <tbody>${custs.map(c => {
        const tierCls = c.tier.toLowerCase();
        const lockEnd = c.lock?.end || '';
        const lockDays = lockEnd ? Math.max(0, Math.ceil((new Date(lockEnd) - new Date()) / 86400000)) : 0;
        const lockStr = lockEnd ? lockDays + 'd left' : '—';
        const nextThresh = TIER_THRESH[{VIP0:'VIP1',VIP1:'VIP2',VIP2:'VIP3',VIP3:'VIP3'}[c.tier]] || 0;
        const progress = nextThresh > 0 ? Math.min(1, c.rolling_spend / nextThresh) : 1;
        const pctColor = c.tier === 'VIP3' ? 'var(--vip3)' : c.tier === 'VIP2' ? 'var(--vip2)' : 'var(--vip1)';
        return `<tr>
          <td>
            <strong>${c.name || c.email || '—'}</strong>
            <br><small style="color:var(--dim);">${c.email || ''}</small>
          </td>
          <td><span class="badge badge-${tierCls}">${c.tier} ${TIER_NAMES[c.tier]||''}</span></td>
          <td>
            $${c.rolling_spend.toFixed(2)}
            <div class="progress-bar"><div class="progress-fill" style="width:${(progress*100).toFixed(0)}%;background:${pctColor};"></div></div>
          </td>
          <td style="color:${c.gap_to_next > 0 ? 'var(--amber)' : 'var(--green)'};">${c.gap_to_next > 0 ? '$'+c.gap_to_next.toFixed(2) : '—'}</td>
          <td style="color:${c.gap_to_maintain > 0 ? 'var(--red)' : 'var(--green)'};">${c.gap_to_maintain > 0 ? '$'+c.gap_to_maintain.toFixed(2) : '✓'}</td>
          <td>${c.orders||0}</td>
          <td style="color:${lockDays > 0 ? 'var(--green)' : 'var(--dim)'};">${lockStr}</td>
        </tr>`;
      }).join('')}</tbody>
    </table>`;
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

loadStats();
loadCustomers();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
