"""
routes/ai_enrichment.py — AI Enrichment backfill console.

Scans Shopify products, generates agentic metadata via Claude API,
provides review/edit/approve UI, then pushes approved fields to Shopify.
"""

import os
import logging
import threading
from flask import Blueprint, request, jsonify, render_template_string

import db

logger = logging.getLogger(__name__)

bp = Blueprint("ai_enrichment", __name__)


def _ensure_table():
    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_enrichment_queue (
            id SERIAL PRIMARY KEY,
            product_gid TEXT NOT NULL UNIQUE,
            product_title TEXT NOT NULL,
            current_description_html TEXT,
            agentic_title TEXT,
            agentic_description TEXT,
            agentic_category TEXT,
            gtin TEXT,
            description_html TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            generated_at TIMESTAMPTZ,
            pushed_at TIMESTAMPTZ
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_aieq_status ON ai_enrichment_queue(status)")


# ─── API Routes ──────────────────────────────────────────────────────────────

@bp.route("/inventory/ai-enrichment")
def ai_enrichment_page():
    return render_template_string(PAGE_HTML)


@bp.route("/api/ai/scan", methods=["POST"])
def api_scan():
    """Pull all products from Shopify into the enrichment queue."""
    from shopify_graphql import shopify_gql

    cursor = None
    total_added = 0
    total_skipped = 0

    while True:
        after = f', after: "{cursor}"' if cursor else ""
        data = shopify_gql(f"""
            query {{
                products(first: 50, sortKey: TITLE{after}) {{
                    edges {{
                        cursor
                        node {{
                            id
                            title
                            descriptionHtml
                            tags
                            priceRangeV2 {{ minVariantPrice {{ amount }} }}
                        }}
                    }}
                    pageInfo {{ hasNextPage }}
                }}
            }}
        """)

        edges = data.get("data", {}).get("products", {}).get("edges", [])
        if not edges:
            break

        for edge in edges:
            node = edge["node"]
            product_gid = node["id"]
            title = node.get("title", "")
            desc_html = node.get("descriptionHtml", "") or ""

            # Upsert — skip if already pushed
            existing = db.query_one(
                "SELECT status FROM ai_enrichment_queue WHERE product_gid = %s", (product_gid,)
            )
            if existing and existing["status"] == "pushed":
                total_skipped += 1
                continue

            if not existing:
                db.execute(
                    """INSERT INTO ai_enrichment_queue (product_gid, product_title, current_description_html, status)
                       VALUES (%s, %s, %s, 'pending')
                       ON CONFLICT (product_gid) DO NOTHING""",
                    (product_gid, title, desc_html),
                )
                total_added += 1
            else:
                total_skipped += 1

        cursor = edges[-1]["cursor"]
        has_next = data.get("data", {}).get("products", {}).get("pageInfo", {}).get("hasNextPage", False)
        if not has_next:
            break

    return jsonify({"added": total_added, "skipped": total_skipped})


@bp.route("/api/ai/queue")
def api_queue():
    """List queue items with optional status filter and search."""
    status = request.args.get("status", "")
    q = request.args.get("q", "").strip().lower()
    limit = min(int(request.args.get("limit", "100")), 500)

    where = ["1=1"]
    params = []

    if status:
        where.append("status = %s")
        params.append(status)
    if q:
        where.append("LOWER(product_title) LIKE %s")
        params.append(f"%{q}%")

    rows = db.query(
        f"""SELECT id, product_gid, product_title, agentic_title, agentic_description,
                   agentic_category, gtin, description_html, status, error,
                   created_at, generated_at, pushed_at
            FROM ai_enrichment_queue
            WHERE {' AND '.join(where)}
            ORDER BY CASE status
                WHEN 'generated' THEN 1
                WHEN 'pending' THEN 2
                WHEN 'approved' THEN 3
                WHEN 'pushed' THEN 4
                WHEN 'skipped' THEN 5
                ELSE 6 END, product_title
            LIMIT %s""",
        (*params, limit),
    )

    # Get counts
    counts = {}
    for row in db.query("SELECT status, COUNT(*) as cnt FROM ai_enrichment_queue GROUP BY status"):
        counts[row["status"]] = row["cnt"]

    items = []
    for r in rows:
        d = dict(r)
        for k in ("created_at", "generated_at", "pushed_at"):
            d[k] = d[k].isoformat() if d.get(k) else None
        items.append(d)

    return jsonify({"items": items, "counts": counts})


@bp.route("/api/ai/generate/<int:item_id>", methods=["POST"])
def api_generate_one(item_id):
    """Generate AI fields for one queue item."""
    row = db.query_one("SELECT * FROM ai_enrichment_queue WHERE id = %s", (item_id,))
    if not row:
        return jsonify({"error": "not found"}), 404

    try:
        from ai_enrichment import generate_ai_fields
        fields = generate_ai_fields(
            product_title=row["product_title"],
        )
        db.execute(
            """UPDATE ai_enrichment_queue
               SET agentic_title = %s, agentic_description = %s, agentic_category = %s,
                   gtin = %s, description_html = %s, status = 'generated',
                   error = NULL, generated_at = NOW()
               WHERE id = %s""",
            (fields.get("agentic_title"), fields.get("agentic_description"),
             fields.get("agentic_category"), fields.get("gtin"),
             fields.get("description_html"), item_id),
        )
        return jsonify({"ok": True, "fields": fields})
    except Exception as e:
        db.execute(
            "UPDATE ai_enrichment_queue SET status = 'error', error = %s WHERE id = %s",
            (str(e)[:500], item_id),
        )
        return jsonify({"error": str(e)}), 500


@bp.route("/api/ai/generate-batch", methods=["POST"])
def api_generate_batch():
    """Generate AI fields for all pending items. Runs in background thread."""
    pending = db.query(
        "SELECT id FROM ai_enrichment_queue WHERE status IN ('pending', 'error') ORDER BY product_title"
    )
    count = len(pending)

    def _run_batch(ids):
        from ai_enrichment import generate_ai_fields
        import time
        for item_id in ids:
            row = db.query_one("SELECT * FROM ai_enrichment_queue WHERE id = %s", (item_id,))
            if not row:
                continue
            try:
                fields = generate_ai_fields(product_title=row["product_title"])
                db.execute(
                    """UPDATE ai_enrichment_queue
                       SET agentic_title = %s, agentic_description = %s, agentic_category = %s,
                           gtin = %s, description_html = %s, status = 'generated',
                           error = NULL, generated_at = NOW()
                       WHERE id = %s""",
                    (fields.get("agentic_title"), fields.get("agentic_description"),
                     fields.get("agentic_category"), fields.get("gtin"),
                     fields.get("description_html"), item_id),
                )
            except Exception as e:
                db.execute(
                    "UPDATE ai_enrichment_queue SET status = 'error', error = %s WHERE id = %s",
                    (str(e)[:500], item_id),
                )
            time.sleep(0.1)  # rate limit courtesy

    thread = threading.Thread(target=_run_batch, args=([r["id"] for r in pending],), daemon=True)
    thread.start()

    return jsonify({"ok": True, "queued": count, "message": f"Generating {count} items in background"})


@bp.route("/api/ai/item/<int:item_id>", methods=["PUT"])
def api_update_item(item_id):
    """Edit generated fields before approval."""
    data = request.get_json(silent=True) or {}
    updates = []
    params = []
    for field in ("agentic_title", "agentic_description", "agentic_category", "gtin", "description_html"):
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return jsonify({"error": "no fields to update"}), 400

    params.append(item_id)
    db.execute(f"UPDATE ai_enrichment_queue SET {', '.join(updates)} WHERE id = %s", tuple(params))
    return jsonify({"ok": True})


@bp.route("/api/ai/approve/<int:item_id>", methods=["POST"])
def api_approve_one(item_id):
    affected = db.execute(
        "UPDATE ai_enrichment_queue SET status = 'approved' WHERE id = %s AND status = 'generated'",
        (item_id,),
    )
    return jsonify({"ok": bool(affected)})


@bp.route("/api/ai/approve-batch", methods=["POST"])
def api_approve_batch():
    """Batch approve all generated items."""
    affected = db.execute(
        "UPDATE ai_enrichment_queue SET status = 'approved' WHERE status = 'generated'"
    )
    return jsonify({"ok": True, "approved": affected})


@bp.route("/api/ai/push-batch", methods=["POST"])
def api_push_batch():
    """Push all approved items to Shopify."""
    rows = db.query(
        "SELECT * FROM ai_enrichment_queue WHERE status = 'approved' ORDER BY product_title"
    )

    pushed = 0
    errors = 0

    from ai_enrichment import push_ai_fields

    for row in rows:
        fields = {
            "agentic_title": row["agentic_title"],
            "agentic_description": row["agentic_description"],
            "agentic_category": row["agentic_category"],
            "gtin": row["gtin"],
        }
        try:
            result = push_ai_fields(row["product_gid"], fields)
            if result.get("errors"):
                db.execute(
                    "UPDATE ai_enrichment_queue SET status = 'error', error = %s WHERE id = %s",
                    ("; ".join(result["errors"])[:500], row["id"]),
                )
                errors += 1
            else:
                db.execute(
                    "UPDATE ai_enrichment_queue SET status = 'pushed', pushed_at = NOW() WHERE id = %s",
                    (row["id"],),
                )
                pushed += 1
        except Exception as e:
            db.execute(
                "UPDATE ai_enrichment_queue SET status = 'error', error = %s WHERE id = %s",
                (str(e)[:500], row["id"]),
            )
            errors += 1

    return jsonify({"pushed": pushed, "errors": errors})


@bp.route("/api/ai/skip/<int:item_id>", methods=["POST"])
def api_skip_one(item_id):
    db.execute("UPDATE ai_enrichment_queue SET status = 'skipped' WHERE id = %s", (item_id,))
    return jsonify({"ok": True})


# ─── Console HTML ────────────────────────────────────────────────────────────

PAGE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI Enrichment · Pack Fresh</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.topbar { padding:16px 20px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
.topbar h1 { font-size:1.1rem; margin:0; }
.stats { display:flex; gap:8px; font-size:0.78rem; }
.stat { padding:3px 10px; border-radius:10px; background:var(--s2); }
.stat.pending { color:var(--amber); }
.stat.generated { color:var(--accent); }
.stat.approved { color:var(--green); }
.stat.pushed { color:var(--dim); }
.stat.error { color:var(--red); }
.controls { display:flex; gap:8px; margin-left:auto; }
.main { max-width:1100px; margin:0 auto; padding:16px; }
.filter-bar { display:flex; gap:8px; margin-bottom:14px; flex-wrap:wrap; align-items:center; }
.filter-bar input { padding:6px 10px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); font-size:0.85rem; }
.filter-bar select { padding:6px 10px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); font-size:0.85rem; }
.queue-item { margin-bottom:8px; }
.queue-row { display:flex; align-items:center; gap:10px; padding:10px 14px; cursor:pointer; }
.queue-row:hover { background:var(--s2); border-radius:8px; }
.queue-title { flex:1; font-weight:500; font-size:0.9rem; }
.queue-preview { flex:1; font-size:0.8rem; color:var(--dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.expand-pane { padding:12px 14px; background:var(--s2); border-radius:0 0 8px 8px; margin-top:-4px; display:none; }
.expand-pane.open { display:block; }
.field-group { margin-bottom:10px; }
.field-group label { font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.05em; display:block; margin-bottom:3px; }
.field-group input, .field-group textarea { width:100%; padding:6px 10px; background:var(--surface); border:1px solid var(--border); border-radius:6px; color:var(--text); font-size:0.85rem; font-family:inherit; }
.field-group textarea { min-height:60px; resize:vertical; }
.field-actions { display:flex; gap:6px; margin-top:8px; }
.progress-bar { height:4px; background:var(--s2); border-radius:2px; margin-bottom:14px; overflow:hidden; }
.progress-fill { height:100%; background:var(--green); transition:width 0.3s; }
</style>
</head>
<body>
<div class="topbar">
  <h1>AI Enrichment</h1>
  <div class="stats" id="stats"></div>
  <div class="controls">
    <a class="btn btn-secondary btn-sm" href="/inventory">Back to Inventory</a>
    <button class="btn btn-sm" onclick="scanProducts()">Scan Shopify</button>
    <button class="btn btn-sm" onclick="generateBatch()">Generate All</button>
    <button class="btn btn-sm btn-green" onclick="approveBatch()">Approve All Generated</button>
    <button class="btn btn-sm" style="background:var(--accent);color:#000;" onclick="pushBatch()">Push Approved</button>
  </div>
</div>

<div class="main">
  <div class="progress-bar" id="progress-bar" style="display:none;"><div class="progress-fill" id="progress-fill"></div></div>

  <div class="filter-bar">
    <input id="search" type="text" placeholder="Search products..." oninput="debounceLoad()">
    <select id="status-filter" onchange="loadQueue()">
      <option value="">All statuses</option>
      <option value="pending">Pending</option>
      <option value="generated">Generated</option>
      <option value="approved">Approved</option>
      <option value="pushed">Pushed</option>
      <option value="error">Error</option>
    </select>
  </div>

  <div id="queue"></div>
</div>

<script>
let _items = [];
let _timer;

function debounceLoad() {
  clearTimeout(_timer);
  _timer = setTimeout(loadQueue, 300);
}

async function loadQueue() {
  const q = document.getElementById('search').value.trim();
  const status = document.getElementById('status-filter').value;
  let url = '/api/ai/queue?limit=200';
  if (q) url += '&q=' + encodeURIComponent(q);
  if (status) url += '&status=' + status;

  const r = await fetch(url);
  const data = await r.json();
  _items = data.items;
  renderStats(data.counts);
  renderQueue(data.items);
}

function renderStats(counts) {
  const el = document.getElementById('stats');
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  el.innerHTML = [
    `<span class="stat">${total} total</span>`,
    counts.pending ? `<span class="stat pending">${counts.pending} pending</span>` : '',
    counts.generated ? `<span class="stat generated">${counts.generated} generated</span>` : '',
    counts.approved ? `<span class="stat approved">${counts.approved} approved</span>` : '',
    counts.pushed ? `<span class="stat pushed">${counts.pushed} pushed</span>` : '',
    counts.error ? `<span class="stat error">${counts.error} errors</span>` : '',
  ].filter(Boolean).join('');

  // Update progress bar
  if (total > 0) {
    const done = (counts.pushed || 0) + (counts.approved || 0);
    const pct = Math.round((done / total) * 100);
    document.getElementById('progress-bar').style.display = '';
    document.getElementById('progress-fill').style.width = pct + '%';
  }
}

function renderQueue(items) {
  const el = document.getElementById('queue');
  if (!items.length) {
    el.innerHTML = '<div style="text-align:center;color:var(--dim);padding:40px;">No items. Click "Scan Shopify" to load products.</div>';
    return;
  }

  el.innerHTML = items.map(item => `
    <div class="card queue-item" id="item-${item.id}">
      <div class="queue-row" onclick="toggleExpand(${item.id})">
        <span class="queue-title">${esc(item.product_title)}</span>
        <span class="queue-preview">${esc(item.agentic_title || '—')}</span>
        <span class="badge ${badgeClass(item.status)}">${item.status}</span>
        ${item.gtin ? '<span class="badge badge-blue" style="font-size:0.7rem;">GTIN</span>' : ''}
        ${item.status === 'pending' ? '<button class="btn btn-sm" onclick="event.stopPropagation();generateOne('+item.id+',this)">Generate</button>' : ''}
        ${item.status === 'error' ? '<button class="btn btn-sm" onclick="event.stopPropagation();generateOne('+item.id+',this)">Retry</button>' : ''}
      </div>
      <div class="expand-pane" id="expand-${item.id}">
        ${renderExpanded(item)}
      </div>
    </div>
  `).join('');
}

function renderExpanded(item) {
  if (item.status === 'pending') return '<div style="color:var(--dim);font-size:0.85rem;">Click Generate to create AI fields for this product.</div>';
  if (item.status === 'error') return '<div style="color:var(--red);font-size:0.85rem;">' + esc(item.error || 'Unknown error') + '</div>';

  return `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
      <div class="field-group">
        <label>Agentic Title</label>
        <input id="f-${item.id}-at" value="${esc(item.agentic_title || '')}">
      </div>
      <div class="field-group">
        <label>Agentic Category</label>
        <input id="f-${item.id}-ac" value="${esc(item.agentic_category || '')}">
      </div>
    </div>
    <div class="field-group">
      <label>Agentic Description</label>
      <textarea id="f-${item.id}-ad">${esc(item.agentic_description || '')}</textarea>
    </div>
    <div class="field-group">
      <label>GTIN / UPC</label>
      <input id="f-${item.id}-gtin" value="${esc(item.gtin || '')}" placeholder="Leave blank if unknown">
    </div>
    <div class="field-group">
      <label>Product Description (HTML) — for reference only, not pushed for existing products</label>
      <textarea id="f-${item.id}-desc" style="min-height:80px;">${esc(item.description_html || '')}</textarea>
    </div>
    <div class="field-actions">
      <button class="btn btn-sm" onclick="saveItem(${item.id})">Save Edits</button>
      ${item.status === 'generated' ? '<button class="btn btn-sm btn-green" onclick="approveOne('+item.id+')">Approve</button>' : ''}
      <button class="btn btn-sm" style="color:var(--dim);" onclick="skipOne(${item.id})">Skip</button>
    </div>
  `;
}

function badgeClass(status) {
  return {pending:'badge-amber', generated:'badge-blue', approved:'badge-green', pushed:'', error:'badge-red', skipped:''}[status] || '';
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

function toggleExpand(id) {
  const el = document.getElementById('expand-' + id);
  el.classList.toggle('open');
}

async function scanProducts() {
  if (!confirm('Scan all Shopify products into the enrichment queue?')) return;
  toast('Scanning Shopify products...', 'blue');
  const r = await fetch('/api/ai/scan', {method:'POST'});
  const d = await r.json();
  toast('Scanned: ' + d.added + ' added, ' + d.skipped + ' skipped', 'green');
  loadQueue();
}

async function generateOne(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = '...'; }
  const r = await fetch('/api/ai/generate/' + id, {method:'POST'});
  const d = await r.json();
  if (r.ok) {
    toast('Generated', 'green');
    loadQueue();
  } else {
    toast(d.error || 'Failed', 'red');
    if (btn) { btn.disabled = false; btn.textContent = 'Retry'; }
  }
}

async function generateBatch() {
  if (!confirm('Generate AI fields for all pending products? This runs in the background.')) return;
  const r = await fetch('/api/ai/generate-batch', {method:'POST'});
  const d = await r.json();
  toast(d.message, 'green');
  // Poll for updates
  const poll = setInterval(() => {
    loadQueue();
  }, 5000);
  setTimeout(() => clearInterval(poll), 600000);
}

async function saveItem(id) {
  const data = {
    agentic_title: document.getElementById('f-'+id+'-at').value,
    agentic_description: document.getElementById('f-'+id+'-ad').value,
    agentic_category: document.getElementById('f-'+id+'-ac').value,
    gtin: document.getElementById('f-'+id+'-gtin').value || null,
    description_html: document.getElementById('f-'+id+'-desc').value,
  };
  const r = await fetch('/api/ai/item/' + id, {
    method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)
  });
  if (r.ok) toast('Saved', 'green');
  else toast('Save failed', 'red');
}

async function approveOne(id) {
  await fetch('/api/ai/approve/' + id, {method:'POST'});
  toast('Approved', 'green');
  loadQueue();
}

async function approveBatch() {
  if (!confirm('Approve ALL generated items?')) return;
  const r = await fetch('/api/ai/approve-batch', {method:'POST'});
  const d = await r.json();
  toast('Approved ' + d.approved + ' items', 'green');
  loadQueue();
}

async function pushBatch() {
  if (!confirm('Push all approved items to Shopify? This writes metafields and GTINs.')) return;
  toast('Pushing to Shopify...', 'blue');
  const r = await fetch('/api/ai/push-batch', {method:'POST'});
  const d = await r.json();
  toast('Pushed ' + d.pushed + ', errors: ' + d.errors, d.errors ? 'red' : 'green');
  loadQueue();
}

async function skipOne(id) {
  await fetch('/api/ai/skip/' + id, {method:'POST'});
  loadQueue();
}

loadQueue();
</script>
</body>
</html>
"""
