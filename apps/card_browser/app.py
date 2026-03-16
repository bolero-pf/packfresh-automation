"""
card-browser — cards.pack-fresh.com
Internal kiosk browser for raw card inventory.

Reads raw_cards + storage_locations. No writes day 1.
Images served directly from TCGPlayer CDN (stable URLs, no caching needed).
"""

import os
import logging
from flask import Flask, request, jsonify, Response

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pack Fresh · Card Browser</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg:       #0b0d10;
  --surface:  #13161c;
  --surface2: #1a1e27;
  --border:   #252932;
  --accent:   #4fd1c5;
  --accent2:  #7c6af7;
  --green:    #34d058;
  --amber:    #f6ad55;
  --red:      #fc5c5c;
  --text:     #e8eaf0;
  --dim:      #6b7280;
  --mono:     'DM Mono', monospace;
  --sans:     'Syne', sans-serif;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; background: var(--bg); color: var(--text); font-family: var(--sans); }

/* ── Layout ── */
.shell { display: grid; grid-template-rows: auto 1fr; height: 100vh; overflow: hidden; }

header {
  display: flex; align-items: center; gap: 20px; padding: 14px 24px;
  background: var(--surface); border-bottom: 1px solid var(--border);
  flex-wrap: wrap;
}
.logo {
  font-size: 1.1rem; font-weight: 800; letter-spacing: -0.02em;
  color: var(--accent); white-space: nowrap;
}
.logo span { color: var(--dim); font-weight: 400; }

.search-bar {
  flex: 1; min-width: 200px; display: flex; gap: 8px;
}
.search-bar input {
  flex: 1; background: var(--surface2); border: 1px solid var(--border);
  color: var(--text); padding: 8px 14px; border-radius: 8px;
  font-family: var(--sans); font-size: 0.9rem; outline: none;
  transition: border-color 0.15s;
}
.search-bar input:focus { border-color: var(--accent); }

.filters { display: flex; gap: 8px; flex-wrap: wrap; }
select {
  background: var(--surface2); border: 1px solid var(--border);
  color: var(--text); padding: 7px 10px; border-radius: 8px;
  font-family: var(--sans); font-size: 0.82rem; outline: none; cursor: pointer;
}
select:focus { border-color: var(--accent); }

.stats-bar {
  display: flex; gap: 20px; padding: 0 8px; font-size: 0.78rem;
  color: var(--dim); white-space: nowrap; align-items: center;
}
.stats-bar strong { color: var(--accent); }

.main { display: grid; grid-template-columns: 1fr; overflow: hidden; }

/* ── Card grid ── */
.grid-wrap { overflow-y: auto; padding: 20px 24px; }
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 16px;
}

.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 12px; overflow: hidden; cursor: pointer;
  transition: border-color 0.15s, transform 0.1s;
  display: flex; flex-direction: column;
}
.card:hover { border-color: var(--accent); transform: translateY(-2px); }

.card-img {
  aspect-ratio: 2.5/3.5; background: var(--surface2);
  display: flex; align-items: center; justify-content: center;
  overflow: hidden; position: relative;
}
.card-img img {
  width: 100%; height: 100%; object-fit: contain;
  transition: transform 0.2s;
}
.card:hover .card-img img { transform: scale(1.04); }
.card-img .no-img {
  color: var(--dim); font-size: 2rem; opacity: 0.3;
}

.card-info { padding: 10px 12px 12px; flex: 1; display: flex; flex-direction: column; gap: 4px; }
.card-name { font-size: 0.88rem; font-weight: 700; line-height: 1.3; }
.card-set  { font-size: 0.72rem; color: var(--dim); }
.card-meta { display: flex; gap: 6px; align-items: center; margin-top: 4px; flex-wrap: wrap; }

.pill {
  font-size: 0.65rem; font-weight: 600; padding: 2px 7px;
  border-radius: 20px; letter-spacing: 0.04em; text-transform: uppercase;
}
.pill-nm   { background: rgba(52,208,88,0.15);  color: var(--green); }
.pill-lp   { background: rgba(246,173,85,0.15); color: var(--amber); }
.pill-mp   { background: rgba(246,173,85,0.2);  color: #f6ad55; }
.pill-hp   { background: rgba(252,92,92,0.15);  color: var(--red); }
.pill-dmg  { background: rgba(252,92,92,0.25);  color: var(--red); }
.pill-grade { background: rgba(124,106,247,0.2); color: var(--accent2); }
.pill-bin  { background: rgba(79,209,197,0.12); color: var(--accent); font-family: var(--mono); }

.card-price {
  margin-top: auto; padding-top: 8px;
  font-size: 1rem; font-weight: 700; color: var(--accent);
  font-family: var(--mono);
}

/* ── Detail panel (slide in) ── */
.detail-overlay {
  display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7);
  z-index: 100; align-items: center; justify-content: center;
}
.detail-overlay.active { display: flex; }

.detail-panel {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 16px; width: min(600px, 95vw); max-height: 90vh;
  overflow-y: auto; padding: 28px;
  animation: slideUp 0.2s ease;
}
@keyframes slideUp {
  from { opacity: 0; transform: translateY(20px); }
  to   { opacity: 1; transform: translateY(0); }
}

.detail-header { display: flex; gap: 20px; margin-bottom: 20px; }
.detail-img {
  width: 140px; min-width: 140px; aspect-ratio: 2.5/3.5;
  border-radius: 8px; overflow: hidden; background: var(--surface2);
}
.detail-img img { width: 100%; height: 100%; object-fit: contain; }

.detail-title { font-size: 1.2rem; font-weight: 800; margin-bottom: 4px; }
.detail-set   { font-size: 0.85rem; color: var(--dim); margin-bottom: 12px; }

.detail-grid {
  display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 16px;
}
.detail-field label {
  display: block; font-size: 0.65rem; color: var(--dim);
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 3px;
}
.detail-field span {
  font-size: 0.9rem; font-weight: 600;
}

.barcode-section {
  border-top: 1px solid var(--border); padding-top: 16px; margin-top: 4px;
}
.barcode-code {
  font-family: var(--mono); font-size: 0.8rem; color: var(--dim);
  background: var(--surface2); padding: 6px 10px; border-radius: 6px;
  display: inline-block; margin-bottom: 10px;
}
.btn {
  padding: 9px 18px; border-radius: 8px; border: none; cursor: pointer;
  font-family: var(--sans); font-size: 0.85rem; font-weight: 600;
  transition: opacity 0.15s;
}
.btn:hover { opacity: 0.85; }
.btn-primary  { background: var(--accent);  color: #000; }
.btn-secondary { background: var(--surface2); color: var(--text); border: 1px solid var(--border); }
.btn-close { float: right; background: none; border: none; color: var(--dim); font-size: 1.3rem; cursor: pointer; padding: 0; line-height: 1; }

/* ── Pagination ── */
.pagination {
  display: flex; gap: 8px; align-items: center; justify-content: center;
  padding: 20px 0 8px;
}
.pg-btn {
  background: var(--surface2); border: 1px solid var(--border); color: var(--text);
  padding: 6px 14px; border-radius: 8px; cursor: pointer; font-size: 0.85rem;
  font-family: var(--sans);
}
.pg-btn.active { background: var(--accent); color: #000; border-color: var(--accent); }
.pg-btn:disabled { opacity: 0.35; cursor: default; }
.pg-info { font-size: 0.82rem; color: var(--dim); }

/* ── Empty / loading ── */
.empty {
  text-align: center; padding: 80px 20px; color: var(--dim);
}
.empty .icon { font-size: 3rem; margin-bottom: 12px; opacity: 0.3; }
.spinner {
  display: inline-block; width: 28px; height: 28px;
  border: 3px solid var(--border); border-top-color: var(--accent);
  border-radius: 50%; animation: spin 0.7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="shell">
  <header>
    <div class="logo">Pack Fresh <span>· Cards</span></div>

    <div class="search-bar">
      <input type="text" id="q" placeholder="Search cards…" oninput="debounceSearch()" onkeydown="if(event.key==='Enter') doSearch()">
    </div>

    <div class="filters">
      <select id="filter-condition" onchange="doSearch()">
        <option value="">All Conditions</option>
        <option>NM</option><option>LP</option><option>MP</option>
        <option>HP</option><option>DMG</option>
      </select>
      <select id="filter-bin" onchange="doSearch()">
        <option value="">All Bins</option>
      </select>
      <select id="filter-state" onchange="doSearch()">
        <option value="STORED">In Stock</option>
        <option value="PULLED">Pulled</option>
        <option value="PENDING_SALE">Pending Sale</option>
        <option value="">All States</option>
      </select>
    </div>

    <div class="stats-bar" id="stats-bar">
      <span>Loading…</span>
    </div>
  </header>

  <div class="main">
    <div class="grid-wrap">
      <div id="grid" class="grid"></div>
      <div id="pagination" class="pagination" style="display:none;"></div>
    </div>
  </div>
</div>

<!-- Detail panel -->
<div class="detail-overlay" id="detail-overlay" onclick="closeDetail(event)">
  <div class="detail-panel" id="detail-panel"></div>
</div>

<script>
let _currentPage  = 1;
let _debounceTimer = null;

// ── Boot ────────────────────────────────────────────────────────────────────
async function boot() {
  await Promise.all([loadStats(), loadBins(), doSearch()]);
}

async function loadStats() {
  try {
    const d = await (await fetch('/api/stats')).json();
    document.getElementById('stats-bar').innerHTML =
      `<span><strong>${d.stored||0}</strong> in stock</span>` +
      `<span><strong>${d.pulled||0}</strong> pulled</span>` +
      (d.total_value ? `<span>Value <strong>$${parseFloat(d.total_value).toFixed(0)}</strong></span>` : '');
  } catch(e) {}
}

async function loadBins() {
  try {
    const d = await (await fetch('/api/bins')).json();
    const sel = document.getElementById('filter-bin');
    (d.bins||[]).forEach(b => {
      const o = document.createElement('option');
      o.value = b.bin_label;
      o.textContent = `${b.bin_label} (${b.current_count}/${b.capacity})`;
      sel.appendChild(o);
    });
  } catch(e) {}
}

// ── Search ───────────────────────────────────────────────────────────────────
function debounceSearch() {
  clearTimeout(_debounceTimer);
  _debounceTimer = setTimeout(doSearch, 350);
}

async function doSearch(page) {
  _currentPage = page || 1;
  const params = new URLSearchParams({
    q:         document.getElementById('q').value.trim(),
    condition: document.getElementById('filter-condition').value,
    bin:       document.getElementById('filter-bin').value,
    state:     document.getElementById('filter-state').value,
    page:      _currentPage,
  });

  const grid = document.getElementById('grid');
  grid.innerHTML = '<div class="empty"><div class="spinner"></div></div>';
  document.getElementById('pagination').style.display = 'none';

  try {
    const d = await (await fetch('/api/cards?' + params)).json();
    renderGrid(d.cards || []);
    renderPagination(d.page, d.pages, d.total);
  } catch(e) {
    grid.innerHTML = `<div class="empty"><div class="icon">⚠</div><p>${e.message}</p></div>`;
  }
}

// ── Render ───────────────────────────────────────────────────────────────────
const COND_PILL = { NM:'pill-nm', LP:'pill-lp', MP:'pill-mp', HP:'pill-hp', DMG:'pill-dmg' };

function renderGrid(cards) {
  const grid = document.getElementById('grid');
  if (!cards.length) {
    grid.innerHTML = '<div class="empty"><div class="icon">🃏</div><p>No cards found</p></div>';
    return;
  }
  grid.innerHTML = cards.map(c => {
    const condPill = c.condition
      ? `<span class="pill ${COND_PILL[c.condition]||'pill-nm'}">${c.condition}</span>` : '';
    const gradePill = c.is_graded
      ? `<span class="pill pill-grade">${c.grade_company} ${c.grade_value}</span>` : '';
    const binPill = c.bin_label
      ? `<span class="pill pill-bin">${c.bin_label}</span>` : '';
    const img = c.image_url
      ? `<img src="${esc(c.image_url)}" alt="${esc(c.card_name)}" loading="lazy">`
      : `<div class="no-img">🃏</div>`;
    const price = c.current_price ? `$${c.current_price.toFixed(2)}` : '';

    return `<div class="card" onclick="openDetail('${esc(c.barcode)}')">
      <div class="card-img">${img}</div>
      <div class="card-info">
        <div class="card-name">${esc(c.card_name)}</div>
        <div class="card-set">${esc(c.set_name||'')}${c.card_number?' #'+c.card_number:''}</div>
        <div class="card-meta">${condPill}${gradePill}${binPill}</div>
        ${price ? `<div class="card-price">${price}</div>` : ''}
      </div>
    </div>`;
  }).join('');
}

function renderPagination(page, pages, total) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.style.display = 'none'; return; }
  el.style.display = 'flex';

  let html = `<button class="pg-btn" onclick="doSearch(${page-1})" ${page<=1?'disabled':''}>←</button>`;

  const show = new Set([1, pages, page-1, page, page+1].filter(p => p>=1 && p<=pages));
  let prev = 0;
  [...show].sort((a,b)=>a-b).forEach(p => {
    if (p - prev > 1) html += `<span class="pg-info">…</span>`;
    html += `<button class="pg-btn${p===page?' active':''}" onclick="doSearch(${p})">${p}</button>`;
    prev = p;
  });

  html += `<button class="pg-btn" onclick="doSearch(${page+1})" ${page>=pages?'disabled':''}>→</button>`;
  html += `<span class="pg-info">${total} cards</span>`;
  el.innerHTML = html;
}

// ── Detail panel ─────────────────────────────────────────────────────────────
async function openDetail(barcode) {
  const overlay = document.getElementById('detail-overlay');
  const panel   = document.getElementById('detail-panel');
  panel.innerHTML = '<div style="text-align:center;padding:40px"><div class="spinner"></div></div>';
  overlay.classList.add('active');

  try {
    const c = await (await fetch('/api/cards/' + barcode)).json();
    const img = c.image_url
      ? `<img src="${esc(c.image_url)}" alt="${esc(c.card_name)}">`
      : '<div style="color:var(--dim);text-align:center;padding:40px;font-size:2rem;opacity:.3">🃏</div>';

    const gradeLine = c.is_graded
      ? `<div class="detail-field"><label>Grade</label><span>${esc(c.grade_company||'')} ${esc(c.grade_value||'')}</span></div>` : '';

    panel.innerHTML = `
      <button class="btn-close" onclick="closeDetail()">×</button>
      <div class="detail-header">
        <div class="detail-img">${img}</div>
        <div style="flex:1">
          <div class="detail-title">${esc(c.card_name)}</div>
          <div class="detail-set">${esc(c.set_name||'')}${c.card_number?' · #'+c.card_number:''}</div>
          <div style="display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap;">
            ${c.condition?`<span class="pill ${COND_PILL[c.condition]||'pill-nm'}">${c.condition}</span>`:''}
            ${c.is_graded?`<span class="pill pill-grade">${c.grade_company} ${c.grade_value}</span>`:''}
            ${c.rarity?`<span class="pill" style="background:var(--surface2);color:var(--dim)">${esc(c.rarity)}</span>`:''}
          </div>
          <div style="font-size:1.4rem;font-weight:800;color:var(--accent);font-family:var(--mono);">
            ${c.current_price ? '$'+c.current_price.toFixed(2) : '—'}
          </div>
        </div>
      </div>

      <div class="detail-grid">
        <div class="detail-field"><label>Location</label>
          <span style="color:var(--accent);font-family:var(--mono);">${esc(c.bin_label||'Unassigned')}</span></div>
        <div class="detail-field"><label>State</label>
          <span style="color:${c.state==='STORED'?'var(--green)':c.state==='PULLED'?'var(--amber)':'var(--dim)'};">${c.state||'—'}</span></div>
        <div class="detail-field"><label>Cost Basis</label>
          <span>${c.cost_basis?'$'+c.cost_basis.toFixed(2):'—'}</span></div>
        <div class="detail-field"><label>Language</label>
          <span>${esc(c.language||'EN')}</span></div>
        ${gradeLine}
        ${c.variant?`<div class="detail-field"><label>Variant</label><span>${esc(c.variant)}</span></div>`:''}
      </div>

      <div class="barcode-section">
        <div class="barcode-code">${esc(c.barcode)}</div><br>
        <button class="btn btn-primary" style="margin-right:8px;" onclick="printLabel('${esc(c.barcode)}')">🖨 Print Label</button>
        <button class="btn btn-secondary" onclick="closeDetail()">Close</button>
      </div>
    `;
  } catch(e) {
    panel.innerHTML = `<div class="empty"><p>Failed to load card: ${e.message}</p></div>`;
  }
}

function closeDetail(event) {
  if (event && event.target !== document.getElementById('detail-overlay')) return;
  document.getElementById('detail-overlay').classList.remove('active');
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') document.getElementById('detail-overlay').classList.remove('active');
});

function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

boot();

function printLabel(barcodeId) {
    const win = window.open('', '_blank');
    win.document.write(`<html><head><style>@media print { @page { margin:0; size: 62mm auto; } body { margin:0; } }</style></head><body><img src="/barcode/${barcodeId}.png" style="width:100%;" onload="window.print();window.close()"></body></html>`);
    win.document.close();
}
</script>
</body>
</html>
"""

logger = logging.getLogger(__name__)

app = Flask(__name__)

PAGE_SIZE = 48


@app.route("/")
def index():
    from flask import Response
    return Response(INDEX_HTML, mimetype="text/html")


@app.route("/api/cards")
def list_cards():
    """
    Search/filter raw cards.
    Query params: q, set, condition, bin, state, page
    """
    q         = (request.args.get("q") or "").strip()
    set_name  = (request.args.get("set") or "").strip()
    condition = (request.args.get("condition") or "").strip().upper()
    bin_label = (request.args.get("bin") or "").strip().upper()
    state     = (request.args.get("state") or "STORED").strip().upper()
    page      = max(1, int(request.args.get("page", 1)))
    offset    = (page - 1) * PAGE_SIZE

    filters = ["rc.state = %s"]
    params  = [state]

    if q:
        filters.append("(rc.card_name ILIKE %s OR rc.set_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    if set_name:
        filters.append("rc.set_name ILIKE %s")
        params.append(f"%{set_name}%")
    if condition:
        filters.append("rc.condition = %s")
        params.append(condition)
    if bin_label:
        filters.append("sl.bin_label ILIKE %s")
        params.append(f"%{bin_label}%")

    where = " AND ".join(filters)

    count_row = db.query_one(f"""
        SELECT COUNT(*) AS total
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE {where}
    """, tuple(params))
    total = count_row["total"] if count_row else 0

    cards = db.query(f"""
        SELECT
            rc.id, rc.barcode, rc.card_name, rc.set_name,
            rc.card_number, rc.condition, rc.rarity,
            rc.is_graded, rc.grade_company, rc.grade_value,
            rc.variant, rc.language,
            rc.state, rc.current_price, rc.cost_basis,
            rc.image_url,
            sl.bin_label, sl.card_type,
            rc.created_at
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE {where}
        ORDER BY rc.created_at DESC
        LIMIT %s OFFSET %s
    """, tuple(params) + (PAGE_SIZE, offset))

    return jsonify({
        "cards":    [_serialize(c) for c in cards],
        "total":    total,
        "page":     page,
        "pages":    max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        "per_page": PAGE_SIZE,
    })


@app.route("/api/cards/<barcode>")
def get_card(barcode):
    card = db.query_one("""
        SELECT rc.*, sl.bin_label, sl.card_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_serialize(card))


@app.route("/api/bins")
def list_bins():
    """Bin occupancy summary — for filter UI."""
    card_type = request.args.get("card_type")
    if card_type:
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
            FROM storage_locations sl
            WHERE sl.card_type = %s AND sl.current_count > 0
            ORDER BY sl.bin_label ASC
        """, (card_type,))
    else:
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
            FROM storage_locations sl
            WHERE sl.current_count > 0
            ORDER BY sl.bin_label ASC
        """)
    return jsonify({"bins": [dict(r) for r in rows]})


@app.route("/api/sets")
def list_sets():
    """Distinct set names for filter UI."""
    rows = db.query("""
        SELECT DISTINCT set_name FROM raw_cards
        WHERE state = 'STORED' AND set_name IS NOT NULL
        ORDER BY set_name ASC
        LIMIT 200
    """)
    return jsonify({"sets": [r["set_name"] for r in rows]})


@app.route("/api/stats")
def stats():
    row = db.query_one("""
        SELECT
            COUNT(*) FILTER (WHERE state='STORED')        AS stored,
            COUNT(*) FILTER (WHERE state='PULLED')        AS pulled,
            COUNT(*) FILTER (WHERE state='PENDING_SALE')  AS pending_sale,
            COUNT(*) FILTER (WHERE state='REMOVED')       AS removed,
            SUM(current_price) FILTER (WHERE state='STORED') AS total_value
        FROM raw_cards
    """)
    return jsonify(dict(row) if row else {})


def _serialize(row) -> dict:
    d = dict(row)
    # Convert decimals/dates to JSON-safe types
    for k in ("current_price", "cost_basis"):
        if d.get(k) is not None:
            d[k] = float(d[k])
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    return d


@app.route("/barcode/<barcode_id>.png")
def get_barcode(barcode_id):
    """Generate barcode label PNG for a raw card — reprintable at any time."""
    try:
        from barcode_gen import generate_barcode_image
    except ImportError:
        return "barcode_gen not available", 503

    card = db.query_one("""
        SELECT card_name, set_name, condition, current_price
        FROM raw_cards WHERE barcode = %s
    """, (barcode_id,))

    if not card:
        return "Barcode not found", 404

    png = generate_barcode_image(
        barcode_id,
        card_name=card["card_name"],
        set_name=card["set_name"],
        condition=card.get("condition", ""),
        price=f"${float(card['current_price']):.2f}" if card.get("current_price") else "",
    )

    from flask import Response
    return Response(png, mimetype="image/png",
                    headers={"Content-Disposition": f'inline; filename="{barcode_id}.png"'})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5004)), debug=False)
