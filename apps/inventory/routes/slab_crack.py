"""
inventory/routes/slab_crack.py

Inventory-side surface for the slab-crack candidates feature. The compute
& execute logic lives in shared/slab_crack.py and is exposed via the
shared breakdown blueprint at /inventory/breakdown/api/cache/slab-crack/*.

This module renders the operator-facing candidate list page — slabs whose
raw equivalent at the grade-mapped condition is worth more than the slab
listing. CGC 9 pile lights up here.
"""

import os
import logging
from functools import wraps
from flask import Blueprint, request, jsonify, Response, g

import db

logger = logging.getLogger(__name__)

bp = Blueprint("slab_crack", __name__, url_prefix="/inventory/slab-crack")

INVENTORY_USER = os.getenv("INVENTORY_USER", "admin")
INVENTORY_PASS = os.getenv("INVENTORY_PASS", "secret")


def _requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, 'user', None):
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != INVENTORY_USER or auth.password != INVENTORY_PASS:
            return Response("Unauthorized", 401,
                            {"WWW-Authenticate": 'Basic realm="Inventory"'})
        return f(*args, **kwargs)
    return decorated


def _delete_shopify_product(product_id: int) -> None:
    """Delete a Shopify product via the shared client."""
    import app as _app
    if _app.shopify_client is None:
        raise RuntimeError("Shopify client not initialized")
    # _rest will raise_for_status; DELETE returns 200 with {} or 404 if gone
    try:
        _app.shopify_client._rest("DELETE", f"/products/{int(product_id)}.json")
    except Exception as e:
        # If product is already gone (404), treat as success — operator
        # already delisted manually.
        msg = str(e).lower()
        if "404" not in msg and "not found" not in msg:
            raise


@bp.route("/")
@_requires_auth
def page():
    """Operator-facing slab-crack candidates page. Lists slabs where the
    raw market at the grade-mapped condition beats the slab listing by
    at least $min_delta."""
    min_delta = request.args.get("min_delta", "1")
    return Response(PAGE_HTML.replace("__MIN_DELTA__", min_delta),
                    mimetype="text/html")


@bp.route("/api/execute/<raw_card_id>", methods=["POST"])
@_requires_auth
def execute(raw_card_id):
    """Execute the crack for a single slab. Thin proxy that wires
    the local shopify_delete into shared/slab_crack.execute_slab_crack."""
    from slab_crack import execute_slab_crack
    data = request.get_json(silent=True) or {}
    target_condition = (data.get("condition") or "").strip().upper() or None
    operator = (getattr(g, "user", None) or {}).get("email") if getattr(g, "user", None) else None
    try:
        result = execute_slab_crack(
            raw_card_id, db,
            target_condition=target_condition,
            delete_shopify=_delete_shopify_product,
            operator=operator,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        logger.exception(f"Slab crack execute failed for {raw_card_id}")
        return jsonify({"error": str(e)}), 500
    return jsonify(result)


PAGE_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crack the Slab — Candidates</title>
<style>
  :root {
    --bg:#0b0f17; --surface:#121826; --surface-2:#1a2233; --border:#2a3447;
    --text:#e6edf7; --muted:#8b97ad; --accent:#4f7df9; --green:#34d399;
    --amber:#fbbf24; --red:#f87171; --purple:#c084fc;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:18px;}
  h1{margin:0 0 4px;font-size:1.45rem}
  .sub{color:var(--muted);font-size:.85rem;margin-bottom:16px;max-width:780px}
  .topnav{margin-bottom:14px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  .topnav a{color:var(--accent);text-decoration:none;font-size:.85rem;padding:4px 8px;border-radius:4px;border:1px solid var(--border)}
  .topnav a:hover{background:var(--surface-2)}
  .controls{display:flex;gap:10px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
  .controls label{color:var(--muted);font-size:.85rem}
  input[type=number]{background:var(--surface);color:var(--text);border:1px solid var(--border);
    border-radius:6px;padding:6px 10px;width:80px;font:inherit}
  button{background:var(--accent);color:#fff;border:0;border-radius:6px;
         padding:8px 14px;font-weight:600;cursor:pointer;font-size:.88rem}
  button:hover{filter:brightness(1.1)}
  button:disabled{opacity:.5;cursor:not-allowed}
  button.secondary{background:var(--surface-2);color:var(--text);border:1px solid var(--border)}
  button.danger{background:var(--red)}
  .stat{display:inline-block;background:var(--surface);border:1px solid var(--border);
        border-radius:8px;padding:8px 12px;margin-right:10px}
  .stat .v{font-weight:700;font-size:1.1rem}
  .stat .l{color:var(--muted);font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;margin-left:6px}
  table{width:100%;border-collapse:collapse;background:var(--surface);
        border:1px solid var(--border);border-radius:8px;overflow:hidden;font-size:.85rem}
  th,td{padding:8px 10px;border-bottom:1px solid var(--border);text-align:left;vertical-align:middle}
  th{background:var(--surface-2);color:var(--muted);font-weight:600;
     text-transform:uppercase;font-size:.7rem;letter-spacing:.05em;cursor:pointer;user-select:none}
  th.sortable:hover{color:var(--text)}
  tr:last-child td{border-bottom:0}
  tr:hover{background:rgba(79,125,249,.05)}
  td.num{text-align:right;font-variant-numeric:tabular-nums}
  .img{width:42px;height:58px;object-fit:cover;border-radius:4px;background:var(--surface-2)}
  .name{font-weight:600;color:var(--text)}
  .name small{display:block;color:var(--muted);font-weight:400;font-size:.78rem;margin-top:2px}
  .delta-pos{color:var(--green);font-weight:600}
  .delta-neg{color:var(--red)}
  .pill{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.7rem;font-weight:600;
        background:var(--surface-2);color:var(--muted)}
  .pill.psa{background:rgba(248,113,113,.16);color:#ff9a9a}
  .pill.cgc{background:rgba(79,125,249,.16);color:#7aadff}
  .pill.bgs{background:rgba(192,132,252,.16);color:var(--purple)}
  .pill.sgc{background:rgba(52,211,153,.16);color:var(--green)}
  .pill.nm{background:rgba(52,211,153,.16);color:var(--green)}
  .pill.lp{background:rgba(79,125,249,.16);color:#7aadff}
  .pill.mp{background:rgba(251,191,36,.16);color:var(--amber)}
  .pill.hp,.pill.dmg{background:rgba(248,113,113,.16);color:#ff9a9a}
  .loading,.empty{padding:40px;text-align:center;color:var(--muted)}
  .toast{position:fixed;bottom:24px;right:24px;padding:12px 18px;border-radius:8px;
         font-weight:600;z-index:9999;background:var(--green);color:#000;
         box-shadow:0 4px 20px rgba(0,0,0,.4)}
  .toast.err{background:var(--red);color:#fff}
  /* Modal */
  .modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.65);display:none;
            align-items:center;justify-content:center;z-index:1000}
  .modal-bg.show{display:flex}
  .modal{background:var(--surface);border:1px solid var(--border);border-radius:10px;
         padding:22px;max-width:560px;width:90%;max-height:90vh;overflow:auto}
  .modal h2{margin:0 0 6px;font-size:1.15rem}
  .modal .sub{margin-bottom:14px}
  .modal-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);font-size:.88rem}
  .modal-row:last-of-type{border-bottom:0}
  .modal-row .v{font-weight:600}
  .modal-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:18px}
  .grade-overrides{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
  .grade-overrides button{padding:4px 10px;font-size:.78rem}
  .grade-overrides button.active{background:var(--green);color:#000}
</style>
</head><body>
<h1>Crack the Slab</h1>
<div class="sub">
  Live slabs (STORED + DISPLAY) where the raw card at the grade-mapped condition
  is worth more than the slab listing. Default mapping: 8-10&nbsp;→&nbsp;NM,
  6-7&nbsp;→&nbsp;LP, 4-5&nbsp;→&nbsp;MP, 2-3&nbsp;→&nbsp;HP, 1&nbsp;→&nbsp;DMG.
  Crack queues the new raw card in <strong>Return Queue</strong> so the
  operator prints the label and scans it into storage.
</div>

<div class="topnav">
  <a href="/inventory">← Inventory</a>
  <a href="/inventory/breakdown/">Breakdown engine</a>
</div>

<div class="controls">
  <label>Min delta $</label>
  <input type="number" id="min-delta" min="0" step="0.5" value="__MIN_DELTA__">
  <button onclick="load()">Apply</button>
  <span style="flex:1"></span>
  <span class="stat"><span class="v" id="s-count">—</span><span class="l">candidates</span></span>
  <span class="stat"><span class="v" id="s-total">—</span><span class="l">total upside</span></span>
</div>

<table id="tbl"><thead><tr>
  <th></th>
  <th class="sortable" data-col="card_name">Card</th>
  <th class="sortable" data-col="grade_value">Grade</th>
  <th class="sortable num" data-col="slab_listing">Slab listed</th>
  <th>Mapped</th>
  <th class="sortable num" data-col="raw_price_mapped">Raw @ mapped</th>
  <th class="sortable num" data-col="raw_price_nm">Raw NM</th>
  <th class="sortable num" data-col="delta_mapped">Δ mapped</th>
  <th></th>
</tr></thead>
<tbody><tr><td colspan="9" class="loading">Loading…</td></tr></tbody>
</table>

<!-- Crack confirmation modal -->
<div class="modal-bg" id="modal-bg" onclick="if(event.target===this)closeModal()">
  <div class="modal" id="modal-body"></div>
</div>

<script>
let DATA = [];
let SORT_COL = "delta_mapped";
let SORT_DIR = "desc";
let ACTIVE = null;
let ACTIVE_OVERRIDE = null;

function toast(msg, ok=true){
  const t = document.createElement('div');
  t.className = 'toast' + (ok?'':' err'); t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(()=>t.remove(), 3500);
}
function fmt(n){ return (n==null) ? '—' : ('$' + Number(n).toFixed(2)); }

async function load(){
  const minDelta = document.getElementById('min-delta').value || '1';
  const tbody = document.querySelector('#tbl tbody');
  tbody.innerHTML = '<tr><td colspan="9" class="loading">Loading…</td></tr>';
  try {
    const r = await fetch('/inventory/breakdown/api/cache/slab-crack/candidates?min_delta=' + encodeURIComponent(minDelta));
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    DATA = d.candidates || [];
    render();
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty">Error: ' + e.message + '</td></tr>';
  }
}

function render(){
  const rows = [...DATA];
  rows.sort((a,b)=>{
    const av = a[SORT_COL]; const bv = b[SORT_COL];
    if (av==null && bv==null) return 0;
    if (av==null) return 1;
    if (bv==null) return -1;
    if (typeof av === 'number') return SORT_DIR==='asc' ? av-bv : bv-av;
    return SORT_DIR==='asc' ? String(av).localeCompare(bv) : String(bv).localeCompare(av);
  });

  document.getElementById('s-count').textContent = rows.length;
  const total = rows.reduce((s,r)=>s + (r.delta_mapped||0), 0);
  document.getElementById('s-total').textContent = fmt(total);

  const tbody = document.querySelector('#tbl tbody');
  if (!rows.length){
    tbody.innerHTML = '<tr><td colspan="9" class="empty">No candidates. Either every slab is priced correctly or no scrydex_id is mapped.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r, i) => {
    const co = (r.grade_company||'').toLowerCase();
    const mc = (r.mapped_condition||'').toLowerCase();
    return `
    <tr>
      <td>${r.image_url ? `<img class="img" src="${r.image_url}" loading="lazy">` : ''}</td>
      <td>
        <div class="name">${escapeHtml(r.card_name||'—')}
          <small>${escapeHtml(r.set_name||'')} ${r.card_number ? '#'+escapeHtml(r.card_number) : ''}</small>
        </div>
      </td>
      <td><span class="pill ${co}">${escapeHtml(r.grade_company||'?')} ${escapeHtml(r.grade_value||'?')}</span></td>
      <td class="num">${fmt(r.slab_listing)}</td>
      <td><span class="pill ${mc}">${r.mapped_condition || '—'}</span></td>
      <td class="num">${fmt(r.raw_price_mapped)}</td>
      <td class="num" style="color:var(--muted)">${fmt(r.raw_price_nm)}</td>
      <td class="num ${r.delta_mapped>0?'delta-pos':'delta-neg'}">${r.delta_mapped>0?'+':''}${fmt(r.delta_mapped)}</td>
      <td><button onclick='openCrack(${i})'>Crack →</button></td>
    </tr>`;
  }).join('');
}

function escapeHtml(s){
  return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
}

document.querySelectorAll('th.sortable').forEach(th=>{
  th.addEventListener('click', ()=>{
    const c = th.dataset.col;
    if (SORT_COL === c) SORT_DIR = SORT_DIR==='asc' ? 'desc' : 'asc';
    else { SORT_COL = c; SORT_DIR = 'desc'; }
    render();
  });
});

function openCrack(i){
  ACTIVE = DATA[i];
  ACTIVE_OVERRIDE = null;
  const r = ACTIVE;
  const conds = ['NM','LP','MP','HP','DMG'];
  document.getElementById('modal-body').innerHTML = `
    <h2>Crack ${escapeHtml(r.grade_company||'?')} ${escapeHtml(r.grade_value||'?')} ${escapeHtml(r.card_name||'')}</h2>
    <div class="sub">${escapeHtml(r.set_name||'')} ${r.card_number?'#'+escapeHtml(r.card_number):''} · barcode <code>${escapeHtml(r.barcode||'')}</code></div>
    <div class="modal-row"><span>Slab currently listed</span><span class="v">${fmt(r.slab_listing)}</span></div>
    <div class="modal-row"><span>Default mapped condition</span><span class="v">${r.mapped_condition || '—'}</span></div>
    <div class="modal-row"><span>Raw @ ${r.mapped_condition||'?'}</span><span class="v">${fmt(r.raw_price_mapped)}</span></div>
    <div class="modal-row"><span>Raw @ NM (ceiling)</span><span class="v">${fmt(r.raw_price_nm)}</span></div>
    <div class="modal-row"><span>Estimated upside</span><span class="v ${r.delta_mapped>0?'delta-pos':'delta-neg'}">${r.delta_mapped>0?'+':''}${fmt(r.delta_mapped)}</span></div>
    <div style="margin-top:14px;font-size:.85rem;color:var(--muted)">Override condition (operator sees the card after cracking):</div>
    <div class="grade-overrides" id="ovr">
      ${conds.map(c=>`<button data-c="${c}" class="${c===r.mapped_condition?'active':''}" onclick="setOvr('${c}')">${c}</button>`).join('')}
    </div>
    <div style="margin-top:14px;font-size:.8rem;color:var(--muted);padding:10px;background:var(--surface-2);border-radius:6px">
      Cracking will: delete the Shopify listing, mark the slab REMOVED, and queue
      a fresh raw card in the Return Queue (card_manager → Returns) so you can
      print the label and scan it into storage.
    </div>
    <div class="modal-actions">
      <button class="secondary" onclick="closeModal()">Cancel</button>
      <button class="danger" onclick="executeCrack()">Crack the slab</button>
    </div>
  `;
  ACTIVE_OVERRIDE = r.mapped_condition;
  document.getElementById('modal-bg').classList.add('show');
}
function setOvr(c){
  ACTIVE_OVERRIDE = c;
  document.querySelectorAll('#ovr button').forEach(b=>{
    b.classList.toggle('active', b.dataset.c===c);
  });
}
function closeModal(){
  document.getElementById('modal-bg').classList.remove('show');
  ACTIVE = null;
}
async function executeCrack(){
  if (!ACTIVE) return;
  const id = ACTIVE.raw_card_id;
  const cond = ACTIVE_OVERRIDE || ACTIVE.mapped_condition;
  const btn = document.querySelector('#modal-body .danger');
  btn.disabled = true; btn.textContent = 'Cracking…';
  try {
    const r = await fetch('/inventory/slab-crack/api/execute/' + id, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({condition: cond})
    });
    const d = await r.json();
    if (!r.ok) { toast(d.error || 'Failed', false); btn.disabled=false; btn.textContent='Crack the slab'; return; }
    toast('Cracked → new raw barcode ' + d.new_barcode + ' queued for return.');
    closeModal();
    DATA = DATA.filter(x => x.raw_card_id !== id);
    render();
  } catch (e) {
    toast(e.message, false); btn.disabled=false; btn.textContent='Crack the slab';
  }
}

load();
</script>
</body></html>"""
