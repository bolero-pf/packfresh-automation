"""
routes/bulk_add.py — Bulk-add for non-TCG products via image upload + Claude vision.

Flow:
    1. Operator drops a folder of product photos. Filenames encode product groups:
       `Foo.jpg` is a single-variant product; `Foo_Blue.jpg` + `Foo_Red.jpg` are
       two variants of the same product (option name auto-inferred).
    2. Server saves images to a temp session dir and groups by filename.
    3. Per group, Claude (sonnet-4-6 + web_search) analyzes images and produces
       a Shopify-ready draft (title, type, body_html, tags, MSRP, variants).
    4. Operator reviews/edits each card and clicks Push to create the Shopify
       draft via shared/generic_product_create.py.

Auth is handled by the app-level register_auth_hooks (manager/owner roles).
"""

import os
import re
import time
import uuid
import secrets
import logging
import tempfile
import unicodedata
from collections import Counter
from pathlib import Path

from flask import Blueprint, request, jsonify, send_file, abort
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

bp = Blueprint("bulk_add", __name__, url_prefix="/inventory")

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
SESSION_TTL_SECONDS = 24 * 3600
MAX_VARIANTS_PER_GROUP = 12


def _root_dir() -> Path:
    d = Path(tempfile.gettempdir()) / "pf_bulk_add"
    d.mkdir(exist_ok=True)
    return d


def _session_dir(session_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,64}", session_id or ""):
        abort(400, "bad session_id")
    return _root_dir() / session_id


def _gc_old_sessions() -> None:
    cutoff = time.time() - SESSION_TTL_SECONDS
    root = _root_dir()
    for child in root.iterdir():
        try:
            if child.is_dir() and child.stat().st_mtime < cutoff:
                for f in child.iterdir():
                    f.unlink(missing_ok=True)
                child.rmdir()
        except Exception:
            pass


def _slug_key(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()


def _parse_filename(filename: str) -> tuple[str, str]:
    """`Monster Prism Tube_Blue.jpg` -> ("Monster Prism Tube", "Blue").
    Caller must still check whether the underscore separator actually marks a
    variant (i.e. multiple files share the base) — see _build_groups."""
    stem = Path(filename).stem
    if "_" in stem:
        name_part, _, variant_part = stem.partition("_")
        return name_part.strip(), variant_part.replace("_", " ").strip()
    return stem.strip(), ""


def _build_groups(parsed: list[dict]) -> dict[str, dict]:
    """
    Cross-file grouping. If a base name appears in multiple files, the
    underscore is a variant separator (`Foo_Blue` + `Foo_Red`). If a base
    appears only once and has a non-empty variant_part, the underscore was
    just a filename-friendly space (download managers do this) — treat the
    whole filename as the product name.
    """
    base_counts = Counter(_slug_key(p["base"]) for p in parsed)
    groups: dict[str, dict] = {}

    for p in parsed:
        base_key = _slug_key(p["base"])
        is_real_variant = base_counts[base_key] > 1

        if not is_real_variant and p["variant"]:
            full_name = f"{p['base']} {p['variant']}".strip()
            key = _slug_key(full_name)
            display = full_name
            option_hint = ""
        else:
            key = base_key
            display = p["base"]
            option_hint = p["variant"]

        g = groups.setdefault(key, {"key": key, "name": display, "variants": []})
        g["variants"].append({
            "filename": p["safe"],
            "original": p["original"],
            "option_hint": option_hint,
        })
    return groups


@bp.route("/bulk-add")
def bulk_add_page():
    return PAGE_HTML


@bp.route("/api/bulk-add/upload", methods=["POST"])
def upload():
    _gc_old_sessions()
    files = request.files.getlist("images")
    if not files:
        return jsonify({"error": "no files"}), 400

    session_id = secrets.token_urlsafe(16).replace("=", "")
    sdir = _session_dir(session_id)
    sdir.mkdir(parents=True, exist_ok=True)

    parsed: list[dict] = []
    skipped: list[str] = []

    for f in files:
        original = f.filename or ""
        ext = Path(original).suffix.lower()
        if ext not in ALLOWED_EXT:
            skipped.append(f"{original} (bad ext)")
            continue

        safe = secure_filename(original) or f"img-{uuid.uuid4().hex[:8]}{ext}"
        if not safe.lower().endswith(ext):
            safe = f"{Path(safe).stem}{ext}"

        save_path = sdir / safe
        f.save(save_path)

        base, variant = _parse_filename(original)
        if not base:
            skipped.append(f"{original} (no name)")
            try:
                save_path.unlink()
            except Exception:
                pass
            continue

        parsed.append({
            "safe": safe,
            "original": original,
            "base": base,
            "variant": variant,
        })

    groups = _build_groups(parsed)

    for key, g in list(groups.items()):
        if len(g["variants"]) > MAX_VARIANTS_PER_GROUP:
            for extra in g["variants"][MAX_VARIANTS_PER_GROUP:]:
                skipped.append(f"{extra['original']} (group full)")
                try:
                    (sdir / extra["filename"]).unlink()
                except Exception:
                    pass
            g["variants"] = g["variants"][:MAX_VARIANTS_PER_GROUP]

    return jsonify({
        "session_id": session_id,
        "groups": list(groups.values()),
        "skipped": skipped,
    })


@bp.route("/api/bulk-add/img/<session_id>/<path:filename>")
def serve_image(session_id, filename):
    sdir = _session_dir(session_id)
    safe = secure_filename(filename)
    if not safe or safe != filename:
        abort(400, "bad filename")
    path = sdir / safe
    if not path.is_file():
        abort(404)
    return send_file(path)


@bp.route("/api/bulk-add/analyze", methods=["POST"])
def analyze():
    from bulk_vision_enrichment import analyze_product_group

    data = request.get_json() or {}
    session_id = data.get("session_id")
    name_hint = (data.get("name_hint") or "").strip()
    variants = data.get("variants") or []
    if not (session_id and name_hint and variants):
        return jsonify({"error": "session_id, name_hint, variants required"}), 400

    sdir = _session_dir(session_id)
    enriched_variants = []
    for v in variants:
        raw_name = v.get("filename") or ""
        fn = secure_filename(raw_name)
        if not fn:
            return jsonify({"error": "missing filename"}), 400
        path = sdir / fn
        if not path.is_file():
            on_disk = sorted(p.name for p in sdir.iterdir()) if sdir.is_dir() else []
            logger.warning(
                "bulk-add image not found: requested=%r safe=%r sdir=%s on_disk=%s",
                raw_name, fn, sdir, on_disk,
            )
            return jsonify({
                "error": f"image not found: {fn}",
                "requested_filename": raw_name,
                "secured_filename": fn,
                "files_on_disk": on_disk,
            }), 404
        enriched_variants.append({
            "filename": fn,
            "image_path": str(path),
            "option_hint": v.get("option_hint", ""),
        })

    try:
        result = analyze_product_group(name_hint, enriched_variants)
        return jsonify(result)
    except Exception as e:
        logger.exception("bulk-add analyze failed")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/bulk-add/push", methods=["POST"])
def push():
    from generic_product_create import create_draft_product

    data = request.get_json() or {}
    session_id = data.get("session_id")
    payload = data.get("payload") or {}
    qty = int(data.get("qty") or 0)

    if not (session_id and payload.get("title") and payload.get("variants")):
        return jsonify({"error": "session_id, payload.title, payload.variants required"}), 400

    sdir = _session_dir(session_id)
    image_paths: dict[str, str] = {}
    for v in payload["variants"]:
        fn = secure_filename(v.get("filename") or "")
        if not fn:
            continue
        path = sdir / fn
        if path.is_file():
            image_paths[v["filename"]] = str(path)

    if not image_paths:
        return jsonify({"error": "no valid images"}), 400

    try:
        summary = create_draft_product(payload, image_paths, qty=qty)
        return jsonify(summary)
    except Exception as e:
        logger.exception("bulk-add push failed")
        return jsonify({"error": str(e)}), 500


PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bulk Add · Common Lands</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
header{background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;height:50px;display:flex;align-items:center;gap:12px;}
header .logo{font-weight:700;}
header .logo span{color:var(--green);}
header .sub{color:var(--dim);font-size:.83rem;}
header a{margin-left:auto;color:var(--dim);font-size:.8rem;text-decoration:none;}
header a:hover{color:var(--text);}
.container{max-width:1100px;margin:0 auto;padding:24px 20px;}
.card{background:var(--surface);border:1px solid var(--border);border-radius:11px;padding:18px 20px;margin-bottom:16px;}
.card h2{font-size:.95rem;font-weight:600;margin-bottom:6px;}
.card p{color:var(--dim);font-size:.82rem;margin-bottom:12px;}
.lbl{font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--dim);margin-bottom:5px;display:block;}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:0 16px;height:38px;font-size:.85rem;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap;}
.btn:hover{filter:brightness(1.08);} .btn:disabled{opacity:.5;cursor:not-allowed;}
.btn-green{background:var(--green);color:#000;}
.btn-ghost{background:var(--surface2);color:var(--text);border:1px solid var(--border);}
.spinner{display:inline-block;width:12px;height:12px;border:2px solid rgba(255,255,255,.2);border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:6px;}
@keyframes spin{to{transform:rotate(360deg);}}

.dropzone{border:2px dashed var(--border);border-radius:11px;padding:34px;text-align:center;color:var(--dim);cursor:pointer;transition:border-color .15s,background .15s;}
.dropzone:hover,.dropzone.drag{border-color:var(--accent);background:var(--surface2);color:var(--text);}
.dropzone strong{display:block;font-size:1rem;color:var(--text);margin-bottom:4px;}
.dropzone small{display:block;margin-top:6px;font-size:.78rem;}

.group{background:var(--surface);border:1px solid var(--border);border-radius:11px;margin-bottom:14px;overflow:hidden;}
.group-head{padding:13px 16px;border-bottom:1px solid var(--border);display:flex;gap:12px;align-items:center;flex-wrap:wrap;}
.group-head h3{font-size:.95rem;font-weight:700;flex:1;min-width:0;}
.group-head .pill{font-size:.7rem;padding:2px 8px;background:var(--surface2);border-radius:18px;color:var(--dim);border:1px solid var(--border);}
.thumbs{display:flex;gap:8px;flex-wrap:wrap;padding:12px 16px;background:var(--surface2);border-bottom:1px solid var(--border);}
.thumb{position:relative;}
.thumb img{width:90px;height:90px;object-fit:contain;background:var(--bg);border:1px solid var(--border);border-radius:6px;}
.thumb .vname{font-size:.7rem;color:var(--dim);text-align:center;margin-top:3px;max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}

.group-body{padding:14px 16px;display:none;}
.group-body.active{display:block;}
.field-row{display:grid;grid-template-columns:1fr 1fr;gap:10px 14px;margin-bottom:10px;}
.field{display:flex;flex-direction:column;}
.field input,.field select,.field textarea{background:var(--bg);border:1px solid var(--border);border-radius:7px;color:var(--text);padding:8px 10px;font-size:.86rem;font-family:inherit;}
.field textarea{min-height:78px;resize:vertical;font-family:ui-monospace,'SF Mono',Consolas,monospace;font-size:.78rem;}
.field input:focus,.field select:focus,.field textarea:focus{outline:none;border-color:var(--accent);}
.full{grid-column:1/-1;}

table.variants{width:100%;border-collapse:collapse;margin-top:6px;font-size:.82rem;}
table.variants th{text-align:left;color:var(--dim);font-weight:600;padding:5px 8px;font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border);}
table.variants td{padding:5px 8px;border-bottom:1px solid var(--border);}
table.variants input{background:var(--bg);border:1px solid var(--border);border-radius:5px;color:var(--text);padding:5px 7px;font-size:.82rem;font-family:inherit;width:100%;box-sizing:border-box;}

.action-bar{padding:12px 16px;display:flex;gap:8px;align-items:center;background:var(--surface2);border-top:1px solid var(--border);flex-wrap:wrap;}
.action-bar .note{margin-left:auto;font-size:.76rem;color:var(--dim);}
.status-tag{font-size:.72rem;padding:2px 8px;border-radius:18px;border:1px solid var(--border);background:var(--surface2);color:var(--dim);}
.status-tag.ok{background:var(--green-bg);border-color:var(--green);color:var(--green);}
.status-tag.warn{background:var(--amber-bg);border-color:var(--amber);color:var(--amber);}
.status-tag.err{background:var(--red-bg);border-color:var(--red);color:var(--red);}
.notes-box{background:var(--amber-bg);border:1px solid var(--amber);color:var(--amber);font-size:.8rem;padding:8px 11px;border-radius:6px;margin-bottom:10px;}
.alert{background:var(--red-bg);border:1px solid var(--red);color:var(--red);padding:10px 14px;border-radius:8px;font-size:.85rem;margin-bottom:12px;}
.msrp-link{font-size:.74rem;color:var(--accent);text-decoration:none;margin-left:6px;}
.msrp-link:hover{text-decoration:underline;}
</style>
</head>
<body>
<header>
  <div class="logo">Common<span>Lands</span></div>
  <div class="sub">Bulk Add</div>
  <a href="/inventory/add">← Back to Add Item</a>
</header>
<div class="container">
  <div id="error-box"></div>

  <div class="card" id="upload-card">
    <h2>Drop product photos</h2>
    <p>Filename = product name. Add an underscore for variants:
       <code>Catan.jpg</code> is a single product;
       <code>Monster Prism Tube_Blue.jpg</code> + <code>Monster Prism Tube_Red.jpg</code>
       become two variants of one product. Claude reads the photos and pre-fills everything.
    </p>
    <div class="dropzone" id="dropzone">
      <strong>Drop images here or click to choose</strong>
      <small>JPG, PNG, WebP, GIF — drop the whole folder if you like</small>
      <input type="file" id="file-input" multiple accept="image/*" style="display:none;">
    </div>
    <div id="upload-status" style="margin-top:10px;font-size:.84rem;color:var(--dim);"></div>
  </div>

  <div id="groups-area"></div>

  <div id="batch-bar" style="display:none;text-align:center;margin:16px 0;">
    <button class="btn btn-ghost" id="analyze-all-btn" onclick="analyzeAll()">⚡ Analyze All</button>
    <div id="batch-progress" style="font-size:.82rem;color:var(--dim);margin-top:6px;"></div>
  </div>
</div>

<script>
let SESSION_ID = null;
let GROUPS = [];

const dz = document.getElementById('dropzone');
const fi = document.getElementById('file-input');
dz.onclick = () => fi.click();
dz.ondragover = e => { e.preventDefault(); dz.classList.add('drag'); };
dz.ondragleave = () => dz.classList.remove('drag');
dz.ondrop = e => { e.preventDefault(); dz.classList.remove('drag'); handleFiles(e.dataTransfer.files); };
fi.onchange = () => handleFiles(fi.files);

async function handleFiles(filelist){
  if(!filelist.length) return;
  hideErr();
  const fd = new FormData();
  for(const f of filelist){
    if(f.type && !f.type.startsWith('image/')) continue;
    fd.append('images', f, f.name);
  }
  const status = document.getElementById('upload-status');
  status.innerHTML = '<span class="spinner"></span>Uploading ' + filelist.length + ' files…';
  try{
    const r = await fetch('/inventory/api/bulk-add/upload', { method:'POST', body: fd });
    const d = await r.json();
    if(!r.ok) throw new Error(d.error || 'upload failed');
    SESSION_ID = d.session_id;
    GROUPS = d.groups;
    let msg = '✓ ' + GROUPS.length + ' product group(s) detected.';
    if(d.skipped && d.skipped.length) msg += ' Skipped ' + d.skipped.length + ': ' + d.skipped.join(', ');
    status.textContent = msg;
    renderGroups();
    document.getElementById('batch-bar').style.display = GROUPS.length > 1 ? 'block' : 'none';
  }catch(e){ showErr(e.message); status.textContent = ''; }
}

function renderGroups(){
  const area = document.getElementById('groups-area');
  area.innerHTML = GROUPS.map((g,i) => `
    <div class="group" id="group-${i}">
      <div class="group-head">
        <h3>${esc(g.name)}</h3>
        <span class="pill">${g.variants.length} image${g.variants.length>1?'s':''}</span>
        <span class="status-tag" id="status-${i}">not analyzed</span>
        <button class="btn btn-ghost" onclick="analyzeGroup(${i})">⚡ Analyze</button>
      </div>
      <div class="thumbs">
        ${g.variants.map(v => `
          <div class="thumb">
            <img src="/inventory/api/bulk-add/img/${SESSION_ID}/${encodeURIComponent(v.filename)}" alt="">
            <div class="vname">${esc(v.option_hint || '(base)')}</div>
          </div>
        `).join('')}
      </div>
      <div class="group-body" id="body-${i}"></div>
    </div>
  `).join('');
}

async function analyzeGroup(idx){
  const g = GROUPS[idx];
  const tag = document.getElementById('status-'+idx);
  tag.className = 'status-tag';
  tag.textContent = 'analyzing…';
  try{
    const r = await fetch('/inventory/api/bulk-add/analyze', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        session_id: SESSION_ID,
        name_hint: g.name,
        variants: g.variants,
      })
    });
    const d = await r.json();
    if(!r.ok){
      let msg = d.error || 'analyze failed';
      if(d.files_on_disk){
        msg += '\\nrequested: ' + d.requested_filename + '\\nsecured: ' + d.secured_filename
             + '\\non disk: ' + (d.files_on_disk.length ? d.files_on_disk.join(', ') : '(empty)');
      }
      throw new Error(msg);
    }
    g.analysis = d;
    renderEditor(idx);
    tag.className = 'status-tag ok';
    tag.textContent = 'analyzed';
  }catch(e){
    tag.className = 'status-tag err';
    tag.textContent = 'error';
    showErr(g.name + ': ' + e.message);
  }
}

async function analyzeAll(){
  const btn = document.getElementById('analyze-all-btn');
  const prog = document.getElementById('batch-progress');
  const todo = GROUPS.map((g,i)=>i).filter(i => !GROUPS[i].analysis);
  if(!todo.length){ prog.textContent = 'Nothing to analyze.'; return; }

  btn.disabled = true;
  let ok = 0, err = 0;
  for(let n=0; n<todo.length; n++){
    const i = todo[n];
    prog.innerHTML = `<span class="spinner"></span>Analyzing ${n+1}/${todo.length} · ${esc(GROUPS[i].name)}`;
    try{
      await analyzeGroup(i);
      if(GROUPS[i].analysis) ok++; else err++;
    }catch(e){ err++; }
  }
  btn.disabled = false;
  prog.textContent = `Done — ${ok} analyzed, ${err} error${err===1?'':'s'}. Re-click Analyze on any errored row to retry.`;
}

const PRODUCT_TYPES = [
  'Board Game', 'Card Game (Non-TCG)', 'Puzzle',
  'Toy / Plush', 'Accessory', 'Collectible', 'Misc'
];

function renderEditor(idx){
  const g = GROUPS[idx];
  const a = g.analysis;
  const body = document.getElementById('body-'+idx);
  const typeOptions = PRODUCT_TYPES.map(t =>
    `<option value="${esc(t)}" ${t===a.product_type?'selected':''}>${esc(t)}</option>`
  ).join('');

  const isSingle = (a.variants || []).length === 1;
  const v0 = (a.variants && a.variants[0]) || {};

  const msrpLink = a.msrp_source_url
    ? `<a class="msrp-link" href="${esc(a.msrp_source_url)}" target="_blank" rel="noopener">source ↗</a>`
    : '';

  const variantsBlock = isSingle ? `
      <div class="field">
        <label class="lbl">SKU</label>
        <input data-vi="0" data-vk="sku" value="${esc(v0.sku || '')}">
      </div>
      <div class="field">
        <label class="lbl">Barcode / UPC</label>
        <input data-vi="0" data-vk="barcode" value="${esc(v0.barcode || '')}" placeholder="(optional)">
      </div>
  ` : `
      <div class="field">
        <label class="lbl">Variant Option Name</label>
        <input data-k="variant_option_name" value="${esc(a.variant_option_name || 'Variant')}">
      </div>
      <div class="field"></div>
      <div class="field full">
        <label class="lbl">Variants</label>
        <table class="variants">
          <thead><tr><th></th><th>Option Value</th><th>SKU</th><th>Barcode</th></tr></thead>
          <tbody>${(a.variants || []).map((v,vi) => `
            <tr>
              <td><img src="/inventory/api/bulk-add/img/${SESSION_ID}/${encodeURIComponent(v.filename)}" style="width:36px;height:36px;object-fit:contain;background:var(--bg);border:1px solid var(--border);border-radius:4px;"></td>
              <td><input data-vi="${vi}" data-vk="option_value" value="${esc(v.option_value || '')}"></td>
              <td><input data-vi="${vi}" data-vk="sku" value="${esc(v.sku || '')}"></td>
              <td><input data-vi="${vi}" data-vk="barcode" value="${esc(v.barcode || '')}" placeholder="(optional)"></td>
            </tr>
          `).join('')}</tbody>
        </table>
      </div>
  `;

  body.innerHTML = `
    ${a.notes ? `<div class="notes-box">⚠ ${esc(a.notes)}</div>` : ''}
    <div class="field-row">
      <div class="field full">
        <label class="lbl">Title</label>
        <input data-k="title" value="${esc(a.title || '')}">
      </div>
      <div class="field">
        <label class="lbl">Product Type</label>
        <select data-k="product_type">${typeOptions}</select>
      </div>
      <div class="field">
        <label class="lbl">Publisher (informational — vendor stays Common Lands)</label>
        <input data-k="publisher" value="${esc(a.publisher || '')}">
      </div>
      <div class="field">
        <label class="lbl">MSRP (USD) ${msrpLink}</label>
        <input data-k="msrp_usd" type="number" step="0.01" value="${a.msrp_usd ?? ''}">
      </div>
      <div class="field">
        <label class="lbl">Weight (oz)</label>
        <input data-k="weight_oz_estimate" type="number" step="0.5" value="${a.weight_oz_estimate ?? 8}">
      </div>
      <div class="field full">
        <label class="lbl">Tags (comma-separated)</label>
        <input data-k="tags" value="${esc((a.tags || []).join(', '))}">
      </div>
      <div class="field full">
        <label class="lbl">Description (HTML)</label>
        <textarea data-k="body_html">${esc(a.body_html || '')}</textarea>
      </div>
      ${variantsBlock}
      <div class="field">
        <label class="lbl">Initial Inventory (qty)</label>
        <input data-k="qty" type="number" min="0" value="0">
      </div>
    </div>
    <div class="action-bar">
      <button class="btn btn-green" onclick="pushGroup(${idx}, this)">✦ Push to Shopify</button>
      <button class="btn btn-ghost" onclick="analyzeGroup(${idx})">↺ Re-analyze</button>
      <span class="note">Creates as DRAFT</span>
    </div>
  `;
  body.classList.add('active');
}

function collectPayload(idx){
  const body = document.getElementById('body-'+idx);
  const a = JSON.parse(JSON.stringify(GROUPS[idx].analysis));

  body.querySelectorAll('[data-k]').forEach(el => {
    const k = el.dataset.k;
    let v = el.value;
    if(k === 'tags') v = v.split(',').map(s=>s.trim()).filter(Boolean);
    else if(k === 'msrp_usd' || k === 'weight_oz_estimate') v = v === '' ? null : parseFloat(v);
    else if(k === 'qty') v = parseInt(v||'0',10) || 0;
    a[k] = v;
  });
  body.querySelectorAll('[data-vi]').forEach(el => {
    const vi = parseInt(el.dataset.vi, 10);
    const vk = el.dataset.vk;
    a.variants[vi][vk] = el.value || (vk === 'barcode' ? null : '');
  });

  if((a.variants || []).length === 1){
    a.variant_option_name = 'Title';
    a.variants[0].option_value = 'Default Title';
  }
  return a;
}

async function pushGroup(idx, btn){
  const payload = collectPayload(idx);
  const qty = payload.qty || 0;
  delete payload.qty;
  const tag = document.getElementById('status-'+idx);
  btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>Creating…';
  try{
    const r = await fetch('/inventory/api/bulk-add/push', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ session_id: SESSION_ID, payload, qty })
    });
    const d = await r.json();
    if(!r.ok) throw new Error(d.error || 'push failed');
    tag.className = 'status-tag ok';
    tag.textContent = 'pushed ✓';
    btn.outerHTML = `<a class="btn btn-green" href="${esc(d.admin_url)}" target="_blank" rel="noopener">Open in Shopify ↗</a>`;
    if(d.errors && d.errors.length){
      const note = document.querySelector('#body-'+idx+' .note');
      note.style.color = 'var(--amber)';
      note.textContent = '⚠ ' + d.errors.length + ' warning(s)';
    }
  }catch(e){
    btn.disabled = false; btn.innerHTML = '✦ Push to Shopify';
    showErr(e.message);
  }
}

function showErr(m){
  const b = document.getElementById('error-box');
  b.innerHTML = '<div class="alert">' + esc(m) + '</div>';
  b.scrollIntoView({behavior:'smooth'});
}
function hideErr(){ document.getElementById('error-box').innerHTML = ''; }
</script>
</body>
</html>"""
