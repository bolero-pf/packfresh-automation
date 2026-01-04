import pandas as pd
import os, math
import shopify
import requests
from flask import Flask, render_template_string, request, render_template, redirect, flash, get_flashed_messages, send_file, Response
from dotenv import load_dotenv
from functools import wraps
from sqlalchemy import create_engine

load_dotenv()

SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
USERNAME = os.environ.get("INVENTORY_USER")
PASSWORD = os.environ.get("INVENTORY_PASS")
LOCATION_ID = os.environ.get("LOCATION_ID")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")
DRY_RUN         = os.getenv("PF_DRY_RUN", "0") == "1"  # default ON for safety

VISIBLE_COLUMNS = [
    "name",
    "total amount (4/1)",
    "rc_qty",
    "rc_price",
    "shopify_qty",
    "shopify_price",
    "shopify_value",
    "rc_value",  # if you want to show the rc_value as well
    "notes"
]
COLUMN_RENAMES = {
    "name": "Name",
    "total amount (4/1)": "Total Inventory",
    "rc_qty": "RC Qty",
    "rc_price": "RC Price",
    "shopify_qty": "Shopify Qty",
    "shopify_price": "Shopify Price",
    "rare_find_id" : "RC ID",
    "rc value": "RC Value",
    "shopify value": "Shopify Value",
    "notes": "Notes",
    "__key__": "__key__",
}
NAV_BUTTONS = [
    {"label": "⬅ Back to Inventory", "href": "/", "class": "btn-outline-secondary", "key": "back"},
    {"label": "📦 Unpublished", "href": "/unpublished", "class": "btn-outline-warning", "key": "unpublished"},
    {"label": "🛍️ Only on Shopify", "href": "/only_shopify", "class": "btn-outline-info", "key": "only_shopify"},
    {"label": "🧪 Only on RC", "href": "/only_rc", "class": "btn-outline-primary", "key": "only_rc"},
    {"label": "🕵️‍♂️ Untouched", "href": "/untouched", "class": "btn-outline-dark", "key": "untouched"},
    {"label": "📤 Export CSV", "href": "/export_csv", "class": "btn-outline-success", "key": "export"},
    {"label": "🔁 Sync Rare Candy", "href": "/sync_rc", "class": "btn-outline-primary", "key": "sync_rc"},
    {"label": "🔁 Sync Shopify", "href": "/sync_shopify", "class": "btn-outline-success", "key": "sync_shopify"},
]
import pandas as pd

# --- DB bootstrap (put near top of lib.py) ---
import os, re
from pathlib import Path
from sqlalchemy import create_engine, text

# 1) Choose a sane local default (relative to your project root)
DB_URL = os.getenv("PF_DB_URL", "sqlite:///./data/inventory.db")

def _resolve_sqlite_fs_path(db_url: str) -> str | None:
    """
    Returns a normalized absolute filesystem path for sqlite URLs.
    Handles Windows 'C:/' correctly and strips a leading '/' when present.
    Returns None for non-sqlite URLs or in-memory DB.
    """
    if not db_url.startswith("sqlite"):
        return None
    # in-memory
    if db_url.startswith("sqlite:///:memory:"):
        return None

    # Accept both 'sqlite:///C:/foo/bar.db' and 'sqlite:///./data/prices.db'
    m = re.match(r"^sqlite:///(.+)$", db_url)
    if not m:
        return None
    raw_path = m.group(1)

    # Windows sometimes gives '/C:/...' — drop the leading slash
    if re.match(r"^/[A-Za-z]:/", raw_path):
        raw_path = raw_path[1:]

    # Expand ~ and make absolute
    abs_path = Path(os.path.expanduser(raw_path)).resolve()
    return str(abs_path)

sqlite_fs_path = _resolve_sqlite_fs_path(DB_URL)

# 2) Ensure parent dir exists for sqlite files
if sqlite_fs_path:
    Path(sqlite_fs_path).parent.mkdir(parents=True, exist_ok=True)

# 3) Build engine; for SQLite in Flask dev, disable same-thread check
connect_args = {"check_same_thread": False} if DB_URL.startswith("sqlite") else {}
engine = create_engine(DB_URL, connect_args=connect_args, future=True)
with engine.begin() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
# 4) Force-open to create the file right now (and set WAL mode if you like)
try:
    with engine.begin() as conn:
        # touching the DB ensures the file exists
        conn.execute(text("PRAGMA journal_mode=WAL;"))
except Exception as e:
    # Helpful diagnostics if path is wrong or we lack permissions
    raise RuntimeError(f"Could not open/create SQLite DB at {sqlite_fs_path or DB_URL}: {e}")


# minimal skeleton so routes & sync can use it immediately
inventory_df = pd.DataFrame(
    columns=[
        "name",
        "shopify_qty",
        "shopify_price",
        "shopify_tags",
        "shopify_status",
        "variant_id",
        "shopify_inventory_id",
        "total amount (4/1)",
        "notes",
    ]
)

def load_inventory_fallback():
    """
    Load the inventory snapshot from the DB if it exists, otherwise
    return the in-memory empty template.
    """
    try:
        with engine.begin() as conn:
            df = pd.read_sql_table("inventory", conn)
    except Exception:
        df = inventory_df
    return df

# on module import, try to load any persisted copy
try:
    inventory_df = load_inventory_fallback()
except Exception:
    pass
def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
def check_auth(u, p):
    return u == USERNAME and p == PASSWORD
def authenticate():
    return Response(
        'Unauthorized', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )
def normalize_name(name):
    if not name or pd.isna(name):
        return ""
    return str(name).strip().lower()
def add_local_inventory_row(name: str, qty: int = 0, price: float | None = None, tags: str = "") -> None:
    """
    Append a local-only row (no Shopify IDs) so you can ingest inventory that isn't on Shopify yet.
    Persists immediately to the inventory DB.
    """
    global inventory_df

    # Make sure the expected columns exist (don’t crash if schema changed)
    for col in ["name", "shopify_qty", "shopify_price", "shopify_tags",
                "variant_id", "shopify_inventory_id", "total amount (4/1)", "notes"]:
        if col not in inventory_df.columns:
            inventory_df[col] = pd.NA

    new_row = {
        "name": name.strip(),
        "shopify_qty": None,                     # not on Shopify yet
        "shopify_price": float(price) if price is not None else None,
        "shopify_tags": tags.strip(),
        "variant_id": None,                      # marks this as local-only
        "shopify_inventory_id": None,            # marks this as local-only
        "total amount (4/1)": int(qty) if qty is not None else None,
        "notes": "local-only",
    }

    # Append and persist
    inventory_df.loc[len(inventory_df)] = new_row
    save_inventory_to_db(inventory_df)

def _activate_session():
    shopify.ShopifyResource.clear_session()
    session = shopify.Session(SHOPIFY_STORE, API_VERSION, SHOPIFY_TOKEN)
    shopify.ShopifyResource.activate_session(session)

def create_shopify_draft_product(title: str, price: float | None = None, tags: str = "") -> dict:
    """
    Create a draft Shopify product with a single variant.
    Returns: {"product_id": int, "variant_id": int, "inventory_item_id": int}
    """
    title = (title or "").strip()
    if not title:
        raise ValueError("title is required")

    if DRY_RUN:
        print(f"[DRY_RUN] Would create DRAFT product title={title!r} price={price} tags={tags!r}")
        return {"product_id": 0, "variant_id": 0, "inventory_item_id": 0}

    _activate_session()

    p = shopify.Product()
    p.title = title
    p.status = "draft"          # Draft product
    p.tags = tags or ""

    v = shopify.Variant()
    if price is not None:
        v.price = f"{float(price):.2f}"
    # ensure it is inventory tracked; Shopify will attach inventory_item_id
    v.inventory_management = "shopify"

    p.variants = [v]

    if not p.save():
        raise RuntimeError(f"Shopify draft create failed: {getattr(p, 'errors', None)}")

    # Re-fetch or use returned fields (python-shopify can be finicky; simplest is to read back)
    created = shopify.Product.find(p.id)
    created_variant = created.variants[0]

    return {
        "product_id": int(created.id),
        "variant_id": int(created_variant.id),
        "inventory_item_id": int(created_variant.inventory_item_id),
    }


def update_shopify_variant_price(variant_id: int, new_price: float) -> bool:
    """
    Updates a single variant's price. Returns True on success.
    """
    if DRY_RUN:
        print(f"[DRY_RUN] Would set price variant_id={variant_id} → {new_price:.2f}")
        return True

    _activate_session()
    v = shopify.Variant.find(variant_id)
    v.price = f"{new_price:.2f}"
    return bool(v.save())

def update_shopify_qty(inventory_item_id: int, location_id: int, new_qty: int) -> bool:
    """
    Sets available quantity for an inventory item at a specific location.
    Requires location_id. Uses classmethod InventoryLevel.set(...).
    """
    if DRY_RUN:
        print(f"[DRY_RUN] Would set qty inventory_item_id={inventory_item_id} @ loc={location_id} → {new_qty}")
        return True

    _activate_session()
    try:
        # This both connects (if needed) and sets available qty.
        shopify.InventoryLevel.set(
            location_id=location_id,
            inventory_item_id=inventory_item_id,
            available=int(new_qty),
        )
        return True
    except Exception as e:
        print(f"[ERROR] InventoryLevel.set failed for inventory_item_id={inventory_item_id} "
              f"loc={location_id} → {new_qty}: {e}")
        return False
# in app_with_render_table_updated.py (where render_inventory_table lives)
def render_inventory_table(
    filtered_df,
    title="Inventory",
    show_columns=None,
    hidden_buttons=None,
    editable_columns=None,
    filters=None,
    meta=None,
):
    meta = meta or {}
    last_sync = meta.get("last_sync", "Never")
    mode_label = meta.get("mode_label", "LIVE")
    totals = meta.get("totals")
    import html
    editable_columns = set(editable_columns or [])
    show_columns = list(show_columns or filtered_df.columns)
    hidden_keys = set(hidden_buttons or [])
    filters = filters or {}
    q = filters.get("q", "")
    in_stock = filters.get("in_stock", False)
    tag_options = filters.get("tag_options", [])
    selected_tags = set([t.lower() for t in filters.get("selected_tags", [])])
    query_string = meta.get("query_string", "")

    qs = f"?{query_string}" if query_string else ""

    # --- small helpers
    def disp(col):
        return COLUMN_RENAMES.get(col, col.replace("_", " ").title())

    # --- top controls: search, in-stock toggle, tag chips
    chips = []
    for t in tag_options:
        checked = "checked" if t in selected_tags else ""
        chips.append(
            f'<label class="me-3 mb-2"><input type="checkbox" name="tag" value="{html.escape(t)}" {checked}> {html.escape(t)}</label>'
        )
    chips_html = "<div class='mb-2 d-flex flex-wrap'>" + "".join(chips) + "</div>" if chips else ""

    # --- nav/actions
    def nav_btn(label, href, cls="btn-outline-secondary"):
        return f'<a class="btn {cls} me-2" href="{href}">{html.escape(label)}</a>'
    nav = []
    if "sync_shopify" not in hidden_keys:
        nav.append(nav_btn("🔁 Sync Shopify", "/inventory/sync", "btn-outline-success"))
    nav.append(
        '<form method="post" action="/inventory/zero_current" style="display:inline">'
        '<button class="btn btn-outline-warning ms-2">🧹 Zero Current Inventory</button>'
        "</form>"
    )
    nav.append(
        '<form method="post" action="/inventory/push_prices" style="display:inline">'
        '<button class="btn btn-outline-primary ms-2">💸 Push Prices to Shopify</button>'
        "</form>"
    )
    nav_html = '<div class="mb-3">' + "".join(nav) + "</div>"
    totals_html = ""
    if totals:
        totals_html = (
            f'<span class="pf-badge">Items: {totals["count"]}</span>'
            f'<span class="pf-badge">Shopify Qty: {totals["shopify_qty"]}</span>'
            f'<span class="pf-badge">Inventory Qty: {totals["inventory_qty"]}</span>'
            f'<span class="pf-badge">Value: ${totals["shopify_value"]:,.2f}</span>'
        )
    topbar = f"""
    <div class="pf-topbar">
      <div class="pf-title">
        <h3 style="margin:0;">{html.escape(title)}</h3>
        <span class="pf-badge">{len(filtered_df)} variants</span>
        <span class="pf-badge">Last sync: {html.escape(last_sync)}</span>
        <span class="pf-badge pf-live">{html.escape(mode_label)}</span>
        {totals_html}
      </div>
      <div class="pf-actions">
        <a class="pf-btn pf-btn-ghost" href="/inventory/sync">🔁 Sync Shopify</a>
        <a class="pf-btn pf-btn-ghost"
           href="/inventory/export.csv{qs}">
           📤 Export CSV
        </a>
        <form method="post" action="/inventory/zero_current" style="display:inline">
          <button class="pf-btn">🧹 Zero Current Inventory</button>
        </form>
        <form method="post" action="/inventory/push_prices" style="display:inline">
          <button class="pf-btn pf-btn-primary">💸 Push Prices to Shopify</button>
        </form>
      </div>
    </div>
    """
    status = filters.get("status", "all")

    def status_chip(label, value):
        active = "pf-chip-active" if status == value else ""
        return (
            f'<a class="pf-chip {active}" '
            f'href="/inventory?status={value}{("&" + query_string.replace("status=" + status, "").lstrip("&")) if query_string else ""}">'
            f'{label}</a>'
        )

    status_chips_html = (
            '<div class="pf-chips mb-2">'
            + status_chip("All", "all")
            + status_chip("Published", "published")
            + status_chip("Drafts", "draft")
            + "</div>"
    )

    # --- filters form (GET)
    filter_bar = f"""
    <form id="pf-filter" method="get" class="pf-toolbar" onsubmit="">
      <div class="pf-search">
        <input name="q" value="{html.escape(q)}" placeholder="Search by name">
      </div>

      <label class="form-check-label d-flex align-items-center" style="gap:6px;">
        <input class="form-check-input" type="checkbox" name="in_stock" value="1" {'checked' if in_stock else ''}>
        In stock only
      </label>

      <input type="hidden" name="limit" value="{html.escape(str(request.args.get('limit', '400')))}">
      <button class="btn btn-outline-light">Filter</button>

      <div style="flex-basis:100%; height:0;"></div>
      {status_chips_html}
      <div class="pf-chips">
        {''.join(
        f'<label class="pf-chip"><input type="checkbox" name="tag" value="{html.escape(t)}" {"checked" if t in selected_tags else ""}>{html.escape(t)}</label>'
        for t in tag_options
    )}
      </div>
    </form>
    """

    # --- table header
    thead_cells = ["<th></th>"] + [f"<th>{html.escape(disp(c))}</th>" for c in show_columns]
    thead_html = "<thead><tr>" + "".join(thead_cells) + "</tr></thead>"

    # --- body rows
    body_rows = []
    df_for_render = filtered_df.reset_index(drop=True)
    for i, row in df_for_render.iterrows():
        tds = [f'<td><input type="checkbox" name="merge_ids" value="{i}"></td>']
        for col in show_columns:
            val = row.get(col, "")
            if pd.isna(val): val = ""
            val_str = html.escape(str(val))

            if col in editable_columns:
                # add data-orig for change tracking; number bumpers for qty/total
                if col == "shopify_price":
                    input_html = (
                        f'<input name="cell_{i}_shopify_price" value="{val_str}" data-orig="{val_str}" '
                        f' inputmode="decimal" step="0.01" class="pf-input" style="max-width:140px">'
                    )
                elif col in ("shopify_qty", "total amount (4/1)"):
                    input_html = (
                        '<div class="pf-qty-wrap">'
                        f'  <button type="button" class="pf-qty-btn pf-dec" data-target="cell_{i}_{col}">−</button>'
                        f'  <input name="cell_{i}_{col}" value="{val_str}" data-orig="{val_str}" inputmode="numeric" step="1" class="pf-input" style="max-width:120px">'
                        f'  <button type="button" class="pf-qty-btn pf-inc" data-target="cell_{i}_{col}">+</button>'
                        '</div>'
                    )
                elif col == "adjust_delta":
                    # one-shot adjust; not persisted; placeholder hints behavior
                    input_html = (
                        f'<input name="cell_{i}_adjust_delta" value="" data-orig="" inputmode="numeric" '
                        f' class="pf-input" placeholder="adjust qty (e.g. 37)" style="max-width:140px">'
                    )
                else:
                    input_html = f'<input name="cell_{i}_{col}" value="{val_str}" data-orig="{val_str}" class="pf-input form-control form-control-sm">'
                tds.append(f"<td>{input_html}</td>")
            else:
                tds.append(f"<td class='text-truncate'>{val_str}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")
    tbody_html = "<tbody>" + "".join(body_rows) + "</tbody>"
    style = """
    <style>
    :root{
      --pf-bg:#242833;
      --pf-panel:#1d1d25;
      --pf-text:#fcf7e1;
      --pf-accent:#dfa260;
      --pf-muted:#a3a7b1;
      --pf-border:rgba(255,255,255,.12);
      --pf-border-strong:rgba(255,255,255,.2);
    }
    body{ background:var(--pf-bg); color:var(--pf-text); }

    /* ===== Topbar ===== */
    .pf-topbar{
      display:flex; align-items:center; justify-content:space-between; gap:12px;
      margin:8px 0 16px;
    }
    .pf-title{
      display:flex; align-items:center; gap:10px; flex-wrap:wrap;
    }
    .pf-badge{
      padding:6px 10px; border-radius:999px; font-size:12px; line-height:1;
      border:1px solid var(--pf-border); background:var(--pf-panel); color:var(--pf-text);
    }
    .pf-badge.pf-live{ border-color: var(--pf-accent); }
    .pf-actions{ display:flex; gap:8px; flex-wrap:wrap; }
    .pf-btn,
    .pf-btn-ghost,
    .pf-btn-primary {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
    
      height: 40px;
      padding: 0 14px;
    
      border-radius: 10px;
      font-size: 14px;
      line-height: 1;
      white-space: nowrap;
    
      text-decoration: none;   /* critical for <a> */
      cursor: pointer;
    
      border: 1px solid var(--pf-border-strong);
      background: transparent;
      color: var(--pf-text);
    }
    
    .pf-btn:hover,
    .pf-btn-ghost:hover {
      border-color: var(--pf-accent);
      color: var(--pf-accent);
      text-decoration: none;
    }
    
    .pf-btn-primary {
      background: var(--pf-accent);
      color: #1b1b1b;
      border-color: var(--pf-accent);
    }
    
    .pf-btn-primary:hover {
      filter: brightness(1.05);
    }


    /* ===== Toolbar (search, toggle, chips) ===== */
    .pf-toolbar{ display:flex; flex-wrap:wrap; align-items:center; gap:12px 16px; margin:0 0 12px; }
    .pf-search{ flex:1 1 360px; max-width:800px; min-width:280px; }
    .pf-search input{
      width:100%; height:44px; font-size:16px; border-radius:12px;
      background:var(--pf-panel); color:var(--pf-text);
      border:1px solid var(--pf-border); padding:10px 14px;
    }
    .pf-search input::placeholder{ color:var(--pf-muted); }
    .pf-toolbar .form-check-input{ width:20px; height:20px; accent-color:var(--pf-accent); }
    .pf-toolbar .btn{ height:44px; border-radius:12px; }

    .pf-chips{ display:flex; flex-wrap:wrap; gap:8px; }
    .pf-chip{
      display:inline-flex; align-items:center; gap:8px; padding:8px 12px; border-radius:999px;
      background:var(--pf-panel); border:1px solid var(--pf-border); font-size:14px;
    }
    .pf-chip input{ accent-color:var(--pf-accent); }
    .pf-chip-active{
      border-color: var(--pf-accent);
      color: var(--pf-accent);
      box-shadow: 0 0 0 1px var(--pf-accent) inset;
    }
    
    /* ===== Table ===== */
    .table{ color:var(--pf-text); font-size:14px; }
    .table thead th{
      background:var(--pf-panel); position:sticky; top:0; z-index:2;
      border-bottom:1px solid var(--pf-border);
    }
    .table tbody tr:nth-child(even){ background:rgba(255,255,255,.06); }
    .table tbody tr:hover{ background:rgba(255,255,255,.10); }
    .table td, .table th{ padding:10px 12px; vertical-align:middle; }

    /* Inputs */
    #pf-form input[type="text"], #pf-form input[type="number"], #pf-form input[type="search"]{
      background:var(--pf-panel); color:var(--pf-text);
      border:1px solid var(--pf-border); border-radius:12px;
      height:38px; font-size:15px; padding:8px 12px;
    }
    #pf-form input::placeholder{ color:var(--pf-muted); }

    /* Qty cluster */
    .pf-qty-wrap{ display:flex; align-items:center; gap:6px; }
    .pf-qty-btn{
      min-width:34px; height:34px; border-radius:10px;
      background:transparent; color:var(--pf-text); border:1px solid var(--pf-border);
    }
    .pf-qty-btn:hover{ border-color:var(--pf-accent); color:var(--pf-accent); }

    /* Edited highlight */
    .is-warning{ outline:2px solid var(--pf-accent); }
    </style>
    """
    # inside render_inventory_table, right after nav_html = ... but before filter_bar
    add_form = """
    <form id="pf-add" method="post" action="/inventory/add"
          class="pf-toolbar mb-3 d-flex flex-wrap gap-2 align-items-center">
      <input name="name" placeholder="New item name" required class="form-control" style="max-width:260px">
      <input name="qty" type="number" inputmode="numeric" placeholder="Qty" class="form-control" style="max-width:110px">
      <button type="submit" class="pf-btn pf-btn-primary">➕ Add</button>
    </form>
    """

    # --- page + form (POST)
    page_html = (
        style +
        topbar +
        add_form +
        filter_bar +
        '<form id="pf-form" method="post">'
        '<input type="hidden" name="save" value="1">'
        '<input type="hidden" id="dirty_keys" name="dirty_keys" value="">'
        '<input type="hidden" id="mode" name="mode" value="save">'
        '<div class="table-responsive">'
        '<table class="table table-sm table-striped align-middle">'
        f"{thead_html}{tbody_html}"
        "</table>"
        "</div>"
        '<button id="pf-save" class="btn btn-success mt-2">💾 Save</button>'
        "</form>"
        # --- lightweight JS: track changes, Enter=save, and +/- buttons
        """
<script>
(function(){
  const editForm   = document.getElementById('pf-form');
  const filterForm = document.getElementById('pf-filter');
  const addForm    = document.getElementById('pf-add'); 
  const dirty = new Set();
  let lastEditedInput = null;  // track the last field the user interacted with

  function markDirty(inp){
    if(!inp) return;
    const orig = inp.getAttribute('data-orig');
    const key  = inp.name;
    if(orig === null || !key) return;
    if((inp.value ?? '') !== (orig ?? '')){
      dirty.add(key);
      inp.classList.add('is-warning');
    } else {
      dirty.delete(key);
      inp.classList.remove('is-warning');
    }
  }

  // Wire up all editable inputs
  document.querySelectorAll('#pf-form .pf-input').forEach(function(inp){
    inp.addEventListener('focus', () => { lastEditedInput = inp; });
    inp.addEventListener('input', () => { lastEditedInput = inp; markDirty(inp); });
    inp.addEventListener('change', () => { lastEditedInput = inp; markDirty(inp); });

    // Enter on an input => push to Shopify (changed-only)
    inp.addEventListener('keydown', function(e){
      if(e.key === 'Enter'){
        e.preventDefault();
        document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
        document.getElementById('mode').value = 'push';
        editForm.submit();
      }
    });
  });

  // Save button => local only
  const saveBtn = document.getElementById('pf-save');
  if (saveBtn) {
    saveBtn.addEventListener('click', function(){
      document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
      document.getElementById('mode').value = 'save';
    });
  }

  // Quantity bumpers
  function bump(targetName, delta){
    const inp = document.querySelector('#pf-form input[name="'+targetName+'"]');
    if(!inp) return;
    const v = parseInt(inp.value || '0', 10) || 0;
    inp.value = String(v + delta);
    lastEditedInput = inp;
    const evt = new Event('input', {bubbles:true});
    inp.dispatchEvent(evt);
    // Move focus into the input so Enter will submit instead of clicking the button again
    inp.focus();
    inp.select();
  }
  document.querySelectorAll('#pf-form .pf-inc').forEach(function(btn){
    btn.addEventListener('click', function(){
      bump(btn.getAttribute('data-target'), +1);
      btn.blur();
    });
  });
  document.querySelectorAll('#pf-form .pf-dec').forEach(function(btn){
    btn.addEventListener('click', function(){
      bump(btn.getAttribute('data-target'), -1);
      btn.blur();
    });
  });

  // Global Enter behavior:
  // - If focus is in the edit form but NOT on a .pf-input (e.g., a button), block it.
  // - If focus is in search, let it submit the filter GET.
    document.addEventListener('keydown', function(e){
    if (e.key !== 'Enter') return;

    const el = document.activeElement;
    const inEditForm = editForm && el && editForm.contains(el);
    const inFilter   = filterForm && el && filterForm.contains(el);
    const inAdd      = addForm && el && addForm.contains(el);

    // If we're in the Add form, do NOTHING here.
    // The addForm-specific listener will handle Enter.
    if (inAdd) {
      return;
    }

    // Let Enter in the filter/search form behave natively (submit GET /inventory)
    if (inFilter) {
      return;
    }

    // Inside the edit form but not in a cell input: block Enter to avoid button “clicks”
    if (inEditForm && (!el.classList || !el.classList.contains('pf-input'))) {
      e.preventDefault();
      return;
    }

    // If nothing focused but we have a lastEditedInput, treat Enter as "push to Shopify"
    if (!inFilter && !inEditForm && lastEditedInput) {
      e.preventDefault();
      document.getElementById('dirty_keys').value = Array.from(dirty).join(',');
      document.getElementById('mode').value = 'push';
      editForm.submit();
    }
  });

  if (addForm) {
    addForm.addEventListener('keydown', function(e){
      if (e.key === 'Enter') {
        e.preventDefault();
        e.stopPropagation();
        addForm.submit();
      }
    }, true);
  }
  // Enter in the search box should submit the GET filter immediately
  if (filterForm) {
    const search = filterForm.querySelector('input[name="q"]');
    if (search) {
      search.addEventListener('keydown', function(e){
        if (e.key === 'Enter') { /* default GET submit is fine */ }
      });
    }
  }
})();
</script>
        """
    )

    return page_html



def save_inventory_to_db(df, table_name: str = "inventory"):
    # Reuse global engine; don't hardcode paths here
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False, method="multi")
def shopify_sync_logic() -> int:
    """
    Populate inventory_df from Shopify; returns number of rows.
    Preserves local-only rows (no variant_id) across syncs.
    """
    global inventory_df
    _activate_session()

    # --- 1) Load previous full snapshot from DB (if exists) ---
    try:
        with engine.begin() as conn:
            prev = pd.read_sql_table("inventory", conn)
    except Exception:
        prev = inventory_df.copy()

    base_cols = [
        "name",
        "shopify_qty",
        "shopify_price",
        "shopify_tags",
        "shopify_status",
        "variant_id",
        "shopify_inventory_id",
        "total amount (4/1)",
        "notes",
    ]
    for c in base_cols:
        if c not in prev.columns:
            prev[c] = pd.NA

    # Local-only rows: no variant_id (or 0)
    local_only = prev[prev["variant_id"].isna() | (prev["variant_id"] == 0)].copy()

    # --- 2) Pull fresh Shopify variants ---
    rows = []
    products = shopify.Product.find(limit=250)
    while True:
        for p in products:
            tags_csv = (p.tags or "").strip()
            for v in (p.variants or []):
                try:
                    rows.append({
                        "name": f"{p.title} — {v.title}" if v.title and v.title.lower() != "default title" else p.title,
                        "shopify_qty": int(v.inventory_quantity) if v.inventory_quantity is not None else None,
                        "shopify_price": float(v.price) if v.price is not None else None,
                        "shopify_tags": tags_csv,
                        "shopify_status": p.status,
                        "variant_id": int(v.id) if v.id else None,
                        "shopify_inventory_id": int(v.inventory_item_id) if v.inventory_item_id else None,
                        "total amount (4/1)": None,
                        "notes": None,
                    })
                except Exception:
                    continue

        if hasattr(products, "has_next_page") and products.has_next_page():
            products = products.next_page()
        else:
            break

    cols = base_cols
    df = pd.DataFrame(rows, columns=cols)

    # --- 3) Carry total amount / notes for Shopify-backed rows by variant_id ---
    if not prev.empty and "variant_id" in prev.columns:
        carry = prev[["variant_id", "total amount (4/1)", "notes"]].drop_duplicates("variant_id")
        df = df.merge(carry, on="variant_id", how="left", suffixes=("", "_prev"))

        if "total amount (4/1)_prev" in df.columns:
            df["total amount (4/1)"] = df["total amount (4/1)"].combine_first(df["total amount (4/1)_prev"])
            df.drop(columns=["total amount (4/1)_prev"], inplace=True, errors="ignore")
        if "notes_prev" in df.columns:
            df["notes"] = df["notes"].combine_first(df["notes_prev"])
            df.drop(columns=["notes_prev"], inplace=True, errors="ignore")

    # --- 4) Append back local-only rows (no variant_id) ---
    if not local_only.empty:
        for c in df.columns:
            if c not in local_only.columns:
                local_only[c] = pd.NA
        for c in local_only.columns:
            if c not in df.columns:
                df[c] = pd.NA

        df = pd.concat([df, local_only], ignore_index=True, sort=False)

    # --- 5) In-place update of global inventory_df + persist ---
    # Maintain object identity so imports in routes still see the same DataFrame.
    inventory_df.drop(inventory_df.index, inplace=True)
    for c in list(inventory_df.columns):
        if c not in df.columns:
            inventory_df.drop(columns=c, inplace=True, errors="ignore")
    for c in df.columns:
        if c not in inventory_df.columns:
            inventory_df[c] = pd.NA

    inventory_df[df.columns] = df
    inventory_df = inventory_df[df.columns]

    try:
        save_inventory_to_db(inventory_df)
    except Exception as e:
        print(f"[WARN] Could not persist inventory_df: {e}")

    print(f"[SYNC] Loaded {len(inventory_df)} Shopify variants (+ local-only rows).")
    return len(inventory_df)

