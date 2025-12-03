from flask import Blueprint, request, redirect, flash


from .lib import (
    render_inventory_table, inventory_df, save_inventory_to_db,
    shopify_sync_logic, requires_auth, update_shopify_variant_price,
    update_shopify_qty, engine
)
import pandas as pd
import time

bp = Blueprint("inventory", __name__, url_prefix="/inventory")

_LAST_SYNC = 0
_TTL = 300  # 5 min
# --- meta helpers (persisted in SQLite) ---
def _meta_engine():
    from sqlalchemy import create_engine
    return create_engine("sqlite:////data/inventory.db")

def get_last_sync_str() -> str:
    try:
        with engine.begin() as con:
            con.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS inventory_meta (k TEXT PRIMARY KEY, v TEXT)"
            )
            row = con.exec_driver_sql(
                "SELECT v FROM inventory_meta WHERE k='last_sync'"
            ).fetchone()
            return (row[0] if row else "Never")
    except Exception:
        return "Unknown"

def set_last_sync_now():
    import datetime as dt
    with engine.begin() as con:
        con.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS inventory_meta (k TEXT PRIMARY KEY, v TEXT)"
        )
        con.exec_driver_sql(
            "INSERT INTO inventory_meta(k, v) VALUES ('last_sync', ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )

def _ensure_sync(force=False):
    global _LAST_SYNC
    if force or (time.time() - _LAST_SYNC) > _TTL:
        try:
            shopify_sync_logic()
            set_last_sync_now()
        finally:
            _LAST_SYNC = time.time()

@bp.route("/sync")
@requires_auth
def sync_now():
    _ensure_sync(True)
    flash("🔁 Synced Shopify.", "success")
    return redirect("/inventory")

@bp.route("/zero_current", methods=["POST"])
@requires_auth
def zero_current():
    col = "total amount (4/1)"
    if col in inventory_df.columns:
        inventory_df[col] = 0
        save_inventory_to_db(inventory_df)
        flash("🧹 Zeroed internal inventory (total amount) for audit prep.", "success")
    else:
        flash(f"Column '{col}' not found.", "warning")
    return redirect("/inventory")

@bp.route("/", methods=["GET", "POST"])
@requires_auth
def index():
    global inventory_df

    # 1) Make sure Shopify sync runs first (or respects TTL)
    _ensure_sync(request.args.get("sync") == "1")
    try:
        with engine.begin() as conn:
            inventory_df = pd.read_sql_table("inventory", conn)
    except Exception:
        pass

    df = inventory_df.copy()
    total_rows = len(df)

    # --- columns to show (hide tags/ids per request)
    keep_cols = [
        "total amount (4/1)", "name", "shopify_qty", "adjust_delta", "shopify_price",
        "shopify_value",
        # hidden: "shopify_tags", "variant_id", "shopify_inventory_id",
        "notes"
    ]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # derived value
    df["shopify_value"] = (df["shopify_qty"].fillna(0) * df["shopify_price"].fillna(0)).round(2)

    # --------- FILTERS ----------
    q = (request.args.get("q") or "").strip().lower()
    in_stock = (request.args.get("in_stock") == "1")
    tag_any = request.args.getlist("tag")  # multi-select via ?tag=a&tag=b

    if q:
        df = df[df["name"].astype(str).str.lower().str.contains(q)]
    if in_stock:
        df = df[df["shopify_qty"].fillna(0) > 0]
    if tag_any:
        wanted = [t.lower() for t in tag_any]

        def has_any(csv):
            s = str(csv or "").lower()
            return any(w in s for w in wanted)

        df = df[df["shopify_tags"].apply(has_any)]

    # pagination to keep it snappy
    limit = int(request.args.get("limit", "400"))
    df = df.head(limit)

    # map back to master df on save
    df["__orig_idx__"] = df.index
    df["adjust_delta"] = ""

    # ---------- SAVE (changed-only) ----------
    if request.method == "POST" and request.form.get("save") == "1":
        updates = request.form.to_dict(flat=True)
        dirty_keys = set((request.form.get("dirty_keys") or "").split(","))  # "cell_0_shopify_qty,..."
        mode = (request.form.get("mode") or "save")  # "push" from Enter, "save" from button

        editable_cast = {
            "shopify_qty": int,
            "shopify_price": float,
            "total amount (4/1)": int,
            "adjust_delta": int,
            "notes": str
        }

        # collect changed rows so we can optionally push to Shopify
        changed_rows = []  # list of {"variant_id":int, "shopify_inventory_id":int, "shopify_qty"?:int, "shopify_price"?:float}
        for i, row in df.reset_index(drop=True).iterrows():
            orig_idx = row.get("__orig_idx__", row.name)
            row_change = {
                "variant_id": int(row.get("variant_id") or 0),
                "shopify_inventory_id": int(row.get("shopify_inventory_id") or 0),
            }
            touched = False
            pending_adjust = None  # ← collect without persisting

            for col, caster in editable_cast.items():
                key = f"cell_{i}_{col}"
                if key not in updates:
                    continue
                if dirty_keys and key not in dirty_keys:
                    continue
                raw = (updates[key] or "").strip()

                if col == "adjust_delta":
                    # don't store the adjust column; just remember it
                    if raw != "":
                        try:
                            pending_adjust = caster(raw)
                        except Exception:
                            pending_adjust = None
                    continue

                if raw == "":
                    inventory_df.at[orig_idx, col] = None
                    if col in ("shopify_qty", "shopify_price"):
                        row_change[col] = None
                        touched = True
                    continue

                try:
                    val = caster(raw)
                    if col == "shopify_price":
                        val = round(val, 2)
                    inventory_df.at[orig_idx, col] = val
                    if col in ("shopify_qty", "shopify_price"):
                        row_change[col] = val
                        touched = True
                except Exception:
                    pass

            # If an adjust was provided, apply it AFTER reading other fields.
            if pending_adjust is not None:
                base_qty = int(inventory_df.at[orig_idx, "shopify_qty"] or 0)
                new_qty = base_qty + int(pending_adjust)
                inventory_df.at[orig_idx, "shopify_qty"] = new_qty
                row_change["shopify_qty"] = new_qty
                touched = True

            if touched:
                changed_rows.append(row_change)

        # persist local snapshot
        save_inventory_to_db(inventory_df)

        # optionally push changed fields to Shopify immediately (Enter path)
        if mode == "push" and changed_rows:
            pushed = 0
            failed = 0

            # need a location id for quantity writes
            from .lib import LOCATION_ID, update_shopify_variant_price, update_shopify_qty, DRY_RUN
            loc_id = None
            try:
                loc_id = int(LOCATION_ID) if LOCATION_ID else None
            except Exception:
                loc_id = None

            for ch in changed_rows:
                # price
                if "shopify_price" in ch and ch.get("variant_id"):
                    ok = update_shopify_variant_price(int(ch["variant_id"]), ch["shopify_price"])
                    pushed += 1 if ok else 0
                    failed += 0 if ok else 1

                # qty
                if "shopify_qty" in ch and ch.get("shopify_inventory_id") and loc_id:
                    ok = update_shopify_qty(int(ch["shopify_inventory_id"]), loc_id,
                                            int(ch["shopify_qty"]) if ch["shopify_qty"] is not None else 0)
                    pushed += 1 if ok else 0
                    failed += 0 if ok else 1
                elif "shopify_qty" in ch and not loc_id:
                    failed += 1  # cannot push qty without a location id

            mode_label = "DRY RUN" if DRY_RUN else "LIVE"
            if failed and not loc_id and any("shopify_qty" in ch for ch in changed_rows):
                flash("⚠️ Missing LOCATION_ID env var: pushed prices, but could not push quantities.", "warning")
            flash(f"🚀 {mode_label}: pushed {pushed} field update(s){' with errors' if failed else ''}.",
                  "success" if failed == 0 else "warning")
        else:
            flash("💾 Saved locally.", "success")

        return redirect(request.full_path or "/inventory")

    # ------- TAG OPTIONS for filters (top 50 by frequency) -------
    # Build once per request from the master df (not just the page slice)
    tag_counts = {}
    for csv in inventory_df.get("shopify_tags", pd.Series(dtype=str)).fillna(""):
        for t in [x.strip() for x in str(csv).split(",") if x.strip()]:
            k = t.lower()
            tag_counts[k] = tag_counts.get(k, 0) + 1
    curated = [
        "sealed", "slab", "collection box", "tin", "etb", "pcetb",
        "booster box", "booster pack",
    ]
    tag_options = curated
    from .lib import DRY_RUN
    meta = {
        "last_sync": get_last_sync_str(),
        "mode_label": ("DRY RUN" if DRY_RUN  else "LIVE"),
    }

    # ------- render -------
    html = render_inventory_table(
        filtered_df=df,
        title=f"🗂️ Shopify Inventory ({total_rows} variants; showing {len(df)})",
        show_columns=keep_cols,
        hidden_buttons=["only_rc", "unpublished", "untouched", "sync_rc"],
        editable_columns=["shopify_qty", "total amount (4/1)", "shopify_price", "notes", "adjust_delta" ],
        filters={
            "q": q,
            "in_stock": in_stock,
            "tag_options": tag_options,
            "selected_tags": tag_any,
        },
        meta = meta,
    )

    if html is None:
        raise RuntimeError("render_inventory_table returned None.")
    return html



@bp.route("/push_prices", methods=["POST"])
@requires_auth
def push_prices():
    """
    Bulk push prices for visible/filtered rows that have 'shopify_price' editable cells in the table form.
    Works with your existing inline-edit form posts.
    """
    # Expect form keys like cell_{row_index}_shopify_price that you already use for saving.
    changed = 0
    failed = 0

    # We use the master df (inventory_df) because your inline save already copied changes there.
    df = inventory_df

    # If you include selected rows via checkboxes (e.g., merge_ids), respect those; else push all rows that have variant_id & price.
    selected = request.form.getlist("merge_ids")
    if selected:
        idxs = [int(i) for i in selected]
        subset = df.iloc[idxs]
    else:
        subset = df[df["variant_id"].notna() & df["shopify_price"].notna()]

    for _, row in subset.iterrows():
        try:
            vid = int(row["variant_id"])
            price = float(row["shopify_price"])
            ok = update_shopify_variant_price(vid, price)
            changed += 1 if ok else 0
            failed  += 0 if ok else 1
        except Exception as e:
            failed += 1

    mode = "DRY RUN" if DRY_RUN else "LIVE"
    flash(f"💸 {mode}: pushed prices for {changed} variant(s){' (some failed)' if failed else ''}.", "success" if failed == 0 else "warning")
    return redirect("/inventory")

@bp.route("/push_price_row/<int:row_index>", methods=["POST"])
@requires_auth
def push_price_row(row_index: int):
    """
    Push price for a single row (add a small button per row).
    """
    try:
        row = inventory_df.iloc[row_index]
        vid = int(row["variant_id"])
        price = float(row["shopify_price"])
        ok = update_shopify_variant_price(vid, price)
        mode = "DRY RUN" if DRY_RUN else "LIVE"
        flash(f"💸 {mode}: {'OK' if ok else 'Failed'} updating variant {vid} to {price:.2f}", "success" if ok else "warning")
    except Exception as e:
        flash(f"Error pushing price for row {row_index}: {e}", "danger")
    return redirect("/inventory")

@bp.route("/add", methods=["POST"])
@requires_auth
def add_local():
    name = (request.form.get("name") or "").strip()
    qty  = request.form.get("qty")
    price = request.form.get("price")
    tags  = (request.form.get("tags") or "").strip()

    if not name:
        flash("Name is required to add an item.", "warning")
        return redirect("/inventory")

    try:
        qty_val = int(qty) if qty not in (None, "",) else 0
    except Exception:
        qty_val = 0

    try:
        price_val = float(price) if price not in (None, "",) else None
    except Exception:
        price_val = None

    from .lib import add_local_inventory_row
    add_local_inventory_row(name=name, qty=qty_val, price=price_val, tags=tags)
    flash(f"➕ Added local-only item: {name}", "success")
    # bounce back with a filter on the new name so it’s visible
    return redirect(f"/inventory?q={name}")