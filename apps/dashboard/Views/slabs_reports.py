# apps/dashboard/views/slabs_reports.py
import os, json, csv, re, time
from flask import Blueprint, current_app, render_template, abort, send_file, request, redirect, url_for, flash
from ..services.slabs.report_io import _BUCKETS  # reuse the same list
from ..services.shopify.variant import update_variant_price

bp_slabs_reports = Blueprint("slabs_reports", __name__, url_prefix="/slabs/reports")

# ---- paths / IO helpers -----------------------------------------------------

OUT_ROOT = "out"

def _runs_root():
    return os.path.join(OUT_ROOT, "runs")

def _csv_path(run_id, bucket):
    return os.path.join(_runs_root(), run_id, f"{bucket}.csv")

def _applied_path(run_id):
    return os.path.join(_runs_root(), run_id, "applied.json")

def _approved_path(run_id):
    return os.path.join(_runs_root(), run_id, "approved.json")

def _load_json_list(path):
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                v = json.load(f)
                return v if isinstance(v, list) else []
        except Exception:
            pass
    return []
def _shopify_client():
    # Prefer your config slot
    sc = current_app.config.get("SHOPIFY_CLIENT")
    if sc:
        return sc

    # Back-compat fallback
    sc = current_app.extensions.get("shopify_client")
    if sc:
        return sc

    raise RuntimeError(
        "Shopify client not configured. Put an instance at "
        "app.config['SHOPIFY_CLIENT'] (preferred) or app.extensions['shopify_client']."
    )

def _save_json_list(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

def _load_applied(run_id):
    return _load_json_list(_applied_path(run_id))

def _save_applied(run_id, rows):
    _save_json_list(_applied_path(run_id), rows)

def _load_approved(run_id):
    return _load_json_list(_approved_path(run_id))

def _save_approved(run_id, rows):
    _save_json_list(_approved_path(run_id), rows)

def _csv_rows(run_id, bucket):
    p = _csv_path(run_id, bucket)
    if not os.path.isfile(p):
        return []
    with open(p, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)

def _parse_price(v):
    try:
        return float(re.sub(r"[^\d\.\-]", "", str(v)))
    except Exception:
        return None

def _find_row(rows, variant_id: str):
    for r in rows:
        if str(r.get("variant_id")) == str(variant_id):
            return r
    return None

# ---- routes: index & summaries (unchanged) ----------------------------------

@bp_slabs_reports.route("/")
def runs_index():
    root = _runs_root()
    runs = []
    if os.path.isdir(root):
        for name in sorted(os.listdir(root), reverse=True):
            p = os.path.join(root, name, "manifest.json")
            if os.path.isfile(p):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        manifest = json.load(f)
                    runs.append(manifest)
                except Exception:
                    pass
    return render_template("slabs/runs_index.html", runs=runs)

@bp_slabs_reports.route("/<run_id>")
def run_summary(run_id):
    manifest_path = os.path.join(_runs_root(), run_id, "manifest.json")
    if not os.path.isfile(manifest_path):
        abort(404)
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    return render_template("slabs/run_summary.html", m=manifest)

# ---- main bucket view (kept), now also passes approved/applied flags --------

@bp_slabs_reports.route("/<run_id>/<bucket>")
def run_bucket(run_id, bucket):
    if bucket not in ("updated","flag_down","no_data","skipped"):
        abort(404)

    csv_path = _csv_path(run_id, bucket)
    if not os.path.isfile(csv_path):
        headers, rows = [], []
    else:
        with open(csv_path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            headers = list(r.fieldnames or [])
            rows = list(r)

    approved = _load_approved(run_id)
    applied  = _load_applied(run_id)

    approved_ids = {str(x.get("variant_id")) for x in approved}
    applied_ids  = {str(x.get("variant_id")) for x in applied}

    # NEW: map variant_id -> saved price
    approved_map = {
        str(x.get("variant_id")): x.get("price")
        for x in approved
        if x.get("price") is not None
    }

    return render_template(
        "slabs/run_bucket.html",
        run_id=run_id,
        bucket=bucket,
        headers=headers,
        rows=rows,
        approved_ids=approved_ids,
        applied_ids=applied_ids,
        approved_map=approved_map,                     # ← add
        allow_push=current_app.config.get("ALLOW_PRICE_WRITES", False),
    )


@bp_slabs_reports.route("/<run_id>/<bucket>.csv")
def download_bucket_csv(run_id, bucket):
    p = _csv_path(run_id, bucket)
    if not os.path.isfile(p): abort(404)
    return send_file(p, mimetype="text/csv", as_attachment=True, download_name=f"{run_id}_{bucket}.csv")

# ---- NEW: SAVE (approve) routes --------------------------------------------

@bp_slabs_reports.post("/<run_id>/<bucket>/save-one")
def save_one(run_id, bucket):
    if bucket not in _BUCKETS: abort(404)
    variant_id = request.form.get("variant_id")
    price_raw  = request.form.get("price")
    source     = request.form.get("source")
    if not variant_id:
        flash("Missing variant_id", "error")
        return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))

    rows = _csv_rows(run_id, bucket)
    row = _find_row(rows, variant_id)
    price = _parse_price(price_raw or (row.get("target") if row else None) or (row.get("new") if row else None))
    if price is None:
        flash("Missing price", "error")
        return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))

    approved = _load_approved(run_id)
    vid_s = str(variant_id)
    found = None
    for a in approved:
        if str(a.get("variant_id")) == vid_s:
            found = a
            break

    if found:
        # overwrite existing saved value
        found["price"]  = price
        found["ts"]     = int(time.time())
        found["bucket"] = bucket
        found["note"]   = f"source={source}" if source else "save-one"
        msg = "Updated"
    else:
        approved.append({
            "ts": int(time.time()),
            "variant_id": variant_id,
            "price": price,
            "bucket": bucket,
            "note": f"source={source}" if source else "save-one"
        })
        msg = "Saved"

    _save_approved(run_id, approved)
    flash(f"{msg} {variant_id} at {price:.2f}", "success")
    return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))


@bp_slabs_reports.post("/<run_id>/<bucket>/save-bulk")
def save_bulk(run_id, bucket):
    if bucket not in _BUCKETS: abort(404)
    ids = set(request.form.getlist("selected"))
    rows = _csv_rows(run_id, bucket)

    # prices posted as price[<vid>]
    price_map = {}
    for k, v in request.form.items():
        m = re.match(r'^price\[(.+)\]$', k)
        if m:
            p = _parse_price(v)
            if p is not None:
                price_map[str(m.group(1))] = p

    approved = _load_approved(run_id)
    index = {str(a.get("variant_id")): a for a in approved}  # quick lookup

    add_cnt = 0
    upd_cnt = 0

    for r in rows:
        vid = str(r.get("variant_id"))
        if ids and vid not in ids:
            continue

        # prefer user-edited; fallback to suggested
        price = price_map.get(vid)
        if price is None:
            price = _parse_price(r.get("target") or r.get("new"))
        if price is None:
            continue

        if vid in index:
            # update existing
            index[vid]["price"]  = price
            index[vid]["ts"]     = int(time.time())
            index[vid]["bucket"] = bucket
            index[vid]["note"]   = "bulk-update"
            upd_cnt += 1
        else:
            rec = {
                "ts": int(time.time()),
                "variant_id": vid,
                "price": price,
                "bucket": bucket,
                "note": "bulk-add"
            }
            approved.append(rec)
            index[vid] = rec
            add_cnt += 1

    _save_approved(run_id, approved)
    flash(f"Saved {add_cnt} new, updated {upd_cnt}", "success" if (add_cnt or upd_cnt) else "warning")
    return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))

# ---- NEW: PUSH (apply to Shopify) routes -----------------------------------

def _apply_update(variant_id: str, price: float, *, dry: bool) -> tuple[bool, str | None]:
    if dry:
        return True, "[DRY RUN]"
    try:
        update_variant_price(_shopify_client(), variant_id, price)
        return True, None
    except Exception as e:
        return False, str(e)

@bp_slabs_reports.post("/<run_id>/<bucket>/push-one")
def push_one(run_id, bucket):
    if bucket not in _BUCKETS: abort(404)
    variant_id = request.form.get("variant_id")
    price = _parse_price(request.form.get("price"))
    if not variant_id or price is None:
        flash("Missing variant_id or price", "error")
        return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))

    dry = not current_app.config.get("ALLOW_PRICE_WRITES", False)
    ok, err = _apply_update(variant_id, price, dry=dry)

    applied = _load_applied(run_id)
    applied.append({
        "ts": int(time.time()),
        "variant_id": variant_id,
        "price": price,
        "bucket": bucket,
        "dry_run": dry,
        "note": "push-one"
    })
    _save_applied(run_id, applied)

    if ok:
        flash(f"{'[DRY] ' if dry else ''}Updated {variant_id} → {price:.2f}", "success" if not dry else "warning")
    else:
        flash(f"Failed to update {variant_id}: {err}", "error")
    return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))

@bp_slabs_reports.post("/<run_id>/<bucket>/push-bulk")
def push_bulk(run_id, bucket):
    if bucket not in _BUCKETS: abort(404)
    ids = set(request.form.getlist("selected"))
    rows = _csv_rows(run_id, bucket)

    # Grab posted bulk prices if present
    price_map = {}
    for k, v in request.form.items():
        m = re.match(r'^price\[(.+)\]$', k)
        if m:
            p = _parse_price(v)
            if p is not None:
                price_map[str(m.group(1))] = p

    dry = not current_app.config.get("ALLOW_PRICE_WRITES", False)
    applied = _load_applied(run_id)
    applied_set = {str(x.get("variant_id")) for x in applied}

    ok_cnt, err_cnt = 0, 0
    for r in rows:
        vid = str(r.get("variant_id"))
        if ids and vid not in ids:
            continue
        if vid in applied_set:
            continue

        # Prefer user-edited → else approved → else suggested
        price = price_map.get(vid)
        if price is None:
            # if you want, pull from approved_map here too
            price = _parse_price(r.get("target") or r.get("new"))
        if price is None:
            err_cnt += 1
            continue

        ok, err = _apply_update(vid, price, dry=dry)
        applied.append({
            "ts": int(time.time()),
            "variant_id": vid,
            "price": price,
            "bucket": bucket,
            "dry_run": dry,
            "note": "push-bulk" if ok else f"push-bulk-error:{err}"
        })
        ok_cnt += 1 if ok else 0
        err_cnt += 0 if ok else 1

    _save_applied(run_id, applied)
    if dry:
        flash(f"[DRY] Would push {ok_cnt} updates, {err_cnt} failed/skipped (writes disabled)", "warning")
    else:
        flash(f"Pushed {ok_cnt} updates, {err_cnt} failed", "success" if err_cnt == 0 else "warning")
    return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))


@bp_slabs_reports.post("/<run_id>/<bucket>/push-approved")
def push_approved(run_id, bucket):
    """Apply everything saved in approved.json (for this run), skipping already-applied."""
    if bucket not in _BUCKETS: abort(404)
    dry = not current_app.config.get("ALLOW_PRICE_WRITES", False)

    approved = _load_approved(run_id)
    applied  = _load_applied(run_id)
    applied_set = {str(x.get("variant_id")) for x in applied}

    ok_cnt, err_cnt = 0, 0
    for a in approved:
        vid = str(a.get("variant_id"))
        price = _parse_price(a.get("price"))
        if not vid or price is None or vid in applied_set:
            continue
        ok, err = _apply_update(vid, price, dry=dry)
        applied.append({
            "ts": int(time.time()),
            "variant_id": vid,
            "price": price,
            "bucket": a.get("bucket") or bucket,
            "dry_run": dry,
            "note": "push-approved" if ok else f"push-approved-error:{err}"
        })
        if ok:
            ok_cnt += 1
        else:
            err_cnt += 1

    _save_applied(run_id, applied)
    if dry:
        flash(f"[DRY] Would push {ok_cnt} approved, {err_cnt} failed/skipped (writes disabled)", "warning")
    else:
        flash(f"Pushed {ok_cnt} approved, {err_cnt} failed", "success" if err_cnt == 0 else "warning")
    return redirect(url_for("slabs_reports.run_bucket", run_id=run_id, bucket=bucket))
