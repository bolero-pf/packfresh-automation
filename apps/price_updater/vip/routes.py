# vip/routes.py
from flask import Blueprint, jsonify, current_app
from flask import request
from .service import fetch_customer_ids_page
from .verify import verify_flow_signature
from pathlib import Path
import os, sys, subprocess, threading
from datetime import datetime, timezone, date

try:
    from integrations.klaviyo import upsert_profile
except Exception:
    # Local dev fallback if project root isn't on sys.path
    import sys, os
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # parent of vip/
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from integrations.klaviyo import upsert_profile
bp = Blueprint("vip", __name__, url_prefix="/vip")
@bp.before_request
def _verify():
    verify_flow_signature()
@bp.get("/ping")
def ping():
    return jsonify({"ok": True, "service": "vip", "msg": "pong"}), 200
@bp.post("/backfill")
def vip_backfill():
    """
    Paged backfill with failure skip.
    POST:
      {"page_size": 50, "cursor": null, "retry_ids": ["gid://shopify/Customer/.."], "dry_run": false}
    If retry_ids is provided, we process exactly those IDs (no paging).
    Returns:
      {"ok":true, "processed": N, "next_cursor": "...", "failed_ids":[...], "items":[...sample]}
    """
    payload = request.get_json(silent=True) or {}
    page_size = int(payload.get("page_size", 50))
    after = payload.get("cursor")
    dry_run = bool(payload.get("dry_run", False))
    retry_ids = payload.get("retry_ids")

    results, failed = [], []

    if retry_ids:
        ids = list(retry_ids)
        next_cursor = None
    else:
        from .service import fetch_customer_ids_page
        ids, next_cursor = fetch_customer_ids_page(first=page_size, after=after)

    if dry_run:
        from .service import compute_rolling_90d_spend, tier_from_spend, current_quarter_window
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc)
        for gid in ids:
            try:
                spend = compute_rolling_90d_spend(gid, today=today)
                tier = tier_from_spend(spend)
                lock = None
                if tier in ("VIP1","VIP2","VIP3"):
                    win = current_quarter_window(today.date())
                    lock = {"start": win["start"], "end": win["end"], "tier": tier}
                results.append({"customer": gid, "spend90d": spend, "tier": tier, "lock": lock})
            except Exception as e:
                failed.append({"customer": gid, "error": str(e)})
        return jsonify({"ok": True, "processed": len(ids), "next_cursor": next_cursor, "failed_ids": failed, "items": results[:10]})

    # real writes
    from .service import backfill_customer
    import time
    for idx, gid in enumerate(ids, start=1):
        try:
            results.append(backfill_customer(gid))
        except Exception as e:
            failed.append({"customer": gid, "error": str(e)})
        # small pause to be gentle on rate limits
        time.sleep(0.05)

    return jsonify({
        "ok": True,
        "processed": len(ids),
        "next_cursor": next_cursor,
        "failed_ids": failed,
        "items": results[:10]
    })



# live event routes
from .service import on_order_paid as _on_paid, on_refund_created as _on_refund
from datetime import date

from .service import (
    compute_rolling_90d_spend, tier_from_spend,
    get_customer_state, write_state, inside_lock, _pick_lock_until,
    _days_to_date, _gap_to_next_tier_cents, _gap_to_requalify_cents
)


def _needs_vip_tag_update(current_tags: set[str], desired: set[str]) -> bool:
    def vip_only(s): return {t for t in s if t.startswith("VIP")}
    return vip_only(current_tags) != vip_only(desired)

def _desired_vip_tags(tier: str, lock_active: bool) -> set[str]:
    out = set()
    if tier != "VIP0":
        out.add(tier)             # VIP1/2/3 only
    # If you want a lock marker, uncomment:
    # if lock_active and tier != "VIP0":
    #     out.add(f"{tier}-lock")
    return out

def _push_vip_to_klaviyo(gid: str):
    now = datetime.now(timezone.utc)
    today = now.date()

    state = get_customer_state(gid)
    raw_lock = state.get("lock") or {}
    lock_active = inside_lock(raw_lock, today)

    # fresh spend + tier
    spend = compute_rolling_90d_spend(gid, today=now)
    new_tier = tier_from_spend(spend)

    # --- LOCK POLICY ---
    if lock_active:
        # keep existing tier+lock during active window
        effective_tier = state["tier"]
        effective_lock = raw_lock
    else:
        # lock expired/not set: choose policy
        if new_tier in ("VIP1","VIP2","VIP3"):
            from .service import rolling_90_lock_for
            effective_lock = rolling_90_lock_for(new_tier, today)  # seed new window
        else:
            effective_lock = {}
        effective_tier = new_tier

    # persist (metafields + any tier change)
    write_state(gid, rolling=spend, tier=effective_tier, lock=effective_lock)

    # --- VIP TAGS (only if needed) ---
    cur_tags = set(state.get("tags") or [])
    desired  = _desired_vip_tags(effective_tier, inside_lock(effective_lock or {}, today))
    if _needs_vip_tag_update(cur_tags, desired):
        from .service import set_vip_tag
        set_vip_tag(gid, effective_tier)

    # --- KLAVIYO PROPS ---
    rolling_cents = int(round(spend * 100))
    lock_until = _pick_lock_until(effective_lock)  # tolerate "end"/"until", strip time
    props = {
        "vip_tier": effective_tier,
        "vip_rolling_90d_cents": rolling_cents,
        "vip_lock_start": (effective_lock or {}).get("start"),
        "vip_lock_until": lock_until,
        "vip_lock_active": bool(inside_lock(effective_lock or {}, today)),
        "vip_days_to_expire": _days_to_date(lock_until, today),
        "vip_requalify_gap_cents": _gap_to_requalify_cents(effective_tier, rolling_cents),
        "vip_next_tier_gap_cents": _gap_to_next_tier_cents(effective_tier, rolling_cents),
        "vip_last_change": now.isoformat(),
    }

    email = state.get("email")
    external_id = gid.split("/")[-1]
    upsert_profile(email=email, external_id=external_id, properties=props)

@bp.post("/order_paid")
def order_paid():
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]      # GID
    order_id    = payload["order_id"]         # GID
    result = _on_paid(customer_id, order_id)
    try:
        _push_vip_to_klaviyo(customer_id)
    except Exception as e:
        current_app.logger.warning(f"Klaviyo push failed: {e}")
    return jsonify({"ok": True, **result})
@bp.post("/price_update")
def price_update():
    """
    Secure trigger (Shopify Flow → POST {}) to start dailyrunner.py in background.
    Requires X-Flow-Secret and application/json.
    """
    try:
        # --- robust absolute paths ---
        # Adjust these to match your repo layout if needed.
        # Assuming this file is project_root/vip/routes.py
        ROOT = Path(__file__).resolve().parents[1]   # project root
        SCRIPT = ROOT / "dailyrunner.py"            # e.g. project_root/dailyrunner.py
        LOG    = ROOT / "run_output.log"

        def launch():
            LOG.parent.mkdir(parents=True, exist_ok=True)
            with open(LOG, "a", buffering=1) as f:
                f.write(f"\n=== RUN {datetime.now().isoformat()} ===\n")
                subprocess.Popen(
                    [sys.executable, str(SCRIPT)],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=str(ROOT),             # <- important if script relies on CWD
                )

        threading.Thread(target=launch, daemon=True).start()
        return jsonify({"ok": True, "started": True}), 200

    except Exception as e:
        # Log full stacktrace to server logs and also surface string for now
        current_app.logger.exception("price_update failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.post("/refund_created")
def refund_created():
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]      # GID
    order_id    = payload["order_id"]         # GID (the refunded order)
    result = _on_refund(customer_id, order_id)
    try:
        _push_vip_to_klaviyo(customer_id)
    except Exception as e:
        current_app.logger.warning(f"Klaviyo push failed: {e}")
    return jsonify({"ok": True, **result})

@bp.post("/recalc")
def recalc_one():
    # POST { "customer_id": "gid://shopify/Customer/123" }
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]
    from .service import compute_rolling_90d_spend, tier_from_spend, get_customer_state
    from .service import rolling_90_lock_for, write_state
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc)
    state = get_customer_state(customer_id)
    spend = compute_rolling_90d_spend(customer_id, today=today)
    new_tier = tier_from_spend(spend)
    # if already locked and active, keep lock; otherwise seed current-quarter if VIP1+
    lock = state["lock"]
    from .service import inside_lock
    if not inside_lock(lock, today.date()):
        lock = rolling_90_lock_for(new_tier, today.date()) if new_tier in ("VIP1","VIP2","VIP3") else {}

    write_state(customer_id, rolling=spend, tier=new_tier, lock=lock, prov={})
    return jsonify({"ok": True, "customer": customer_id, "spend90d": spend, "tier": new_tier, "lock": lock})

@bp.get("/diag/customer")
def diag_customer():
    # GET /vip/diag/customer?id=gid://shopify/Customer/123
    from flask import request
    cid = request.args.get("id")
    from .service import get_customer_state
    state = get_customer_state(cid)
    return jsonify({"ok": True, "state": state})

@bp.post("/promote_public")
def vip_promote_public():
    """
    Fill custom.vip_public for customers using existing internal metafields.
    POST: {"page_size": 250, "cursor": null, "retry_ids": []}
    Returns: {"ok":true,"processed":N,"next_cursor": "...","failed_ids":[...]}
    """
    payload = request.get_json(silent=True) or {}
    page_size = int(payload.get("page_size", 250))
    after = payload.get("cursor")
    retry_ids = payload.get("retry_ids")

    from .service import fetch_customer_ids_page, get_customer_state, build_public_from_state, upsert_customer_metafields
    results, failed = [], []

    if retry_ids:
        ids = list(retry_ids)
        next_cursor = None
    else:
        ids, next_cursor = fetch_customer_ids_page(first=page_size, after=after)

    for gid in ids:
        try:
            state = get_customer_state(gid)  # reads existing internal metafields only
            public = build_public_from_state(state)
            upsert_customer_metafields(gid, {
                ("custom","vip_public","json"): public,
            })
            results.append({"customer": gid, "tier": state["tier"]})
        except Exception as e:
            failed.append({"customer": gid, "error": str(e)})

    return jsonify({
        "ok": True,
        "processed": len(ids),
        "next_cursor": next_cursor,
        "failed_ids": failed,
        "items": results[:10]
    })


@bp.post("/sweep_vips")
def sweep_vips():
    """
    Process a SINGLE page per call and return next_cursor.
    The caller (runner/Flow) is responsible for calling again with cursor.
    """
    from flask import request, jsonify
    from .service import shopify_gql

    payload = request.get_json(silent=True) or {}
    page_size = int(payload.get("page_size", 25))
    after = payload.get("cursor")

    VIP_TIERS = ["VIP1", "VIP2", "VIP3"]
    SUFFIXES  = ["", "-risk", "-hopeful"]
    terms     = [f'tag:"{t}{s}"' for t in VIP_TIERS for s in SUFFIXES]
    q = " OR ".join(terms) if terms else 'tag:"VIP1" OR tag:"VIP2" OR tag:"VIP3"'

    QUERY = """
    query($first:Int!, $after:String, $q:String!){
      customers(first:$first, after:$after, query:$q, sortKey:ID){
        edges{ cursor node{ id } }
        pageInfo{ hasNextPage endCursor }
      }
    }"""

    processed, failed = 0, []
    data = shopify_gql(QUERY, {"first": page_size, "after": after, "q": q})
    cs = data["data"]["customers"]
    ids = [e["node"]["id"] for e in cs["edges"]]

    for gid in ids:
        try:
            _push_vip_to_klaviyo(gid)  # canonical logic
            processed += 1
        except Exception as e:
            failed.append({"customer": gid, "error": str(e)})

    next_cursor = cs["pageInfo"]["endCursor"] if cs["pageInfo"]["hasNextPage"] else None
    return jsonify({"ok": True, "processed": processed, "next_cursor": next_cursor, "failed_ids": failed})





@bp.post("/retag_only")
def vip_retag_only():
    """
    Tags-only normalization (no lock/renew/date changes).
    POST:
      {"page_size": 50, "cursor": null, "retry_ids": ["gid://shopify/Customer/..."], "dry_run": false}
    Returns:
      {"ok":true, "processed": N, "next_cursor": "...", "failed_ids":[...], "items":[...sample]}
    """
    payload = request.get_json(silent=True) or {}
    page_size = int(payload.get("page_size", 50))
    after = payload.get("cursor")
    dry_run = bool(payload.get("dry_run", False))
    retry_ids = payload.get("retry_ids")

    if retry_ids:
        ids = list(retry_ids)
        next_cursor = None
    else:
        ids, next_cursor = fetch_customer_ids_page(first=page_size, after=after)

    results, failed = [], []

    if dry_run:
        # Just echo intended action, don’t write
        from .service import get_customer_state, normalize_tier
        for gid in ids:
            try:
                state = get_customer_state(gid)
                tier = normalize_tier(state.get("tier") or "VIP0")
                results.append({"customer": gid, "would_remove": ["VIP","VIP1","VIP2","VIP3"], "would_add": [tier] if tier in {"VIP1","VIP2","VIP3"} else []})
            except Exception as e:
                failed.append({"customer": gid, "error": str(e)})
        return jsonify({"ok": True, "processed": len(ids), "next_cursor": next_cursor, "failed_ids": failed, "items": results[:10]})

    # Real writes
    from .service import retag_customer_tags_only
    import time
    for gid in ids:
        try:
            results.append(retag_customer_tags_only(gid))
        except Exception as e:
            failed.append({"customer": gid, "error": str(e)})
        time.sleep(0.05)  # be gentle on API limits

    return jsonify({"ok": True, "processed": len(ids), "next_cursor": next_cursor, "failed_ids": failed, "items": results[:10]})

@bp.post("/sweep_kick")
def sweep_kick():
    """
    Fast trigger for Shopify Flow: start a background VIP sweep that pages
    through /vip/sweep_vips until done. Returns immediately.
    """
    import threading, time, requests, os
    from datetime import datetime

    # simple overlap lock (prevents double-runs)
    LOCK = "/tmp/vip_sweep.lock"
    def try_lock():
        try:
            # stale lock cleanup (2h)
            if os.path.exists(LOCK) and (time.time() - os.path.getmtime(LOCK)) > 2*60*60:
                os.remove(LOCK)
            fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            return False

    def release_lock():
        if os.path.exists(LOCK):
            os.remove(LOCK)

    def worker():
        try:
            base = os.environ.get("BASE_ORIGIN", "https://prices.pack-fresh.com")
            url  = f"{base}/vip/sweep_vips"
            headers = {
                "Content-Type": "application/json",
                "X-Flow-Secret": os.environ.get("VIP_FLOW_SECRET",""),
            }
            cursor = None
            total  = 0
            while True:
                body = {"page_size": 25}
                if cursor:
                    body["cursor"] = cursor
                r = requests.post(url, json=body, headers=headers, timeout=60)
                r.raise_for_status()
                data = r.json()
                total += int(data.get("processed", 0))
                cursor = data.get("next_cursor")
                # small breath for API limits
                time.sleep(0.2)
                if not cursor:
                    break
            print(f"[VIP SWEEP] DONE {datetime.now().isoformat()} total={total}")
        finally:
            release_lock()

    if not try_lock():
        return jsonify({"ok": True, "status": "already_running"}), 202

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"ok": True, "status": "started"}), 200
