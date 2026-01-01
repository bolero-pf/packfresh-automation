# vip/routes.py
from flask import Blueprint, jsonify, current_app
from flask import request
from .service import fetch_customer_ids_page, _push_vip_to_klaviyo
from .verify import verify_flow_signature
from pathlib import Path
import os, sys, subprocess, threading
from datetime import datetime, timezone, date
TIER_RANK = {"VIP0":0, "VIP1":1, "VIP2":2, "VIP3":3}
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
    _days_to_date, _gap_to_next_tier_cents, _gap_to_requalify_cents, normalize_tier,
    get_customer_lifetime_spend, _push_vip_transition
)

@bp.post("/seed_vip2_lock_2025")
def seed_vip2_lock_2025():
    """
    Fast trigger for Shopify Flow: start a background job that:
      - walks all customers
      - if lifetime_spend >= $500 and NOT already VIP2/VIP3
        → force VIP2
        → lock from today through 2025-12-31
        → sync to Klaviyo

    Returns immediately so Flow doesn’t time out.
    """
    import threading, time, os
    from datetime import datetime, timezone

    from .service import (
        fetch_customer_ids_page,
        get_customer_state,
        normalize_tier,
        write_state,
        get_customer_lifetime_spend,
    )

    LOCK = "/tmp/vip_seed_vip2_lock_2025.lock"
    logger = current_app.logger

    def try_lock():
        try:
            # stale lock cleanup (2h)
            if os.path.exists(LOCK) and (time.time() - os.path.getmtime(LOCK)) > 2 * 60 * 60:
                os.remove(LOCK)
            fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            return False

    def release_lock():
        try:
            os.remove(LOCK)
        except FileNotFoundError:
            pass

    def worker():
        from datetime import date as _date

        total = 0
        promoted = 0
        skipped_high = 0
        skipped_low = 0
        today = _date.today()
        lock_end = "2025-12-31"

        try:
            cursor = None
            while True:
                ids, next_cursor = fetch_customer_ids_page(first=250, after=cursor)
                if not ids:
                    break

                for gid in ids:
                    total += 1
                    try:
                        state = get_customer_state(gid)
                        tier = normalize_tier(state.get("tier") or "VIP0")

                        if tier in ("VIP2", "VIP3"):
                            skipped_high += 1
                            continue

                        lifetime = get_customer_lifetime_spend(gid)
                        if lifetime < 500.0:
                            skipped_low += 1
                            continue

                        lock = {
                            "start": today.isoformat(),
                            "end": lock_end,
                            "tier": "VIP2",
                        }

                        write_state(
                            gid,
                            rolling=None,
                            tier="VIP2",
                            lock=lock,
                            prov={"source": "seed_vip2_lock_2025"},
                        )

                        try:
                            _push_vip_to_klaviyo(gid)
                        except Exception as e:
                            logger.warning(  # <-- use captured logger
                                "[seed_vip2_lock_2025] Klaviyo push failed for %s: %s",
                                gid, e,
                            )

                        promoted += 1

                    except Exception as e:
                        logger.exception(  # <-- use captured logger
                            "[seed_vip2_lock_2025] error for %s: %s", gid, e
                        )

                cursor = next_cursor
                if not cursor:
                    break
                time.sleep(0.2)

            print(
                f"[VIP SEED VIP2] DONE {datetime.now(timezone.utc).isoformat()} "
                f"total={total} promoted={promoted} "
                f"skipped_high={skipped_high} skipped_low={skipped_low}",
                flush=True,
            )
        finally:
            release_lock()

    if not try_lock():
        return jsonify({"ok": True, "status": "already_running"}), 202

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"ok": True, "status": "started"}), 200


@bp.post("/order_paid")
def order_paid():
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]      # GID
    order_id    = payload["order_id"]         # GID

    order_created_at = payload.get("order_created_at")
    eval_time = None
    if order_created_at:
        eval_time = datetime.fromisoformat(
            order_created_at.replace("Z", "+00:00")
        )

    result = _on_paid(customer_id, order_id, today=eval_time)
    try:
        _push_vip_to_klaviyo(customer_id)
    except Exception as e:
        current_app.logger.warning(f"Klaviyo push failed: {e}")
    return jsonify({"ok": True, **result})

@bp.post("/price_update")
def price_update():
    try:
        ROOT   = Path(__file__).resolve().parents[1]
        SCRIPT = ROOT / "dailyrunner.py"
        LOG    = ROOT / "run_output.log"

        def launch():
            LOG.parent.mkdir(parents=True, exist_ok=True)
            with open(LOG, "a", buffering=1, encoding="utf-8") as f:
                f.write(f"\n=== RUN {datetime.now().isoformat()} ===\n")
                # Unbuffered python + tee stdout to both Railway and the file
                p = subprocess.Popen(
                    [sys.executable, "-u", str(SCRIPT)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=str(ROOT),
                    text=True,
                    bufsize=1,
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
                # Stream to Railway AND file
                for line in p.stdout:
                    print(line, end="")       # -> Railway
                    f.write(line)             # -> run_output.log
                p.wait()
                f.write(f"=== EXIT code={p.returncode} at {datetime.now().isoformat()} ===\n")

        threading.Thread(target=launch, daemon=True).start()
        return jsonify({"ok": True, "started": True}), 200

    except Exception as e:
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
    payload = request.get_json(silent=True) or {}
    page_size = int(payload.get("page_size", 25))
    cursor = payload.get("cursor")

    from .service import sweep_vips_page

    processed, next_cursor = sweep_vips_page(
        page_size=page_size,
        cursor=cursor,
    )

    return jsonify({
        "ok": True,
        "processed": processed,
        "next_cursor": next_cursor,
    })




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
            from .service import sweep_vips_page
            cursor = None
            total = 0

            while True:
                processed, cursor = sweep_vips_page(
                    page_size=25,
                    cursor=cursor,
                )
                total += processed

                time.sleep(0.2)  # keep your throttle

                if not cursor:
                    break
            print(f"[VIP SWEEP] DONE {datetime.now().isoformat()} total={total}")
        finally:
            release_lock()

    if not try_lock():
        return jsonify({"ok": True, "status": "already_running"}), 202

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"ok": True, "status": "started"}), 200
