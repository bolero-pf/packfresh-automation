# vip/routes.py
from flask import Blueprint, jsonify
from flask import request
from .service import fetch_customer_ids_page
from datetime import date
from .verify import verify_flow_signature
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
from .service import on_order_paid as _on_paid, on_refund_created as _on_refund, on_quarter_roll as _on_qroll

@bp.post("/order_paid")
def order_paid():
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]      # GID
    order_id    = payload["order_id"]         # GID
    result = _on_paid(customer_id, order_id)
    return jsonify({"ok": True, **result})

@bp.post("/refund_created")
def refund_created():
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]      # GID
    order_id    = payload["order_id"]         # GID (the refunded order)
    result = _on_refund(customer_id, order_id)
    return jsonify({"ok": True, **result})

@bp.post("/quarter_roll")
def quarter_roll():
    payload = request.get_json(silent=True) or {}
    limit = payload.get("limit")
    result = _on_qroll(limit=limit)
    return jsonify({"ok": True, **result})

@bp.post("/recalc")
def recalc_one():
    # POST { "customer_id": "gid://shopify/Customer/123" }
    payload = request.get_json(force=True)
    customer_id = payload["customer_id"]
    from .service import compute_rolling_90d_spend, tier_from_spend, get_customer_state
    from .service import current_quarter_lock_for, write_state
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc)
    state = get_customer_state(customer_id)
    spend = compute_rolling_90d_spend(customer_id, today=today)
    new_tier = tier_from_spend(spend)
    # if already locked and active, keep lock; otherwise seed current-quarter if VIP1+
    lock = state["lock"]
    from .service import inside_lock
    if not inside_lock(lock, today.date()):
        lock = current_quarter_lock_for(new_tier, today.date()) if new_tier in ("VIP1","VIP2","VIP3") else {}

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
