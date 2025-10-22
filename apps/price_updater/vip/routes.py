# vip/routes.py
from flask import Blueprint, jsonify
from flask import request
from .service import backfill_customer, iterate_customer_ids
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
    One-time initialization. POST JSON: {"limit": 50, "dry_run": true}
    - limit: optional int to cap how many customers to process
    - dry_run: if true, only computes (no writes) -> useful to test shopify_gql wiring
    """
    payload = request.get_json(silent=True) or {}
    limit = payload.get("limit")
    dry_run = bool(payload.get("dry_run", False))

    results = []
    for gid in iterate_customer_ids(limit=limit):
        if dry_run:
            # compute only
            from .service import compute_rolling_90d_spend, tier_from_spend, current_quarter_window
            spend = compute_rolling_90d_spend(gid)
            tier = tier_from_spend(spend)
            lock = None
            if tier in ("VIP1", "VIP2", "VIP3"):
                win = current_quarter_window(date.today())
                lock = {"start": win["start"], "end": win["end"], "tier": tier}
            results.append({"customer": gid, "spend90d": spend, "tier": tier, "lock": lock})
        else:
            results.append(backfill_customer(gid))
    return jsonify({"ok": True, "count": len(results), "items": results[:10]})  # show first 10 for sanity

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
