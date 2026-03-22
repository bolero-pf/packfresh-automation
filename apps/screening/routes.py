# screening/routes.py
"""
Flask routes for order screening.

Endpoints:
  POST /screening/order_created   — first-time: FIRSTTIME5 abuse only
  POST /screening/order_combine   — every order: verification, spike, combine, signature
  POST /screening/fraud_risk      — Shopify fraud risk (medium → verify, high → cancel)
  POST /screening/order_cancelled — FIRSTTIME5 abuse → Klaviyo notification
  POST /screening/order_fulfilled — cleanup tags & holds
"""
from flask import Blueprint, jsonify, request, current_app
from service import (
    screen_order,
    screen_every_order,
    check_fraud_risk,
    on_order_cancelled,
    on_order_fulfilled,
)
from webhook_verify import verify_flow_signature

bp = Blueprint("screening", __name__, url_prefix="/screening")


@bp.before_request
def _verify():
    verify_flow_signature()


@bp.get("/ping")
def ping():
    return jsonify({"ok": True, "service": "screening", "msg": "pong"}), 200


@bp.post("/order_created")
def order_created():
    """
    Flow trigger: Order created → condition: ordersCount == 1
    Runs FIRSTTIME5 abuse check only.
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")
    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] order_created order={order_id}")
    try:
        result = screen_order(order_id)
        if result.get("any_flagged"):
            current_app.logger.warning(f"[screening] FIRSTTIME5 FLAGGED order={order_id}")
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Error processing {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/order_combine")
def order_combine():
    """
    Flow trigger: Order created → no conditions (every order)
    Runs: cumulative verification, spend spike, combine shipping, signature.
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")
    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] order_combine (every-order) order={order_id}")
    try:
        result = screen_every_order(order_id)
        if result.get("any_flagged"):
            flags = []
            if (result.get("verification") or {}).get("flagged"): flags.append("verification")
            if (result.get("spend_spike") or {}).get("flagged"): flags.append("spike")
            if (result.get("combine") or {}).get("flagged"): flags.append("combine")
            if (result.get("signature") or {}).get("flagged"): flags.append("signature")
            current_app.logger.warning(f"[screening] FLAGGED order={order_id} checks={','.join(flags)}")
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Every-order error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/fraud_risk")
def fraud_risk():
    """
    Flow trigger: Order risk analyzed → condition: risk is MEDIUM or HIGH
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")
    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] fraud_risk order={order_id}")
    try:
        result = check_fraud_risk(order_id)
        if result.get("flagged"):
            current_app.logger.warning(
                f"[screening] FRAUD order={order_id} reason={result.get('reason')} risk={result.get('risk_level')}")
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Fraud risk error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/order_cancelled")
def order_cancelled():
    """
    Flow trigger: Order cancelled → condition: order has FIRSTTIME5-review tag
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")
    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] order_cancelled order={order_id}")
    try:
        result = on_order_cancelled(order_id)
        if result.get("firsttime5_abuse"):
            current_app.logger.info(f"[screening] FIRSTTIME5 abuse confirmed for {order_id}")
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Order cancelled error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/order_fulfilled")
def order_fulfilled():
    """
    Flow trigger: Order fulfilled → condition: order has hold-for-review tag
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")
    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] order_fulfilled order={order_id}")
    try:
        result = on_order_fulfilled(order_id)
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Order fulfilled error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500
