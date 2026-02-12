# screening/routes.py
"""
Flask routes for order screening.

Endpoints:
  POST /screening/order_created   — first-time order checks (FIRSTTIME5 + high-value)
  POST /screening/fraud_risk      — Shopify fraud risk (medium → verify, high → cancel)
  POST /screening/order_cancelled — FIRSTTIME5 abuse → Klaviyo notification
  POST /screening/order_fulfilled — cleanup tags & holds
"""
from flask import Blueprint, jsonify, request, current_app
from .service import (
    screen_order,
    check_fraud_risk,
    on_order_cancelled,
    on_order_fulfilled,
)

import sys, os
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from vip.verify import verify_flow_signature

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
    Runs FIRSTTIME5 abuse check + high-value first order check.
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")

    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    customer_id = payload.get("customer_id", "unknown")
    current_app.logger.info(f"[screening] order_created order={order_id} customer={customer_id}")

    try:
        result = screen_order(order_id)
        if result.get("any_flagged"):
            current_app.logger.warning(
                f"[screening] FLAGGED order={order_id} "
                f"firsttime5={result['firsttime5'].get('flagged')} "
                f"high_value={result['high_value'].get('flagged')}"
            )
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Error processing {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/fraud_risk")
def fraud_risk():
    """
    Flow trigger: Order risk analyzed → condition: risk is MEDIUM or HIGH
    Medium: hold + Klaviyo verification email.
    High: auto-cancel + tag.
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
                f"[screening] FRAUD order={order_id} reason={result.get('reason')} "
                f"risk={result.get('risk_level')}"
            )
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Fraud risk error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/order_cancelled")
def order_cancelled():
    """
    Flow trigger: Order cancelled → condition: order has FIRSTTIME5-review tag
    Sets Klaviyo properties for the FIRSTTIME5 abuse notification email.
    """
    payload = request.get_json(force=True)
    order_id = payload.get("order_id")

    if not order_id or not order_id.startswith("gid://shopify/Order/"):
        return jsonify({"ok": False, "error": "Missing or invalid order_id"}), 400

    current_app.logger.info(f"[screening] order_cancelled order={order_id}")

    try:
        result = on_order_cancelled(order_id)
        if result.get("firsttime5_abuse"):
            current_app.logger.info(
                f"[screening] FIRSTTIME5 abuse confirmed for {order_id}, Klaviyo set={result.get('klaviyo_set')}"
            )
        return jsonify({"ok": True, **result})
    except Exception as e:
        current_app.logger.exception(f"[screening] Order cancelled error for {order_id}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.post("/order_fulfilled")
def order_fulfilled():
    """
    Flow trigger: Order fulfilled → condition: order has hold-for-review tag
    Cleans up screening tags from the order. Customer tags kept as history.
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
