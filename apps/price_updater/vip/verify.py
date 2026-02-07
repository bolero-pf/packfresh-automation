# vip/verify.py
import os
from flask import request, abort
from dotenv import load_dotenv

load_dotenv()

VIP_FLOW_SECRET = os.environ.get("VIP_FLOW_SECRET", "")

SAFE_PATHS = {"/vip/ping"}  # allow ping without secret

def verify_flow_signature():
    if request.path in SAFE_PATHS:
        return
    # Require POST JSON
    if request.method != "POST" or request.content_type != "application/json":
        abort(415)
    token = request.headers.get("X-Flow-Secret", "")
    if not VIP_FLOW_SECRET or token != VIP_FLOW_SECRET:
        abort(401)
    # Minimal payload sanity to avoid random spam
    if request.path in ("/vip/order_paid", "/vip/refund_created"):
        data = request.get_json(silent=True) or {}
        if not (isinstance(data.get("order_id"), str) and data.get("order_id","").startswith("gid://shopify/Order/")
                and isinstance(data.get("customer_id"), str) and data.get("customer_id","").startswith("gid://shopify/Customer/")):
            abort(400)