"""
Shared webhook signature verification middleware.

Validates X-Flow-Secret header on incoming Shopify Flow webhooks.
Used by both screening/ and vip/ services.
"""

import os
from flask import request, abort
from dotenv import load_dotenv

load_dotenv()

FLOW_SECRET = os.environ.get("VIP_FLOW_SECRET", "")

# Paths that don't require auth (health checks)
SAFE_PATHS = set()


def verify_flow_signature(safe_paths=None):
    """
    Flask before_request handler to verify webhook signatures.
    Call with safe_paths to extend the default set.
    """
    all_safe = SAFE_PATHS | (safe_paths or set())
    if request.path in all_safe:
        return
    if request.method != "POST" or request.content_type != "application/json":
        abort(415)
    token = request.headers.get("X-Flow-Secret", "")
    if not FLOW_SECRET or token != FLOW_SECRET:
        abort(401)
    # Minimal payload sanity — require order_id GID on all POST endpoints
    data = request.get_json(silent=True) or {}
    if not (isinstance(data.get("order_id"), str) and data["order_id"].startswith("gid://shopify/Order/")):
        abort(400)
