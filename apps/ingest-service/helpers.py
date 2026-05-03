"""Shared helpers for intake blueprints."""
import logging
from decimal import Decimal, InvalidOperation
from functools import wraps
from flask import g, request, jsonify

import db

logger = logging.getLogger("intake.helpers")


def _serialize(obj):
    """Convert a dict with Decimal/datetime values to JSON-safe types."""
    if obj is None:
        return None
    BOOL_FIELDS = {"is_graded", "is_mapped", "is_distribution", "is_walk_in", "needsDetailedScrape"}
    out = {}
    for k, v in obj.items():
        if k in BOOL_FIELDS:
            out[k] = bool(v)
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ── Offer-percentage role caps + override token validation ─────────────
#
# Server is the authority on percentage caps. Associates are pinned to the
# canonical defaults (65/75) unless a manager-or-better override token is
# attached. Managers and owners are unrestricted — the policy is "ADMIN
# users have zero friction; only associates need a PIN."
OVERRIDE_ACTION = "offer_percentage"
ASSOCIATE_DEFAULT_CASH = Decimal("65")
ASSOCIATE_DEFAULT_CREDIT = Decimal("75")
# Kept for backward import compatibility — managers are no longer capped at 80%.
MANAGER_CAP = Decimal("100")


def _decode_override(data: dict):
    """Decode a manager-override token from the request body, scoped to
    OVERRIDE_ACTION. Returns the payload (manager info) or None.
    """
    token = (data or {}).get("override_token")
    if not token:
        return None
    try:
        from auth import decode_override_token
    except Exception:
        return None
    return decode_override_token(token, action=OVERRIDE_ACTION)


def _effective_caps_from_role(user_role: str, override_payload):
    """Return (cash_min, cash_max, credit_min, credit_max) the requestor
    is allowed to submit, given their JWT role plus any attached override.
    """
    role = (user_role or "associate").lower()
    eff_role = role
    if override_payload:
        rank = {"associate": 0, "manager": 1, "owner": 2}
        if rank.get(override_payload.get("role", ""), 0) > rank.get(role, 0):
            eff_role = override_payload.get("role")

    if eff_role in ("owner", "manager"):
        return (Decimal("0"), Decimal("100"), Decimal("0"), Decimal("100"))
    return (ASSOCIATE_DEFAULT_CASH, ASSOCIATE_DEFAULT_CASH,
            ASSOCIATE_DEFAULT_CREDIT, ASSOCIATE_DEFAULT_CREDIT)


def _validate_offer_caps(data: dict, cash_pct, credit_pct, session_id=None):
    """Return None if percentages are acceptable for caller, else an error
    dict suitable for a 403 JSON body. Either pct may be None (unchanged).
    """
    user = getattr(g, "user", None) or {}
    role = (user.get("role") or "associate").lower()
    override = _decode_override(data)

    cash_min, cash_max, credit_min, credit_max = _effective_caps_from_role(role, override)

    def _outside(val, lo, hi):
        return val is not None and (val < lo or val > hi)

    if _outside(cash_pct, cash_min, cash_max) or _outside(credit_pct, credit_min, credit_max):
        return {
            "error": "Override required",
            "code": "override_required",
            "role": role,
            "cash_max": float(cash_max),
            "credit_max": float(credit_max),
        }
    return None


def _log_override_if_present(data: dict, session_id: str,
                              cash_pct=None, credit_pct=None):
    """Audit-log a successful override into session_overrides. Best-effort."""
    override = _decode_override(data)
    if not override:
        return
    user = getattr(g, "user", None) or {}
    try:
        db.execute("""
            INSERT INTO session_overrides
                (session_id, approved_by_user_id, approver_role,
                 approved_for_user_id, action,
                 approved_cash_pct, approved_credit_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            session_id,
            override.get("sub"),
            override.get("role"),
            user.get("id"),
            override.get("action") or OVERRIDE_ACTION,
            cash_pct,
            credit_pct,
        ))
    except Exception as e:
        logger.warning(f"session_overrides insert failed: {e}")


# ── Cap-enforcement decorator ──────────────────────────────────────────
#
# Wraps any route that writes session percentages so the cap policy is
# applied *before* the route runs. JSON bodies and multipart-form bodies
# (file uploads) are both supported — file uploads is critical because
# the three CSV/HTML upload endpoints set initial cash/credit percentages
# from form fields and previously bypassed validation entirely.
#
# The decorator stashes the merged request data on g.percentage_data so
# routes can pass it to _log_override_if_present after they have a
# session_id without re-parsing.

def _coerce_decimal(v):
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _gather_request_data() -> dict:
    """Merge JSON + form into a single dict. JSON wins on key conflicts."""
    data = {}
    if request.form:
        data.update(request.form.to_dict())
    j = request.get_json(silent=True)
    if isinstance(j, dict):
        data.update(j)
    return data


def enforce_offer_caps(fn):
    """Reject the request if the caller's role can't submit the percentages
    in the request body. Owners and managers pass through with no cap;
    associates are pinned to ASSOCIATE_DEFAULT_* unless a valid manager
    override token is attached. The legacy `offer_percentage` parameter is
    treated as a synonym for `cash_percentage` when no explicit cash value
    is supplied.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        data = _gather_request_data()
        cash_pct = _coerce_decimal(data.get("cash_percentage"))
        credit_pct = _coerce_decimal(data.get("credit_percentage"))
        if cash_pct is None:
            cash_pct = _coerce_decimal(data.get("offer_percentage"))
        if cash_pct is not None or credit_pct is not None:
            err = _validate_offer_caps(data, cash_pct, credit_pct)
            if err:
                return jsonify(err), 403
        g.percentage_data = data  # for downstream _log_override_if_present
        return fn(*args, **kwargs)
    return wrapper
