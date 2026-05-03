"""Shared helpers for intake blueprints."""
import logging
from decimal import Decimal
from flask import g

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
# Server is the authority on percentage caps. The frontend renders
# associate inputs as read-only and offers a "Manager Override" button,
# but a malicious associate could still hit the API directly. These
# helpers enforce role policy on every endpoint that writes a
# percentage.
OVERRIDE_ACTION = "offer_percentage"
ASSOCIATE_DEFAULT_CASH = Decimal("65")
ASSOCIATE_DEFAULT_CREDIT = Decimal("75")
MANAGER_CAP = Decimal("80")


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

    if eff_role == "owner":
        return (Decimal("0"), Decimal("100"), Decimal("0"), Decimal("100"))
    if eff_role == "manager":
        return (Decimal("0"), MANAGER_CAP, Decimal("0"), MANAGER_CAP)
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
