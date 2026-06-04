"""
product_rules — Shopify operations.

- Tag parsing (limit-N[-per-{day,week,month}|-all-time], preorder-YYYY-MM-DD)
- Product metafield sync (custom.qty_limit_rule, custom.preorder_rule)
- Customer purchase log metafield (custom.purchase_log)
- HMAC verification for Shopify product/order webhooks
"""

import os
import re
import json
import hmac
import base64
import hashlib
import logging
from datetime import date, datetime, timedelta

from shopify_graphql import shopify_gql, gid_numeric

logger = logging.getLogger(__name__)

SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")

NAMESPACE = "custom"
PREORDER_KEY = "preorder_rule"
QTY_LIMIT_KEY = "qty_limit_rule"
PURCHASE_LOG_KEY = "purchase_log"

# How long we keep per-customer purchase dates. Anything past this can't
# possibly affect any qty_limit window (longest = month) but we keep a year
# so future "per quarter" / "per year" windows just work without a backfill.
PURCHASE_LOG_RETAIN_DAYS = 365

LIMIT_TAG_RE = re.compile(
    r"^limit-(\d+)(?:-(per-day|per-week|per-month|all-time))?$",
    re.IGNORECASE,
)
PREORDER_TAG_RE = re.compile(
    r"^preorder-(\d{4})-(\d{2})-(\d{2})$",
    re.IGNORECASE,
)

WINDOW_BY_SUFFIX = {
    None:         ("order",    1),
    "per-day":    ("day",      1),
    "per-week":   ("week",     1),
    "per-month":  ("month",    1),
    "all-time":   ("all_time", 1),
}


# ═══════════════════════════════════════════════════════════════════════════════
# Tag parsing
# ═══════════════════════════════════════════════════════════════════════════════

def parse_rule_tags(tags):
    """
    Pull rule-bearing tags out of a product's full tag list.

    Returns dict:
      {
        "qty_limit":  {"tag", "max_qty", "window_unit", "window_count", "scope"} | None,
        "preorder":   {"tag", "street_date"} | None,
        "rule_tags":  list of all matched tags (for product_rule_state storage),
      }

    First-match-wins for each rule type. A product with both limit-2 and
    limit-3-per-week is malformed but we don't reject — we pick the first
    and log a warning so the operator notices when they look.
    """
    qty_limit = None
    preorder = None
    matched = []

    for raw in tags or []:
        tag = raw.strip()
        if not tag:
            continue
        m = LIMIT_TAG_RE.match(tag)
        if m:
            n = int(m.group(1))
            unit, count = WINDOW_BY_SUFFIX[(m.group(2) or "").lower() or None]
            matched.append(tag)
            if qty_limit is None:
                qty_limit = {
                    "tag": tag,
                    "max_qty": n,
                    "window_unit": unit,
                    "window_count": count,
                    "scope": "customer",
                }
            else:
                logger.warning(
                    "Multiple limit-* tags on product (kept %s, ignored %s)",
                    qty_limit["tag"], tag,
                )
            continue
        m = PREORDER_TAG_RE.match(tag)
        if m:
            matched.append(tag)
            if preorder is None:
                preorder = {
                    "tag": tag,
                    "street_date": f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
                }
            else:
                logger.warning(
                    "Multiple preorder-* tags on product (kept %s, ignored %s)",
                    preorder["tag"], tag,
                )

    return {"qty_limit": qty_limit, "preorder": preorder, "rule_tags": matched}


def _pretty_date(iso_yyyy_mm_dd):
    try:
        d = datetime.strptime(iso_yyyy_mm_dd, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return iso_yyyy_mm_dd
    # %-d is POSIX; Windows uses %#d. Production runs on Linux but local
    # tests don't, so try both rather than failing.
    try:
        return d.strftime("%B %-d, %Y")
    except ValueError:
        return d.strftime("%B %#d, %Y")


def build_preorder_config(street_date, override_row=None):
    """
    Compose the JSON written to custom.preorder_rule.
    `override_row` is the preorder_overrides row for this tag (dict or None).
    """
    pretty = _pretty_date(street_date)
    o = override_row or {}
    return {
        "street_date":  street_date,
        "display_name": o.get("display_name") or "",
        "button_text":  o.get("button_text") or "Pre-Order Now",
        "pdp_message":  o.get("pdp_message") or f"Releases {pretty}",
        "cart_message": o.get("cart_message") or f"Pre-order — releases {pretty}",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Shopify product metafield ops
# ═══════════════════════════════════════════════════════════════════════════════

PRODUCT_BY_ID_Q = """
query ProductById($id: ID!) {
  product(id: $id) {
    id
    title
    tags
  }
}
"""

METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""

METAFIELDS_DELETE = """
mutation MetafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields { ownerId namespace key }
    userErrors { field message }
  }
}
"""

TAGS_ADD = """
mutation TagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    userErrors { field message }
  }
}
"""

CUSTOMER_PURCHASE_LOG_Q = """
query CustomerPurchaseLog($id: ID!) {
  customer(id: $id) {
    id
    metafield(namespace: "custom", key: "purchase_log") {
      value
    }
  }
}
"""


def get_product(product_gid):
    data = shopify_gql(PRODUCT_BY_ID_Q, {"id": product_gid})
    return (data.get("data") or {}).get("product")


def _set_metafield(owner_gid, key, value_json):
    data = shopify_gql(METAFIELDS_SET, {"metafields": [{
        "ownerId": owner_gid,
        "namespace": NAMESPACE,
        "key": key,
        "type": "json",
        "value": value_json,
    }]})
    errs = (data.get("data") or {}).get("metafieldsSet", {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metafieldsSet {key} failed: {errs}")


def _delete_metafield(owner_gid, key):
    data = shopify_gql(METAFIELDS_DELETE, {"metafields": [{
        "ownerId": owner_gid,
        "namespace": NAMESPACE,
        "key": key,
    }]})
    errs = (data.get("data") or {}).get("metafieldsDelete", {}).get("userErrors") or []
    # Deleting a metafield that doesn't exist is fine — Shopify returns no error,
    # but if it ever does we just log it; the desired state (absent) is the same.
    if errs:
        logger.info("metafieldsDelete %s warnings: %s", key, errs)


def _ensure_tag(product_gid, tag, existing_tags):
    if any((t or "").strip().lower() == tag.lower() for t in (existing_tags or [])):
        return
    shopify_gql(TAGS_ADD, {"id": product_gid, "tags": [tag]})


def sync_product_metafields(product_gid, tags, db_module):
    """
    Resolve rules from tags, write/clear product metafields, and update
    product_rule_state so the dashboard can count usage.
    """
    rules = parse_rule_tags(tags)

    # qty_limit
    if rules["qty_limit"]:
        cfg = {k: v for k, v in rules["qty_limit"].items() if k != "tag"}
        _set_metafield(product_gid, QTY_LIMIT_KEY, json.dumps(cfg))
    else:
        _delete_metafield(product_gid, QTY_LIMIT_KEY)

    # preorder
    if rules["preorder"]:
        tag = rules["preorder"]["tag"]
        override = db_module.query_one(
            "SELECT display_name, button_text, pdp_message, cart_message "
            "FROM preorder_overrides WHERE tag = %s",
            (tag,),
        )
        cfg = build_preorder_config(rules["preorder"]["street_date"], override)
        _set_metafield(product_gid, PREORDER_KEY, json.dumps(cfg))
        # /screening/ already looks for the bare 'pre-order' tag to skip
        # combine+signature checks. Mirror it so adding preorder-YYYY-MM-DD
        # is the only thing an employee has to do. Idempotent.
        _ensure_tag(product_gid, "pre-order", tags)
    else:
        _delete_metafield(product_gid, PREORDER_KEY)

    # product_rule_state — drives dashboard counts and lets us find products
    # to clear at release time without re-listing the whole catalog.
    pid = gid_numeric(product_gid)
    if rules["rule_tags"]:
        db_module.execute("""
            INSERT INTO product_rule_state (shopify_product_id, rule_tags, last_synced_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (shopify_product_id) DO UPDATE
            SET rule_tags = EXCLUDED.rule_tags, last_synced_at = NOW()
        """, (pid, rules["rule_tags"]))
    else:
        db_module.execute(
            "DELETE FROM product_rule_state WHERE shopify_product_id = %s",
            (pid,),
        )

    return rules


# ═══════════════════════════════════════════════════════════════════════════════
# Customer purchase log
# ═══════════════════════════════════════════════════════════════════════════════

def _read_purchase_log(customer_gid):
    data = shopify_gql(CUSTOMER_PURCHASE_LOG_Q, {"id": customer_gid})
    mf = ((data.get("data") or {}).get("customer") or {}).get("metafield")
    if not mf or not mf.get("value"):
        return {}
    try:
        v = json.loads(mf["value"])
        return v if isinstance(v, dict) else {}
    except (ValueError, TypeError):
        return {}


def append_customer_purchases(customer_gid, line_items):
    """
    line_items: [{ "product_id": "<numeric str>", "qty": int }, ...]
    Reads the customer's purchase_log metafield, appends today's date once per
    unit purchased per product, trims entries older than the retention window,
    and writes back.
    """
    if not customer_gid or not line_items:
        return
    today = date.today().isoformat()
    cutoff = (date.today() - timedelta(days=PURCHASE_LOG_RETAIN_DAYS)).isoformat()

    log = _read_purchase_log(customer_gid)
    for item in line_items:
        pid = str(item.get("product_id") or "").strip()
        qty = int(item.get("qty") or 0)
        if not pid or qty <= 0:
            continue
        log.setdefault(pid, []).extend([today] * qty)

    trimmed = {}
    for pid, dates in log.items():
        kept = [d for d in dates if d >= cutoff]
        if kept:
            trimmed[pid] = kept

    _set_metafield(customer_gid, PURCHASE_LOG_KEY, json.dumps(trimmed))


# ═══════════════════════════════════════════════════════════════════════════════
# Webhook HMAC verification
# ═══════════════════════════════════════════════════════════════════════════════

def verify_shopify_hmac(raw_body, header_signature):
    """Constant-time compare of the X-Shopify-Hmac-Sha256 header."""
    if not SHOPIFY_WEBHOOK_SECRET or not header_signature:
        return False
    computed = base64.b64encode(
        hmac.new(
            SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    return hmac.compare_digest(computed, header_signature)


# ═══════════════════════════════════════════════════════════════════════════════
# Release (Phase 4 helper, exposed here so dashboard "Release Now" can call it)
# ═══════════════════════════════════════════════════════════════════════════════

TAGS_REMOVE = """
mutation TagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    userErrors { field message }
  }
}
"""


def release_preorder_tag(tag, db_module):
    """
    For every product currently carrying `tag`: strip both the dated tag and
    the bare 'pre-order' tag, clear custom.preorder_rule, and resync state.
    Returns list of product IDs touched.
    """
    rows = db_module.query(
        "SELECT shopify_product_id, rule_tags FROM product_rule_state WHERE %s = ANY(rule_tags)",
        (tag,),
    )
    touched = []
    for row in rows:
        pid = row["shopify_product_id"]
        product_gid = f"gid://shopify/Product/{pid}"
        try:
            shopify_gql(TAGS_REMOVE, {
                "id": product_gid,
                "tags": [tag, "pre-order"],
            })
            # Re-fetch tags + resync (handles the case where other rule tags
            # remain on the product after we stripped preorder-*).
            product = get_product(product_gid) or {}
            sync_product_metafields(product_gid, product.get("tags") or [], db_module)
            touched.append(pid)
        except Exception as e:
            logger.error("release_preorder_tag failed for %s: %s", pid, e)
    return touched
