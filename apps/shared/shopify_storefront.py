"""
Shared Shopify Storefront API client.

Used by kiosk/ for creating carts + checkout URLs via the headless "Kiosk" channel.
"""

import os
import time
import requests

_STOREFRONT_TOKEN = os.environ.get("SHOPIFY_STOREFRONT_TOKEN")
_SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
_STOREFRONT_ENDPOINT = f"https://{_SHOPIFY_STORE}/api/2025-01/graphql.json" if _SHOPIFY_STORE else ""
_PER_CALL_TIMEOUT = int(os.environ.get("SHOPIFY_HTTP_TIMEOUT", "60"))


def storefront_gql(query: str, variables=None):
    """Execute a Shopify Storefront API GraphQL query with retry."""
    if not _STOREFRONT_TOKEN or not _SHOPIFY_STORE:
        raise RuntimeError("Missing SHOPIFY_STOREFRONT_TOKEN or SHOPIFY_STORE in environment.")
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": _STOREFRONT_TOKEN,
    }
    payload = {"query": query, "variables": variables or {}}

    for attempt in range(4):
        try:
            resp = requests.post(
                _STOREFRONT_ENDPOINT, headers=headers, json=payload, timeout=_PER_CALL_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errors"):
                messages = " ".join(e.get("message", "") for e in data["errors"])
                if "Throttled" in messages or "throttle" in messages.lower():
                    raise requests.HTTPError("Throttled")
                raise RuntimeError(f"Storefront GraphQL errors: {data['errors']}")
            return data
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            if attempt >= 3:
                raise
            time.sleep(1.0 * (1.5 ** attempt))


def create_cart(variant_gids: list, buyer_email: str, discount_codes: list = None) -> str:
    """
    Create a Storefront API cart and return the checkout URL.

    Args:
        variant_gids: list of Shopify ProductVariant GIDs
        buyer_email: customer email for checkout pre-fill
        discount_codes: optional list of discount code strings

    Returns:
        checkout URL string
    """
    mutation = """
    mutation cartCreate($input: CartInput!) {
      cartCreate(input: $input) {
        cart {
          id
          checkoutUrl
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    lines = [{"merchandiseId": gid, "quantity": 1} for gid in variant_gids]
    cart_input = {
        "lines": lines,
        "buyerIdentity": {"email": buyer_email},
    }
    if discount_codes:
        cart_input["discountCodes"] = discount_codes

    resp = storefront_gql(mutation, {"input": cart_input})
    cart_data = resp.get("data", {}).get("cartCreate", {})
    errors = cart_data.get("userErrors", [])
    if errors:
        raise RuntimeError(f"cartCreate errors: {errors}")

    cart = cart_data.get("cart")
    if not cart or not cart.get("checkoutUrl"):
        raise RuntimeError("cartCreate returned no checkout URL")

    return cart["checkoutUrl"]
