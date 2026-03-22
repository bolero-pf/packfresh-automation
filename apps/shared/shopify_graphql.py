"""
Shared Shopify Admin GraphQL client.

Used by vip/ and screening/ services for all Shopify mutations and queries.
"""

import os
import time
import requests

_SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
_SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
_GRAPHQL_ENDPOINT = f"https://{_SHOPIFY_STORE}/admin/api/2025-10/graphql.json" if _SHOPIFY_STORE else ""
_PER_CALL_TIMEOUT = int(os.environ.get("SHOPIFY_HTTP_TIMEOUT", "60"))


def shopify_gql(query: str, variables=None):
    """Execute a Shopify Admin GraphQL query with retry + throttle handling."""
    if not _SHOPIFY_TOKEN or not _SHOPIFY_STORE:
        raise RuntimeError("Missing SHOPIFY_TOKEN or SHOPIFY_STORE in environment.")
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": _SHOPIFY_TOKEN,
    }
    payload = {"query": query, "variables": variables or {}}

    for attempt in range(6):
        try:
            resp = requests.post(
                _GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=_PER_CALL_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errors"):
                messages = " ".join(e.get("message", "") for e in data["errors"])
                if "Throttled" in messages or "throttle" in messages.lower():
                    raise requests.HTTPError("Throttled")
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            if attempt >= 5:
                raise
            sleep_s = min(10.0, 1.0 * (1.8 ** attempt))
            time.sleep(sleep_s)


def shopify_metafields_set(inputs):
    """Convenience wrapper around metafieldsSet mutation."""
    mutation = """
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key }
        userErrors { field message }
      }
    }
    """
    resp = shopify_gql(mutation, {"metafields": list(inputs)})
    errs = resp.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")


def gid_numeric(gid: str) -> str:
    """Extract numeric ID from Shopify GID. 'gid://shopify/Customer/123' -> '123'"""
    return gid.rsplit("/", 1)[-1]
