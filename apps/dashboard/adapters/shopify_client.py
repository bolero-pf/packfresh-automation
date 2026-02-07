import time, requests, json
from flask import current_app

class ShopifyClient:
    def __init__(self):
        cfg = current_app.config
        self.url = f"https://{cfg['SHOPIFY_STORE']}/admin/api/{cfg['API_VERSION']}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": cfg["SHOPIFY_TOKEN"],
            "Content-Type": "application/json"
        }
        # simple token bucket
        self.cost_budget = 1000  # soft ceiling; Shopify manages real limit
        self.reset_at = time.time()

    def graphql(self, query, variables=None):
        payload = {"query": query, "variables": variables or {}}
        r = requests.post(self.url, headers=self.headers, json=payload)
        r.raise_for_status()
        j = r.json()
        if "errors" in j:
            raise RuntimeError(j["errors"])

        # read GraphQL cost and throttle if needed
        ext = j.get("extensions", {}).get("cost", {})
        requested = ext.get("requestedQueryCost", 0) or 0
        throttle = ext.get("throttleStatus", {})
        restore_rate = throttle.get("restoreRate", 0)
        currently_available = throttle.get("currentlyAvailable", 1000)

        # naive pacing: if we're close to empty, sleep briefly
        if currently_available < requested + 50:
            time.sleep(1.0)

        return j["data"]
