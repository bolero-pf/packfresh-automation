from flask import Blueprint, render_template, request
from ..adapters.shopify_client import ShopifyClient

bp = Blueprint("inventory", __name__)

QUERY = """
query ListProducts($first:Int!, $cursor:String, $q:String){
  products(first:$first, after:$cursor, query:$q) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id title status tags
        variants(first: 50) {
          edges { node { id title sku barcode price inventoryQuantity } }
        }
      }
    }
  }
}
"""

@bp.get("/")
def index():
    q   = request.args.get("q", "status:active")
    cur = request.args.get("cursor")
    first = min(int(request.args.get("first", 50)), 250)  # safety cap

    client = ShopifyClient()
    data = client.graphql(QUERY, {"first": first, "cursor": cur, "q": q})
    products = data["products"]

    rows = []
    for edge in products["edges"]:
        p = edge["node"]
        for v in p["variants"]["edges"]:
            n = v["node"]
            rows.append({
                "product": p["title"],
                "variant": n["title"],
                "id": n["id"],
                "sku": n["sku"] or "",
                "gtin": n["barcode"] or "",
                "price": n["price"],
                "qty": n["inventoryQuantity"]
            })

    return render_template(
        "inventory.html",
        title="Inventory",
        rows=rows,
        has_next=products["pageInfo"]["hasNextPage"],
        end_cursor=products["pageInfo"]["endCursor"],
        q=q, first=first
    )
