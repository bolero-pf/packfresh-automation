
SLABS_BY_TAG_QUERY = """
query SlabsByTag($cursor: String, $query: String!) {
  products(first: 100, after: $cursor, query: $query) {
    edges {
      cursor
      node {
        id
        title
        tags
        productType
        bodyHtml
        tcg: metafield(namespace: "tcg", key: "tcgplayer_id") { value }
        variants(first: 100) {
          edges {
            node {
              id
              sku
              barcode
              price
              tcg: metafield(namespace: "tcg", key: "tcgplayer_id") { value }
              compareAtPrice
            }
          }
        }
      }
    }
    pageInfo { hasNextPage }
  }
}
"""


def iter_products_by_tag(shopify_client, tag="slab"):
    """
    Generator over products that include the given tag (default 'slab').
    Uses your ShopifyClient.graphql() method.
    """
    cursor = None
    # Shopify product query syntax: tag:slab   (no quotes needed unless spaces)
    query_str = f"tag:{tag}"
    while True:
        data = shopify_client.graphql(
            SLABS_BY_TAG_QUERY,
            {"cursor": cursor, "query": query_str}
        )
        products = data["products"]
        edges = products["edges"]
        if not edges:
            break

        for edge in edges:
            yield edge["node"]

        if not products["pageInfo"]["hasNextPage"]:
            break
        cursor = edges[-1]["cursor"]

