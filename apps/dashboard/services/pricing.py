from adapters.shopify_client import ShopifyClient

def normalize_variant_gid(variant_id: str) -> str:
    # Accepts either numeric "123456789" or full GID "gid://shopify/ProductVariant/123456789"
    if isinstance(variant_id, str) and variant_id.startswith("gid://shopify/ProductVariant/"):
        return variant_id
    return f"gid://shopify/ProductVariant/{variant_id}"

def update_variant_price(variant_id, new_price):
    mutation = """
    mutation variantUpdate($input: ProductVariantInput!) {
      productVariantUpdate(input: $input) {
        productVariant { id price }
        userErrors { field message }
      }
    }
    """
    variables = {"input": {"id": normalize_variant_gid(variant_id), "price": str(new_price)}}
    data = ShopifyClient().graphql(mutation, variables)
    return data["productVariantUpdate"]