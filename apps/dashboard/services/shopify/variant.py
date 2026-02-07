VARIANT_UPDATE_MUT = """
mutation variantUpdate($input: ProductVariantInput!) {
  productVariantUpdate(input: $input) {
    productVariant { id price compareAtPrice }
    userErrors { field message }
  }
}
"""


def update_variant_price(shopify_client, variant_id: str, new_price: float, compare_at: float | None = None):
    variables = {"input": {"id": variant_id, "price": f"{new_price:.2f}"}}
    if compare_at is not None:
        variables["input"]["compareAtPrice"] = f"{compare_at:.2f}"
    data = shopify_client.graphql(VARIANT_UPDATE_MUT, variables)
    errs = data["productVariantUpdate"]["userErrors"]
    if errs:
        raise RuntimeError(f"Shopify errors: {errs}")
    return data["productVariantUpdate"]["productVariant"]