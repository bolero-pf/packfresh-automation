"""
psa_client.py — PSA API + graded slab listing creation.

Responsibilities:
  - Fetch PSA cert data and images
  - Build clean titles/descriptions using PPT card data as name source of truth
  - Find existing Shopify graded listings by TCGPlayer ID + company + grade
  - Create new graded listings or add variants to existing ones
  - Price variant at cost, inventory = 1

PSA API docs: https://api.psacard.com/publicapi/cert/
Rate limits:  429/529 = quota hit; stop and resume next day.

Graceful fallback for non-PSA companies (BGS/CGC/SGC):
  - Skip PSA API entirely
  - Use PPT card data + manual grade info
  - Create listing without cert images
"""

import io
import os
import re
import time
import logging
import unicodedata
from typing import Optional

import requests
from PIL import Image

logger = logging.getLogger(__name__)

PSA_API_BASE  = "https://api.psacard.com/publicapi/cert"
PSA_API_KEY   = os.environ.get("PSA_API_KEY", "")

# ─── Shopify config (injected at runtime from app) ───────────────────────────
SHOPIFY_VERSION = "2025-01"


class PSAQuotaHit(Exception):
    pass


class PSANotFound(Exception):
    pass


class ShopifyCreateError(Exception):
    """Shopify refused a product/variant create — carries status + parsed body."""
    def __init__(self, message, status_code=None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# ═══════════════════════════════════════════════════════════════════════════════
# PSA API
# ═══════════════════════════════════════════════════════════════════════════════

def _psa_headers() -> dict:
    return {
        "Authorization": f"bearer {PSA_API_KEY}",
        "User-Agent": "PackFreshBot/1.0 (+https://pack-fresh.com)",
    }


def _psa_get(url: str, stop_on_quota: bool = True) -> dict:
    """GET with retry on 5xx, stop on 429/529."""
    for attempt in range(3):
        resp = requests.get(url, headers=_psa_headers(), timeout=20)
        if resp.status_code in (429, 529):
            if stop_on_quota:
                raise PSAQuotaHit(f"PSA rate limited: HTTP {resp.status_code}")
            time.sleep(30)
            continue
        if 500 <= resp.status_code < 600:
            time.sleep(5 * (attempt + 1))
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


def get_psa_data(cert_number: str) -> dict:
    """
    Fetch PSA cert data. Returns the PSACert dict.
    Raises PSANotFound if cert doesn't exist, PSAQuotaHit on rate limit.
    """
    if not PSA_API_KEY:
        raise RuntimeError("PSA_API_KEY not configured")
    url = f"{PSA_API_BASE}/GetByCertNumber/{cert_number}"
    data = _psa_get(url)
    cert = data.get("PSACert")
    if not cert:
        raise PSANotFound(f"No PSACert in response for {cert_number}")
    return cert


def get_psa_images(cert_number: str) -> list[str]:
    """
    Fetch PSA cert image URLs. Front image first.
    Returns empty list on failure (non-fatal).
    """
    if not PSA_API_KEY:
        return []
    try:
        url = f"{PSA_API_BASE}/GetImagesByCertNumber/{cert_number}"
        data = _psa_get(url)
        if not isinstance(data, list):
            return []
        # Sort so IsFrontImage=True comes first
        data = sorted(data, key=lambda x: not x.get("IsFrontImage", False))
        return [entry["ImageURL"] for entry in data if entry.get("ImageURL")]
    except Exception as e:
        logger.warning(f"PSA image fetch failed for {cert_number}: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Title / Description builders
# Uses PPT card data as the source of truth for card name + set.
# PSA data contributes: year, grade, cert number.
# ═══════════════════════════════════════════════════════════════════════════════

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )


def _normalize_pokemon(s: str) -> str:
    s = _strip_accents(s or "")
    return re.sub(r"(?i)\bpokemon\b", "Pokemon", s)


def _normalize_grade(raw: str) -> str:
    """'GEM MT 10' → '10', '9.5' → '9.5'"""
    raw = (raw or "").strip()
    if raw.upper().startswith("GEM MT"):
        return "10"
    m = re.search(r"(\d+(\.\d)?)", raw)
    return m.group(1) if m else raw


def build_title(psa_cert: Optional[dict], ppt_card: Optional[dict],
                grade_company: str, grade_value: str) -> str:
    """
    Build a clean Shopify title for a graded slab.

    Priority:
      - Card name + set + card number: PPT wins (clean, TCGPlayer-normalized)
      - Year + grade: PSA cert data
      - Fallback to PSA subject if PPT unavailable

    Example: "2023 Pokemon Paldean Fates Charizard ex #6 PSA 10"
    """
    # Grade
    grade = _normalize_grade(psa_cert.get("CardGrade", "") if psa_cert else grade_value)
    company = grade_company.upper()

    # Year — PSA has this, PPT usually doesn't
    year = ""
    if psa_cert:
        year = (psa_cert.get("Year") or "").strip()

    if ppt_card:
        # PPT is source of truth for name + set
        card_name   = (ppt_card.get("name") or ppt_card.get("productName") or "").strip()
        set_name    = (ppt_card.get("setName") or ppt_card.get("set_name") or "").strip()
        card_number = (ppt_card.get("cardNumber") or ppt_card.get("number") or "").strip()

        parts = [p for p in [year, "Pokemon", set_name, card_name] if p]
        title = " ".join(parts)
        if card_number:
            title += f" #{card_number}"
        title += f" {company} {grade}"
    elif psa_cert:
        # Fallback: build from PSA data only
        subject = (psa_cert.get("Subject") or "").strip()
        # Clean common PSA label noise
        subject = re.sub(r"^(FA|AR|RR|UR|HR|CSR|CHR|SR)/", "", subject, flags=re.IGNORECASE)
        subject = re.sub(r"-HOLO\b", "", subject, flags=re.IGNORECASE)
        subject = re.sub(r"-REV\.?FOIL", " (Reverse Holo)", subject, flags=re.IGNORECASE)
        brand   = (psa_cert.get("Brand") or "").strip()
        brand   = re.sub(r"(?i)^POKEMON\s*", "", brand)
        card_num = (psa_cert.get("CardNumber") or "").strip()

        parts = [p for p in [year, "Pokemon", brand, subject] if p]
        title = " ".join(parts)
        if card_num:
            title += f" #{card_num}"
        title += f" {company} {grade}"
    else:
        title = f"Graded Card {company} {grade}"

    return _normalize_pokemon(title.strip())


def build_description(psa_cert: Optional[dict], ppt_card: Optional[dict],
                      grade_company: str, grade_value: str) -> str:
    """Build HTML description block for a graded slab listing."""
    grade   = _normalize_grade(psa_cert.get("CardGrade", "") if psa_cert else grade_value)
    company = grade_company.upper()

    cert_num   = psa_cert.get("CertNumber", "") if psa_cert else ""
    year       = (psa_cert.get("Year") or "") if psa_cert else ""
    population = (psa_cert.get("TotalPopulation") or "") if psa_cert else ""

    if ppt_card:
        card_name   = ppt_card.get("name") or ppt_card.get("productName") or ""
        set_name    = ppt_card.get("setName") or ppt_card.get("set_name") or ""
        card_number = ppt_card.get("cardNumber") or ppt_card.get("number") or ""
        rarity      = ppt_card.get("rarity") or ""
    elif psa_cert:
        card_name   = (psa_cert.get("Subject") or "").strip()
        set_name    = (psa_cert.get("Brand") or psa_cert.get("Variety") or "").strip()
        card_number = (psa_cert.get("CardNumber") or "").strip()
        rarity      = ""
    else:
        card_name = card_number = set_name = rarity = ""

    lines = ["<p><strong>Card Information</strong></p>"]
    if cert_num:  lines.append(f"<p>Cert Number: {cert_num}</p>")
    if year:      lines.append(f"<p>Year: {year}</p>")
    if set_name:  lines.append(f"<p>Set: {set_name}</p>")
    if card_name: lines.append(f"<p>Card: {card_name}</p>")
    if card_number: lines.append(f"<p>Card Number: {card_number}</p>")
    if rarity:    lines.append(f"<p>Rarity: {rarity}</p>")
    lines.append(f"<p>Grade: {company} {grade}</p>")
    if population: lines.append(f"<p>Population ({company} {grade}): {population}</p>")

    return _normalize_pokemon("\n".join(lines))


def infer_tags(title: str, grade_company: str, grade_value: str) -> list[str]:
    """Generate Shopify tags for a graded slab."""
    tags = ["slab", grade_company.upper(), f"grade-{grade_value}"]
    tl = _strip_accents(title).lower()
    if "pokemon" in tl:
        tags.append("pokemon")
    if "magic" in tl or "mtg" in tl:
        tags.append("magic")
    return list(set(tags))


# ═══════════════════════════════════════════════════════════════════════════════
# Shopify — find existing / create / add variant
# ═══════════════════════════════════════════════════════════════════════════════

def find_existing_slab(tcgplayer_id: int, grade_company: str, grade_value: str,
                       db) -> Optional[dict]:
    """
    Look up an existing graded slab listing in our inventory cache.
    Keyed by tcgplayer_id + tags containing the grade company and grade value.

    Returns dict with shopify_product_id + shopify_variant_id, or None.
    """
    if not tcgplayer_id:
        return None
    company_tag = grade_company.upper()
    grade_tag   = f"grade-{grade_value}"

    row = db.query_one("""
        SELECT shopify_product_id, shopify_variant_id, title
        FROM inventory_product_cache
        WHERE tcgplayer_id = %s
          AND tags ILIKE %s
          AND tags ILIKE %s
        LIMIT 1
    """, (tcgplayer_id, f"%{company_tag}%", f"%{grade_tag}%"))

    return row if row else None


def _shopify_headers(shopify_token: str) -> dict:
    return {
        "X-Shopify-Access-Token": shopify_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _gql(shopify_domain: str, shopify_token: str, query: str, variables: dict = None) -> dict:
    url = f"https://{shopify_domain}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    r = requests.post(url, headers=_shopify_headers(shopify_token),
                      json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def _prewire_publications(product_gid: str, shopify_domain: str, shopify_token: str):
    """Pre-wire all sales channels while product is still DRAFT."""
    q_pubs = "query { publications(first: 50) { nodes { id name } } }"
    pubs = _gql(shopify_domain, shopify_token, q_pubs)
    pub_nodes = pubs["data"]["publications"]["nodes"]

    m = """
    mutation Publish($id: ID!, $pub: ID!) {
      publishablePublish(id: $id, input: { publicationId: $pub }) {
        userErrors { field message }
      }
    }"""
    for node in pub_nodes:
        out = _gql(shopify_domain, shopify_token, m,
                   {"id": product_gid, "pub": node["id"]})
        errs = out.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
        if errs:
            msg = "; ".join(e.get("message", "") for e in errs)
            if "already published" not in msg.lower():
                logger.warning(f"prewire publication error: {msg}")


def create_graded_listing(
    title: str,
    description: str,
    tags: list[str],
    cert_number: str,
    price: float,
    tcgplayer_id: Optional[int],
    image_urls: list[str],
    shopify_domain: str,
    shopify_token: str,
) -> dict:
    """
    Create a new Shopify DRAFT product for a graded slab.
    Variant option = "Certification ID", value = cert_number.
    Inventory = 1, price = cost we paid.

    Returns the full product REST response dict.
    """
    url = f"https://{shopify_domain}/admin/api/{SHOPIFY_VERSION}/products.json"
    payload = {
        "product": {
            "title": title,
            "body_html": description,
            "tags": ", ".join(tags),
            "status": "draft",
            "product_type": "Pokemon",
            "published_scope": "web",
            "template_suffix": "cro-alt",
            "images": [{"src": img} for img in image_urls[:5]],
            "options": [{"name": "Certification ID", "values": [str(cert_number)]}],
            "variants": [{
                "option1": str(cert_number),
                "sku": str(cert_number),
                "barcode": str(cert_number),
                "price": f"{price:.2f}",
                "inventory_management": "shopify",
                "inventory_quantity": 1,
                "requires_shipping": True,
            }],
            # Store tcgplayer_id as metafield so cache can key on it later
            "metafields": [
                {"namespace": "pf", "key": "tcgplayer_id",
                 "value": str(tcgplayer_id or ""), "type": "single_line_text_field"},
                {"namespace": "pf", "key": "grade_company",
                 "value": tags[1] if len(tags) > 1 else "", "type": "single_line_text_field"},
                {"namespace": "pf", "key": "grade_value",
                 "value": cert_number, "type": "single_line_text_field"},
            ] if tcgplayer_id else [],
        }
    }

    r = requests.post(url, headers=_shopify_headers(shopify_token),
                      json=payload, timeout=30)
    if not r.ok:
        # Shopify 422 errors are meaningless without the body — surface it
        try:
            body = r.json()
        except Exception:
            body = r.text
        logger.error(f"Shopify product create failed: {r.status_code} — "
                     f"body: {body} — payload keys: {list(payload['product'].keys())}")
        raise ShopifyCreateError(
            f"Shopify rejected product create ({r.status_code}): {body}",
            status_code=r.status_code, body=body,
        )
    result = r.json()

    # Pre-wire publications so flipping to ACTIVE later is instant
    product_gid = result["product"].get("admin_graphql_api_id")
    if product_gid:
        try:
            _prewire_publications(product_gid, shopify_domain, shopify_token)
        except Exception as e:
            logger.warning(f"prewire failed (non-fatal): {e}")

    return result


def add_graded_variant(
    shopify_product_id: int,
    cert_number: str,
    price: float,
    image_urls: list[str],
    shopify_domain: str,
    shopify_token: str,
) -> dict:
    """
    Add a new variant (cert_number) to an existing graded slab product.
    Optionally uploads images to the product.

    Returns variant REST response dict.
    """
    base = f"https://{shopify_domain}/admin/api/{SHOPIFY_VERSION}"
    hdrs = _shopify_headers(shopify_token)

    # Upload images if provided
    image_ids = []
    for src in image_urls[:3]:
        try:
            r = requests.post(f"{base}/products/{shopify_product_id}/images.json",
                              headers=hdrs, json={"image": {"src": src}}, timeout=20)
            r.raise_for_status()
            img = r.json().get("image", {})
            if img.get("id"):
                image_ids.append(img["id"])
        except Exception as e:
            logger.warning(f"Image upload failed (non-fatal): {e}")

    variant_payload = {
        "variant": {
            "option1": str(cert_number),
            "sku": str(cert_number),
            "barcode": str(cert_number),
            "price": f"{price:.2f}",
            "inventory_management": "shopify",
            "inventory_quantity": 1,
            "requires_shipping": True,
        }
    }
    if image_ids:
        variant_payload["variant"]["image_id"] = image_ids[0]

    r = requests.post(f"{base}/products/{shopify_product_id}/variants.json",
                      headers=hdrs, json=variant_payload, timeout=20)
    if not r.ok:
        try:
            body = r.json()
        except Exception:
            body = r.text
        logger.error(f"Shopify add-variant failed: {r.status_code} — body: {body}")
        raise ShopifyCreateError(
            f"Shopify rejected variant add ({r.status_code}): {body}",
            status_code=r.status_code, body=body,
        )
    return r.json()


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point called from ingestion push-live
# ═══════════════════════════════════════════════════════════════════════════════

def push_graded_slab(
    *,
    tcgplayer_id: Optional[int],
    grade_company: str,
    grade_value: str,
    cert_number: str,
    price: float,
    ppt_card: Optional[dict],
    shopify_domain: str,
    shopify_token: str,
    db,
) -> dict:
    """
    Full push flow for a graded slab:
      1. Fetch PSA data + images (PSA only; fallback gracefully for others)
      2. Find existing listing in cache by tcgplayer_id + grade tags
      3. Add variant if exists, create new product if not
      4. Return result dict with action + shopify IDs

    Returns:
        {
            "action":             "added_variant" | "created_listing",
            "shopify_product_id": int,
            "shopify_variant_id": int,
            "title":              str,
            "cert_number":        str,
        }
    """
    company = grade_company.upper()

    # ── 1. Fetch PSA data (PSA only) ────────────────────────────────────────
    psa_cert    = None
    image_urls  = []

    if company == "PSA" and cert_number and PSA_API_KEY:
        try:
            psa_cert   = get_psa_data(cert_number)
            image_urls = get_psa_images(cert_number)
            logger.info(f"PSA data fetched for cert {cert_number}")
        except PSANotFound:
            logger.warning(f"PSA cert {cert_number} not found — proceeding without PSA data")
        except PSAQuotaHit:
            raise  # bubble up — caller should halt the batch
        except Exception as e:
            logger.warning(f"PSA fetch failed for {cert_number}: {e} — proceeding without")

    # ── 2. Build title + description ────────────────────────────────────────
    title       = build_title(psa_cert, ppt_card, company, grade_value)
    description = build_description(psa_cert, ppt_card, company, grade_value)
    tags        = infer_tags(title, company, grade_value)

    # ── 3. Find existing listing ─────────────────────────────────────────────
    existing = find_existing_slab(tcgplayer_id, company, grade_value, db)

    if existing:
        logger.info(f"Found existing product {existing['shopify_product_id']} for "
                    f"TCG#{tcgplayer_id} {company} {grade_value} — adding variant")
        result = add_graded_variant(
            shopify_product_id=existing["shopify_product_id"],
            cert_number=cert_number,
            price=price,
            image_urls=image_urls,
            shopify_domain=shopify_domain,
            shopify_token=shopify_token,
        )
        variant = result.get("variant", {})
        return {
            "action":             "added_variant",
            "shopify_product_id": existing["shopify_product_id"],
            "shopify_variant_id": variant.get("id"),
            "title":              title,
            "cert_number":        cert_number,
        }
    else:
        logger.info(f"No existing product for TCG#{tcgplayer_id} {company} {grade_value} "
                    f"— creating new listing")
        result = create_graded_listing(
            title=title,
            description=description,
            tags=tags,
            cert_number=cert_number,
            price=price,
            tcgplayer_id=tcgplayer_id,
            image_urls=image_urls,
            shopify_domain=shopify_domain,
            shopify_token=shopify_token,
        )
        product  = result.get("product", {})
        variants = product.get("variants", [{}])
        return {
            "action":             "created_listing",
            "shopify_product_id": product.get("id"),
            "shopify_variant_id": variants[0].get("id") if variants else None,
            "title":              title,
            "cert_number":        cert_number,
        }
