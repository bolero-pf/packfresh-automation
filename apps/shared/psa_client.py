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


# ── PSA response cache ────────────────────────────────────────────────────────
# Preview + push both call get_psa_data/get_psa_images for the same cert.
# Without caching that's 4 PSA API hits per slab (2 preview + 2 push) on a
# 50/day quota. Cache responses in memory — TTL 2 hours, plenty for a
# preview→push cycle.

_psa_cert_cache: dict[str, dict]     = {}  # cert_number -> PSACert dict
_psa_image_cache: dict[str, list]    = {}  # cert_number -> [image_urls]
_psa_cache_times: dict[str, float]   = {}  # cert_number -> timestamp
_PSA_CACHE_TTL = 7200  # 2 hours


def _psa_cache_valid(cert_number: str) -> bool:
    return (cert_number in _psa_cache_times
            and (time.time() - _psa_cache_times[cert_number]) < _PSA_CACHE_TTL)


def get_psa_data(cert_number: str) -> dict:
    """
    Fetch PSA cert data. Returns the PSACert dict.
    Cached in memory for 2 hours — preview + push share the same response.
    Raises PSANotFound if cert doesn't exist, PSAQuotaHit on rate limit.
    """
    if cert_number in _psa_cert_cache and _psa_cache_valid(cert_number):
        logger.debug(f"PSA cert cache HIT for {cert_number}")
        return _psa_cert_cache[cert_number]

    if not PSA_API_KEY:
        raise RuntimeError("PSA_API_KEY not configured")
    url = f"{PSA_API_BASE}/GetByCertNumber/{cert_number}"
    data = _psa_get(url)
    cert = data.get("PSACert")
    if not cert:
        raise PSANotFound(f"No PSACert in response for {cert_number}")

    _psa_cert_cache[cert_number] = cert
    _psa_cache_times[cert_number] = time.time()
    return cert


def get_psa_images(cert_number: str) -> list[str]:
    """
    Fetch PSA cert image URLs. Front image first.
    Cached in memory for 2 hours alongside cert data.
    Returns empty list on failure (non-fatal).
    """
    if cert_number in _psa_image_cache and _psa_cache_valid(cert_number):
        logger.debug(f"PSA image cache HIT for {cert_number}")
        return _psa_image_cache[cert_number]

    if not PSA_API_KEY:
        return []
    try:
        url = f"{PSA_API_BASE}/GetImagesByCertNumber/{cert_number}"
        data = _psa_get(url)
        if not isinstance(data, list):
            return []
        # Sort so IsFrontImage=True comes first
        data = sorted(data, key=lambda x: not x.get("IsFrontImage", False))
        urls = [entry["ImageURL"] for entry in data if entry.get("ImageURL")]
        _psa_image_cache[cert_number] = urls
        _psa_cache_times[cert_number] = time.time()
        return urls
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

    # Check if PPT data is usable (not Japanese/non-ASCII) — PSA labels are
    # always English so for JP cards we prefer PSA as the title source.
    def _is_ascii(s: str) -> bool:
        try:
            s.encode("ascii")
            return True
        except UnicodeEncodeError:
            return False

    ppt_usable = False
    if ppt_card:
        card_name   = (ppt_card.get("name") or ppt_card.get("productName") or "").strip()
        set_name    = (ppt_card.get("setName") or ppt_card.get("set_name") or "").strip()
        ppt_usable  = _is_ascii(card_name) and _is_ascii(set_name)

    if ppt_card and ppt_usable:
        # PPT is source of truth for name + set (English cards)
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

    # Same ASCII check as build_title — use PSA for JP cards
    def _is_ascii(s: str) -> bool:
        try:
            s.encode("ascii")
            return True
        except UnicodeEncodeError:
            return False

    _ppt_name = (ppt_card.get("name") or ppt_card.get("productName") or "") if ppt_card else ""
    _ppt_set  = (ppt_card.get("setName") or ppt_card.get("set_name") or "") if ppt_card else ""
    _ppt_ok   = ppt_card and _is_ascii(_ppt_name) and _is_ascii(_ppt_set)

    if _ppt_ok:
        card_name   = _ppt_name
        set_name    = _ppt_set
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


# ═══════════════════════════════════════════════════════════════════════════════
# PSA image pipeline — download → matte on 2000×2000 transparent canvas → stage
# upload to Shopify → fileCreate → wait for public URL. Keeps all graded slab
# listings visually consistent on the storefront.
#
# Content area is 950×1600 centered, giving ~26% side margins and ~10% top/bottom
# so the slab sits inside a clean border on PDP zoom.
# ═══════════════════════════════════════════════════════════════════════════════

_MATTE_SIZE   = (2000, 2000)
_CONTENT_SIZE = (950, 1600)


def _download_image(url: str) -> Image.Image:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return Image.open(io.BytesIO(r.content)).convert("RGBA")


def _matte_card_rgba(src_im: Image.Image) -> Image.Image:
    """Scale-to-fit within CONTENT_SIZE, center on transparent 2000×2000 canvas."""
    cw, ch = _CONTENT_SIZE
    mw, mh = _MATTE_SIZE
    sw, sh = src_im.size
    scale = min(cw / sw, ch / sh)
    nw, nh = max(1, int(round(sw * scale))), max(1, int(round(sh * scale)))
    # Pillow ≥10 removed top-level Image.LANCZOS — use Resampling enum
    resized = src_im.resize((nw, nh), Image.Resampling.LANCZOS)
    canvas = Image.new("RGBA", (mw, mh), (0, 0, 0, 0))
    canvas.alpha_composite(resized, ((mw - nw) // 2, (mh - nh) // 2))
    return canvas


def _png_bytes(im: Image.Image) -> bytes:
    buf = io.BytesIO()
    im.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _stage_upload_png(filename: str, data: bytes,
                      shopify_domain: str, shopify_token: str) -> dict:
    """stagedUploadsCreate → POST to S3 → return stagedTarget (has resourceUrl)."""
    m = """
    mutation StagedUploads($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets { url resourceUrl parameters { name value } }
        userErrors { field message }
      }
    }"""
    out = _gql(shopify_domain, shopify_token, m, {
        "input": [{
            "resource": "IMAGE",
            "filename": filename,
            "mimeType": "image/png",
            "httpMethod": "POST",
        }],
    })
    errs = (out.get("data", {}).get("stagedUploadsCreate", {}) or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"stagedUploadsCreate: {errs}")
    targets = out["data"]["stagedUploadsCreate"]["stagedTargets"]
    if not targets:
        raise RuntimeError("stagedUploadsCreate returned no targets")
    target = targets[0]

    form  = {p["name"]: p["value"] for p in target["parameters"]}
    files = {"file": (filename, data, "image/png")}
    r = requests.post(target["url"], data=form, files=files, timeout=60)
    r.raise_for_status()
    return target


def _file_create_public(staged_target: dict, filename: str, alt_text: str,
                        shopify_domain: str, shopify_token: str) -> str:
    """fileCreate from stagedTarget.resourceUrl, poll until MediaImage.url populates."""
    m = """
    mutation FileCreate($files: [FileCreateInput!]!) {
      fileCreate(files: $files) {
        files {
          __typename
          ... on MediaImage { id image { url altText } }
          ... on GenericFile { id }
        }
        userErrors { field message }
      }
    }"""
    out = _gql(shopify_domain, shopify_token, m, {
        "files": [{
            "originalSource": staged_target["resourceUrl"],
            "contentType": "IMAGE",
            "alt":      alt_text or None,
            "filename": filename or None,
        }],
    })
    errs = out["data"]["fileCreate"].get("userErrors") or []
    if errs:
        raise RuntimeError(f"fileCreate: {errs}")
    files = out["data"]["fileCreate"]["files"]
    if not files:
        raise RuntimeError("fileCreate returned no files")
    fid = files[0]["id"]

    q = """
    query FileNode($id: ID!) {
      node(id: $id) {
        __typename
        ... on MediaImage { image { url } }
        ... on GenericFile { url }
      }
    }"""
    for _ in range(20):  # ~10s
        jj = _gql(shopify_domain, shopify_token, q, {"id": fid})
        node = jj["data"]["node"]
        if node["__typename"] == "MediaImage":
            url = (node.get("image") or {}).get("url")
            if url:
                return url
        elif node["__typename"] == "GenericFile":
            url = node.get("url")
            if url:
                return url
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for MediaImage URL (file {fid})")


def _slugify_filename(title: str, idx: int) -> str:
    v = unicodedata.normalize("NFKD", title or "slab").encode("ascii", "ignore").decode("ascii")
    v = re.sub(r"[^a-zA-Z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-").lower() or "slab"
    return f"{v}-{idx + 1}.png"


def matte_and_host_psa_images(raw_urls: list[str], title: str,
                              shopify_domain: str, shopify_token: str) -> list[str]:
    """
    Download each PSA CDN image → matte on transparent 2000×2000 → upload to Shopify
    Files → return the Shopify-hosted URLs.

    Any per-image failure logs a warning and falls back to the raw CDN URL for that
    image so one corrupt scan doesn't block the whole slab.
    """
    processed: list[str] = []
    for i, src in enumerate(raw_urls or []):
        fname = _slugify_filename(title, i)
        try:
            im     = _download_image(src)
            matted = _matte_card_rgba(im)
            png    = _png_bytes(matted)
            staged = _stage_upload_png(fname, png, shopify_domain, shopify_token)
            public = _file_create_public(staged, fname, title, shopify_domain, shopify_token)
            processed.append(public)
        except Exception as e:
            logger.warning(f"PSA image matte/host failed for {src} — falling back to raw CDN: {e}")
            processed.append(src)
    return processed


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
            "vendor": "Pack Fresh",
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
                {"namespace": "pf_slab", "key": "tcgplayer_id",
                 "value": str(tcgplayer_id or ""), "type": "single_line_text_field"},
                {"namespace": "pf_slab", "key": "grade_company",
                 "value": tags[1] if len(tags) > 1 else "", "type": "single_line_text_field"},
                {"namespace": "pf_slab", "key": "grade_value",
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

    # ── 2a. Matte PSA scans onto 2000×2000 transparent canvas + host on Shopify ─
    # Legacy psa_lookup behavior — keeps graded PDP look consistent. Per-image
    # failures fall back to the raw PSA CDN URL so one bad scan doesn't block push.
    if image_urls:
        image_urls = matte_and_host_psa_images(
            image_urls, title, shopify_domain, shopify_token,
        )

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
