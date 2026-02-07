import os
import requests
from dotenv import load_dotenv
import re

import io

load_dotenv()

PSA_API_KEY = os.environ.get("PSA_API_KEY")
SHOPIFY_DOMAIN = os.environ.get("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
SHOPIFY_VERSION = "2024-04"
from PIL import Image
MATTE_W, MATTE_H   = 2000, 2000
CONTENT_W, CONTENT_H = 950, 1600

PSA_API_BASE = "https://api.psacard.com/publicapi/cert"

import json
import time
def _download_image(url: str) -> Image.Image:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    im = Image.open(io.BytesIO(r.content))
    # Ensure RGBA for transparency handling later
    return im.convert("RGBA")

def _matte_card_rgba(src_im: Image.Image) -> Image.Image:
    # Scale-to-fit within CONTENT_W x CONTENT_H (no cropping)
    src_w, src_h = src_im.size
    scale = min(CONTENT_W / src_w, CONTENT_H / src_h)
    new_w, new_h = max(1, int(round(src_w * scale))), max(1, int(round(src_h * scale)))
    resized = src_im.resize((new_w, new_h), Image.LANCZOS)

    # Create 2000x2000 transparent canvas + center paste
    canvas = Image.new("RGBA", (MATTE_W, MATTE_H), (0, 0, 0, 0))
    off_x = (MATTE_W - new_w) // 2
    off_y = (MATTE_H - new_h) // 2
    canvas.alpha_composite(resized, (off_x, off_y))
    return canvas

def _png_bytes(im: Image.Image) -> bytes:
    buf = io.BytesIO()
    # Optimize but keep quality high; PNG supports transparency
    im.save(buf, format="PNG", optimize=True)
    return buf.getvalue()

def normalize_pokemon_text(s: str) -> str:
    """
    Force ASCII-safe 'Pokemon' spelling (no accents) everywhere.
    """
    s = strip_accents(s or "")
    return re.sub(r"(?i)\bpokemon\b", "Pokemon", s)
def _shopify_staged_upload_png(filename: str, data: bytes) -> dict:
    """Stage a file to Shopify and return { 'resourceUrl', 'mimeType' } etc."""
    gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json", "Accept": "application/json"}
    # 1) Ask Shopify for a staging target (S3)
    m1 = """
    mutation StagedUploads($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets { url resourceUrl parameters { name value } }
        userErrors { field message }
      }
    }"""
    variables = {
        "input": [{
            "resource": "IMAGE",
            "filename": filename,
            "mimeType": "image/png",
            "httpMethod": "POST"
        }]
    }
    r = requests.post(gql_url, headers=headers, json={"query": m1, "variables": variables})
    r.raise_for_status()
    j = r.json()
    errs = j.get("data", {}).get("stagedUploadsCreate", {}).get("userErrors")
    if errs:
        raise RuntimeError(f"stagedUploadsCreate error: {errs}")
    target = j["data"]["stagedUploadsCreate"]["stagedTargets"][0]

    # 2) POST the file to S3 with the given fields
    files = {"file": (filename, data, "image/png")}
    form = {p["name"]: p["value"] for p in target["parameters"]}
    post = requests.post(target["url"], data=form, files=files)
    post.raise_for_status()

    return target  # includes resourceUrl we can pass to fileCreate

def add_variant_to_existing_product(product_id: int, cert_number: str, image_urls: list[str]) -> dict:
    """
    Add a new variant to an existing product, using Certification ID as the option value.
    Also uploads provided images to the product and attaches the first one to the variant.
    """
    url_base = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # 1) Upload images to the product and collect their IDs
    image_ids = []
    for src in image_urls:
        img_payload = {"image": {"src": src}}
        r_img = requests.post(
            f"{url_base}/products/{product_id}/images.json",
            headers=headers,
            json=img_payload,
        )
        r_img.raise_for_status()
        img_data = r_img.json().get("image")
        if img_data and img_data.get("id"):
            image_ids.append(img_data["id"])

    # 2) Create the new variant keyed by Certification ID
    variant_payload = {
        "variant": {
            "option1": str(cert_number),
            "sku": str(cert_number),
            "barcode": str(cert_number),
            "price": "0.00",
            "inventory_management": "shopify",
            "inventory_quantity": 1,
            "requires_shipping": True,
        }
    }

    if image_ids:
        variant_payload["variant"]["image_id"] = image_ids[0]

    # IMPORTANT: use the product-scoped endpoint
    r_var = requests.post(
        f"{url_base}/products/{product_id}/variants.json",
        headers=headers,
        json=variant_payload,
    )
    try:
        r_var.raise_for_status()
    except requests.HTTPError as e:
        # Optional: extra debugging if this ever fires again
        print("Variant create failed:", r_var.status_code, r_var.text)
        raise

    return r_var.json()

def _shopify_files_create_public_png(staged_target: dict, alt_text: str = "", filename: str = "") -> str:
    gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json", "Accept": "application/json"}

    m2 = """
    mutation FileCreate($files: [FileCreateInput!]!) {
      fileCreate(files: $files) {
        files {
          __typename
          ... on MediaImage { id image { url altText } }
          ... on GenericFile { id }  # fallback, but we don't want this path
        }
        userErrors { field message }
      }
    }"""

    variables = {
        "files": [{
            "originalSource": staged_target["resourceUrl"],
            "contentType": "IMAGE",            # <— key change
            "alt": alt_text or None,
            "filename": filename or None
        }]
    }

    r = requests.post(gql_url, headers=headers, json={"query": m2, "variables": variables})
    r.raise_for_status()
    j = r.json()
    errs = j["data"]["fileCreate"]["userErrors"]
    if errs:
        raise RuntimeError(f"fileCreate error: {errs}")

    # Grab id and poll until url is populated (processing can be async)
    files = j["data"]["fileCreate"]["files"]
    if not files:
        raise RuntimeError("fileCreate returned no files")

    fid = files[0]["id"]  # MediaImage or GenericFile

    q = """
    query FileNode($id: ID!) {
      node(id: $id) {
        __typename
        ... on MediaImage { image { url altText } }
        ... on GenericFile { url }  # usually null for FILEs
      }
    }"""

    import time
    for _ in range(20):  # ~10s total
        rr = requests.post(gql_url, headers=headers, json={"query": q, "variables": {"id": fid}})
        rr.raise_for_status()
        jj = rr.json()
        node = jj["data"]["node"]
        if node["__typename"] == "MediaImage":
            url = (node["image"] or {}).get("url")
            if url:
                return url
        elif node["__typename"] == "GenericFile":
            url = node.get("url")
            if url:     # unlikely on GenericFile
                return url
        time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for MediaImage URL for file {fid}")

def matte_and_host(url: str, out_name: str) -> str:
    """Return a Shopify-hosted URL of a mattted 2000x2000 transparent PNG."""
    im = _download_image(url)
    mat = _matte_card_rgba(im)
    png = _png_bytes(mat)
    staged = _shopify_staged_upload_png(out_name, png)
    public_url = _shopify_files_create_public_png(staged, alt_text=out_name, filename=out_name)
    return public_url

class QuotaHit(Exception):
    pass

def safe_load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default

def request_json(url, headers, *, stop_on_429=True, max_retries=3, retry_sleep=5):
    """Generic GET with basic retry on 5xx and an immediate stop on 429/529."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code in (429, 529):  # PSA sometimes returns 529 too
            # We hit daily/temporary rate limit. Stop so we can resume tomorrow.
            if stop_on_429:
                raise QuotaHit(f"Rate limited: HTTP {resp.status_code}")
        if 500 <= resp.status_code < 600:
            # brief backoff for transient server errors
            time.sleep(retry_sleep * (attempt + 1))
            continue
        resp.raise_for_status()
        return resp.json()
    # If we’re here, retries didn’t fix it.
    resp.raise_for_status()

def get_psa_data(cert_number):
    headers = {
        "Authorization": f"bearer {PSA_API_KEY}",
        "User-Agent": "PackFreshBot/1.0 (+https://pack-fresh.com)"
    }
    url = f"https://api.psacard.com/publicapi/cert/GetByCertNumber/{cert_number}"
    return request_json(url, headers)

import unicodedata

def strip_accents(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )



def infer_tags_from_title(title):
    tags = ["slab"]

    title_lower = strip_accents(title).lower()
    if "pokemon" in title_lower:
        tags.append("pokemon")

    set_keywords = {
        "brilliant stars": "brilliant stars",
        "evolving skies": "evolving skies",
        "base set": "base set",
        "vivid voltage": "vivid voltage",
        "fusion strike": "fusion strike",
        # Add more here as needed
    }

    era_map = {
        "brilliant stars": "sword shield era",
        "fusion strike": "sword shield era",
        "base set": "wotc era",
        "evolving skies": "sword shield era",
        # Add more mappings as needed
    }

    for keyword, tag in set_keywords.items():
        if keyword in title_lower:
            tags.append(tag)
            era = era_map.get(tag)
            if era:
                tags.append(era)
            break

    return list(set(tags))  # deduplicate
def create_listings_from_cache(dry_run=True):
    cache = safe_load_json(CACHE_FILE, {})
    for cert, entry in cache.items():
        if entry.get("created"):
            continue
        psa = entry.get("psa", {})
        images = entry.get("images", [])

        title = build_clean_title(psa)
        description = build_clean_description(psa)
        tags = infer_tags_from_title(title)

        if dry_run:
            print("="*80)
            print(f"DRY RUN — {cert}")
            print(f"Title: {title}")
            print(f"Tags:  {tags}")
            print(f"Imgs:  {len(images)}")
        else:
            if find_product_by_psa_cert(cert):
                print(f"↩️  Skip {cert} — already exists")
                entry["created"] = True
                with open(CACHE_FILE, "w") as f: json.dump(cache, f, indent=2)
                continue
            res = create_shopify_listing(title, description, images, tags, cert)
            print(f"✅ Created listing for {cert}: {res['product']['admin_graphql_api_id']}")
            entry["created"] = True
            with open(CACHE_FILE, "w") as f:
                json.dump(cache, f, indent=2)


import requests

def prewire_publications_gql(product_gid):
    """
    Pre-create channel availability for a product while it's still DRAFT.
    Later, flipping status->ACTIVE makes it live everywhere immediately.
    """
    gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # 1) Fetch all publications (Online Store, Shop, POS, etc.)
    q = """
    query {
      publications(first: 50) {
        nodes { id name }
      }
    }
    """
    pubs = requests.post(gql_url, headers=headers, json={"query": q})
    pubs.raise_for_status()
    data = pubs.json()
    pub_nodes = data["data"]["publications"]["nodes"]

    # 2) Publish this product into every publication
    m = """
    mutation Publish($id: ID!, $pub: ID!) {
      publishablePublish(id: $id, input: { publicationId: $pub }) {
        publishable { id }
        userErrors { field message }
      }
    }
    """
    for node in pub_nodes:
        variables = {"id": product_gid, "pub": node["id"]}
        resp = requests.post(gql_url, headers=headers, json={"query": m, "variables": variables})
        if resp.status_code >= 400:
            raise requests.HTTPError(f"GraphQL HTTP {resp.status_code}: {resp.text}")
        out = resp.json()
        errs = out.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
        if errs:
            # benign if already published; otherwise surface details
            msg = "; ".join([e.get("message", "unknown error") for e in errs])
            if "already published" not in msg.lower():
                raise RuntimeError(f"publishablePublish error: {msg}")


def ensure_product_category_id(path="Collectible Trading Cards > Gaming Cards"):
    """
    Returns the numeric product_category_id for the given taxonomy path.
    Caches the result in-memory to avoid repeated fetches.
    """
    if not hasattr(ensure_product_category_id, "_cache"):
        ensure_product_category_id._cache = {}
    if path in ensure_product_category_id._cache:
        return ensure_product_category_id._cache[path]

    url_base = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Accept": "application/json"}
    # Pull categories (paged if needed). If your store is large, loop with page_info.
    r = requests.get(f"{url_base}/product_categories.json?limit=250", headers=headers)
    r.raise_for_status()
    cats = r.json().get("product_categories", [])
    # Some shops nest; match on full path/name
    target = path.strip().lower()
    match = next((c for c in cats if (c.get("full_name") or "").strip().lower() == target), None)
    if not match:
        # Fallback: try a contains match on full_name
        match = next((c for c in cats if target in (c.get("full_name") or "").strip().lower()), None)
    if not match:
        raise RuntimeError(f"Product category not found for path: {path}")

    cid = match["id"]
    ensure_product_category_id._cache[path] = cid
    return cid

def clean_set_display(brand: str, variety: str) -> str:
    """
    Build a human-readable set display from PSA Brand/Variety.
    - Strips 'POKEMON' prefix and language codes like EN/JP
    - Keeps legitimate era markers like 'EX', 'XY', 'SV', 'SWSH'
    - Formats '... Radiant Collection' as 'Generations – Radiant Collection'
    - Collapses extra spaces/dashes and Title-cases words (while leaving ALL-CAPS acronyms intact)
    """
    raw = (brand or variety or "").strip()

    # Strip leading POKEMON
    raw = re.sub(r"(?i)^POKEMON\s*", "", raw)

    # Remove common language/country codes that clutter the set line
    # e.g., "EN-151", "EN-SV", "EN", "JPN", "ENG"
    raw = re.sub(r"(?i)\b(EN|ENG|JP|JPN|FR|DE|IT|ES)(-[A-Z0-9]+)?\b", "", raw)

    # Normalize dashes/spaces
    raw = re.sub(r"\s*[-_/]\s*", " ", raw)
    raw = re.sub(r"\s{2,}", " ", raw).strip()

    # Radiant Collection special case
    if re.search(r"(?i)\bradiant collection\b", raw):
        # Try to split off the RC tag from the main set name
        main = re.sub(r"(?i)\bradiant collection\b", "", raw).strip(" -")
        raw = f"{main} – Radiant Collection"

    # Title-case, but preserve ALL-CAPS acronyms (EX, GX, VMAX, VSTAR, SV, SWSH, XY, BW, SM)
    def smart_title_token(tok: str) -> str:
        keep_caps = {"EX","GX","VMAX","VSTAR","SV","SWSH","XY","BW","SM","RC","LV.X"}
        return tok if tok.upper() in keep_caps else tok.capitalize()

    tokens = re.split(r"(\s+|–)", raw)  # keep separators to preserve the en dash
    tokens = [smart_title_token(t) if t.strip() and t.strip() not in {"–"} and not t.isspace() else t for t in tokens]
    set_display = "".join(tokens).strip()

    # Clean leftover double spaces again
    set_display = re.sub(r"\s{2,}", " ", set_display).strip()
    return set_display


def clean_card_name(subject: str) -> str:
    """
    Clean PSA Subject into a display-ready card name.
    - Remove FA/AR/HR/etc prefixes
    - -HOLO removed; -REV.FOIL -> (Reverse Holo)
    - Keep EX, GX, V, VSTAR, VMAX, TAG, BREAK, LV.X uppercase
    """
    s = (subject or "").strip()

    # Strip common prefix flags like FA/ AR/ HR/ SR/ etc.
    s = re.sub(r"^(FA|AR|RR|UR|HR|CSR|CHR|SR)/", "", s, flags=re.IGNORECASE)

    # Handle reverse holo vs holo
    if re.search(r"-REV\.?FOIL", s, flags=re.IGNORECASE):
        s = re.sub(r"-REV\.?FOIL", "(Reverse Holo)", s, flags=re.IGNORECASE)
    else:
        s = re.sub(r"-HOLO\b", "", s, flags=re.IGNORECASE)

    s = s.strip()

    # Uppercase special tokens; capitalize the rest
    special = {"GX","EX","V","VSTAR","VMAX","TAG","BREAK","LV.X","M"}  # keep 'M' for Mega as-is
    parts = []
    for w in s.split():
        parts.append(w.upper() if w.upper() in special else w.capitalize())
    return " ".join(parts).replace("  ", " ").strip()
def find_product_by_psa_cert(cert_number):
    # REST search by title can be fuzzy; GraphQL is better, but here’s a REST fallback using metafields:
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}
    # 1) find metafields that match the cert
    mf_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/metafields.json?namespace=psa&key=cert&value={cert_number}"
    mres = requests.get(mf_url, headers=headers)
    mres.raise_for_status()
    mfs = mres.json().get("metafields", [])
    for mf in mfs:
        if mf.get("owner_resource") == "product":
            return mf.get("owner_id")  # product id

    # 2) (optional) fall back to title exact match if you want
    # t_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/products.json?title={requests.utils.quote(title)}&status=any"
    # ...
    return None
def create_shopify_listing(title, description, image_urls, tags, cert_number: str):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/products.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json"
    }

    payload = {
        "product": {
            "title": title,
            "body_html": f"<p>{description}</p>",
            "tags": ", ".join(tags),
            "status": "draft",
            "product_type": "Pokemon",
            "published_scope": "web",
            "template_suffix": "cro-alt",
            "standardized_product_type": "Collectible Trading Cards > Gaming Cards",
            "images": [{"src": img} for img in image_urls],

            # NEW: single option "Certification ID"
            "options": [
                {
                    "name": "Certification ID",
                    "values": [str(cert_number)]
                }
            ],

            # NEW: first variant keyed by Certification ID
            "variants": [
                {
                    "option1": str(cert_number),
                    "sku": str(cert_number),
                    "barcode": str(cert_number),
                    "price": "0.00",
                    "inventory_management": "shopify",
                    "inventory_quantity": 1,
                    "requires_shipping": True,
                }
            ],
        }
    }

    res = requests.post(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json()


def taxonomy_gid_to_product_category_id(tax_cat_gid: str, fallback_full_name: str | None = None) -> int:
    """
    Resolve a TaxonomyCategory GID (e.g., gid://shopify/TaxonomyCategory/ae-2-2-3-2)
    to the numeric product_category_id used by REST.
    If the node lookup fails, optionally falls back to a fullName search.
    """
    gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # 1) Try node() lookup for legacyResourceId
    q_node = """
    query($id: ID!) {
      node(id: $id) {
        ... on TaxonomyCategory {
          id
          fullName
          legacyResourceId
        }
      }
    }
    """
    r = requests.post(gql_url, headers=headers, json={"query": q_node, "variables": {"id": tax_cat_gid}})
    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")

    if r.status_code >= 400 or "errors" in j:
        # Print detailed error for debugging
        raise RuntimeError(f"GraphQL error (node): HTTP {r.status_code}, body={j}")

    node = (j.get("data") or {}).get("node")
    if node and node.get("legacyResourceId"):
        return int(node["legacyResourceId"])

    # 2) Fallback: search ProductCategory by fullName (requires name)
    # If we didn't get fullName from node, use caller-provided fallback_full_name
    full_name = (node or {}).get("fullName") or fallback_full_name
    if not full_name:
        raise RuntimeError(
            "Could not resolve legacyResourceId via node(), and no fallback_full_name provided. "
            f"Response was: {j}"
        )

    q_search = """
    query($q: String!) {
      productCategories(first: 50, query: $q) {
        nodes {
          id
          fullName
          legacyResourceId
        }
      }
    }
    """
    # Exact full_name match first, then a broader pass if needed
    for qtext in (f'full_name:"{full_name}"', full_name):
        r2 = requests.post(gql_url, headers=headers, json={"query": q_search, "variables": {"q": qtext}})
        j2 = r2.json()
        if r2.status_code >= 400 or "errors" in j2:
            continue
        nodes = ((j2.get("data") or {}).get("productCategories") or {}).get("nodes") or []
        # Prefer exact fullName match
        for n in nodes:
            if (n.get("fullName") or "").strip().lower() == full_name.strip().lower():
                if n.get("legacyResourceId"):
                    return int(n["legacyResourceId"])
                # If no legacyResourceId, derive from GID as last resort
                gid = n.get("id", "")
                try:
                    return int(gid.split("/")[-1])
                except Exception:
                    pass
        # If no exact match, but we have any node with legacyResourceId, use the first
        if nodes:
            n0 = nodes[0]
            if n0.get("legacyResourceId"):
                return int(n0["legacyResourceId"])
            gid = n0.get("id", "")
            try:
                return int(gid.split("/")[-1])
            except Exception:
                pass

    raise RuntimeError(
        f"Failed to resolve product_category_id. "
        f"TaxonomyCategory GID={tax_cat_gid}, tried fullName='{full_name}'."
    )


def get_psa_image_urls(cert_number):
    headers = {
        "Authorization": f"bearer {PSA_API_KEY}",
        "User-Agent": "PackFreshBot/1.0"
    }
    url = f"https://api.psacard.com/publicapi/cert/GetImagesByCertNumber/{cert_number}"
    data = request_json(url, headers)
    # Sort so front image (IsFrontImage=True) comes first if present
    data = sorted(data, key=lambda x: not x.get("IsFrontImage", False))
    return [entry["ImageURL"] for entry in data]


def build_clean_description(psa):
    """
    Build a Shopify-safe 'Card Information' block from PSA data.
    Cleans brand/set names, subject/card names, and applies special-case fixes.
    """

    cert_number = psa.get("CertNumber", "").strip()
    year = psa.get("Year", "").strip()

    # --- IP extraction (always 'Pokémon' from Brand for your use case)
    ip = "Pokemon"

    # --- Set extraction from Brand
    brand = psa.get("Brand", "").strip()
    variety = psa.get("Variety", "").strip()

    # Start with Brand, fallback to Variety if missing
    set_raw = brand or variety
    set_clean = re.sub(r"(?i)^POKEMON", "", set_raw).strip()

    # Handle Radiant Collection special case
    if "RADIANT COLLECTION" in set_clean.upper():
        # Example: "XY Generations Radiant Collection"
        parts = re.split(r"(?i)RADIANT COLLECTION", set_clean, maxsplit=1)
        main_set = parts[0].strip(" -")
        set_clean = f"{main_set} – Radiant Collection"

    # Remove language codes, dashes, extra clutter like EN-151, SVP, etc.
    set_clean = re.sub(r"\b(EN|JP|ENG|JPN|FR|SVP|SWSH|SVI|XY|BW|SM|EX|PROMO)([-\s]|$)", "", set_clean, flags=re.IGNORECASE)
    set_clean = re.sub(r"\s{2,}", " ", set_clean).strip()
    set_clean = set_clean.title()

    # --- Card Name cleanup from Subject
    subject_raw = psa.get("Subject", "").strip()

    # Strip FA/, AR/, HR/, etc.
    card_name = re.sub(r"^(FA|AR|RR|UR|HR|CSR|CHR|SR)/", "", subject_raw, flags=re.IGNORECASE)

    # Handle -HOLO → remove entirely, -REV.FOIL → Reverse Holo
    if re.search(r"-REV\.?FOIL", card_name, flags=re.IGNORECASE):
        card_name = re.sub(r"-REV\.?FOIL", "(Reverse Holo)", card_name, flags=re.IGNORECASE)
    else:
        card_name = re.sub(r"-HOLO", "", card_name, flags=re.IGNORECASE)

    card_name = card_name.strip()

    # Uppercase special keywords (EX, GX, V, VSTAR, VMAX, LV.X, TAG, BREAK)
    card_name = " ".join([
        word.upper() if word.upper() in ["GX", "EX", "V", "VSTAR", "VMAX", "TAG", "BREAK", "LV.X"]
        else word.capitalize()
        for word in card_name.split()
    ])

    # --- Card Number
    card_number = psa.get("CardNumber", "").strip()

    # --- Grade
    grade = normalize_grade(psa.get("CardGrade", ""))

    # --- Population
    population = psa.get("TotalPopulation", "")

    # Build HTML block
    html_lines = [
        "<p><strong>Card Information:</strong></p>",
        f"<p>Cert Number: {cert_number}</p>",
        f"<p>Year: {year}</p>",
        f"<p>IP: {ip}</p>",
        f"<p>Set: {set_clean}</p>",
        f"<p>Card Name: {card_name}</p>",
        f"<p>Card Number: {card_number}</p>",
        f"<p>Grade: {grade}</p>",
        f"<p>Population: {population}</p>"
    ]

    return normalize_pokemon_text("\n".join(html_lines))


def normalize_grade(raw):
    if raw.upper().startswith("GEM MT"):
        return "10"
    match = re.search(r"(\d+(\.\d)?)", raw)
    return match.group(1) if match else raw.strip()

def normalize_set_name(variety, brand):
    raw = variety or brand or ""
    raw = raw.upper().replace("POKEMON", "").strip()

    # Strip suffixes like "-SECRET", "-PROMO", etc.
    base = re.sub(r"-.*$", "", raw)
    base = base.replace("SWORD AND SHIELD", "").strip()
    return " ".join(word.capitalize() for word in base.split())

def build_clean_title(psa):
    year = (psa.get("Year") or "").strip()
    card_number = (psa.get("CardNumber") or "").strip()
    grade = normalize_grade(psa.get("CardGrade", "") or "")

    set_display = clean_set_display(psa.get("Brand", ""), psa.get("Variety", ""))
    card_name   = clean_card_name(psa.get("Subject", "") or "")

    # Always use “Pokémon” IP label in titles
    ip = "Pokemon"

    # e.g., "2016 Pokémon Generations – Radiant Collection Gardevoir EX #RC30 PSA 9"
    title = f"{year} {ip} {set_display} {card_name} #{card_number} PSA {grade}".strip()
    return normalize_pokemon_text(title)
def find_product_existing(title, cert_number=None, gtin_as_sku=None):
    url_base = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}

    # 1) If you have GTIN and you saved it as the variant SKU previously:
    if gtin_as_sku:
        v_url = f"{url_base}/variants.json?sku={requests.utils.quote(gtin_as_sku)}"
        v_res = requests.get(v_url, headers=headers)
        v_res.raise_for_status()
        variants = v_res.json().get("variants", [])
        if variants:
            return variants[0]["product_id"]

    # 2) Exact title match (case must match Shopify's saved title)
    p_url = f"{url_base}/products.json?title={requests.utils.quote(title)}&status=any&limit=50"
    p_res = requests.get(p_url, headers=headers)
    p_res.raise_for_status()
    for p in p_res.json().get("products", []):
        # Require exact match to be safe
        if (p.get("title") or "") == title:
            return p["id"]

    # 3) If you’ve already started adding psa.cert metafields, check them too
    if cert_number:
        mf_url = (f"{url_base}/metafields.json?namespace=psa&key=cert")
        m_res = requests.get(mf_url, headers=headers)
        m_res.raise_for_status()
        for mf in m_res.json().get("metafields", []):
            if mf.get("owner_resource") == "product" and str(mf.get("value")) == str(cert_number):
                return mf.get("owner_id")

    return None


import requests
import unicodedata


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s or "")

def find_existing_product_id(title, gtin=None):
    gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

    # 1) Fast path: variant by SKU / barcode
    v_terms = []
    if gtin:
        v_terms.append(f"sku:{gtin}")
        v_terms.append(f"barcode:{gtin}")
    if v_terms:
        v_q = " OR ".join(v_terms)
        q1 = """
        query($q:String!){
          productVariants(first: 1, query: $q) {
            edges {
              node {
                product { id }
              }
            }
          }
        }"""
        r1 = requests.post(gql_url, headers=headers, json={"query": q1, "variables": {"q": v_q}})
        r1.raise_for_status()
        edges = (r1.json().get("data", {}).get("productVariants", {}) or {}).get("edges", [])
        if edges:
            prod_gid = edges[0]["node"]["product"]["id"]  # gid://shopify/Product/123
            return int(prod_gid.split("/")[-1])

    # 2) Product search by exact title (CORRECT OPERATOR: title)
    safe_title = _nfc(title).replace('"', '\\"')
    p_q_str = f'title:"{safe_title}"'
    q2 = """
    query($q:String!){
      products(first: 1, query: $q) {
        edges { node { id title } }
      }
    }"""
    r2 = requests.post(gql_url, headers=headers, json={"query": q2, "variables": {"q": p_q_str}})
    r2.raise_for_status()
    pedges = (r2.json().get("data", {}).get("products", {}) or {}).get("edges", [])
    if pedges:
        prod_gid = pedges[0]["node"]["id"]
        return int(prod_gid.split("/")[-1])

    return None
def get_processed_image_urls(title, raw_urls, cert_number) -> list[str]:
    # your existing PSA URLs
    processed = []
    for i, u in enumerate(raw_urls):
        fname = slugify_filename(title, i)
        processed.append(matte_and_host(u, fname))
    return processed

def build_listing_from_cert(cert_number):
    data = get_psa_data(cert_number)
    psa = data.get("PSACert", {})

    # Build title and description
    title = build_clean_title(psa)
    description = build_clean_description(psa)

    # Raw PSA images → processed Shopify URLs
    raw_images = get_psa_image_urls(cert_number)
    image_urls = get_processed_image_urls(title, raw_images, cert_number)

    # Tags
    tags = infer_tags_from_title(title)

    # Create product with Certification ID as variant option
    result = create_shopify_listing(title, description, image_urls, tags, cert_number)
    print(f"✅ Created listing: {result['product']['admin_graphql_api_id']}")


import requests

GQL_VERSION_CANDIDATES = ["2025-01", "2024-10", "2024-07"]  # try in order

def set_category_with_taxonomy_gid(product_gid: str, taxonomy_cat_gid: str):
    """
    Set Admin 'Category' for a product using a TaxonomyCategory GID.
    Works while the product is DRAFT. Tries multiple API versions and prints errors if any.
    """
    for ver in GQL_VERSION_CANDIDATES:
        gql_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{ver}/graphql.json"
        headers = {
            "X-Shopify-Access-Token": SHOPIFY_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Modern schema: taxonomyCategoryId
        m = """
        mutation SetCategory($id: ID!, $tax: ID!) {
          productUpdate(input: {
            id: $id,
            productCategory: { taxonomyCategoryId: $tax }
          }) {
            product { id productCategory { taxonomyCategory { id fullName } } }
            userErrors { field message }
          }
        }"""

        r = requests.post(gql_url, headers=headers, json={"query": m, "variables": {"id": product_gid, "tax": taxonomy_cat_gid}})
        try:
            j = r.json()
        except Exception:
            print(f"[Category:{ver}] HTTP {r.status_code} non-JSON: {r.text}")
            continue

        if r.status_code >= 400:
            print(f"[Category:{ver}] HTTP {r.status_code}: {j}")
            continue

        errs = (j.get("data") or {}).get("productUpdate", {}).get("userErrors", [])
        if not errs:
            # success
            return
        # If the error is schema-related we'll try next version; otherwise surface it
        msg = "; ".join(e.get("message", "unknown") for e in errs)
        if "unknown" in msg.lower() or "argument" in msg.lower() or "field" in msg.lower():
            print(f"[Category:{ver}] schema/userErrors: {errs} (will try next version)")
            continue
        else:
            raise RuntimeError(f"[Category:{ver}] productUpdate errors: {errs}")

    raise RuntimeError("Failed to set category via all GraphQL versions tested.")

import csv
import json
from time import sleep

CACHE_FILE = "psa_cache.json"
CERTS_FILE = "Certs.csv"

def load_certs_from_csv():
    with open(CERTS_FILE, newline='') as f:
        reader = csv.reader(f)
        certs = [row[0].strip() for row in reader if row]
    return certs

def build_cache():
    certs = load_certs_from_csv()
    cache = safe_load_json(CACHE_FILE, {})

    total = len(certs)
    processed = 0

    try:
        for i, cert in enumerate(certs, start=1):
            if cert in cache:
                print(f"[{i}/{total}] Skip {cert} (already cached)")
                continue

            try:
                psa_data = get_psa_data(cert)

                try:
                    images = get_psa_image_urls(cert)
                except QuotaHit as qh:  # image call can also rate-limit
                    # Save what we have so far (without images) and re-raise
                    cache[cert] = {"psa": psa_data.get("PSACert", {}), "images": []}
                    with open(CACHE_FILE, "w") as f:
                        json.dump(cache, f, indent=2)
                    raise
                except requests.exceptions.RequestException as img_err:
                    print(f"⚠️  No images for {cert} (continuing): {img_err}")
                    images = []

                cache[cert] = {
                    "psa": psa_data.get("PSACert", {}),
                    "images": images
                }
                # Persist after each cert
                with open(CACHE_FILE, "w") as f:
                    json.dump(cache, f, indent=2)

                processed += 1
                print(f"[{i}/{total}] Cached {cert} ✅ (processed this run: {processed})")

                # Light throttle—tune as needed
                time.sleep(2)

            except QuotaHit as qh:
                print(f"⛔ Quota hit while fetching {cert}: {qh}")
                print(f"✅ Progress saved to {CACHE_FILE}. Resuming tomorrow is safe.")
                break
            except Exception as e:
                print(f"❌ Skipped {cert} due to error: {e}")
                # Persist current state even on unexpected errors
                with open(CACHE_FILE, "w") as f:
                    json.dump(cache, f, indent=2)
                continue

    finally:
        # Final write, just in case
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)

def slugify_filename(title: str, idx: int = 0) -> str:
    """
    Normalize title into a safe filename:
    - strip accents
    - lowercase
    - replace spaces/specials with '-'
    - collapse multiple dashes
    - strip leading/trailing dashes
    """
    # normalize unicode → ascii
    value = unicodedata.normalize("NFKD", title)
    value = value.encode("ascii", "ignore").decode("ascii")

    # replace non-alphanumeric with dashes
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)  # collapse
    value = value.strip("-").lower()

    # add index suffix so front/back don’t overwrite each other
    return f"{value}-{idx+1}.png"
def iterate_cached_results():
    with open(CACHE_FILE) as f:
        cache = json.load(f)

    for cert, entry in cache.items():
        if entry.get("created"):  # our own flag (see below)
            continue

        psa = entry["psa"]
        images = entry["images"]

        title = build_clean_title(psa)
        description = build_clean_description(psa)
        tags = infer_tags_from_title(title)

        # check existing
        existing_id = find_existing_product_id(
            title,
            gtin=entry.get("gtin") or entry.get("sku") or entry.get("barcode")
        )

        # Always process PSA → Shopify-hosted images
        processed_images = get_processed_image_urls(title, images, cert)

        if existing_id:
            print(f"↩️  Adding new variant for cert {cert} to existing product {existing_id}")
            add_variant_to_existing_product(existing_id, cert, processed_images)
            product_gid = f"gid://shopify/Product/{existing_id}"
        else:
            result = create_shopify_listing(title, description, processed_images, tags, cert)
            product_gid = result["product"]["admin_graphql_api_id"]

            prewire_publications_gql(product_gid)
            tax_cat_gid = "gid://shopify/TaxonomyCategory/ae-2-2-3-2"
            set_category_with_taxonomy_gid(product_gid, tax_cat_gid)

        # One unified log line, works for both paths
        print(f"✅ Created listing: {product_gid}")
        print("=" * 80)
        print(f"✅ {cert}")
        print(f"Title: {title}")
        print(f"Description: {description}")
        print(f"Tags: {tags}")
        print(f"Images: {images}")
        print()

        entry["created"] = True
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)

if __name__ == "__main__":
    # Step 1: Only run once to pull data
    build_cache()

    # Step 2: Iterate over results
    iterate_cached_results()
