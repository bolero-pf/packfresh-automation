"""
Generic CSV Parser.
Flexible CSV import that auto-detects columns from common naming patterns.

Required fields (at least one column must match each):
    - Product Name (name, product, item, title, product_name, etc.)
    - Quantity (qty, quantity, count, amount, etc.)
    - Market Price (price, market_price, market, value, cost, etc.)

Optional fields (auto-detected if present):
    - Set Name (set, set_name, expansion, series)
    - Card Number (card_number, number, card_no, collector_number)
    - Rarity (rarity)
    - Condition (condition, card_condition, cond)
    - TCGPlayer ID (tcgplayer_id, tcg_id, tcgplayer)
    - Product Type (type, product_type, category)
"""

import csv
import hashlib
import re
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import NamedTuple


class GenericParsedItem(NamedTuple):
    product_name: str
    product_type: str  # 'sealed' or 'raw'
    set_name: str
    card_number: str
    rarity: str
    condition: str
    quantity: int
    market_price: Decimal  # per-unit
    tcgplayer_id: int | None
    is_graded: bool
    grade_company: str  # 'PSA' / 'BGS' / 'CGC' / 'SGC' or ''
    grade_value: str    # '10', '9.5', etc., or ''


class GenericParseResult(NamedTuple):
    items: list[GenericParsedItem]
    file_hash: str
    total_market_value: Decimal
    raw_count: int
    sealed_count: int
    errors: list[str]
    column_mapping: dict  # what we matched: {"name": "Product Name", "qty": "Quantity", ...}
    unmapped_headers: list[str]  # headers we couldn't map


# ── Column detection patterns ──────────────────────────────────────

COLUMN_PATTERNS = {
    "name": [
        r"^product[_ ]?name$", r"^name$", r"^product$", r"^item$", r"^title$",
        r"^item[_ ]?name$", r"^description$", r"^card[_ ]?name$",
    ],
    "quantity": [
        r"^qty$", r"^quantity$", r"^count$", r"^amount$", r"^num$",
        r"^#$", r"^units$", r"^total[_ ]?quantity$",
    ],
    "price": [
        r"^market[_ ]?price", r"^price$", r"^market$", r"^value$",
        r"^cost$", r"^unit[_ ]?price$", r"^each$", r"^msrp$",
        r"^market[_ ]?value$", r"^retail$",
        r"^tcg[_ ]?market[_ ]?price$", r"^tcg[_ ]?marketplace[_ ]?price$",
        r"^tcg[_ ]?low[_ ]?price$",
    ],
    "set_name": [
        r"^set$", r"^set[_ ]?name$", r"^expansion$", r"^series$",
        r"^collection$",
    ],
    "card_number": [
        r"^card[_ ]?number$", r"^number$", r"^card[_ ]?no$",
        r"^collector[_ ]?number$", r"^card[_ ]?#$",
    ],
    "rarity": [
        r"^rarity$", r"^rare$",
    ],
    "condition": [
        r"^condition$", r"^card[_ ]?condition$", r"^cond$",
    ],
    "grade_company": [
        r"^grade[_ ]?company$", r"^grading[_ ]?company$",
        r"^grading[_ ]?service$", r"^grader$",
    ],
    "grade_value": [
        r"^grade$", r"^grade[_ ]?value$", r"^grade[_ ]?#$",
        r"^grade[_ ]?number$",
    ],
    "tcgplayer_id": [
        r"^tcgplayer[_ ]?id$", r"^tcg[_ ]?id$", r"^tcgplayer$",
        r"^tcg$", r"^tcgid$",
    ],
    "product_type": [
        r"^type$", r"^product[_ ]?type$", r"^category$",
        r"^product[_ ]?line$",
    ],
    "photo_url": [
        r"^photo[_ ]?url$", r"^image[_ ]?url$", r"^image$", r"^photo$",
        r"^picture[_ ]?url$", r"^img$",
    ],
}


def _match_column(header: str, patterns: list[str]) -> bool:
    """Check if a header matches any of the patterns."""
    h = header.strip().lower()
    return any(re.match(p, h) for p in patterns)


def _detect_columns(headers: list[str]) -> tuple[dict, list[str]]:
    """
    Auto-detect which CSV columns map to which fields.
    Returns (mapping, unmapped_headers).
    mapping: {"name": "Product Name", "quantity": "Qty", ...}
    """
    mapping = {}
    used = set()

    for field, patterns in COLUMN_PATTERNS.items():
        for h in headers:
            if h in used:
                continue
            if _match_column(h, patterns):
                mapping[field] = h
                used.add(h)
                break

    unmapped = [h for h in headers if h not in used]
    return mapping, unmapped


def _parse_decimal(val: str) -> Decimal:
    """Parse a price string like '$1,234.56' or '93.87' to Decimal."""
    cleaned = val.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return Decimal("0")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return Decimal("0")


def _extract_product_id_from_url(url: str) -> int | None:
    """
    Extract TCGPlayer product ID from a CDN photo URL.
    e.g. https://tcgplayer-cdn.tcgplayer.com/product/646592_in_200x200.jpg -> 646592
         https://tcgplayer-cdn.tcgplayer.com/product/646592__in__200x200.jpg -> 646592
    """
    m = re.search(r'/product/(\d+)', url)
    if m:
        return int(m.group(1))
    return None


def _parse_int(val: str) -> int:
    """Parse quantity, defaulting to 1."""
    cleaned = val.strip().replace(",", "")
    if not cleaned:
        return 1
    try:
        return max(1, int(float(cleaned)))
    except (ValueError, TypeError):
        return 1


def _normalize_condition(raw: str) -> str:
    """Normalize condition to standard codes."""
    mapping = {
        "near mint": "NM", "nm": "NM", "mint": "NM", "m": "NM",
        "lightly played": "LP", "lp": "LP", "excellent": "LP",
        "moderately played": "MP", "mp": "MP", "good": "MP",
        "heavily played": "HP", "hp": "HP", "played": "HP",
        "damaged": "DMG", "dmg": "DMG", "poor": "DMG",
    }
    return mapping.get(raw.strip().lower(), "NM")


def _guess_type(row: dict, mapping: dict) -> str:
    """Guess if an item is raw or sealed based on available data."""
    # If explicitly provided
    type_col = mapping.get("product_type")
    if type_col:
        val = (row.get(type_col) or "").strip().lower()
        if val in ("raw", "card", "single", "singles"):
            return "raw"
        if val in ("sealed", "box", "pack", "etb", "bundle"):
            return "sealed"

    # If card number + rarity present, it's raw
    # Card numbers can be numeric (26/83), alphanumeric promos (SWSH039, SM102),
    # or special subsets (GG28/GG70, RC8/RC32, TG15/TG30, XY123)
    card_col = mapping.get("card_number")
    rarity_col = mapping.get("rarity")
    if card_col and rarity_col:
        card_num = (row.get(card_col) or "").strip()
        rarity = (row.get(rarity_col) or "").strip()
        if card_num and rarity:
            return "raw"

    return "sealed"


def parse_generic_csv(file_content: str, column_overrides: dict = None) -> GenericParseResult:
    """
    Parse a generic CSV file.

    column_overrides: optional dict to force column mapping, e.g.
        {"name": "Item Description", "quantity": "Qty", "price": "Unit Price"}
    """
    file_hash = hashlib.sha256(file_content.encode("utf-8")).hexdigest()

    # Handle BOM
    if file_content.startswith("\ufeff"):
        file_content = file_content[1:]

    reader = csv.DictReader(StringIO(file_content))
    headers = reader.fieldnames or []

    if not headers:
        return GenericParseResult(
            items=[], file_hash=file_hash, total_market_value=Decimal("0"),
            raw_count=0, sealed_count=0,
            errors=["CSV has no headers"],
            column_mapping={}, unmapped_headers=[],
        )

    # Detect columns (or use overrides)
    mapping, unmapped = _detect_columns(headers)
    if column_overrides:
        mapping.update(column_overrides)
        unmapped = [h for h in headers if h not in mapping.values()]

    # Validate required fields
    missing = []
    if "name" not in mapping:
        missing.append("Product Name")
    if "quantity" not in mapping:
        missing.append("Quantity")
    if "price" not in mapping:
        missing.append("Price")

    if missing:
        return GenericParseResult(
            items=[], file_hash=file_hash, total_market_value=Decimal("0"),
            raw_count=0, sealed_count=0,
            errors=[f"Could not auto-detect required columns: {', '.join(missing)}. "
                    f"Found columns: {', '.join(headers)}. "
                    f"Auto-detected: {mapping}"],
            column_mapping=mapping, unmapped_headers=unmapped,
        )

    items: list[GenericParsedItem] = []
    errors: list[str] = []
    raw_count = 0
    sealed_count = 0
    total_market = Decimal("0")

    name_col = mapping["name"]
    qty_col = mapping["quantity"]
    price_col = mapping["price"]
    set_col = mapping.get("set_name")
    card_col = mapping.get("card_number")
    rarity_col = mapping.get("rarity")
    cond_col = mapping.get("condition")
    tcg_col = mapping.get("tcgplayer_id")
    photo_col = mapping.get("photo_url")
    gc_col = mapping.get("grade_company")
    gv_col = mapping.get("grade_value")
    _GRADERS = {"PSA", "BGS", "CGC", "SGC"}

    for i, row in enumerate(reader, start=2):
        try:
            product_name = (row.get(name_col) or "").strip()
            if not product_name:
                errors.append(f"Row {i}: missing product name, skipped")
                continue

            quantity = _parse_int(row.get(qty_col, "1"))
            market_price = _parse_decimal(row.get(price_col, "0"))
            product_type = _guess_type(row, mapping)

            # Pull grade fields. Either column may carry a combined "PSA 10"
            # string (some CSVs only have one column called "Grade") — split
            # when we see a known grader prefix in either slot.
            gc_raw = (row.get(gc_col) or "").strip().upper() if gc_col else ""
            gv_raw = (row.get(gv_col) or "").strip() if gv_col else ""
            if gv_raw and not gc_raw:
                parts = gv_raw.split(None, 1)
                if len(parts) == 2 and parts[0].upper() in _GRADERS:
                    gc_raw, gv_raw = parts[0].upper(), parts[1].strip()
            if gc_raw and not gv_raw:
                parts = gc_raw.split(None, 1)
                if len(parts) == 2 and parts[0].upper() in _GRADERS:
                    gc_raw, gv_raw = parts[0].upper(), parts[1].strip()
            is_graded = bool(gc_raw and gv_raw and gc_raw in _GRADERS)
            if is_graded:
                product_type = "raw"  # graded slabs are raw items downstream

            # Extract tcgplayer product ID — prefer photo URL extraction over the
            # "TCGplayer Id" column, which is often an internal SKU, not the product ID
            tcgplayer_id = None
            if photo_col and row.get(photo_col):
                tcgplayer_id = _extract_product_id_from_url(row[photo_col])
            if tcgplayer_id is None and tcg_col and row.get(tcg_col):
                try:
                    tcgplayer_id = int(row[tcg_col].strip())
                except (ValueError, TypeError):
                    pass

            if product_type == "raw":
                raw_count += 1
            else:
                sealed_count += 1

            total_market += market_price * quantity

            item = GenericParsedItem(
                product_name=product_name,
                product_type=product_type,
                set_name=(row.get(set_col) or "").strip() if set_col else "",
                card_number=(row.get(card_col) or "").strip() if card_col else "",
                rarity=(row.get(rarity_col) or "").strip() if rarity_col else "",
                condition=_normalize_condition(row.get(cond_col, "NM")) if cond_col else "NM",
                quantity=quantity,
                market_price=market_price,
                tcgplayer_id=tcgplayer_id,
                is_graded=is_graded,
                grade_company=gc_raw if is_graded else "",
                grade_value=gv_raw if is_graded else "",
            )
            items.append(item)

        except Exception as e:
            errors.append(f"Row {i}: {e}")

    return GenericParseResult(
        items=items,
        file_hash=file_hash,
        total_market_value=total_market,
        raw_count=raw_count,
        sealed_count=sealed_count,
        errors=errors,
        column_mapping=mapping,
        unmapped_headers=unmapped,
    )


def detect_csv_columns(file_content: str) -> dict:
    """
    Preview a CSV: return headers, auto-detected mapping, and first few rows.
    Used by the frontend to show a mapping UI before committing the import.
    """
    if file_content.startswith("\ufeff"):
        file_content = file_content[1:]

    reader = csv.DictReader(StringIO(file_content))
    headers = reader.fieldnames or []
    mapping, unmapped = _detect_columns(headers)

    # Grab first 5 rows as preview
    preview = []
    for i, row in enumerate(reader):
        if i >= 5:
            break
        preview.append(dict(row))

    return {
        "headers": headers,
        "mapping": mapping,
        "unmapped": unmapped,
        "preview_rows": preview,
        "required_fields": ["name", "quantity", "price"],
        "optional_fields": ["set_name", "card_number", "rarity", "condition",
                            "grade_company", "grade_value",
                            "tcgplayer_id", "product_type", "photo_url"],
    }
