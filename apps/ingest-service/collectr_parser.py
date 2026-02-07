"""
Collectr CSV Parser.
Handles actual Collectr export format with correct column names.

Actual Collectr columns (from real export):
    Portfolio Name, Category, Set, Product Name, Card Number, Rarity,
    Variance, Grade, Card Condition, Average Cost Paid, Quantity,
    Market Price (As of YYYY-MM-DD), Price Override, Watchlist, Date Added, Notes

Detection logic:
    - If Card Number AND Rarity are populated -> raw card
    - Otherwise -> sealed product
    
Note: Market Price column name includes a date that changes per export.
"""

import csv
import hashlib
import re
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import NamedTuple


class ParsedItem(NamedTuple):
    product_name: str
    product_type: str  # 'sealed' or 'raw'
    set_name: str
    card_number: str  # empty for sealed
    rarity: str       # empty for sealed
    condition: str    # NM, LP, MP, HP, DMG
    variance: str     # Normal, Reverse Holo, etc.
    grade: str        # Ungraded, PSA 10, etc.
    quantity: int
    market_price: Decimal  # per-unit price from Collectr
    portfolio_name: str    # customer portfolio name in Collectr


class ParseResult(NamedTuple):
    items: list[ParsedItem]
    file_hash: str
    portfolio_name: str       # first non-empty portfolio name found
    total_market_value: Decimal  # sum of (market_price * quantity) 
    raw_count: int
    sealed_count: int
    errors: list[str]


def _find_market_price_column(headers: list[str]) -> str | None:
    """
    Find the market price column which includes a dynamic date.
    Looks for 'Market Price (As of YYYY-MM-DD)' or similar patterns.
    """
    for h in headers:
        if h.lower().startswith("market price"):
            return h
    return None


def _normalize_condition(raw: str) -> str:
    """Normalize condition to our standard codes."""
    mapping = {
        "near mint": "NM",
        "nm": "NM",
        "lightly played": "LP",
        "lp": "LP",
        "moderately played": "MP",
        "mp": "MP",
        "heavily played": "HP",
        "hp": "HP",
        "damaged": "DMG",
        "dmg": "DMG",
    }
    return mapping.get(raw.strip().lower(), "NM")


def _parse_decimal(val: str) -> Decimal:
    """Parse a price string like '$1,234.56' or '93.87' to Decimal."""
    cleaned = val.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return Decimal("0")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return Decimal("0")


def _is_raw_card(row: dict) -> bool:
    """
    Determine if a row represents a raw card vs sealed product.
    A row is raw if it has a meaningful Card Number AND Rarity.
    
    Note: Some sealed blisters have bracket text that might look like card data
    but won't have actual card numbers.
    """
    card_number = (row.get("Card Number") or "").strip()
    rarity = (row.get("Rarity") or "").strip()
    
    # Must have both a non-empty card number and rarity to be considered raw
    if card_number and rarity:
        # Additional check: card number should look like a card number (digits, possibly with /)
        if re.match(r"^[\d]+(/[\d]+)?$", card_number):
            return True
    return False


def parse_collectr_csv(file_content: str) -> ParseResult:
    """
    Parse a Collectr CSV export.
    
    Returns a ParseResult with all items, stats, and any parsing errors.
    """
    file_hash = hashlib.sha256(file_content.encode("utf-8")).hexdigest()

    # Handle BOM
    if file_content.startswith("\ufeff"):
        file_content = file_content[1:]

    reader = csv.DictReader(StringIO(file_content))
    headers = reader.fieldnames or []

    # Find the market price column (dynamic name with date)
    market_price_col = _find_market_price_column(headers)
    if not market_price_col:
        return ParseResult(
            items=[], file_hash=file_hash, portfolio_name="",
            total_market_value=Decimal("0"), raw_count=0, sealed_count=0,
            errors=["Could not find 'Market Price' column in CSV headers. "
                    f"Found columns: {', '.join(headers)}"]
        )

    items: list[ParsedItem] = []
    errors: list[str] = []
    portfolio_name = ""
    raw_count = 0
    sealed_count = 0
    total_market = Decimal("0")

    for i, row in enumerate(reader, start=2):  # start=2 because row 1 is headers
        try:
            product_name = (row.get("Product Name") or "").strip()
            if not product_name:
                errors.append(f"Row {i}: missing Product Name, skipped")
                continue

            quantity = int(row.get("Quantity") or "1")
            if quantity <= 0:
                errors.append(f"Row {i}: quantity is {quantity}, skipped")
                continue

            market_price = _parse_decimal(row.get(market_price_col, "0"))
            is_raw = _is_raw_card(row)

            if not portfolio_name:
                portfolio_name = (row.get("Portfolio Name") or "").strip()

            product_type = "raw" if is_raw else "sealed"
            if is_raw:
                raw_count += 1
            else:
                sealed_count += 1

            total_market += market_price * quantity

            item = ParsedItem(
                product_name=product_name,
                product_type=product_type,
                set_name=(row.get("Set") or "").strip(),
                card_number=(row.get("Card Number") or "").strip() if is_raw else "",
                rarity=(row.get("Rarity") or "").strip() if is_raw else "",
                condition=_normalize_condition(row.get("Card Condition") or "Near Mint"),
                variance=(row.get("Variance") or "Normal").strip(),
                grade=(row.get("Grade") or "Ungraded").strip(),
                quantity=quantity,
                market_price=market_price,
                portfolio_name=portfolio_name,
            )
            items.append(item)

        except Exception as e:
            errors.append(f"Row {i}: {e}")

    return ParseResult(
        items=items,
        file_hash=file_hash,
        portfolio_name=portfolio_name,
        total_market_value=total_market,
        raw_count=raw_count,
        sealed_count=sealed_count,
        errors=errors,
    )
