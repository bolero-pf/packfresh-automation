"""
Collectr HTML Parser — extracts sealed product data from Collectr portfolio HTML.

When you can't export a CSV from Collectr but can view the portfolio page,
copy the HTML list (the <ul class="contents ..."> block) and paste it here.

Produces the same item structure as collectr_parser.parse_collectr_csv()
so the intake pipeline can handle both identically.
"""

import hashlib
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass
class CollectrHTMLItem:
    """A single item parsed from Collectr HTML."""
    product_name: str
    product_type: str  # always "sealed" for Collectr HTML (portfolio view)
    set_name: str = ""
    card_number: str = ""
    condition: str = "NM"
    rarity: str = ""
    quantity: int = 1
    market_price: Decimal = Decimal("0")
    price_change: Optional[Decimal] = None
    price_change_pct: Optional[float] = None


@dataclass
class CollectrHTMLResult:
    """Result of parsing Collectr HTML."""
    items: list[CollectrHTMLItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    file_hash: str = ""
    portfolio_name: str = ""
    total_market_value: Decimal = Decimal("0")
    raw_count: int = 0
    sealed_count: int = 0


def parse_collectr_html(html_content: str) -> CollectrHTMLResult:
    """
    Parse Collectr portfolio HTML into structured items.

    Expects the HTML from the portfolio list view — either the full page
    or just the <ul class="contents"> block with <li> items.

    Each <li> contains:
        - Product name (bold span with line-clamp-2)
        - Set name (underline text-muted-foreground span)
        - Market price (bold leading-tight span with $ prefix)
        - Price change amount and percentage (red/green spans)
        - Quantity (text matching "Qty: N")
    """
    result = CollectrHTMLResult()
    result.file_hash = hashlib.md5(html_content.encode("utf-8")).hexdigest()

    # Split into individual <li> items
    li_blocks = re.findall(r'<li[^>]*>.*?</li>', html_content, re.DOTALL)

    if not li_blocks:
        result.errors.append("No <li> items found in HTML. Make sure you copied the product list.")
        return result

    for i, block in enumerate(li_blocks):
        try:
            item = _parse_li_block(block, i)
            if item:
                result.items.append(item)
                result.sealed_count += 1
                result.total_market_value += item.market_price * item.quantity
        except Exception as e:
            result.errors.append(f"Item {i+1}: {str(e)}")

    return result


def _parse_li_block(html: str, index: int) -> Optional[CollectrHTMLItem]:
    """Parse a single <li> block into a CollectrHTMLItem."""

    # ── Product name: first bold span with line-clamp-2 ──
    name_match = re.search(
        r'font-bold\s+line-clamp-2[^>]*>(.*?)</span>',
        html, re.DOTALL
    )
    if not name_match:
        return None  # skip items we can't identify

    product_name = _clean_text(name_match.group(1))
    if not product_name:
        return None

    # ── Set name: underline muted span ──
    set_match = re.search(
        r'underline\s+text-muted-foreground["\s][^>]*>(.*?)</span>',
        html, re.DOTALL
    )
    set_name = _clean_text(set_match.group(1)) if set_match else ""

    # ── Market price: bold leading-tight span with $ ──
    price_match = re.search(
        r'font-bold\s+leading-tight[^>]*>\$?([\d,]+\.?\d*)</span>',
        html
    )
    market_price = Decimal("0")
    if price_match:
        try:
            market_price = Decimal(price_match.group(1).replace(",", ""))
        except InvalidOperation:
            pass

    # ── Quantity ──
    qty_match = re.search(r'Qty:\s*(\d+)', html)
    quantity = int(qty_match.group(1)) if qty_match else 1

    # ── Price change (informational, not used in intake calc) ──
    price_change = None
    price_change_pct = None

    # Negative change: red text with -$
    neg_change = re.search(r'text-red-\d+[^>]*>-\$?([\d,]+\.?\d*)</span>', html)
    if neg_change:
        try:
            price_change = -Decimal(neg_change.group(1).replace(",", ""))
        except InvalidOperation:
            pass
    else:
        # Positive change: muted text with +$
        pos_change = re.search(r'text-muted-foreground[^>]*>\+\$?([\d,]+\.?\d*)</span>', html)
        if pos_change:
            try:
                price_change = Decimal(pos_change.group(1).replace(",", ""))
            except InvalidOperation:
                pass

    # Percentage
    pct_match = re.search(r'\((-?[\d.]+)%\)', html)
    if pct_match:
        try:
            price_change_pct = float(pct_match.group(1))
        except ValueError:
            pass

    return CollectrHTMLItem(
        product_name=product_name,
        product_type="sealed",
        set_name=set_name,
        quantity=quantity,
        market_price=market_price,
        price_change=price_change,
        price_change_pct=price_change_pct,
    )


def _clean_text(text: str) -> str:
    """Strip HTML entities and whitespace from extracted text."""
    text = re.sub(r'<[^>]+>', '', text)  # remove any nested tags
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&#39;", "'").replace("&quot;", '"')
    return text.strip()
