"""
Collectr HTML Parser — extracts raw card and sealed product data from Collectr portfolio HTML.

When you can't export a CSV from Collectr but can view the portfolio page,
copy the HTML list (the <ul class="contents ..."> block) and paste it here.

Detection logic (same as CSV parser):
    - If card_number looks like a card number AND rarity is populated → raw card
    - Otherwise → sealed product

Each <li> block contains:
    - Product name: bold span with line-clamp-2
    - Set name: underline text-muted-foreground span
    - Rarity + card number: two <span> inside flex-col text-muted-foreground div
    - Condition: font-medium span with inline color style (Near Mint, etc.)
    - Variant: trailing text-muted-foreground p (Holofoil, Reverse Holofoil, etc.)
    - Market price: bold leading-tight span with $ prefix
    - Quantity: "Qty: N" text
    - Price change: muted/red spans with +/- prefix
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
    product_type: str       # 'raw' or 'sealed'
    set_name: str = ""
    card_number: str = ""
    rarity: str = ""
    condition: str = "NM"
    variance: str = ""
    quantity: int = 1
    market_price: Decimal = Decimal("0")
    price_change: Optional[Decimal] = None
    price_change_pct: Optional[float] = None
    is_graded: bool = False
    grade_company: str = ""
    grade_value: str = ""


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


# Set name normalisation — Collectr uses full names, PPT expects abbreviations
_SET_NAME_MAP = {
    "sun & moon promo": "SM Promo",
    "sun & moon promos": "SM Promo",
    "sun and moon promo": "SM Promo",
    "sun and moon promos": "SM Promo",
    "xy promo": "XY Promo",
    "xy promos": "XY Promo",
    "mega evolution": "ME Promo",
    "mega evolution promo": "ME Promo",
    "mega evolution promos": "ME Promo",
    "sword & shield promo": "SWSH Promo",
    "sword & shield promos": "SWSH Promo",
    "sword and shield promo": "SWSH Promo",
    "sword and shield promos": "SWSH Promo",
    "scarlet & violet promo": "SV Promo",
    "scarlet & violet promos": "SV Promo",
    "scarlet and violet promo": "SV Promo",
    "scarlet and violet promos": "SV Promo",
    "black & white promo": "BW Promo",
    "black & white promos": "BW Promo",
    "black and white promo": "BW Promo",
    "black and white promos": "BW Promo",
    "heartgold & soulsilver promo": "HGSS Promo",
    "heartgold & soulsilver promos": "HGSS Promo",
    "diamond & pearl promo": "DP Promo",
    "diamond & pearl promos": "DP Promo",
    "scarlet & violet base set": "Scarlet & Violet",
    "sv: 151": "151",
    "crown zenith: galarian gallery": "Crown Zenith Galarian Gallery",
    "brilliant stars trainer gallery": "Brilliant Stars Trainer Gallery",
    "astral radiance trainer gallery": "Astral Radiance Trainer Gallery",
    "lost origin trainer gallery": "Lost Origin Trainer Gallery",
    "silver tempest trainer gallery": "Silver Tempest Trainer Gallery",
    "alternate art promos": "Alt Art Promo",
    "wizards of the coast promo": "WoTC Promo",
    "wotc promo": "WoTC Promo",
    "wotc promos": "WoTC Promo",
    "team rocket (japanese)": "Team Rocket Japanese",
}


def _normalize_set_name(raw: str) -> str:
    """Normalize Collectr set names to PPT-friendly equivalents."""
    mapped = _SET_NAME_MAP.get(raw.strip().lower())
    return mapped if mapped else raw.strip()


# Condition normalisation (same mapping as CSV parser)
_CONDITION_MAP = {
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


def _normalize_condition(raw: str) -> str:
    return _CONDITION_MAP.get(raw.strip().lower(), "NM")


def _is_card_number(val: str) -> bool:
    """Return True if the string looks like a Pokemon card number.
    Handles: 4, 107, 79/73, 096/182, SV13/SV94, SVP 200, SWSH 262,
             TG16/TG30, 55a, 11/108, PROMO, etc.
    """
    v = val.strip()
    if not v:
        return False
    # Pure number or number/number (with optional leading zeros)
    if re.match(r"^\d+(/\d+)?$", v):
        return True
    # Alphanumeric (with optional spaces) and optional slash:
    # SVP 200, SWSH001, SV13/SV94, RC01/RC32, TG16/TG30, 55a, etc.
    if re.match(r"^[A-Z0-9]+(\s+[A-Z0-9]+)?(/[A-Z0-9]+(\s+[A-Z0-9]+)?)?$", v, re.IGNORECASE):
        return True
    return False


def _clean(text: str) -> str:
    """Strip nested tags, HTML entities, and whitespace."""
    text = re.sub(r"<[^>]+>", "", text)
    for ent, ch in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                    ("&#39;", "'"), ("&quot;", '"'), ("&nbsp;", " ")]:
        text = text.replace(ent, ch)
    return text.strip()


def parse_collectr_html(html_content: str) -> CollectrHTMLResult:
    """
    Parse Collectr portfolio HTML into structured items.

    Accepts either the full page or just the <ul class="contents ..."> block.
    """
    result = CollectrHTMLResult()
    result.file_hash = hashlib.md5(html_content.encode("utf-8")).hexdigest()

    # Split on <li — each item is one card/product
    li_blocks = re.findall(r"<li[^>]*>.*?</li>", html_content, re.DOTALL)

    if not li_blocks:
        result.errors.append(
            "No <li> items found in HTML. "
            "Make sure you copied the full <ul class=\"contents ...\"> block."
        )
        return result

    for i, block in enumerate(li_blocks):
        try:
            item = _parse_li_block(block, i)
            if item:
                result.items.append(item)
                result.total_market_value += item.market_price * item.quantity
                if item.product_type == "raw":
                    result.raw_count += 1
                else:
                    result.sealed_count += 1
        except Exception as e:
            result.errors.append(f"Item {i + 1}: {e}")

    return result


def _parse_li_block(html: str, index: int) -> Optional[CollectrHTMLItem]:
    """Parse one <li> into a CollectrHTMLItem, or return None to skip."""

    # ── Product name ────────────────────────────────────────────────
    name_m = re.search(
        r"font-bold\s+line-clamp-2[^>]*>(.*?)</span>", html, re.DOTALL
    )
    if not name_m:
        return None
    product_name = _clean(name_m.group(1))
    if not product_name:
        return None

    # ── Set name ────────────────────────────────────────────────────
    set_m = re.search(
        r'underline\s+text-muted-foreground["\s][^>]*>(.*?)</span>', html, re.DOTALL
    )
    set_name = _normalize_set_name(_clean(set_m.group(1))) if set_m else ""

    # ── Rarity + card number ─────────────────────────────────────────
    # Current Collectr structure (flex-row with bullet separators):
    #   <div class="flex flex-row flex-wrap items-center space-x-1 text-muted-foreground sm:text-sm text-xs">
    #       <span>Special Illustration Rare</span><span class="text-xs">•</span><span>199/165</span>
    #   </div>
    # Older structure (flex-col without bullets):
    #   <div class="flex flex-col text-xs sm:text-sm text-muted-foreground">
    #       <span>Holo Rare</span><span>9</span>
    #   </div>
    rarity = ""
    card_number = ""
    muted_block_m = re.search(
        r"flex\s+flex-(?:row|col)[^>]*text-muted-foreground[^>]*>(.*?)</div>",
        html, re.DOTALL
    )
    if not muted_block_m:
        # Try alternate class order (text-muted-foreground before flex)
        muted_block_m = re.search(
            r"text-muted-foreground[^>]*flex\s+flex-(?:row|col)[^>]*>(.*?)</div>",
            html, re.DOTALL
        )
    if muted_block_m:
        spans = re.findall(r"<span[^>]*>(.*?)</span>", muted_block_m.group(1), re.DOTALL)
        # Filter out bullet separators and empty spans
        cleaned = [_clean(s) for s in spans if _clean(s) and _clean(s) not in ("•", "·", "|", "-")]
        if len(cleaned) >= 2:
            rarity = cleaned[0]
            card_number = cleaned[1]
        elif len(cleaned) == 1:
            # Could be rarity-only or card-number-only
            if _is_card_number(cleaned[0]):
                card_number = cleaned[0]
            else:
                rarity = cleaned[0]

    # ── Condition ───────────────────────────────────────────────────
    # <span class="font-medium text-xs sm:text-sm" style="color: rgb(...);">Near Mint</span>
    cond_m = re.search(
        r'font-medium[^>]+style="color:[^"]*"[^>]*>(.*?)</span>', html
    )
    condition = _normalize_condition(_clean(cond_m.group(1))) if cond_m else "NM"

    # ── Variant / printing ───────────────────────────────────────────
    # <p class="text-xs sm:text-sm text-muted-foreground ...">Holofoil</p>
    variant_m = re.search(
        r'<p[^>]+text-muted-foreground[^>]*truncate[^>]*>(.*?)</p>', html, re.DOTALL
    )
    variance = _clean(variant_m.group(1)) if variant_m else ""

    # ── Market price ─────────────────────────────────────────────────
    price_m = re.search(
        r"font-bold\s+leading-tight[^>]*>\$?([\d,]+\.?\d*)</span>", html
    )
    market_price = Decimal("0")
    if price_m:
        try:
            market_price = Decimal(price_m.group(1).replace(",", ""))
        except InvalidOperation:
            pass

    # ── Quantity ─────────────────────────────────────────────────────
    qty_m = re.search(r"Qty:\s*(\d+)", html)
    quantity = int(qty_m.group(1)) if qty_m else 1

    # ── Price change ─────────────────────────────────────────────────
    price_change = None
    price_change_pct = None
    neg_m = re.search(r"text-red-\d+[^>]*>-\$?([\d,]+\.?\d*)</span>", html)
    if neg_m:
        try:
            price_change = -Decimal(neg_m.group(1).replace(",", ""))
        except InvalidOperation:
            pass
    else:
        pos_m = re.search(r"text-muted-foreground[^>]*>\+\$?([\d,]+\.?\d*)</span>", html)
        if pos_m:
            try:
                price_change = Decimal(pos_m.group(1).replace(",", ""))
            except InvalidOperation:
                pass
    pct_m = re.search(r"\((-?[\d.]+)%\)", html)
    if pct_m:
        try:
            price_change_pct = float(pct_m.group(1))
        except ValueError:
            pass

    # ── Determine raw vs sealed ──────────────────────────────────────
    product_type = "raw" if (card_number and _is_card_number(card_number) and rarity) else "sealed"

    return CollectrHTMLItem(
        product_name=product_name,
        product_type=product_type,
        set_name=set_name,
        card_number=card_number if product_type == "raw" else "",
        rarity=rarity if product_type == "raw" else "",
        condition=condition,
        variance=variance,
        quantity=quantity,
        market_price=market_price,
        price_change=price_change,
        price_change_pct=price_change_pct,
    )
