"""
Barcode generation for raw card inventory.

Generates Code 128 barcodes compatible with:
    - Brother QL label printers (via brother_ql or direct image printing)
    - Any standard USB barcode scanner
    - Standard adhesive label stock (29mm x 90mm or similar)

Barcode format: PF-YYYYMMDD-XXXXXX
    PF = Pack Fresh (brand prefix)
    YYYYMMDD = date card entered system
    XXXXXX = 6-char unique suffix (base36 for density)

Dependencies: python-barcode, Pillow
    pip install python-barcode Pillow
"""

import io
import os
import string
import random
from datetime import datetime
from typing import Optional

import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont


# Characters for random suffix (alphanumeric, uppercase, no confusing chars)
SUFFIX_CHARS = string.digits + string.ascii_uppercase
# Remove confusing characters: 0/O, 1/I/L
SUFFIX_CHARS = SUFFIX_CHARS.replace("O", "").replace("I", "").replace("L", "")


def generate_barcode_id(prefix: str = "PF") -> str:
    """
    Generate a unique barcode string.
    Format: PF-YYYYMMDD-XXXXXX
    
    Collision probability: ~30 chars ^ 6 positions = ~729M combinations per day.
    For a card store doing < 10k cards/day, this is effectively zero risk.
    """
    date_str = datetime.now().strftime("%Y%m%d")
    suffix = "".join(random.choices(SUFFIX_CHARS, k=6))
    return f"{prefix}-{date_str}-{suffix}"


def generate_barcode_image(barcode_id: str, *,
                           card_name: str = "",
                           set_name: str = "",
                           condition: str = "",
                           price: str = "",
                           width_mm: float = 62,
                           height_mm: float = 29) -> bytes:
    """
    Generate a barcode label image suitable for thermal printing.
    
    Returns PNG bytes.
    
    Label layout (62mm x 29mm, landscape):
    ┌──────────────────────────────────┐
    │ Card Name (truncated)            │
    │ Set Name • Condition             │
    │ ║║║║║║║║║║║║║║║║║║║║║║║║║║║║║║  │
    │ PF-20260207-A3K9X2    $125.50   │
    └──────────────────────────────────┘
    """
    # DPI for thermal printers (300 is standard for Brother QL)
    dpi = 300
    width_px = int(width_mm / 25.4 * dpi)
    height_px = int(height_mm / 25.4 * dpi)

    # Generate Code 128 barcode as image
    code128 = barcode.get("code128", barcode_id, writer=ImageWriter())
    
    # Write barcode to bytes buffer
    buf = io.BytesIO()
    code128.write(buf, options={
        "module_width": 0.25,     # narrow bar width in mm
        "module_height": 6.0,     # bar height in mm
        "font_size": 0,           # we'll add our own text
        "text_distance": 0,
        "quiet_zone": 2.0,
        "write_text": False,
    })
    buf.seek(0)
    barcode_img = Image.open(buf)

    # Create label canvas
    label = Image.new("RGB", (width_px, height_px), "white")
    draw = ImageDraw.Draw(label)

    # Try to load a decent font, fall back to default
    font_path = None
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]:
        if os.path.exists(candidate):
            font_path = candidate
            break

    if font_path:
        font_title = ImageFont.truetype(font_path, 28)
        font_detail = ImageFont.truetype(font_path, 22)
        font_barcode = ImageFont.truetype(font_path, 20)
    else:
        font_title = ImageFont.load_default()
        font_detail = font_title
        font_barcode = font_title

    padding = 15
    y_cursor = padding

    # Card name (truncated to fit)
    if card_name:
        max_chars = int(width_px / 16)  # rough estimate
        display_name = card_name[:max_chars] + ("…" if len(card_name) > max_chars else "")
        draw.text((padding, y_cursor), display_name, fill="black", font=font_title)
        y_cursor += 34

    # Set + condition line
    detail_parts = []
    if set_name:
        detail_parts.append(set_name[:30])
    if condition:
        detail_parts.append(condition)
    if detail_parts:
        draw.text((padding, y_cursor), " • ".join(detail_parts), fill="#444444", font=font_detail)
        y_cursor += 28

    # Resize and paste barcode
    barcode_target_width = width_px - (padding * 2)
    barcode_target_height = height_px - y_cursor - 35  # leave room for text below
    barcode_target_height = max(barcode_target_height, 40)

    barcode_resized = barcode_img.resize(
        (barcode_target_width, barcode_target_height),
        Image.Resampling.NEAREST  # keep crisp bars
    )
    label.paste(barcode_resized, (padding, y_cursor))
    y_cursor += barcode_target_height + 2

    # Barcode ID + price at bottom
    draw.text((padding, y_cursor), barcode_id, fill="black", font=font_barcode)
    if price:
        # Right-align price
        price_bbox = draw.textbbox((0, 0), price, font=font_barcode)
        price_width = price_bbox[2] - price_bbox[0]
        draw.text((width_px - padding - price_width, y_cursor), price, fill="black", font=font_barcode)

    # Export as PNG
    output = io.BytesIO()
    label.save(output, format="PNG", dpi=(dpi, dpi))
    output.seek(0)
    return output.getvalue()


def generate_barcode_batch(cards: list[dict], output_dir: str) -> list[str]:
    """
    Generate barcode label images for a batch of cards.
    
    cards: list of dicts with keys: barcode, card_name, set_name, condition, current_price
    output_dir: directory to save PNG files
    
    Returns list of file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    paths = []

    for card in cards:
        png_bytes = generate_barcode_image(
            card["barcode"],
            card_name=card.get("card_name", ""),
            set_name=card.get("set_name", ""),
            condition=card.get("condition", ""),
            price=f"${card['current_price']:.2f}" if card.get("current_price") else "",
        )
        path = os.path.join(output_dir, f"{card['barcode']}.png")
        with open(path, "wb") as f:
            f.write(png_bytes)
        paths.append(path)

    return paths
