"""
Barcode generation for raw card inventory.

Label size: 62mm x 29mm landscape at 300dpi
New layout prioritises human-readable text at the top, barcode below.

  ┌─────────────────────────────────────┐
  │ Charizard ex              (large)   │
  │ #079/091  •  NM           (medium)  │
  │ ▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌  │
  │ PF-20260316-A3K9X2        (small)   │
  └─────────────────────────────────────┘

No price — price changes, barcode is the lookup key.
"""

import io
import os
import string
import random
from datetime import datetime

import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont


SUFFIX_CHARS = string.digits + string.ascii_uppercase
SUFFIX_CHARS = SUFFIX_CHARS.replace("O", "").replace("I", "").replace("L", "")


def generate_barcode_id(prefix: str = "PF") -> str:
    date_str = datetime.now().strftime("%Y%m%d")
    suffix = "".join(random.choices(SUFFIX_CHARS, k=6))
    return f"{prefix}-{date_str}-{suffix}"


def _load_font(path, size):
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return None


def _best_font(size):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ]:
        f = _load_font(candidate, size)
        if f:
            return f
    return ImageFont.load_default()


def generate_barcode_image(barcode_id: str, *,
                           card_name: str = "",
                           set_name: str = "",
                           condition: str = "",
                           card_number: str = "",
                           price: str = "",        # ignored — kept for backward compat
                           width_mm: float = 62,
                           height_mm: float = 29) -> bytes:
    """
    Generate a barcode label image for thermal printing.
    62mm x 29mm landscape at 300 DPI.

    Layout (top to bottom):
      1. Card name — large, bold, truncated to fit
      2. Card number + condition — medium
      3. Barcode bars
      4. Barcode ID string — small
    """
    dpi = 300
    width_px  = int(width_mm  / 25.4 * dpi)   # 732px
    height_px = int(height_mm / 25.4 * dpi)   # 343px

    PAD = 12   # px padding on left/right

    # ── Fonts ────────────────────────────────────────────────────────────────
    # Scale sizes to fit within height_px
    font_name   = _best_font(52)   # card name — big and readable
    font_detail = _best_font(38)   # card# + condition
    font_code   = _best_font(24)   # barcode ID at bottom

    # ── Canvas ───────────────────────────────────────────────────────────────
    label = Image.new("RGB", (width_px, height_px), "white")
    draw  = ImageDraw.Draw(label)

    y = PAD

    # ── Line 1: Card name ────────────────────────────────────────────────────
    if card_name:
        # Truncate to fit width
        name = card_name
        while name and draw.textlength(name, font=font_name) > (width_px - PAD * 2):
            name = name[:-1]
        if name != card_name:
            name = name[:-1] + "…"
        draw.text((PAD, y), name, fill="black", font=font_name)
        bbox = draw.textbbox((PAD, y), name, font=font_name)
        y = bbox[3] + 6

    # ── Line 2: Card number + condition ──────────────────────────────────────
    parts = []
    if card_number:
        parts.append(f"#{card_number}")
    if condition:
        parts.append(condition)
    if not parts and set_name:
        # Fallback: show set if no number/condition
        parts.append(set_name[:28])
    if parts:
        detail = "  •  ".join(parts)
        draw.text((PAD, y), detail, fill="#333333", font=font_detail)
        bbox = draw.textbbox((PAD, y), detail, font=font_detail)
        y = bbox[3] + 8

    # ── Barcode ───────────────────────────────────────────────────────────────
    code128 = barcode.get("code128", barcode_id, writer=ImageWriter())
    buf = io.BytesIO()
    code128.write(buf, options={
        "module_width":  0.3,
        "module_height": 8.0,
        "font_size":     0,
        "text_distance": 0,
        "quiet_zone":    2.0,
        "write_text":    False,
    })
    buf.seek(0)
    barcode_img = Image.open(buf)

    # Reserve bottom for barcode ID text
    code_h = draw.textbbox((0, 0), barcode_id, font=font_code)[3] + 4
    barcode_h = max(height_px - y - code_h - PAD, 30)
    barcode_w = width_px - PAD * 2

    barcode_resized = barcode_img.resize(
        (barcode_w, barcode_h),
        Image.Resampling.NEAREST
    )
    label.paste(barcode_resized, (PAD, y))
    y += barcode_h + 2

    # ── Barcode ID text ────────────────────────────────────────────────────────
    draw.text((PAD, y), barcode_id, fill="#555555", font=font_code)

    # ── Export ────────────────────────────────────────────────────────────────
    output = io.BytesIO()
    label.save(output, format="PNG", dpi=(dpi, dpi))
    output.seek(0)
    return output.getvalue()


def generate_barcode_batch(cards: list[dict], output_dir: str) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    paths = []
    for card in cards:
        png_bytes = generate_barcode_image(
            card["barcode"],
            card_name=card.get("card_name", ""),
            set_name=card.get("set_name", ""),
            condition=card.get("condition", ""),
            card_number=card.get("card_number", ""),
        )
        path = os.path.join(output_dir, f"{card['barcode']}.png")
        with open(path, "wb") as f:
            f.write(png_bytes)
        paths.append(path)
    return paths
