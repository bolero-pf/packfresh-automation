"""
Barcode generation for raw card inventory.
Label: 62mm x 50mm at 300 DPI = 732 x 591 px

Requires fonts-dejavu-core installed in the container:
    apt-get install -y fonts-dejavu-core
"""

import io
import os
import string
import random
import logging
from datetime import datetime

import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

SUFFIX_CHARS = string.digits + string.ascii_uppercase
SUFFIX_CHARS = SUFFIX_CHARS.replace("O", "").replace("I", "").replace("L", "")

FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans-Bold.ttf",
]


def _find_font_path():
    for p in FONT_CANDIDATES:
        if os.path.exists(p):
            logger.info(f"barcode_gen: using font {p}")
            return p
    logger.warning("barcode_gen: no TTF font found — text will be tiny. "
                   "Install fonts-dejavu-core in the container.")
    return None


_FONT_PATH = _find_font_path()


def _font(size):
    if _FONT_PATH:
        try:
            return ImageFont.truetype(_FONT_PATH, size)
        except Exception:
            pass
    return ImageFont.load_default()


def generate_barcode_id(prefix: str = "PF") -> str:
    date_str = datetime.now().strftime("%Y%m%d")
    suffix = "".join(random.choices(SUFFIX_CHARS, k=6))
    return f"{prefix}-{date_str}-{suffix}"


def generate_barcode_image(barcode_id: str, *,
                           card_name: str = "",
                           set_name: str = "",
                           condition: str = "",
                           card_number: str = "",
                           price: str = "",        # ignored
                           width_mm: float = 62,
                           height_mm: float = 50) -> bytes:
    """
    62mm x 50mm at 300 DPI.

    Fixed layout — text zone gets top 40%, barcode gets bottom 60%.
    This prevents the barcode from swallowing everything when fonts are large.

      ┌──────────────────────────────────┐
      │ Charizard ex           (big)     │  ~top 40%
      │ #079/091  •  NM        (medium)  │
      ├──────────────────────────────────┤
      │ ▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌▌  │  ~bottom 55%
      │ PF-20260316-A3K9X2     (small)  │
      └──────────────────────────────────┘
    """
    dpi       = 300
    width_px  = int(width_mm  / 25.4 * dpi)   # 732
    height_px = int(height_mm / 25.4 * dpi)   # 591

    PAD = 14

    # Fixed zone heights
    text_zone_h    = int(height_px * 0.38)   # ~225px for name + detail
    barcode_zone_h = int(height_px * 0.48)   # ~284px for bars
    code_zone_h    = height_px - text_zone_h - barcode_zone_h  # remainder for ID text

    # Font sizes — these are pixels, scaled to fit text_zone_h
    name_size   = int(text_zone_h * 0.20)    # ~45px ≈ 11pt physical
    detail_size = int(text_zone_h * 0.15)    # ~34px ≈ 8pt physical
    code_size   = int(code_zone_h * 0.65)    # small ID text

    font_name   = _font(name_size)
    font_detail = _font(detail_size)
    font_code   = _font(max(code_size, 18))

    label = Image.new("RGB", (width_px, height_px), "white")
    draw  = ImageDraw.Draw(label)

    # ── Text zone ─────────────────────────────────────────────────────────────
    y = PAD

    # Card name — strip trailing set code suffixes like "- SWSH138", "- BW001" etc.
    # These come from Collectr export format and are redundant with card_number field
    import re as _re
    if card_name:
        card_name = _re.sub(r'\s*-\s*[A-Z]{1,4}\d{1,4}[A-Z]?$', '', card_name).strip()

    # Card name — truncate to fit width
    if card_name:
        name = card_name
        while name and draw.textlength(name, font=font_name) > (width_px - PAD * 2):
            name = name[:-1]
        if name != card_name:
            name = name[:-1] + "…"
        draw.text((PAD, y), name, fill="black", font=font_name)
        y += name_size + 6

    # Card number + condition
    # Skip card_number if it looks like a Collectr set code (e.g. SWSH138, BW001)
    # rather than a real card number (e.g. 004/125, RC05)
    import re as _re2
    _is_set_code = card_number and bool(_re2.match(r'^[A-Z]{2,4}\d{1,4}[A-Z]?$', card_number.strip()))
    parts = []
    if card_number and not _is_set_code:
        parts.append(f"#{card_number}")
    if condition:
        parts.append(condition)
    if not parts and set_name:
        parts.append(set_name[:25])
    if parts:
        detail = "  •  ".join(parts)
        draw.text((PAD, y), detail, fill="#222222", font=font_detail)

    # ── Dividing line ─────────────────────────────────────────────────────────
    div_y = text_zone_h
    draw.line([(PAD, div_y), (width_px - PAD, div_y)], fill="#cccccc", width=1)

    # ── Barcode zone ──────────────────────────────────────────────────────────
    code128 = barcode.get("code128", barcode_id, writer=ImageWriter())
    buf = io.BytesIO()
    code128.write(buf, options={
        "module_width":  0.28,
        "module_height": 10.0,
        "font_size":     0,
        "text_distance": 0,
        "quiet_zone":    1.5,
        "write_text":    False,
    })
    buf.seek(0)
    barcode_img = Image.open(buf)

    bc_y = div_y + 4
    bc_h = barcode_zone_h - 8
    bc_w = width_px - PAD * 2
    barcode_resized = barcode_img.resize((bc_w, bc_h), Image.Resampling.NEAREST)
    label.paste(barcode_resized, (PAD, bc_y))

    # ── Barcode ID text ────────────────────────────────────────────────────────
    id_y = div_y + barcode_zone_h + 2
    draw.text((PAD, id_y), barcode_id, fill="#555555", font=font_code)

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
