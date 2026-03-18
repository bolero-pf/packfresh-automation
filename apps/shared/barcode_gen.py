"""
Barcode generation for raw card inventory.
Label: 89mm x 28mm at 300 DPI = 1051 x 331 px (landscape)

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
                           width_mm: float = 89,
                           height_mm: float = 28) -> bytes:
    """
    89mm x 28mm landscape at 300 DPI.

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
    width_px  = int(width_mm  / 25.4 * dpi)   # 1051px for 89mm
    height_px = int(height_mm / 25.4 * dpi)   # 331px for 28mm

    PAD = 10

    # 28mm tall is a strip — tight layout
    # Text (name + detail) across the top ~35%, barcode ~50%, ID text ~15%
    text_zone_h    = int(height_px * 0.35)   # ~116px
    barcode_zone_h = int(height_px * 0.50)   # ~166px
    code_zone_h    = height_px - text_zone_h - barcode_zone_h

    # At 300dpi: 1pt physical = ~11.8px
    # name_size 46px ≈ 3.9pt physical — readable on 28mm label
    # detail_size 34px ≈ 2.9pt physical
    name_size   = int(text_zone_h * 0.40)    # ~46px
    detail_size = int(text_zone_h * 0.28)    # ~32px
    code_size   = max(int(code_zone_h * 0.55), 18)

    font_name   = _font(name_size)
    font_detail = _font(detail_size)
    font_code   = _font(max(code_size, 18))

    label = Image.new("RGB", (width_px, height_px), "white")
    draw  = ImageDraw.Draw(label)

    # ── Text zone ─────────────────────────────────────────────────────────────
    y = PAD

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
    parts = []
    if card_number:
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
