"""
Barcode generation for raw card inventory.
Label: 51mm x 19mm at 300 DPI = 602 x 224 px (landscape)

Requires fonts-dejavu-core installed in the container:
    apt-get install -y fonts-dejavu-core
"""

import io
import os
import string
import random
import logging

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
    suffix = "".join(random.choices(SUFFIX_CHARS, k=6))
    return f"{prefix}-{suffix}"


def generate_barcode_image(barcode_id: str, *,
                           card_name: str = "",
                           set_name: str = "",
                           condition: str = "",
                           card_number: str = "",
                           price: str = "",        # ignored
                           width_mm: float = 51,
                           height_mm: float = 19) -> bytes:
    """
    51mm x 19mm landscape at 300 DPI (~2" x 0.75").

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
    width_px  = int(width_mm  / 25.4 * dpi)   # 602px for 51mm
    height_px = int(height_mm / 25.4 * dpi)   # 224px for 19mm

    V_PAD       = 6        # ~0.5mm top padding
    BOTTOM_PAD  = 32       # ~2.7mm bottom margin — Dymo 30330 unprintable bottom zone
    RIGHT_PAD   = 35       # ~3mm right margin — Dymo 30330 unprintable right zone
    LEFT_PAD    = 80       # ~6.7mm left margin — Dymo 30330 unprintable left zone clips ~6mm

    # Layout zones fit within a reduced usable height so nothing sits in the bottom clip zone
    usable_h = height_px - BOTTOM_PAD
    text_zone_h    = int(usable_h * 0.35)
    barcode_zone_h = int(usable_h * 0.50)
    code_zone_h    = usable_h - text_zone_h - barcode_zone_h

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
    y = V_PAD

    # Card name — truncate to fit width
    if card_name:
        name = card_name
        while name and draw.textlength(name, font=font_name) > (width_px - LEFT_PAD - RIGHT_PAD):
            name = name[:-1]
        if name != card_name:
            name = name[:-1] + "…"
        draw.text((LEFT_PAD, y), name, fill="black", font=font_name)
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
        draw.text((LEFT_PAD, y), detail, fill="#222222", font=font_detail)

    # ── Dividing line ─────────────────────────────────────────────────────────
    div_y = text_zone_h
    draw.line([(LEFT_PAD, div_y), (width_px - RIGHT_PAD, div_y)], fill="#cccccc", width=1)

    # ── Barcode zone ──────────────────────────────────────────────────────────
    # Render at a fixed module width so short IDs produce a short barcode (and
    # don't get stretched to fill the label, which produces uneven bars).
    code128 = barcode.get("code128", barcode_id, writer=ImageWriter())
    buf = io.BytesIO()
    code128.write(buf, options={
        "module_width":  0.30,    # mm per bar — readable on 19mm-tall labels
        "module_height": 10.0,    # mm
        "font_size":     0,
        "text_distance": 0,
        "quiet_zone":    1.0,     # mm
        "write_text":    False,
        "dpi":           300,
    })
    buf.seek(0)
    barcode_img = Image.open(buf)

    bc_y = div_y + 4
    bc_h = barcode_zone_h - 8
    max_bc_w = width_px - LEFT_PAD - RIGHT_PAD

    # Keep natural barcode width if it fits — only scale down if a legacy
    # long-format ID overflows the printable zone.
    natural_w, _ = barcode_img.size
    final_w = min(natural_w, max_bc_w)
    barcode_resized = barcode_img.resize((final_w, bc_h), Image.Resampling.NEAREST)
    label.paste(barcode_resized, (LEFT_PAD, bc_y))

    # ── Barcode ID text ────────────────────────────────────────────────────────
    id_y = div_y + barcode_zone_h + 2
    draw.text((LEFT_PAD, id_y), barcode_id, fill="#555555", font=font_code)

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
