"""
Barcode generation for raw card inventory.

Label: 62mm x 50mm landscape at 300 DPI = 732 x 591 px
Big readable text — card name, number, condition — then barcode below.
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


def _best_font(size):
    for candidate in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ]:
        if os.path.exists(candidate):
            try:
                return ImageFont.truetype(candidate, size)
            except Exception:
                continue
    return ImageFont.load_default()


def generate_barcode_image(barcode_id: str, *,
                           card_name: str = "",
                           set_name: str = "",
                           condition: str = "",
                           card_number: str = "",
                           price: str = "",        # ignored — kept for compat
                           width_mm: float = 62,
                           height_mm: float = 50) -> bytes:
    """
    62mm x 50mm at 300 DPI.

    Layout:
      [CARD NAME — very large                  ]
      [#079/091  •  NM        — large          ]
      [barcode bars                            ]
      [PF-20260316-A3K9X2     — small          ]
    """
    dpi       = 300
    width_px  = int(width_mm  / 25.4 * dpi)   # 732
    height_px = int(height_mm / 25.4 * dpi)   # 591

    PAD = 16

    # Font sizes in pixels — at 300dpi, 1pt ≈ 4.17px
    # 80px ≈ 19pt,  60px ≈ 14pt,  30px ≈ 7pt
    font_name   = _best_font(90)   # card name
    font_detail = _best_font(64)   # card# + condition
    font_code   = _best_font(30)   # barcode ID string

    label = Image.new("RGB", (width_px, height_px), "white")
    draw  = ImageDraw.Draw(label)

    y = PAD

    # ── Card name ─────────────────────────────────────────────────────────────
    if card_name:
        name = card_name
        while name and draw.textlength(name, font=font_name) > (width_px - PAD * 2):
            name = name[:-1]
        if name != card_name:
            name = name[:-1] + "…"
        draw.text((PAD, y), name, fill="black", font=font_name)
        bbox = draw.textbbox((PAD, y), name, font=font_name)
        y = bbox[3] + 8

    # ── Card number + condition ───────────────────────────────────────────────
    parts = []
    if card_number:
        parts.append(f"#{card_number}")
    if condition:
        parts.append(condition)
    if not parts and set_name:
        parts.append(set_name[:28])
    if parts:
        detail = "   •   ".join(parts)
        draw.text((PAD, y), detail, fill="#222222", font=font_detail)
        bbox = draw.textbbox((PAD, y), detail, font=font_detail)
        y = bbox[3] + 10

    # ── Barcode ───────────────────────────────────────────────────────────────
    code128 = barcode.get("code128", barcode_id, writer=ImageWriter())
    buf = io.BytesIO()
    code128.write(buf, options={
        "module_width":  0.35,
        "module_height": 12.0,
        "font_size":     0,
        "text_distance": 0,
        "quiet_zone":    2.0,
        "write_text":    False,
    })
    buf.seek(0)
    barcode_img = Image.open(buf)

    # Reserve space for barcode ID at bottom
    code_bbox = draw.textbbox((0, 0), barcode_id, font=font_code)
    code_h    = code_bbox[3] - code_bbox[1] + 6

    barcode_h = max(height_px - y - code_h - PAD, 60)
    barcode_w = width_px - PAD * 2

    barcode_resized = barcode_img.resize(
        (barcode_w, barcode_h),
        Image.Resampling.NEAREST
    )
    label.paste(barcode_resized, (PAD, y))
    y += barcode_h + 4

    # ── Barcode ID ────────────────────────────────────────────────────────────
    draw.text((PAD, y), barcode_id, fill="#666666", font=font_code)

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
