
import re, csv, os, sys, argparse
import unicodedata

def ocr_pdf_to_text(pdf_path, dpi=300, lang="eng"):
    # Requires: pdf2image, pillow, pytesseract; plus system Tesseract + Poppler.
    from pdf2image import convert_from_path
    import pytesseract

    images = convert_from_path(pdf_path, dpi=dpi)
    chunks = []
    for img in images:
        chunks.append(pytesseract.image_to_string(img, lang=lang))
    return "\n\n<<<PAGE BREAK>>>\n\n".join(chunks)

def clean_buyer_name(s: str) -> str:
    s = s.strip()

    # 1) If there's a handle in parens, prefer "Name (handle)" only.
    m = re.match(r'^(.+?\([^)]+\))\b', s)
    if m:
        return m.group(1).strip()

    # 2) Otherwise, cut off anything that looks like addressy stuff.
    #    - stop before first digit (street number/zip)
    #    - or before 'PO Box', commas/periods that start an address
    s = re.split(r'\s(?=\d)|\bPO\s*Box\b|,|\.{1,3}', s, maxsplit=1)[0].strip()

    # 3) Squash extra spaces and trailing punctuation
    s = re.sub(r'\s+', ' ', s).rstrip('.,- ')
    return s
def _is_useful_text(s: str) -> bool:
    if not s: return False
    s = s.strip()
    if not s: return False
    has_keywords = re.search(r'(Whatnot|Packing\s*Slip|Name\s*[:：﹕])', s, re.I) is not None
    return has_keywords

def _print_probe(label: str, txt: str):
    print(f"[probe] {label}: len={len(txt or '')}, useful={_is_useful_text(txt or '')}")
    if txt:
        print("         preview:", repr((txt.replace('\r','').replace('\n',' ')[:120])))

def normalize_text(s: str) -> str:
    # Normalize full-width chars, smart quotes, etc.
    s = unicodedata.normalize("NFKC", s)
    # Replace non-breaking/zero-width spaces with plain spaces
    s = s.replace("\xa0", " ").replace("\u200b", "").replace("\ufeff", "")
    # Collapse stray Windows newlines (\r\n → \n)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s

def extract_text_from_pdf(pdf_path):
    # 1) pypdf
    try:
        from pypdf import PdfReader
        with open(pdf_path, "rb") as f:
            r = PdfReader(f)
            t = "\n".join((p.extract_text() or "") for p in r.pages)
        _print_probe("pypdf", t)
        if _is_useful_text(t): return t
    except Exception as e:
        print("[probe] pypdf error:", e)

    # 2) PyPDF2
    try:
        import PyPDF2
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            t = "\n".join((p.extract_text() or "") for p in r.pages)
        _print_probe("PyPDF2", t)
        if _is_useful_text(t): return t
    except Exception as e:
        print("[probe] PyPDF2 error:", e)

    # 3) pdfminer.six
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract_text
        t = pdfminer_extract_text(pdf_path) or ""
        _print_probe("pdfminer", t)
        if _is_useful_text(t): return t
    except Exception as e:
        print("[probe] pdfminer error:", e)

    # If nothing worked, return empty; caller may try OCR.
    return ""




def parse_slips(text):
    import re
    blocks = re.split(r'(?im)^\s*Whatnot\s*[-–—]\s*Packing\s*Slip\s*$', text)
    orders_rows = []
    lots_rows = []
    slip_index = 0

    for block in blocks:
        if not block.strip():
            continue
        slip_index += 1
        # Buyer line after "Ships to:" and before "Tracking code:" (skip NEW BUYER marker)
        buyer_name = ""
        after_ships = False
        for line in block.splitlines():
            line = line.strip()
            if not after_ships:
                if re.search(r'^Ships\s*to:', line, re.I):
                    after_ships = True
                continue
            else:
                if not line:
                    continue
                if "( NEW BUYER! )" in line:
                    continue
                if line.lower().startswith("tracking code"):
                    break
                buyer_name_raw = line
                buyer_name = clean_buyer_name(buyer_name_raw)
                break

        m_total = re.search(r'(?i)Total\s*:\s*(\d+)\s*items?', block)
        total_items = int(m_total.group(1)) if m_total else ""

        header_orders = re.findall(r'Orders\s*#(\d+)', block, re.I)

        lot_matches = list(re.finditer(
            r'(?im)^\s*Name\s*[:：﹕]{1,2}\s*(?P<buyer>.+?)\s*(?:[\-\u2013\u2014|•*]\s*)?(?:[#＃]\s*|\bNo\.?\s*)(?P<lot>\d{1,6})\b',
            block
        ))
        if not lot_matches:
            lot_matches = list(re.finditer(
                r'(?ims)^\s*Name\s*[:：﹕]{1,2}\s*(?P<buyer>.+?)\s*(?:[\-\u2013\u2014|•*]\s*)?(?:\n|\r|\s){0,3}(?:[#＃]\s*|\bNo\.?\s*)(?P<lot>\d{1,6})\b',
                block
            ))

        orders_rows.append({
            "slip_pos": slip_index,
            "buyer": buyer_name,
            "total_items": total_items,
            "order_ids": f"{buyer_name} | slip{slip_index}" if buyer_name else ""
        })

        for m in lot_matches:
            # m can capture lot number as group(2) or named 'lot' depending on your pattern
            lot_no = int(m.group(2) if m.lastindex and m.lastindex >= 2 else m.group('lot'))
            # Use the real buyer from the "Ships to:" section:
            lots_rows.append({
                "slip_pos": slip_index,
                "buyer": buyer_name,  # <<< key change
                "lot": lot_no,
                "order_id": f"{buyer_name} | slip{slip_index}"  # keeps your stable key style
            })

    return orders_rows, lots_rows

def write_csv(path, fieldnames, rows):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    import argparse, os
    ap = argparse.ArgumentParser(description="Parse Whatnot packing slip PDFs into CSVs for picking/fulfillment.")
    ap.add_argument("--pdf", action="append", required=True, help="Path to a Whatnot packing slips PDF (can pass multiple)")
    ap.add_argument("--outdir", default=".", help="Directory to write CSV outputs")
    args = ap.parse_args()

    all_orders = []
    all_lots = []
    for pdf in args.pdf:
        text = extract_text_from_pdf(pdf)
        text = normalize_text(text)  # <<< add this line
        if not _is_useful_text(text):
            try:
                text = ocr_pdf_to_text(pdf)
                text = normalize_text(text)
            except Exception as e:
                # Leave text empty; we’ll emit debug files below
                text = text or ""
        orders_rows, lots_rows = parse_slips(text)
        if not lots_rows:
            preview_path = os.path.join(args.outdir, "debug_slip_preview.txt")
            with open(preview_path, "w", encoding="utf-8") as dbg:
                dbg.write(text[:8000])

            name_dbg = os.path.join(args.outdir, "debug_name_lines.txt")
            with open(name_dbg, "w", encoding="utf-8") as dbg2:
                for i, ln in enumerate(text.splitlines()):
                    if "Name" in ln or "NAME" in ln:
                        dbg2.write(f"{i:06d}: {ln}\n")
                        # also show next line or two—OCR sometimes splits the '#NN'
                        if i + 1 < len(text):
                            dbg2.write(f"{i + 1:06d}: {text.splitlines()[i + 1]}\n")
                        dbg2.write("----\n")

            print("[ERROR] Parsed 0 lots from:", pdf)
            print("Wrote debug_slip_preview.txt and debug_name_lines.txt")

        all_orders.extend(orders_rows)
        all_lots.extend(lots_rows)

    os.makedirs(args.outdir, exist_ok=True)
    orders_csv = os.path.join(args.outdir, "slips_from_pdf_orders.csv")
    lots_csv = os.path.join(args.outdir, "slips_from_pdf_lots.csv")

    write_csv(orders_csv, ["slip_pos", "buyer", "total_items", "order_ids"], all_orders)
    write_csv(lots_csv, ["slip_pos", "buyer", "lot", "order_id"], all_lots)

    print("Wrote:", orders_csv)
    print("Wrote:", lots_csv)

if __name__ == "__main__":
    main()
