import pdfplumber
import pandas as pd
import re
from pathlib import Path

# --- SETTINGS ---
pdf_folder = Path(".")     # folder containing all your PDFs
output_csv = "packfresh_invoices.csv"

# regex to capture line items: description, item#, qty, UM, price, MSRP, ext price
line_re = re.compile(
    r"^(?P<desc>.+?)\s+(?P<item>[A-Z0-9\-#]+)\s+"
    r"(?P<ordered>\d+)\s+(?P<shipped>\d+)\s+(?P<um>[A-Z]+)\s+\$?(?P<unit>\d+\.\d{2})\s+\$?(?P<msrp>\d+\.\d{2})\s+\$?(?P<ext>\d+\.\d{2})$"
)

def categorize(desc: str) -> str:
    d = desc.upper()
    if d.startswith("PKMN"): return "Pokémon"
    if d.startswith("WSE"): return "Weiss Schwarz"
    if d.startswith("GODZILLA"): return "Godzilla TCG"
    if any(x in d for x in ["DRAGON SHIELD", "ULTRA PRO", "UP ", "SOUBX", "ULPTL"]): return "Supplies"
    return "Other"

rows = []
for pdf_path in sorted(pdf_folder.glob("*.pdf")):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    order_no, date = None, None
    for ln in lines[:30]:
        if "Order #" in ln:
            order_no = ln.split("Order #")[-1].split()[0].strip()
        if "Invoice Date:" in ln:
            date = ln.split("Invoice Date:")[-1].split()[0].strip()
    # find start after "Description"
    try:
        start = next(i for i,l in enumerate(lines) if l.upper().startswith("DESCRIPTION"))
    except StopIteration:
        start = 0
    for ln in lines[start+1:]:
        if ln.startswith("Sub Total") or ln.startswith("SubTotal"):
            break
        m = line_re.match(ln)
        if m:
            desc = m.group("desc").strip()
            item = m.group("item").strip()
            qty = int(m.group("shipped"))
            um = m.group("um").strip()
            unit = float(m.group("unit"))
            msrp = float(m.group("msrp"))
            ext = float(m.group("ext"))
            cat = categorize(desc)
            rows.append({
                "Order #": order_no,
                "Invoice Date": date,
                "Item Description": desc,
                "Item #": item,
                "Qty": qty,
                "UM": um,
                "Unit Price": unit,
                "MSRP": msrp,
                "Ext. Price": ext,
                "Category": cat,
            })

df = pd.DataFrame(rows)
df.to_csv(output_csv, index=False)
print(f"Wrote {len(df)} line items to {output_csv}")
