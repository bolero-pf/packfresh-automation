import os
import fitz  # PyMuPDF
from collections import defaultdict, OrderedDict
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

# ===== CONFIGURATION =====
PDF_FOLDER = 'input_pdfs'
OUTPUT_FILE = 'pull_list.pdf'

# Warehouse walking order - edit as needed
WAREHOUSE_ORDER = [
    'Pokemon 151 Poster Collection',
    'Crown Zenith Tin - Articuno',
    'Pikachu VMAX Premium Figure Collection',
    # Add more in your actual walking order...
]

def extract_items_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    items = []

    for page in doc:
        text = page.get_text()
        for line in text.split('\n'):
            line = line.strip()
            if ' x' in line:
                try:
                    item, qty = line.rsplit(' x', 1)
                    qty = int(qty)
                    items.append((item.strip(), qty))
                except:
                    continue
    return items

def collect_totals(pdf_folder):
    item_totals = defaultdict(int)

    for filename in os.listdir(pdf_folder):
        if filename.endswith('.pdf'):
            pdf_path = os.path.join(pdf_folder, filename)
            items = extract_items_from_pdf(pdf_path)
            for item, qty in items:
                item_totals[item] += qty

    return item_totals

def sort_by_warehouse_order(item_totals, order):
    ordered_items = OrderedDict()
    remaining_items = item_totals.copy()

    for item in order:
        if item in item_totals:
            ordered_items[item] = item_totals[item]
            remaining_items.pop(item)

    # Add any items not in the order list at the end
    for item, qty in sorted(remaining_items.items()):
        ordered_items[item] = qty

    return ordered_items

def create_pull_list_pdf(item_totals, output_file):
    c = canvas.Canvas(output_file, pagesize=LETTER)
    width, height = LETTER
    y = height - 50
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "Warehouse Pull List")
    y -= 30
    c.setFont("Helvetica", 12)

    for item, qty in item_totals.items():
        line = f"{item.ljust(50, '.')} x{qty}"
        c.drawString(50, y, line)
        y -= 20
        if y < 50:
            c.showPage()
            c.setFont("Helvetica", 12)
            y = height - 50

    c.save()

if __name__ == "__main__":
    raw_totals = collect_totals(PDF_FOLDER)
    sorted_totals = sort_by_warehouse_order(raw_totals, WAREHOUSE_ORDER)
    create_pull_list_pdf(sorted_totals, OUTPUT_FILE)
    print(f"✅ Pull list created: {OUTPUT_FILE}")
