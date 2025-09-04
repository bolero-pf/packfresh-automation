# pip install pypdf pandas
import pandas as pd
from pypdf import PdfReader, PdfWriter
import argparse

ap = argparse.ArgumentParser(description="Reorder USPS label PDF pages to match finalization order")
ap.add_argument("--labels", required=True, help="input USPS labels PDF")
ap.add_argument("--manifest", default="slip_pull_labels.csv", help="CSV with final_seq, slip_pos")
ap.add_argument("--out", default="labels_finalized_order.pdf", help="output PDF")
args = ap.parse_args()

# 1. Load manifest
man = pd.read_csv(args.manifest)
if not {"final_seq","slip_pos"}.issubset(man.columns):
    raise SystemExit("Manifest must have 'final_seq' and 'slip_pos' columns")

# 2. Sort by final_seq to get the new order
man = man.sort_values("final_seq").reset_index(drop=True)

# 3. Load the original labels PDF
reader = PdfReader(args.labels)
writer = PdfWriter()

# 4. Map slip_pos -> page index (1-based slip_pos to 0-based page index)
for _, row in man.iterrows():
    slip_pos = int(row["slip_pos"])
    page_index = slip_pos - 1  # PDF pages are 0-based
    if page_index < 0 or page_index >= len(reader.pages):
        raise SystemExit(f"Slip_pos {slip_pos} is out of range (PDF has {len(reader.pages)} pages).")
    writer.add_page(reader.pages[page_index])

# 5. Write new PDF
with open(args.out, "wb") as f:
    writer.write(f)

print(f"Wrote {args.out} with {len(man)} pages in finalization order")
