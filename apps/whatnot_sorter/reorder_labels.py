# pip install pypdf pandas
import pandas as pd
from pypdf import PdfReader, PdfWriter
import argparse, re
from pathlib import Path

def parse_range_list(s: str):
    """
    Parse a string like '5,6,12-14, 20' into a set of 0-based page indices.
    Input is 1-based; output is 0-based.
    """
    out = set()
    if not s:
        return out
    for token in re.split(r"[,\s]+", s.strip()):
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            a, b = int(a), int(b)
            for v in range(min(a, b), max(a, b) + 1):
                out.add(v - 1)
        else:
            out.add(int(token) - 1)
    return out

def parse_continuations_from_file(p: Path):
    """
    Read a file containing 1-based page numbers and/or ranges (like 5 or 12-14).
    Lines may include comments after a #.
    """
    if not p:
        return set()
    out = set()
    for line in p.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        out |= parse_range_list(line)
    return out

ap = argparse.ArgumentParser(description="Reorder USPS label PDF pages to match finalization order, pairing customs continuation pages by index.")
ap.add_argument("--labels", default="Labels.pdf", help="input USPS labels PDF")
ap.add_argument("--manifest", default="slip_pull_labels.csv", help="CSV with final_seq, slip_pos")
ap.add_argument("--out", default="labels_finalized_order.pdf", help="output PDF")
# NEW: continuation inputs (index-based only)
ap.add_argument("--continuations", default="", help='Comma/range list of 1-based continuation page numbers, e.g. "5,6,12-14"')
ap.add_argument("--continuations-file", default="", help="Path to a file listing 1-based continuation pages/ranges (one per line).")
args = ap.parse_args()

# 1) Load manifest
man = pd.read_csv(args.manifest)
if not {"final_seq", "slip_pos"}.issubset(man.columns):
    raise SystemExit("Manifest must have 'final_seq' and 'slip_pos' columns")
man = man.sort_values("final_seq").reset_index(drop=True)

# 2) Load PDF
reader = PdfReader(args.labels)
writer = PdfWriter()
n_pages = len(reader.pages)

# 3) Build continuation set (0-based)
continuation_pages = set()
if args.continuations:
    continuation_pages |= parse_range_list(args.continuations)
if args.continuations_file:
    continuation_pages |= parse_continuations_from_file(Path(args.continuations_file))

# Bound-check continuation pages
bad = [i for i in continuation_pages if i < 0 or i >= n_pages]
if bad:
    raise SystemExit(f"Continuation pages out of range (PDF has {n_pages} pages): {', '.join(str(b+1) for b in bad)}")

# 4) Build label → [page_idx,...] buckets
# Rule: a page NOT in continuations starts a new label; any immediately-following continuation pages
# are paired to that label. Continuations at the very beginning are ignored (no prior label).
label_to_pages = {}
label_ord = 1
i = 0
while i < n_pages:
    if i in continuation_pages:
        # Continuation without a prior label: skip it (there's nothing to pair to).
        i += 1
        continue
    # Start a new label at page i
    bucket = [i]
    j = i + 1
    # Append all immediate continuation pages after this label page
    while j < n_pages and j in continuation_pages:
        bucket.append(j)
        j += 1
    label_to_pages[label_ord] = bucket
    label_ord += 1
    i = j

labels_detected = label_ord - 1

# 5) Validate slip_pos against detected labels (continuations excluded from counting)
max_manifest_label = int(man["slip_pos"].max())
if max_manifest_label > labels_detected:
    raise SystemExit(
        f"Manifest refers to slip_pos up to {max_manifest_label}, "
        f"but only {labels_detected} label(s) were detected after pairing continuations."
    )

# 6) Assemble output in finalization order (include continuations for each label)
pages_written = 0
for _, row in man.iterrows():
    slip_pos = int(row["slip_pos"])  # 1-based label ordinal
    pages = label_to_pages.get(slip_pos)
    if not pages:
        raise SystemExit(f"Label ordinal {slip_pos} not found in label mapping.")
    for p in pages:
        writer.add_page(reader.pages[p])
        pages_written += 1

# 7) Write new PDF
with open(args.out, "wb") as f:
    writer.write(f)

print(f"Wrote {args.out} with {pages_written} page(s) "
      f"from {len(man)} label(s); continuations paired by index.")
