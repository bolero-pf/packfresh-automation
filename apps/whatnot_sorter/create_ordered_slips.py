# pip install reportlab
import csv, argparse, math
from collections import defaultdict
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

def read_finalq(path):
    # expects at least: step,order_id  (buyer,slip_pos optional)
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            step = int(row.get("step", "0") or "0")
            rows.append({
                "step": step,
                "order_id": row.get("order_id", "").strip(),
                "buyer": (row.get("buyer") or "").strip(),
                "slip_pos": (row.get("slip_pos") or "").strip(),
            })
    rows.sort(key=lambda x: x["step"])
    return rows

def read_lots(path):
    # expects: order_id, lot   (plus slip_pos/buyer ok)
    lots = defaultdict(list)
    buyer_hint = {}
    slip_hint = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        lower = {k.lower(): k for k in r.fieldnames}
        lotk = next((lower[k] for k in ["lot","lot_number","lot#","lot id","lotid"] if k in lower), "lot")
        oidk = next((lower[k] for k in ["order_id","orderid","order"] if k in lower), "order_id")
        buyk = lower.get("buyer")
        slipk= next((lower[k] for k in ["slip_pos","slip position","slip"] if k in lower), None)
        for row in r:
            oid = (row.get(oidk) or "").strip()
            if not oid: continue
            lot_raw = (row.get(lotk) or "").strip()
            digits = "".join(ch for ch in lot_raw if ch.isdigit())
            if not digits: continue
            lots[oid].append(int(digits))
            if buyk and row.get(buyk): buyer_hint.setdefault(oid, row[buyk].strip())
            if slipk and row.get(slipk):
                try: slip_hint.setdefault(oid, int(str(row[slipk]).strip()))
                except: pass
    # sort lots numeric
    for k in lots: lots[k].sort()
    return lots, buyer_hint, slip_hint

def draw_slip(c: canvas.Canvas, idx, buyer, slip_pos, oid, lot_list):
    W, H = LETTER
    LM, TM = 0.7*inch, 0.7*inch
    y = H - TM
    # Header
    c.setFont("Helvetica-Bold", 14); c.drawString(LM, y, f"{idx}. {buyer}"); y -= 18
    if slip_pos:
        c.setFont("Helvetica", 10); c.drawString(LM, y, f"(slip {slip_pos})"); y -= 12
    c.setFont("Helvetica", 10); c.drawString(LM, y, f"Order: {oid}"); y -= 16
    # Lots
    c.setFont("Helvetica-Bold", 12); c.drawString(LM, y, f"Lots ({len(lot_list)}):"); y -= 16
    # simple wrap for lots line
    text = ", ".join(str(x) for x in lot_list) if lot_list else "—"
    c.setFont("Helvetica", 11)
    max_chars = 95
    while text:
        line, text = text[:max_chars], text[max_chars:]
        c.drawString(LM, y, line); y -= 14
        if y < 72: break  # 1 page per slip
    c.showPage()

def main():
    ap = argparse.ArgumentParser(description="Make reordered Whatnot slips (one per page) in FINALIZE order.")
    ap.add_argument("--final", default="finalization_queue.csv", help="finalization_queue.csv from planner")
    ap.add_argument("--lots",  default="slips_from_pdf_lots.csv", help="lots CSV (order_id, lot, buyer)")
    ap.add_argument("--out",   default="slips_finalized_order.pdf", help="output PDF path")
    args = ap.parse_args()

    finalq = read_finalq(args.final)
    lots_map, buyer_hint, slip_hint = read_lots(args.lots)

    c = canvas.Canvas(args.out, pagesize=LETTER)
    for i, row in enumerate(finalq, start=1):
        oid = row["order_id"]
        buyer = row["buyer"] or buyer_hint.get(oid, "")
        slip  = row["slip_pos"] or slip_hint.get(oid, "")
        draw_slip(c, i, buyer or "(unknown buyer)", slip, oid, lots_map.get(oid, []))
    c.save()
    print(f"Wrote {args.out} with {len(finalq)} pages")

if __name__ == "__main__":
    main()
