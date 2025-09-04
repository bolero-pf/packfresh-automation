
import argparse
import csv
from collections import defaultdict
from pathlib import Path

def sniff_headers(row):
    # Map flexible headers to canonical names
    lower = {k.strip().lower(): k for k in row.keys()}
    lot_key = None
    order_key = None
    for cand in ["lot", "lot_number", "lot#", "lot id", "lotid"]:
        if cand in lower:
            lot_key = lower[cand]
            break
    for cand in ["order", "order_id", "order id", "buyer", "buyer_id", "orderid"]:
        if cand in lower:
            order_key = lower[cand]
            break
    if lot_key is None or order_key is None:
        raise ValueError("Could not find headers for lot and order. "
                         "Expected columns include: lot/lot_number and order/order_id.")
    return lot_key, order_key

def read_lots(input_csv):
    # Detect encoding (UTF-16 vs UTF-8)
    with open(input_csv, "rb") as fb:
        head = fb.read(4096)
    encoding = "utf-8-sig"
    if head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff"):
        encoding = "utf-16"

    # Sniff delimiter
    with open(input_csv, "r", encoding=encoding, newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)
        if not reader.fieldnames:
            raise ValueError("Input CSV missing header row")

        # Find headers + optional slip_pos
        header_row = {name: "" for name in reader.fieldnames}
        lot_key, order_key = sniff_headers(header_row)
        # after: lot_key, order_key = sniff_headers(header_row)
        lower = {k.strip().lower(): k for k in reader.fieldnames}
        buyer_key = None
        for cand in ["buyer", "name", "buyer_name"]:
            if cand in lower:
                buyer_key = lower[cand]
                break
        lower = {k.strip().lower(): k for k in reader.fieldnames}
        slip_key = None
        for cand in ["slip_pos", "slip position", "slipposition", "slip"]:
            if cand in lower:
                slip_key = lower[cand]; break

        rows = list(reader)

    if not rows:
        raise ValueError(f"No data rows found. Headers={reader.fieldnames}.")

    items = []
    order_to_slip = {}  # order_id -> slip_pos
    order_to_buyer = {}  # order_id -> buyer
    bad_rows = []
    for r in rows:
        lot_raw = (r.get(lot_key) or "").strip()
        oid = (r.get(order_key) or "").strip()
        if not lot_raw or not oid:
            continue
        lot_digits = "".join(ch for ch in lot_raw if ch.isdigit())
        if not lot_digits:
            bad_rows.append(lot_raw);
            continue
        lot = int(lot_digits)

        # NEW: grab buyer straight from this row
        buyer_val = (r.get(buyer_key) or "").strip() if buyer_key else ""

        # CHANGE: include buyer in the sequence tuple
        items.append((lot, oid, buyer_val))

        if slip_key and (r.get(slip_key) or "").strip():
            try:
                order_to_slip.setdefault(oid, int(str(r[slip_key]).strip()))
            except Exception:
                pass
        if buyer_key and buyer_val:
            order_to_buyer.setdefault(oid, buyer_val)

    if not items:
        raise ValueError("Found rows but none usable. Examples of bad 'lot' values: " + ", ".join(repr(x) for x in bad_rows[:5]))

    items.sort(key=lambda x: x[0])
    return items, order_to_slip, order_to_buyer




def read_slips_csv(slips_csv):
    with open(slips_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        lower = {k.strip().lower(): k for k in reader.fieldnames}
        slip_key  = next((lower[k] for k in ["slip_pos","slip position","slip"] if k in lower), None)
        oid_key   = next((lower[k] for k in ["order_id","orderid","order ids","order_ids"] if k in lower), None)
        buyer_key = next((lower[k] for k in ["buyer","name","buyer_name"] if k in lower), None)
        if not slip_key or not oid_key:
            raise ValueError("Slips CSV must have slip_pos and order_id(s) columns.")
        slip_map, buyer_map = {}, {}
        for r in reader:
            oid = (r.get(oid_key) or "").strip()
            pos = (r.get(slip_key) or "").strip()
            if oid and pos:
                try: slip_map[oid] = int(pos)
                except: pass
            if buyer_key and r.get(buyer_key):
                buyer_map[oid] = str(r[buyer_key]).strip()
        return slip_map, buyer_map


def build_future_arrays(order_ids):
    # Precompute next occurrence index and remaining counts for each position
    n = len(order_ids)
    next_idx = [None] * n
    last_seen = {}
    for i in range(n - 1, -1, -1):
        oid = order_ids[i]
        next_idx[i] = last_seen.get(oid, None)
        last_seen[oid] = i

    rem_counts = [0] * n
    counts = defaultdict(int)
    for i in range(n - 1, -1, -1):
        oid = order_ids[i]
        counts[oid] += 1
        rem_counts[i] = counts[oid]
    total_counts = dict(counts)
    return next_idx, rem_counts, total_counts

def choose_eviction(active_orders, next_use_map):
    # Evict the active order whose next use is farthest in the future.
    # If an active order has no future use (None), evict that one.
    to_evict = None
    farthest = -1
    for oid in active_orders:
        nxt = next_use_map.get(oid, None)
        if nxt is None:
            return oid, "no_future"
        if nxt > farthest:
            farthest = nxt
            to_evict = oid
    return to_evict, "farthest_future"

def plan(seq, max_stacks, order_to_slip, order_to_buyer):
    # Build arrays for fast processing
    order_ids = [oid for (_, oid, _) in seq]
    n = len(seq)
    finalization_events = []  # list of (step, order_id)

    next_idx_arr, rem_counts_arr, total_counts = build_future_arrays(order_ids)

    # Active structures
    active_slots = {i + 1: None for i in range(max_stacks)}  # slot -> order_id or None
    order_to_slot = {}  # order_id -> slot

    placed_counts = defaultdict(int)

    # next_occurrence_map[oid] = index of next occurrence relative to current position
    first_occ = {}
    for i, oid in enumerate(order_ids):
        if oid not in first_occ:
            first_occ[oid] = i
    next_occurrence_map = dict(first_occ)

    plan_rows = []
    labels_rows = []
    order_summary = defaultdict(lambda: {"lots": [], "first_slot": None, "last_slot": None, "total": 0})

    def log_plan(step, lot, oid, buyer_val, action, slot, order_total, order_remaining_after, slip_pos, notes=""):
        plan_rows.append({
            "step": step,
            "lot_number": lot,
            "order_id": oid,
            "buyer": buyer_val,
            "action": action,
            "slot": slot if slot is not None else "",
            "slip_pos": slip_pos if slip_pos is not None else "",
            "order_total": order_total,
            "order_remaining_after": order_remaining_after,
            "pull_slip": "Y" if (action == "FINALIZE") else "",
            "notes": notes,
        })
    def assign_slot(oid, step_counter):
        for s, val in active_slots.items():
            if val is None:
                active_slots[s] = oid
                order_to_slot[oid] = s
                labels_rows.append({"step": step_counter, "slot": s, "assigned_order": oid, "event": "ASSIGN"})
                if order_summary[oid]["first_slot"] is None:
                    order_summary[oid]["first_slot"] = s
                order_summary[oid]["last_slot"] = s
                return s
        return None

    def free_slot(oid, step_counter, reason="FINALIZE"):
        s = order_to_slot.get(oid)
        if s is None:
            return None
        active_slots[s] = None
        order_to_slot.pop(oid, None)
        labels_rows.append({"step": step_counter, "slot": s, "assigned_order": "", "event": reason})
        if reason == "FINALIZE":
            finalization_events.append((step_counter, oid))  # <-- record for finalization_queue
        return s

    step_counter = 0

    for i, (lot, oid, buyer_from_row) in enumerate(seq):
        step_counter += 1

        # Update this order's next occurrence (we consumed index i)
        next_occurrence_map[oid] = next_idx_arr[i]

        if oid in order_to_slot:
            s = order_to_slot[oid]
            placed_counts[oid] += 1
            order_summary[oid]["lots"].append(lot)
            order_summary[oid]["total"] += 1
            total = total_counts.get(oid, 0)
            remaining = total - placed_counts[oid]
            pos_hint = order_to_slip.get(oid, "")
            log_plan(step_counter, lot, oid, buyer_from_row, "PLACE", s, total, remaining, pos_hint,
                     notes=f"Order active in slot {s}")

        else:
            free_s = assign_slot(oid, step_counter)
            if free_s is not None:
                placed_counts[oid] += 1
                order_summary[oid]["lots"].append(lot)
                order_summary[oid]["total"] += 1
                total = total_counts.get(oid, 0)
                remaining = total - placed_counts[oid]
                pos_hint = order_to_slip.get(oid, "")
                log_plan(step_counter, lot, oid, buyer_from_row, "ASSIGN_AND_PLACE", free_s, total, remaining, pos_hint,
                         notes="Using free slot")  # or "Took freed slot"/"Took evicted slot"

            else:
                # Build next use map for active orders
                next_use_map = {}
                for a_oid in list(order_to_slot.keys()):
                    next_use_map[a_oid] = next_occurrence_map.get(a_oid, None)

                # Try to finalize any order with no future lots
                done_oid = None
                for a_oid, nx in next_use_map.items():
                    if nx is None:
                        done_oid = a_oid
                        break
                if done_oid is not None:
                    freed = free_slot(done_oid, step_counter, reason="FINALIZE")
                    log_plan(step_counter, lot, oid,buyer_from_row, "NOTE", freed,
                             notes=f"Finalized order {done_oid} to free slot {freed}")
                    s = assign_slot(oid, step_counter)
                    placed_counts[oid] += 1
                    order_summary[oid]["lots"].append(lot)
                    order_summary[oid]["total"] += 1
                    total = total_counts.get(oid, 0)
                    remaining = total - placed_counts[oid]
                    pos_hint = order_to_slip.get(oid, "")
                    log_plan(step_counter, lot, oid, buyer_from_row, "ASSIGN_AND_PLACE", s, total, remaining, pos_hint,
                             notes="Using free slot")  # or "Took freed slot"/"Took evicted slot"


                else:

                    # Evict farthest-future
                    evict_oid, why = choose_eviction(order_to_slot, next_use_map)
                    evicted_slot = free_slot(evict_oid, step_counter, reason="EVICT")
                    # --- Row 1: log the eviction (about the evicted order) ---
                    evicted_buyer = order_to_buyer.get(evict_oid, "")
                    evicted_total = total_counts.get(evict_oid, 0)
                    evicted_remaining = evicted_total - placed_counts[
                        evict_oid]  # eviction doesn't change placed_counts
                    evicted_pos_hint = order_to_slip.get(evict_oid, "")
                    # No lot is being placed for the evicted order on this step; pass "" for lot_number to avoid confusion
                    log_plan(
                        step_counter,
                        lot="",  # <- this row is about the eviction event itself
                        oid=evict_oid,
                        buyer_val=evicted_buyer,
                        action="EVICT_NOTE",
                        slot=evicted_slot,
                        order_total=evicted_total,
                        order_remaining_after=evicted_remaining,
                        slip_pos=evicted_pos_hint,
                        notes=f"Evicted order {evict_oid} ({why}) to free slot {evicted_slot}"
                    )
                   # --- Row 2: now assign & place the CURRENT order on the freed slot ---
                    s = assign_slot(oid, step_counter)
                    placed_counts[oid] += 1
                    order_summary[oid]["lots"].append(lot)
                    order_summary[oid]["total"] += 1
                    total = total_counts.get(oid, 0)
                    remaining = total - placed_counts[oid]
                    pos_hint = order_to_slip.get(oid, "")
                    log_plan(
                       step_counter,
                        lot=lot,
                        oid=oid,
                        buyer_val=buyer_from_row,
                        action="ASSIGN_AND_PLACE",
                        slot=s,
                        order_total=total,
                        order_remaining_after=remaining,
                        slip_pos=pos_hint,
                        notes="Took evicted slot"
                    )
        # If this order has no future occurrences, finalize immediately
        if next_occurrence_map.get(oid, None) is None and oid in order_to_slot:
            s = free_slot(oid, step_counter, reason="FINALIZE")
            if s is not None:
                # after free_slot(...):
                total = total_counts.get(oid, 0)
                remaining = 0
                pos_hint = order_to_slip.get(oid, "")
                log_plan(step_counter, lot, oid, buyer_from_row, "FINALIZE", s, total, remaining, pos_hint,
                         notes="No more lots for this order")

    return plan_rows, order_summary, labels_rows, finalization_events

def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser(description="Generate a one-pass Whatnot picking/fulfillment plan with limited active stacks.")
    ap.add_argument("--input", required=True, help="Input CSV with columns: lot, order_id (headers flexible)")
    ap.add_argument("--max-stacks", type=int, default=24, help="Max number of active physical stacks (trays)")
    ap.add_argument("--out", default="plan.csv", help="Output plan CSV path")
    ap.add_argument("--slips", default=None, help="Optional slips CSV to annotate slip_pos")

    args = ap.parse_args()

    seq, slip_map_from_input, buyer_map_from_input = read_lots(args.input)
    order_to_slip = dict(slip_map_from_input)
    order_to_buyer = dict(buyer_map_from_input)
    if args.slips:
        slip_map2, buyer_map2 = read_slips_csv(args.slips)
        order_to_slip.update(slip_map2)
        order_to_buyer.update(buyer_map2)
    plan_rows, order_summary, labels_rows, finalization_events = plan(
        seq, args.max_stacks, order_to_slip, order_to_buyer)

    plan_path = Path(args.out)
    write_csv(
        plan_path,
        ["step", "lot_number", "order_id", "buyer", "action", "slot", "slip_pos", "order_total",
         "order_remaining_after", "pull_slip", "notes"],
        plan_rows
    )
    # Compact view for the table
    compact_rows = [
        {
            "step": r["step"],
            "lot_number": r["lot_number"],
            "slot": r["slot"],
            "buyer": r["buyer"],
            "pull_slip": r["pull_slip"],
        }
        for r in plan_rows
    ]
    write_csv(plan_path.parent / "plan_compact.csv", ["step", "lot_number", "slot", "buyer", "pull_slip"], compact_rows)

    final_rows = []
    for step, oid in finalization_events:
        final_rows.append({
            "step": step,
            "order_id": oid,
            "buyer": order_to_buyer.get(oid, ""),  # <<< add
            "slip_pos": order_to_slip.get(oid, ""),
            "note": "Pull this slip now (order finalized)."
        })

    finalq_path = plan_path.parent / "finalization_queue.csv"
    write_csv(finalq_path, ["step", "order_id", "buyer", "slip_pos", "note"], final_rows)
    summary_rows = []
    for oid, info in order_summary.items():
        summary_rows.append({
            "order_id": oid,
            "num_lots": info["total"],
            "first_slot_seen": info["first_slot"] if info["first_slot"] is not None else "",
            "last_slot_seen": info["last_slot"] if info["last_slot"] is not None else "",
            "lots": " ".join(str(x) for x in info["lots"]),
        })
    import pandas as pd
    finalq = pd.read_csv("finalization_queue.csv")  # has step, order_id, buyer, slip_pos
    finalq = finalq.sort_values(["step"]).reset_index(drop=True)
    finalq.insert(0, "final_seq", finalq.index + 1)
    finalq["notes"] = ["Put this slip at the top" if i == 0 else f"Place {i + 1}th" for i in finalq.index]

    finalq[["final_seq", "slip_pos", "buyer", "order_id", "notes"]].to_csv(
        "slip_pull_labels.csv", index=False
    )
    print("Wrote slip_pull_labels.csv")
    summary_path = plan_path.parent / "summary_orders.csv"
    write_csv(summary_path, ["order_id", "num_lots", "first_slot_seen", "last_slot_seen", "lots"], summary_rows)

    labels_path = plan_path.parent / "labels_slots.csv"
    write_csv(labels_path, ["step", "slot", "assigned_order", "event"], labels_rows)

    print(f"Wrote plan to: {plan_path}")
    print(f"Wrote order summary to: {summary_path}")
    print(f"Wrote slot timeline to: {labels_path}")

if __name__ == "__main__":
    main()
