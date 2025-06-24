import re
import fitz  # PyMuPDF
import pandas as pd
import argparse
import os
from collections import defaultdict

# Define sort order
CONDITION_PRIORITY = [
    "Near Mint", "Lightly Played", "Moderately Played", "Heavily Played", "Damaged", "Unknown"
]
CONDITION_MAP = {cond: i for i, cond in enumerate(CONDITION_PRIORITY)}

def parse_args():
    parser = argparse.ArgumentParser(description="Generate segmented pull lists from a TCGPlayer packing slip PDF.")
    parser.add_argument("input_pdf", help="Path to the input PDF")
    parser.add_argument("-output_dir", default="pull_list_batches", help="Folder to save output XLSX files")
    parser.add_argument("-batch_size", type=int, default=30, help="How many orders per batch")
    return parser.parse_args()

def extract_orders_from_packing_slip(pdf_path):
    doc = fitz.open(pdf_path)
    orders = []
    order_to_bin = {}
    bin_counter = 1

    current_order = None
    current_qty = None
    current_desc_lines = []
    capture_desc = False

    for page in doc:
        lines = page.get_text().splitlines()
        for line in lines:
            line = line.strip()

            if line.startswith("Order Number:"):
                current_order = line.split("Order Number:")[1].strip()
                if current_order not in order_to_bin:
                    order_to_bin[current_order] = bin_counter
                    bin_counter += 1

            elif line.isdigit():
                current_qty = int(line)
                current_desc_lines = []
                capture_desc = True

            elif re.search(r"\$\d+\.\d{2}$", line):
                if capture_desc and current_order and current_qty is not None:
                    full_description = " ".join(current_desc_lines).strip()
                    if full_description.lower().startswith("total"):
                        capture_desc = False
                        continue

                    condition = "Unknown"
                    for c in CONDITION_PRIORITY:
                        if re.search(rf"\b{re.escape(c)}\b", full_description):
                            condition = c
                            break

                    parts = full_description.split(" - ")
                    if len(parts) > 3 and parts[1].strip() == "SM":
                        cleaned_name = " - ".join(parts[3:])
                    else:
                        cleaned_name = " - ".join(parts[2:]) if len(parts) > 2 else full_description

                    if cleaned_name.lower() != "total":
                        orders.append({
                            "Order": current_order,
                            "Product": cleaned_name,
                            "Condition": condition,
                            "Qty": current_qty,
                            "Bin": order_to_bin[current_order],
                        })

                capture_desc = False

            elif capture_desc:
                current_desc_lines.append(line)

    return pd.DataFrame(orders)

def normalize_and_group_orders(df):
    pull_data = defaultdict(lambda: defaultdict(int))

    for _, row in df.iterrows():
        product = row["Product"]
        condition = row["Condition"]
        qty = row["Qty"]
        bin_number = row["Bin"]

        key = f"{product} | {condition}"
        pull_data[key][bin_number] += qty

    rows = []
    for key, bins in pull_data.items():
        product_name, condition = key.split(" | ")
        for bin_number, qty in bins.items():
            rows.append({
                "Qty": qty,
                "Product": product_name,
                "Bin": bin_number,
                "Condition": condition
            })

    result_df = pd.DataFrame(rows)
    result_df["ConditionSort"] = result_df["Condition"].map(CONDITION_MAP)
    result_df.sort_values(by=["ConditionSort", "Product"], inplace=True)
    return result_df.drop(columns=["ConditionSort"])[["Qty", "Product", "Bin", "Condition"]]

def write_batches_to_excel(df, batch_size, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    unique_bins = sorted(df["Bin"].unique())

    for i in range(0, len(unique_bins), batch_size):
        bins = unique_bins[i:i + batch_size]
        batch_df = df[df["Bin"].isin(bins)].copy()

        # ✅ Reassign Bin as 1–30 within the batch using modulo
        bin_mapping = {bin_val: (idx % batch_size) + 1 for idx, bin_val in enumerate(bins)}
        batch_df["Bin"] = batch_df["Bin"].map(bin_mapping)

        batch_df.to_excel(os.path.join(output_dir, f"pull_list_{i//batch_size + 1}.xlsx"), index=False)

if __name__ == "__main__":
    args = parse_args()
    orders_df = extract_orders_from_packing_slip(args.input_pdf)
    pull_list_df = normalize_and_group_orders(orders_df)
    write_batches_to_excel(pull_list_df, args.batch_size, args.output_dir)
    print(f"✅ {len(pull_list_df)} cards processed into {args.output_dir}")
