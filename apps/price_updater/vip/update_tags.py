#!/usr/bin/env python3
"""
Retag backfill runner for /vip/retag_only

Usage examples:
  # Process everyone (pages of 50) until cursor is exhausted
  python retag_backfill.py --base https://prices.pack-fresh.com --secret YOUR_FLOW_SECRET

  # Dry-run a single page (no writes)
  python retag_backfill.py --dry-run --page-size 25

  # Target specific customers by GID (comma-separated) and run for real
  python retag_backfill.py --retry-ids gid://shopify/Customer/8492221530332,gid://shopify/Customer/123...

  # Read retry IDs from a file (one GID per line)
  python retag_backfill.py --retry-file stubborn_gids.txt
"""

import argparse
import json
import os
import sys
import time
from typing import List, Optional

import requests
from dotenv import load_dotenv


def env_or_default(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def post_retag(
    base: str,
    secret: str,
    page_size: int = 50,
    cursor: Optional[str] = None,
    retry_ids: Optional[List[str]] = None,
    dry_run: bool = False,
    timeout: float = 20.0,
    max_retries: int = 6,
) -> dict:
    """
    Call POST {base}/vip/retag_only with robust retries.
    Returns parsed JSON dict or raises on fatal errors.
    """
    url = base.rstrip("/") + "/vip/retag_only"
    payload = {
        "page_size": page_size,
        "dry_run": dry_run,
    }
    if cursor:
        payload["cursor"] = cursor
    if retry_ids:
        payload["retry_ids"] = retry_ids

    headers = {
        "Content-Type": "application/json",
        "X-Flow-Secret": secret,
    }

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            # Handle rate limits & transient server errors with backoff
            if resp.status_code in (429, 500, 502, 503, 504):
                msg = f"{resp.status_code} {resp.reason}"
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 16.0)
                    continue
                raise RuntimeError(f"Retag call failed after retries: {msg}\n{resp.text[:400]}")
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 16.0)
    # Should not reach here
    raise RuntimeError("Unexpected retry loop exit")


def parse_args():
    p = argparse.ArgumentParser(description="Backfill retag-only normalization via /vip/retag_only")
    p.add_argument("--base", default=env_or_default("VIP_BASE_URL", "https://prices.pack-fresh.com"),
                   help="Base URL for your service (default from VIP_BASE_URL or https://prices.pack-fresh.com)")
    p.add_argument("--secret", default=env_or_default("VIP_FLOW_SECRET", None), required=False,
                   help="X-Flow-Secret header (default from VIP_FLOW_SECRET)")
    p.add_argument("--page-size", type=int, default=50, help="Page size when paging customers (default 50)")
    p.add_argument("--dry-run", action="store_true", help="Don’t write anything; just preview actions")
    p.add_argument("--retry-ids", default="", help="Comma-separated list of customer GIDs to process explicitly")
    p.add_argument("--retry-file", default="", help="Path to a file with customer GIDs (one per line)")
    p.add_argument("--once", action="store_true", help="Run a single page (don’t follow next_cursor)")
    return p.parse_args()


def gather_retry_ids(arg_csv: str, file_path: str) -> List[str]:
    ids: List[str] = []
    if arg_csv.strip():
        ids.extend([x.strip() for x in arg_csv.split(",") if x.strip()])
    if file_path.strip():
        with open(file_path, "r", encoding="utf-8") as f:
            ids.extend([line.strip() for line in f if line.strip()])
    # de-dup preserve order
    seen = set()
    unique = []
    for gid in ids:
        if gid not in seen:
            seen.add(gid)
            unique.append(gid)
    return unique


def main():
    args = parse_args()
    load_dotenv()
    base = args.base
    secret = os.environ.get("VIP_FLOW_SECRET")
    if not secret:
        print("ERROR: Provide X-Flow-Secret via --secret or VIP_FLOW_SECRET env var", file=sys.stderr)
        sys.exit(2)

    retry_ids = gather_retry_ids(args.retry_ids, args.retry_file)
    cursor = None
    total_processed = 0
    total_failed = 0
    page = 0

    # If retry_ids given, we’ll do a single call (or multiple batches if many)
    if retry_ids:
        batch = retry_ids
        print(f"Processing explicit list of {len(batch)} customer(s) (dry_run={args.dry_run})...")
        res = post_retag(base, secret, page_size=len(batch), retry_ids=batch, dry_run=args.dry_run)
        total_processed += int(res.get("processed", 0))
        failed_ids = res.get("failed_ids") or []
        total_failed += len(failed_ids)
        print(json.dumps({"processed": total_processed, "failed": failed_ids, "sample": res.get("items")}, indent=2))
        sys.exit(0)

    # Otherwise page across all customers
    print(f"Paging retag_only (page_size={args.page_size}, dry_run={args.dry_run}) against {base} ...")
    while True:
        page += 1
        res = post_retag(base, secret, page_size=args.page_size, cursor=cursor, dry_run=args.dry_run)
        processed = int(res.get("processed", 0))
        total_processed += processed
        failed_ids = res.get("failed_ids") or []
        total_failed += len(failed_ids)
        sample = res.get("items") or []
        cursor = res.get("next_cursor")

        print(f"[page {page}] processed={processed} failed={len(failed_ids)} cursor={'<end>' if not cursor else cursor[:24]+'...'}")
        if failed_ids:
            # Print only first few in-line to keep logs readable
            print("  failed_ids (first 3):", [f.get("customer") for f in failed_ids[:3]])

        # Optional: show a tiny sample of what happened on this page
        if sample:
            print("  sample:", json.dumps(sample[:3], indent=2))

        if args.once:
            break
        if not cursor:
            break
        # tiny pace control so we don’t hammer your service
        time.sleep(0.2)

    print(f"\nDONE. total_processed={total_processed} total_failed={total_failed}")
    if total_failed:
        print("Some customers failed; re-run with --retry-ids on those GIDs to replay.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
