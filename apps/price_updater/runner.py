import json, time, requests

# runner.py
import os, sys, json, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()


BASE_ORIGIN = os.getenv("BASE_ORIGIN", "https://prices.pack-fresh.com")
SECRET      = os.getenv("VIP_FLOW_SECRET", "")
MODE        = (sys.argv[1] if len(sys.argv) > 1 else "backfill").lower()

# keep pages quick; tune after you watch timings
PAGE_SIZE   = int(os.getenv("PAGE_SIZE", "75"))

# use tuple (connect, read). keep read < common edge caps
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "10"))
READ_TIMEOUT    = int(os.getenv("READ_TIMEOUT", "60"))
TIMEOUT         = (CONNECT_TIMEOUT, READ_TIMEOUT)

if MODE not in ("backfill","sweep"):
    print("Usage: python runner.py [backfill|sweep]")
    sys.exit(2)

endpoint = "/vip/backfill" if MODE=="backfill" else "/vip/sweep_vips"
URL = f"{BASE_ORIGIN}{endpoint}"

session = requests.Session()
# robust transport retries on transient net issues (not read timeouts mid-response)
retry = Retry(
    total=3, backoff_factor=0.8,
    status_forcelist=[502, 503, 504, 520, 522, 524],
    allowed_methods=["POST"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://", HTTPAdapter(max_retries=retry))

headers = {
    "Content-Type": "application/json",
    # send both; your verifier will use the one it expects
    "X-Flow-Secret": SECRET,
    "X-Flow-Signature": SECRET,
}

cursor = None
total  = 0
failed_all = []

def call(json_body):
    # Manual retry for read timeouts with exponential backoff
    delay = 1.0
    for attempt in range(5):
        t0 = time.time()
        try:
            r = session.post(URL, headers=headers, json=json_body, timeout=TIMEOUT)
            elapsed = time.time() - t0
            r.raise_for_status()
            return r.json(), elapsed
        except requests.exceptions.ReadTimeout:
            if attempt == 4:
                raise
            print(f"Timeout, retrying in {delay:.1f}s …")
            time.sleep(delay)
            delay *= 2
        except requests.HTTPError as e:
            # Surface body for debugging if available
            print(f"HTTP {r.status_code if 'r' in locals() else '?'}: {getattr(r,'text','')[:300]}")
            raise e

while True:
    body = {"page_size": PAGE_SIZE}
    if cursor:
        body["cursor"] = cursor

    data, elapsed = call(body)
    processed = int(data.get("processed", 0))
    total += processed
    failed = data.get("failed_ids") or []

    # normalize failures to a list of ids
    for f in failed:
        failed_all.append(f["customer"] if isinstance(f, dict) and "customer" in f else f)

    print(f"[{MODE}] page processed={processed:4d}  total={total:5d}  failures={len(failed):3d}  elapsed={elapsed:5.1f}s")

    cursor = data.get("next_cursor")
    if not cursor:
        break

    # small breath to avoid hammering origin
    time.sleep(0.3)

# retry failures (small batches)
if failed_all:
    print(f"[{MODE}] retrying {len(failed_all)} failures in batches of 20…")
    for i in range(0, len(failed_all), 20):
        batch = failed_all[i:i+20]
        d2, elapsed = call({"retry_ids": batch})
        again = d2.get("failed_ids") or []
        print(f"  retried {len(batch):3d} → remaining_on_last_call={len(again):3d}  elapsed={elapsed:4.1f}s")

print(f"[{MODE}] DONE. total_processed={total}")
