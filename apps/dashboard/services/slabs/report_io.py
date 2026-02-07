import os, csv, json, time
from datetime import datetime

_BUCKETS = ("updated", "flag_down", "no_data", "skipped")

def _ts_run_id():
    # e.g. 2025-09-16T01-23-45Z
    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

def write_report_files(report: dict, *, out_root="out") -> dict:
    """
    Writes CSVs + manifest JSON into out/runs/<run_id>/.
    Returns dict with 'run_id' and 'paths': {bucket: path}.
    """
    run_id = _ts_run_id()
    run_dir = os.path.join(out_root, "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    paths = {}
    for bucket in _BUCKETS:
        rows = list(report.get(bucket) or [])
        if not rows:
            continue
        # preserve a stable column order
        # union of all keys seen, but front-load the usual suspects
        preferred = [
            "variant_id","product_title","variant_title","sku",
            "card_name","set","num",
            "old","new","raw","source","reason",
            "tcgplayer_id"
        ]
        keys = list(dict.fromkeys(preferred + [k for r in rows for k in r.keys()]))

        path = os.path.join(run_dir, f"{bucket}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in keys})
        paths[bucket] = path

    # manifest for the run
    manifest = {
        "run_id": run_id,
        "created_at": int(time.time()),
        "counts": {b: len(report.get(b) or []) for b in _BUCKETS},
        "paths": paths,
    }
    with open(os.path.join(run_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # pointer to "latest"
    latest_symlink = os.path.join(out_root, "latest")
    try:
        if os.path.islink(latest_symlink) or os.path.exists(latest_symlink):
            try: os.remove(latest_symlink)
            except OSError: pass
        os.symlink(os.path.relpath(run_dir, out_root), latest_symlink)
    except Exception:
        # on Windows/limited FS, just write a small JSON pointer
        with open(os.path.join(out_root, "latest.json"), "w", encoding="utf-8") as f:
            json.dump({"run_id": run_id}, f)

    return {"run_id": run_id, "paths": paths, "dir": run_dir}
