import click, csv, datetime, json, pathlib, re, time, requests, random
from pprint import pprint
from .services.slabs.sync_runner import run_slabs_sync
from .services.slabs.cardnum import canon_card_number, variants_for_match
from .services.integrations.pokemon_price_tracker.client import PPTClient
from .services.slabs.iterators import iter_slab_variants_with_meta
from .services.slabs.setmap import to_ppt_set_id, extend_aliases, load_set_map, save_set_map
from .services.slabs.sync_v2 import run_slabs_sync_v2
from .services.slabs.slugmap import to_set_slug
from .services.slabs.cardnum import normalize_number
from .services.pricing.extract_v2 import pick_series
from .services.slabs.sync_v2 import _ppt_fetch_by_id, _fetch_wider_ebay
from .services.pricing.strategies import smart_price

def register_cli(app):
    @app.cli.command("slabs.sync")
    @click.option("--dry-run/--no-dry-run", default=True)
    @click.option("--half-life", default=7.0, help="EMA half-life in days")
    def slabs_sync_cmd(dry_run, half_life):
        shopify = app.config["SHOPIFY_CLIENT"]
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])
        rpt = run_slabs_sync(shopify, ppt, dry_run=dry_run, half_life_days=half_life)
        click.echo(rpt)

    @app.cli.command("slabs.discover-sets")
    @click.option("--tag", default="slab")
    @click.option("--out", default="slab_set_discovery.csv")
    def slabs_discover_sets(tag, out):
        """Scan slabs and report unique Set: strings and their current mapping."""
        shopify = app.config["SHOPIFY_CLIENT"]
        seen = {}
        for it in iter_slab_variants_with_meta(shopify, tag=tag):
            s = (it["meta"].set_name or "").strip()
            if not s:
                continue
            seen.setdefault(s, 0)
            seen[s] += 1

        rows = []
        for set_name, count in sorted(seen.items(), key=lambda x: (-x[1], x[0].lower())):
            ppt = to_ppt_set_id(set_name)
            rows.append({"set_name": set_name, "ppt_set_id": ppt or "", "count": count})

        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["set_name", "ppt_set_id", "count"])
            w.writeheader()
            w.writerows(rows)

        unmapped = [r for r in rows if not r["ppt_set_id"]]
        click.echo(f"Total unique sets: {len(rows)}")
        click.echo(f"Unmapped sets: {len(unmapped)} -> wrote {out}")

    @app.cli.command("ppt.sets-dump")
    @click.option("--out-json", default="ppt_sets.json")
    @click.option("--out-csv",  default="ppt_sets.csv")
    def ppt_sets_dump(out_json, out_csv):
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])
        data = ppt.list_sets(limit=10000)  # big number to avoid pagination if supported
        # data shape is provider-defined; most APIs return {"data":[{...}, ...]}
        rows = data.get("data", data)  # fall back if they return a bare list

        # heuristics to find the “ppt_set_id” you need for building cardIds
        # prefer a short code like sv95/swsh12, else id/slug
        def pick_code(x):
            for k in ("code", "id", "setId", "slug", "ptcgoCode"):
                v = x.get(k)
                if isinstance(v, str) and v:
                    return v
            return ""
        # write JSON
        import json, pathlib
        pathlib.Path(out_json).write_text(json.dumps(rows, indent=2), encoding="utf-8")
        # write CSV
        keys = set()
        for r in rows:
            keys.update(r.keys())
        ordered = ["name","code","id","slug","setId","series","releaseDate","total","ptcgoCode"]
        header = [k for k in ordered if k in keys] + [k for k in sorted(keys) if k not in ordered]
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in rows:
                w.writerow(r)

        # print a compact preview of (name -> chosen code)
        preview = [(r.get("name",""), pick_code(r)) for r in rows[:15]]
        click.echo(f"Wrote {len(rows)} sets → {out_json} / {out_csv}")
        for name, code in preview:
            click.echo(f"• {name}  =>  {code}")

    @app.cli.command("ppt.sets-dump")
    @click.option("--out-json", default="ppt_sets.json")
    @click.option("--out-csv",  default="ppt_sets.csv")
    def ppt_sets_dump(out_json, out_csv):
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])
        data = ppt.list_sets(limit=10000)  # big number to avoid pagination if supported
        # data shape is provider-defined; most APIs return {"data":[{...}, ...]}
        rows = data.get("data", data)  # fall back if they return a bare list

        # heuristics to find the “ppt_set_id” you need for building cardIds
        # prefer a short code like sv95/swsh12, else id/slug
        def pick_code(x):
            for k in ("code", "id", "setId", "slug", "ptcgoCode"):
                v = x.get(k)
                if isinstance(v, str) and v:
                    return v
            return ""
        # write JSON
        import json, pathlib
        pathlib.Path(out_json).write_text(json.dumps(rows, indent=2), encoding="utf-8")
        # write CSV
        keys = set()
        for r in rows:
            keys.update(r.keys())
        ordered = ["name","code","id","slug","setId","series","releaseDate","total","ptcgoCode"]
        header = [k for k in ordered if k in keys] + [k for k in sorted(keys) if k not in ordered]
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in rows:
                w.writerow(r)

        # print a compact preview of (name -> chosen code)
        preview = [(r.get("name",""), pick_code(r)) for r in rows[:15]]
        click.echo(f"Wrote {len(rows)} sets → {out_json} / {out_csv}")
        for name, code in preview:
            click.echo(f"• {name}  =>  {code}")

    @app.cli.command("ppt.cards-peek")
    @click.option("--set-id", "set_id", required=True, help="e.g. sv08-surging-sparks")
    @click.option("--limit", default=10)
    def ppt_cards_peek(set_id, limit):
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])
        j = ppt.list_cards(setId=set_id, fetchAllInSet=True)
        rows = j.get("data", j)[:limit]
        for c in rows:
            click.echo(f"{c.get('id')}   #{c.get('number')}   {c.get('name')}")

    def _norm(s):
        if not isinstance(s, str):
            return ""
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s.lower())).strip()

    def _rows(payload):
        # Accept list or dict; return list[dict]
        if payload is None:
            return []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("data", "sets", "results"):
                if key in payload and isinstance(payload[key], list):
                    return [r for r in payload[key] if isinstance(r, dict)]
            # last resort: take dict values if they look like rows
            vals = [v for v in payload.values() if isinstance(v, list)]
            if vals:
                return [r for r in vals[0] if isinstance(r, dict)]
        return []

    @app.cli.command("ppt.sets-build-code-map")
    @click.option("--out-json", default="apps/dashboard/data/ppt_set_code_map.json")
    def ppt_sets_build_code_map(out_json):
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])

        v1 = ppt.list_sets_v1()  # expect short codes like 'sv8', 'base1'
        v2 = ppt.list_sets(limit=10000)  # v2 slugs like 'sv08-surging-sparks'

        v1_rows = _rows(v1)
        v2_rows = _rows(v2)

        if not v1_rows:
            click.echo("Warning: v1 /api/sets returned no rows or unexpected shape.")
            click.echo(f"Payload type={type(v1)} sample={str(v1)[:300]}")

        if not v2_rows:
            click.echo("Warning: v2 /api/v2/sets returned no rows or unexpected shape.")
            click.echo(f"Payload type={type(v2)} sample={str(v2)[:300]}")

        # Build name -> short code from v1
        name_to_code = {}
        for r in v1_rows:
            code = r.get("id") or r.get("code") or r.get("setId")
            name = r.get("name")
            if code and name:
                name_to_code[_norm(name)] = code

        # Unified alias map: normalized aliases → short code
        out = {}

        # Names from v1
        for nm, code in name_to_code.items():
            if nm:
                out[nm] = code

        # Add v2 slugs (tcgPlayerId) pointing to the same code via name match
        for r in v2_rows:
            nm = _norm(r.get("name"))
            slug = (r.get("tcgPlayerId") or r.get("slug") or "").lower()
            code = name_to_code.get(nm)
            if code and slug:
                out[slug] = code

        p = pathlib.Path(out_json)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(out, indent=2), encoding="utf-8")

        click.echo(f"Wrote {len(out)} mappings → {out_json}")

    @app.cli.command("slabs.sync-v2")
    @click.option("--dry-run/--no-dry-run", default=True)
    @click.option("--days", default=30, type=int)
    @click.option("--sample", default=0, type=int, help="Process only the first N slab variants")
    @click.option("--per-card", is_flag=True, default=False, help="Use per-card API calls (fast for small N)")
    @click.option("--only-sets", default="", help="Comma-separated set slugs or PSA set names to include")
    @click.option("--per-set-limit", default=0, type=int, help="Max items per set (0 = no limit)")
    @click.option("--half-life", default=7.0, type=float)
    @click.option("--limit", default=0, type=int, help="process only N slabs (0 = all)")
    @click.option("--fetch-all/--no-fetch-all", default=True, help="Use v2 fetchAllInSet to minimize minute throttling")
    @click.option("--include-ebay/--no-include-ebay", default=True)
    @click.option("--include-history/--no-include-history", default=False)
    @click.option("--both", is_flag=True, default=False)
    @click.option("--out-updated", default="", type=click.Path(dir_okay=False, writable=True),
                  help="CSV of rows that would update (or did if --no-dry-run)")
    @click.option("--out-flag-down", default="", type=click.Path(dir_okay=False, writable=True),
                  help="CSV of rows flagged for review / going down")
    @click.option("--out-missing", default="", type=click.Path(dir_okay=False, writable=True),
                  help="CSV of rows missing slug/number (fix your mappings)")
    @click.option("--out-nodata", default="", type=click.Path(dir_okay=False, writable=True),
                  help="CSV of rows we couldn’t price (no history / fetch issues)")
    @click.option("--out-json", default="", type=click.Path(dir_okay=False, writable=True),
                  help="Write the full report dict to JSON")
    @click.option("--progress/--no-progress", default=True,
                  help="Print a one-line status per set while syncing")
    @click.option("--prefer-ebay-aggregate/--no-prefer-ebay-aggregate", default=True,
                  help="When eBay+grade available, prefer PPT salesByGrade.marketPrice7Day (fallback smartMarketPrice) over EMA.")
    def slabs_sync_v2_cmd(dry_run, days, half_life, fetch_all, sample, per_card, only_sets, per_set_limit, include_ebay,
                          include_history, both, out_updated, prefer_ebay_aggregate, out_missing, out_nodata, out_json, out_flag_down, progress, limit):
        shopify = app.config["SHOPIFY_CLIENT"]
        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])
        rpt = run_slabs_sync_v2(
            shopify,
            days=days,
            half_life=half_life,
            include_ebay=include_ebay,
            include_history=include_history,
            use_both=both,
            dry_run=dry_run,
            sample=sample,
            per_card=per_card,
            only_sets=only_sets,
            per_set_limit=per_set_limit,
            prefer_ebay_aggregate=prefer_ebay_aggregate
        )
        click.echo(rpt)

        def _dump_csv(path, rows, cols):
            import csv, pathlib
            pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                for r in rows:
                    w.writerow({k: r.get(k, "") for k in cols})

        # after rpt = run_slabs_sync_v2(...)
        click.echo(f"updated     : {len(rpt['updated'])}")
        click.echo(f"flag_down   : {len(rpt['flag_down'])}")
        click.echo(f"parse_missing: {len(rpt['parse_missing'])}")
        click.echo(f"no_data     : {len(rpt['no_data'])}")

        if out_updated:
            _dump_csv(out_updated, rpt["updated"],
                      ["variant_id", "set", "num", "old", "new", "dry_run"])
        if out_flag_down:
            _dump_csv(out_flag_down, rpt["flag_down"],
                      ["variant_id", "set", "num", "current", "target", "reason"])
        if out_missing:
            # great place to see what set strings need mapping
            _dump_csv(out_missing, rpt["parse_missing"],
                      ["variant_id", "set", "num", "product_title", "variant_title", "reason"])
        if out_nodata:
            _dump_csv(out_nodata, rpt["no_data"],
                      ["variant_id", "set", "num", "reason"])
        if out_json:
            import json, pathlib
            pathlib.Path(out_json).parent.mkdir(parents=True, exist_ok=True)
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(rpt, f, indent=2)
        from apps.dashboard.services.slabs.report_io import write_report_files

        sink = write_report_files(rpt, out_root="out")
        print(f"[OK] wrote report run_id={sink['run_id']} dir={sink['dir']}")

    @app.cli.command("slabs.audit-missing")
    @click.option("--limit", default=100)
    def slabs_audit_missing(limit):
        shopify = app.config["SHOPIFY_CLIENT"]
        from .services.slabs.parse import parse_slab_fields
        rows = []
        c = 0
        for it in iter_slab_variants_with_meta(shopify, tag="slab"):
            fields = parse_slab_fields(
                body_html=it.get("body_html", ""),
                title=it.get("product_title", ""),
                variant_title=it.get("variant_title", ""),
                sku=it.get("sku", ""),
            )
            set_name, num = fields["set"], fields["card_number"]
            if not set_name or not num:
                rows.append({
                    "variant_id": it["variant_id"],
                    "title": it.get("product_title"),
                    "variant": it.get("variant_title"),
                    "set_parsed": set_name,
                    "num_parsed": num,
                    "desc_snippet": (re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", it.get("body_html", "")))[:220] + "…"),
                })
                c += 1
                if c >= limit:
                    break
        import csv, pathlib
        path = pathlib.Path("slabs_missing_audit.csv")
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f,
                               fieldnames=rows[0].keys() if rows else ["variant_id", "title", "variant", "set_parsed",
                                                                       "num_parsed", "desc_snippet"])
            w.writeheader()
            for r in rows: w.writerow(r)
        click.echo(f"Wrote {len(rows)} rows → {path}")

    # tries alternative param names & number formats
    def _smart_fetch_card(ppt: PPTClient, slug: str, number: str, days: int, include_ebay: bool):
        url = f"{ppt.base_url}/v2/cards"
        headers = getattr(ppt, "headers", {})
        # try multiple keys & number formats
        set_keys = ["tcgPlayerId", "setSlug", "setId"]
        num_candidates = [str(number)]

        # try zero-padded (025) for classic sets, if num is int-ish
        if number.isdigit() and int(number) < 1000:
            num_candidates.append(f"{int(number):03d}")
            # try common denominators (25/102, etc.)
            for denom in ("102", "100", "110", "130"):
                num_candidates.append(f"{int(number)}/{denom}")

        # add original number again last (de-duped)
        seen = set()
        num_candidates = [n for n in num_candidates if not (n in seen or seen.add(n))]

        # simple rpm pacing if available
        if hasattr(ppt, "set_rpm"):
            ppt.set_rpm(1)

        # shuffle num_candidates minimally to avoid predictable bursts
        for key in set_keys:
            for n in num_candidates:
                params = {"includeHistory": True, "days": int(days)}
                if include_ebay:
                    params["includeEbay"] = True
                params[key] = slug
                params["number"] = n

                # gentle throttle
                time.sleep(0.5)

                r = requests.get(url, headers=headers, params=params, timeout=30)
                if r.status_code == 429:
                    # respect Retry-After if present
                    ra = r.headers.get("Retry-After")
                    try:
                        sleep_s = float(ra)
                    except (TypeError, ValueError):
                        sleep_s = 1.0 + random.uniform(0, 0.5)
                    time.sleep(sleep_s)
                    # retry once for this combo
                    r = requests.get(url, headers=headers, params=params, timeout=30)

                if r.status_code >= 400:
                    # keep trying other combos
                    continue

                data = r.json()
                rows = data.get("data", data)
                if not isinstance(rows, list) or not rows:
                    continue

                # sanity: prefer exact or prefix number match
                card = rows[0]
                ret_num = str(card.get("number") or "")
                if ret_num.startswith(str(number)) or ret_num == str(number) or n in (ret_num,):
                    return {"card": card, "params_used": {"set_key": key, "number": n}}

                # if nothing exact, still return the first row so we can inspect
                return {"card": card, "params_used": {"set_key": key, "number": n}}

        return {"card": None, "params_used": None}

    # optional: fetch the full set to see numbering
    def _probe_set_listing(ppt: PPTClient, slug: str, days: int, include_ebay: bool = False):
        url = f"{ppt.base_url}/v2/cards"
        headers = getattr(ppt, "headers", {})
        params = {"setId": slug, "fetchAllInSet": True, "includeHistory": False, "days": int(days)}
        if include_ebay:
            params["includeEbay"] = True
        r = requests.get(url, headers=headers, params=params, timeout=45)
        r.raise_for_status()
        data = r.json()
        rows = data.get("data", data)
        if not isinstance(rows, list):
            return []
        # return a tiny projection
        listing = []
        for c in rows:
            listing.append({
                "number": c.get("number"),
                "name": c.get("name"),
            })
        return listing

    @app.cli.command("slabs.test-one")
    @click.option("--psa-set", required=True)
    @click.option("--num", required=True)
    @click.option("--year", default="")
    @click.option("--days", default=30, type=int)
    @click.option("--include-ebay/--no-include-ebay", default=True)
    @click.option("--both", is_flag=True, default=False)
    @click.option("--limit", default=50, type=int)
    @click.option("--grade", default="", help="Target grade, e.g. 9 or 9.5")
    # NEW introspection flags:
    @click.option("--debug-arrays", is_flag=True, default=False, help="List all arrays with JSON paths and lengths")
    @click.option("--show-picked", default=0, type=int,
                  help="Print N sample rows from the PSA/grade-filtered series we priced")
    @click.option("--print-keys", is_flag=True, default=False, help="Print top-level keys of the card payload")
    @click.option("--print-distribution", is_flag=True, default=False,
                  help="Print counts by company/grade and date range")
    @click.option("--show-sample", default=0, type=int, help="Print N flattened points as samples")
    @click.option("--dump-json", default="", help="Write raw card JSON to this path")
    @click.option("--dump-points-csv", default="", help="Write flattened points CSV to this path")
    @click.option("--inspect-ebay", is_flag=True,
                  help="Show ebay.salesByGrade and last entries from ebay.priceHistory for the target grade")
    @click.option("--show-tail", type=int, default=0,
                  help="Print the last N points used for EMA (after grade filtering)")
    @click.option("--ema-debug", is_flag=True, help="Print per-point EMA weights/contributions")
    def slabs_test_one(psa_set, num, year, days, include_ebay, both, limit, grade,
                       print_keys,debug_arrays, show_picked, inspect_ebay, show_tail, ema_debug, print_distribution, show_sample, dump_json, dump_points_csv):
        ppt = PPTClient(app.config["PPT_API_KEY"])

        set_slug = to_set_slug(psa_set, year=year or None)
        bundle = ppt.list_set_cards_v2_fetch_all(
            set_slug=set_slug,
            days=days,
            include_history=(not both),  # if you're not using includeBoth
            include_ebay=include_ebay,
            include_both=both,
            set_key="setId",
        )
        rows = bundle.get("data", bundle) or []

        def _raw_num(c):
            return c.get("cardNumber") or c.get("number") or c.get("collectorNumber") or ""

        def _canon_num(s: str) -> str:
            s = (s or "").strip()
            if s.isdigit():
                return str(int(s))  # '004' -> '4'
            return s.upper().replace(" ", "").replace("-", "")

        want = canon_card_number(num)  # you already have this; same as _canon_num for digits
        found = None
        for c in rows:
            if _canon_num(_raw_num(c)) == want:
                found = c
                break

        if not found:
            sample = [{"raw": _raw_num(c), "canon": _canon_num(_raw_num(c)), "name": c.get("name")} for c in rows[:15]]
            click.echo("No number match in set. Sample:")
            pprint(sample)
            return

        name = found.get("name")
        raw = _raw_num(found)
        tpid = found.get("tcgplayerId") or found.get("tcgPlayerId") or found.get("id")
        click.echo(f"Matched: {name}  #{raw}  (tcgplayerId={tpid})")

        if not tpid:
            click.echo("⚠️ Matched card lacks tcgplayerId; cannot fetch history.")
            return

        data = ppt.get_card_by_id_v2(
            tcgplayer_id=tpid,
            days=days,
            include_ebay=include_ebay,
            include_both=both,
            include_history=not both
        )
        rows = data.get("data", data)
        card = rows[0] if isinstance(rows, list) and rows else rows

        if print_keys:
            click.echo("Top-level keys:")
            pprint(list(card.keys()))

        from .services.pricing.extract_v2 import collect_arrays_inventory, flatten_all_points_with_paths
        if debug_arrays:
            inv = collect_arrays_inventory(card)
            click.echo("Arrays inventory (path, len, sample_type):")
            for path, ln, typ in sorted(inv, key=lambda x: (-x[1], x[0]))[:50]:
                click.echo(f"  {ln:4d}  {typ:12s}  {path}")

        flat = flatten_all_points_with_paths(card)

        if print_distribution:
            from collections import Counter
            by_cg = Counter((p["company"], p["grade"]) for p in flat)
            click.echo("Counts by (company, grade):")
            for (co, gr), cnt in sorted(by_cg.items(), key=lambda x: (-x[1], x[0])):
                click.echo(f"  {co or '-':4s} {gr or '-':6s} : {cnt}")
            # show some likely graded paths
            from collections import Counter as C2
            path_counts = C2(p["path"] for p in flat)
            top_paths = path_counts.most_common(10)
            click.echo("Top paths:")
            for pth, cnt in top_paths:
                click.echo(f"  {cnt:4d}  {pth}")

        if show_sample > 0:
            click.echo(f"Flattened sample ({min(show_sample, len(flat))} rows):")
            pprint(flat[:show_sample])

        if dump_points_csv:
            import csv, pathlib
            pathlib.Path(dump_points_csv).parent.mkdir(parents=True, exist_ok=True)
            cols = ["t", "p", "company", "grade", "vendor", "raw_time_key", "raw_price_key", "path"]
            with open(dump_points_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                for r in flat:
                    w.writerow({k: r.get(k, "") for k in cols})
            click.echo(f"Wrote flattened points CSV → {dump_points_csv}")

        # ----- the series we actually price (PSA-grade filtered) -----
        target_grade = grade.strip() if grade else None
        series = pick_series(card, company="PSA", grade=target_grade, strict_grade=True)
        points = [{"timestamp": s["t"], "price": s["p"]} for s in series if s.get("p") is not None]
        click.echo(f"History points: {len(points)} (PSA {target_grade or 'any'})")

        # Optional debug you already had (inspect-ebay, show-tail, etc) can stay as-is.

        # ----- Prefer PPT aggregate when available -----
        from .services.pricing.extract_v2 import get_graded_aggregate
        agg = None
        if include_ebay and target_grade:
            agg = get_graded_aggregate(card, company="PSA", grade=target_grade)

        if agg:
            click.echo(f"PSA {target_grade} aggregate ({agg['method']} 7d) → {agg['price']:.2f}")
            target_price = agg["price"]
        else:
            # Fallback to EMA over the graded points
            if len(points) < 2:
                click.echo("Not enough history.")
                return
            from .services.pricing.strategies import ema_price
            # Use the same function the pipeline uses; one print only.
            target_price = ema_price(points, half_life_days=7.0)

        click.echo(f"Target price → {target_price:.2f}")

        # ---- B) Show the last N points actually used in EMA (post-filter) ----
        if show_tail and show_tail > 0:
            tail = sorted(points, key=lambda r: r["timestamp"])[-show_tail:]
            from datetime import datetime, timezone
            click.echo(f"Tail ({len(tail)}) used points:")
            for r in tail:
                dt = datetime.fromtimestamp(r["timestamp"], tz=timezone.utc)
                click.echo(f"  {dt.date().isoformat()}  ${r['price']:.2f}")

        # ---- C) Optional EMA breakdown: weights & contributions ----
        if ema_debug and len(points) >= 2:
            from datetime import datetime, timezone
            # Recompute EMA exactly like strategies.ema_price (time-decay by half-life)
            half_life_days = 7.0  # or use the --half-life arg if you expose it here
            pts = sorted(points, key=lambda r: r["timestamp"])
            ema = float(pts[0]["price"])
            click.echo("\nEMA breakdown (date, price, delta_days, alpha, contribution, ema_after):")
            prev_t = pts[0]["timestamp"]
            for r in pts[1:]:
                t = r["timestamp"]
                p = float(r["price"])
                delta_days = max(0.0, (t - prev_t) / 86400.0)
                # alpha for variable spacing:
                alpha = 1.0 - (0.5 ** (delta_days / half_life_days)) if half_life_days > 0 else 1.0
                new_ema = ema + alpha * (p - ema)
                contrib = alpha * (p - ema)
                dt = datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat()
                click.echo(f"  {dt}  ${p:.2f}  Δ={delta_days:5.2f}d  α={alpha:0.4f}  +{contrib:7.2f}  → {new_ema:7.2f}")
                ema, prev_t = new_ema, t
            click.echo(f"\nEMA({half_life_days:.0f}d) result → {ema:.2f}")

        if show_picked > 0:
            click.echo(f"Picked series sample ({min(show_picked, len(series))} rows):")
            pprint(series[:show_picked])

        if len(points) >= 2:
            from .services.pricing.strategies import smart_price
            target = smart_price(points, half_life_days=7.0, rounding=True)
            click.echo(f"EMA(7d) → {target}")
        else:
            click.echo("Not enough history.")


    @app.cli.command("slabs.inspect-id")
    @click.option("--id", "tcg_id", type=str, help="Single tcgplayerId to inspect (e.g. 200123).")
    @click.option("--ids-file", type=click.Path(exists=True, dir_okay=False), help="File of tcgplayerIds (CSV/TSV or one-per-line).")
    @click.option("--days", default=30, type=int, help="History window.")
    @click.option("--include-ebay/--no-include-ebay", default=True)
    @click.option("--include-history/--no-include-history", default=True)
    @click.option("--both", is_flag=True, default=False, help="Use includeBoth payload (skip history arrays).")
    @click.option("--company", default="PSA", help="Grading company to filter (PSA/CGC/BGS/etc).")
    @click.option("--grade", default="", help="Target grade (e.g. 9, 9.5, 10). Empty = any.")
    @click.option("--half-life", default=7.0, type=float, help="EMA half-life days.")
    @click.option("--out-dir", default="out/inspect", type=click.Path(file_okay=False))
    @click.option("--dump-json", is_flag=True, default=True, help="Write raw JSON for each id.")
    @click.option("--dump-points", is_flag=True, default=True, help="Write flattened points CSV for each id.")
    @click.option("--show-tail", default=8, type=int, help="Print last N points used in EMA path.")
    @click.option("--ema-debug", is_flag=True, default=False, help="Print per-point EMA contributions.")
    def slabs_inspect_id(tcg_id, ids_file, days, include_ebay, include_history, both,
                         company, grade, half_life, out_dir, dump_json, dump_points, show_tail, ema_debug):
        """
        Pull by tcgplayerId, show pricing inputs + the price we'd select.
        """
        from pathlib import Path
        import csv, json, os
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        # 1) build the id list
        ids = []
        if tcg_id:
            ids.append(str(tcg_id).strip())
        if ids_file:
            with open(ids_file, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            # CSV/TSV or newline
            if "," in txt or "\t" in txt:
                sniff = csv.Sniffer().sniff(txt.splitlines()[0])
                for row in csv.reader(txt.splitlines(), sniff):
                    if row:
                        ids.append(row[0].strip())
            else:
                for line in txt.splitlines():
                    s = line.strip()
                    if s:
                        ids.append(s)
        # de-dupe
        ids = [i for i in dict.fromkeys(ids) if i]
        if not ids:
            click.echo("Provide --id or --ids-file")
            return

        ppt = PPTClient(api_key=app.config["PPT_API_KEY"])

        # helpers from your pricing stack
        from .services.pricing.extract_v2 import (
            collect_arrays_inventory,
            flatten_all_points_with_paths,
            pick_series,
            get_graded_aggregate,
        )
        from .services.pricing.strategies import ema_price, smart_price

        def fetch_card_payload(tpid: str):
            # 1) exact same fetcher your sync uses (includes camelCase fallback for eBay)
            card = _ppt_fetch_by_id(
                ppt, int(tpid),
                days=int(days),
                include_ebay=bool(include_ebay),
                include_history=bool(include_history),
                use_both=bool(both or include_ebay)  # prefer both to guarantee ebay
            )

            # 2) If eBay still missing, try a one-time wider pull (same as sync)
            if (not isinstance(card, dict)) or (include_ebay and not (card or {}).get("ebay")):
                wider_days = 90 if int(days) < 90 else int(days)
                widened = _fetch_wider_ebay(int(tpid), wider_days)
                if isinstance(widened, dict) and widened.get("ebay"):
                    card = {**(card or {}), **{"ebay": widened["ebay"]}}

            return card

        for tpid in ids:
            click.echo(f"\n=== tcgplayerId {tpid} ===")
            try:
                card = fetch_card_payload(tpid)
            except Exception as e:
                click.echo(f"  ! fetch error: {e}")
                continue
            if not isinstance(card, dict) or not card:
                click.echo("  ! no payload")
                continue

            has_ebay = isinstance(card, dict) and bool((card or {}).get("ebay"))
            click.echo(f"  ebay: {'✅ present' if has_ebay else '❌ missing'}")
            if has_ebay:
                # show which grade buckets exist so you know what key you can target
                sbg = ((card or {}).get("ebay") or {}).get("salesByGrade") or {}
                click.echo(f"  ebay.salesByGrade keys: {', '.join(list(sbg.keys())[:8]) or '(none)'}")

            # Basic identity line
            nm = card.get("name") or card.get("cardName") or "-"
            number = card.get("number") or card.get("cardNumber") or "-"
            set_name = (card.get("set") or {}).get("name") or card.get("setName") or "-"
            click.echo(f"  {nm}  #{number}  ·  {set_name}")

            # Optional dumps for forensic diffing
            stem = os.path.join(out_dir, f"tpid_{tpid}")
            if dump_json:
                with open(stem + ".json", "w", encoding="utf-8") as f:
                    json.dump(card, f, indent=2)
                click.echo(f"  • wrote {stem}.json")

            flat = flatten_all_points_with_paths(card)
            if dump_points:
                cols = ["t","p","company","grade","vendor","raw_time_key","raw_price_key","path"]
                with open(stem + "_points.csv", "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=cols)
                    w.writeheader()
                    for r in flat:
                        w.writerow({k: r.get(k, "") for k in cols})
                click.echo(f"  • wrote {stem}_points.csv  (n={len(flat)})")

            # Build the exact series your pricing uses: graded, company+grade filtered
            target_grade = grade.strip() if grade else None
            series = pick_series(card, company=company, grade=target_grade, strict_grade=True)
            points = [{"timestamp": s["t"], "price": s["p"]} for s in series if s.get("p") is not None]
            click.echo(f"  graded series: company={company} grade={target_grade or 'any'}  points={len(points)}")

            # Prefer PPT's aggregate if available (eBay-by-grade 7d), then fall back to EMA
            agg = None
            if include_ebay and target_grade:
                agg = get_graded_aggregate(card, company=company, grade=target_grade)

            if agg:
                target_price = float(agg["price"])
                method = f"{agg.get('method','agg')} (7d)"
            else:
                if len(points) >= 2:
                    target_price = float(ema_price(points, half_life_days=half_life))
                    method = f"EMA({half_life:.1f}d)"
                elif points:
                    target_price = float(points[-1]["price"])
                    method = "last"
                else:
                    click.echo("  ! no usable history for pricing")
                    continue

            click.echo(f"  ⇒ target price: ${target_price:,.2f}   via {method}")

            # Also show your round+guard rails path for parity with runtime:
            if len(points) >= 2:
                sp = smart_price(points, half_life_days=half_life, rounding=True)
                click.echo(f"  smart_price(rounding=True): ${sp:,.2f}")

            # tail
            if show_tail and len(points) > 0:
                tail = sorted(points, key=lambda r: r["timestamp"])[-show_tail:]
                from datetime import datetime, timezone
                click.echo("  tail used points:")
                for r in tail:
                    dt = datetime.fromtimestamp(r["timestamp"], tz=timezone.utc).date().isoformat()
                    click.echo(f"    {dt}  ${r['price']:.2f}")

            # optional EMA breakdown
            if ema_debug and len(points) >= 2:
                from datetime import datetime, timezone
                pts = sorted(points, key=lambda r: r["timestamp"])
                ema = float(pts[0]["price"])
                prev_t = pts[0]["timestamp"]
                click.echo("  EMA breakdown (date, price, Δdays, α, +contrib, ema_after):")
                for r in pts[1:]:
                    t = r["timestamp"]; p = float(r["price"])
                    delta_days = max(0.0, (t - prev_t)/86400.0)
                    alpha = 1.0 - (0.5 ** (delta_days / half_life)) if half_life > 0 else 1.0
                    new_ema = ema + alpha * (p - ema)
                    dt = datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat()
                    click.echo(f"    {dt}  ${p:.2f}  {delta_days:4.1f}d  α={alpha:0.4f}  +{alpha*(p-ema):7.2f}  → {new_ema:7.2f}")
                    ema, prev_t = new_ema, t
                click.echo(f"  EMA({half_life:.1f}d) result: ${ema:.2f}")

