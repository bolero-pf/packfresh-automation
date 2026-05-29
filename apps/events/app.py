"""
events — events.pack-fresh.com
Staff console for managing public events: EventSeries + EventOccurrence
metaobject CRUD, weekly occurrence generator, FB URL workflow, hero image upload.

Source of truth: Shopify metaobjects (types `event` and `event_occurence`).
No local DB.
"""

import os
import json
import logging
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify, render_template_string, redirect

import events_shopify as sc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STORE_TZ = ZoneInfo("America/Chicago")

app = Flask(__name__)

from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner", "manager"], public_prefixes=())


# ---------- Helpers ----------

def _iso_from_local(date_str: str, time_str: str) -> str:
    """Combine 'YYYY-MM-DD' + 'HH:MM' (24h) in store TZ → ISO 8601 string with offset."""
    if not date_str:
        raise ValueError("date required")
    t = time_str or "19:00"
    dt = datetime.fromisoformat(f"{date_str}T{t}:00").replace(tzinfo=STORE_TZ)
    return dt.isoformat()


def _to_store_local(iso_str: str) -> dict:
    """Parse a Shopify-returned datetime back into store-local components."""
    if not iso_str:
        return {"date": "", "time": ""}
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(STORE_TZ)
        return {
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "iso": iso_str,
        }
    except Exception:
        return {"date": "", "time": "", "iso": iso_str}


def _series_summary(s: dict) -> dict:
    """Trim a series dict for client consumption."""
    hero = s.get("hero_image") or {}

    # entry_cost comes back from Shopify Money field as JSON string
    # like {"amount":"5.00","currency_code":"USD"}. Extract just the amount
    # so the <input type="number"> can render it cleanly.
    raw_cost = s.get("entry_cost")
    cost_amount = ""
    if raw_cost:
        try:
            cost_data = json.loads(raw_cost) if isinstance(raw_cost, str) else raw_cost
            if isinstance(cost_data, dict):
                cost_amount = str(cost_data.get("amount") or "")
            else:
                cost_amount = str(raw_cost)
        except Exception:
            cost_amount = str(raw_cost)

    return {
        "id": s.get("id"),
        "handle": s.get("handle"),
        "title": s.get("title") or "",
        "color": s.get("color") or "",
        # Null status = active (not draft). Only explicit "draft" hides a series.
        "status": s.get("status") or "active",
        "schedule_description": s.get("schedule_description") or "",
        "entry_cost": cost_amount,
        "description_short": s.get("description_short") or "",
        "description_long_plain": sc.rich_to_plain_text(s.get("description_long") or ""),
        "hero_image_id": hero.get("id"),
        "hero_image_url": hero.get("url"),
    }


def _occurrence_summary(o: dict) -> dict:
    series_ref = o.get("series") or {}
    series_fields = {}
    # `series` reference from GraphQL includes nested fields when resolved
    raw = o.get("series")
    if isinstance(raw, dict):
        # We stored just {id,handle,type} in our parser; for richer detail
        # the UI fetches series list separately and joins by ID. Still expose ID/handle.
        pass
    start = _to_store_local(o.get("start_datetime", ""))
    end = _to_store_local(o.get("end_datetime", ""))
    return {
        "id": o.get("id"),
        "handle": o.get("handle"),
        "series_id": o.get("series_id") or series_ref.get("id"),
        "series_handle": o.get("series_handle") or series_ref.get("handle"),
        "label": o.get("label", ""),
        "start_date": start["date"],
        "start_time": start["time"],
        "start_iso": o.get("start_datetime", ""),
        "end_date": end["date"],
        "end_time": end["time"],
        "fb_event_url": o.get("fb_event_url", ""),
        "cancelled": bool(o.get("cancelled", False)),
    }


# ---------- Routes ----------

@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "events"})


@app.route("/")
def index():
    return render_template_string(CONSOLE_HTML)


# ----- Series API -----

@app.route("/api/series", methods=["GET"])
def api_list_series():
    try:
        items = sc.list_series()
        return jsonify({"series": [_series_summary(s) for s in items]})
    except Exception as e:
        logger.exception("list_series failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/series", methods=["POST"])
def api_create_series():
    body = request.get_json(silent=True) or {}
    fields = _series_fields_from_body(body)
    if not fields.get("title"):
        return jsonify({"error": "title required"}), 400
    try:
        result = sc.create_series(fields)
        return jsonify({"ok": True, "id": result.get("id"), "handle": result.get("handle")})
    except Exception as e:
        logger.exception("create_series failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/series/save", methods=["POST"])
def api_update_series():
    body = request.get_json(silent=True) or {}
    gid = body.get("id")
    if not gid:
        return jsonify({"error": "id required"}), 400
    fields = _series_fields_from_body(body)
    try:
        result = sc.update_series(gid, fields)
        return jsonify({"ok": True, "id": result.get("id"), "handle": result.get("handle")})
    except Exception as e:
        logger.exception("update_series failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/series/delete", methods=["POST"])
def api_delete_series():
    body = request.get_json(silent=True) or {}
    gid = body.get("id")
    if not gid:
        return jsonify({"error": "id required"}), 400
    try:
        sc.delete_metaobject(gid)
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("delete_series failed")
        return jsonify({"error": str(e)}), 500


def _series_fields_from_body(body: dict) -> dict:
    """Translate the form/JSON payload into metaobject field input shape."""
    out = {}
    for k in ("title", "color", "status", "schedule_description",
              "description_short", "hero_image"):
        if k in body and body[k] is not None:
            out[k] = body[k]
    # Money field: store cents as decimal string in metaobject Money input
    # Shopify Money expects {"amount":"5.00","currency_code":"USD"}
    if "entry_cost" in body and body["entry_cost"] not in (None, ""):
        amount = str(body["entry_cost"]).strip()
        if amount:
            out["entry_cost"] = {"amount": amount, "currency_code": "USD"}
    # Description long: convert plain text to rich text JSON
    if "description_long_plain" in body:
        out["description_long"] = sc.plain_text_to_rich(body["description_long_plain"] or "")
    return out


# ----- Occurrence API -----

@app.route("/api/occurrences", methods=["GET"])
def api_list_occurrences():
    try:
        items = sc.list_occurrences()
        return jsonify({"occurrences": [_occurrence_summary(o) for o in items]})
    except Exception as e:
        logger.exception("list_occurrences failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/occurrences", methods=["POST"])
def api_create_occurrence():
    body = request.get_json(silent=True) or {}
    fields = _occurrence_fields_from_body(body)
    if not fields.get("series") or not fields.get("start_datetime"):
        return jsonify({"error": "series + start_datetime required"}), 400
    try:
        result = sc.create_occurrence(fields)
        return jsonify({"ok": True, "id": result.get("id")})
    except Exception as e:
        logger.exception("create_occurrence failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/occurrences/save", methods=["POST"])
def api_update_occurrence():
    body = request.get_json(silent=True) or {}
    gid = body.get("id")
    if not gid:
        return jsonify({"error": "id required"}), 400
    fields = _occurrence_fields_from_body(body)
    try:
        result = sc.update_occurrence(gid, fields)
        return jsonify({"ok": True, "id": result.get("id")})
    except Exception as e:
        logger.exception("update_occurrence failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/occurrences/delete", methods=["POST"])
def api_delete_occurrence():
    body = request.get_json(silent=True) or {}
    gid = body.get("id")
    if not gid:
        return jsonify({"error": "id required"}), 400
    try:
        sc.delete_metaobject(gid)
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("delete_occurrence failed")
        return jsonify({"error": str(e)}), 500


def _occurrence_fields_from_body(body: dict) -> dict:
    out = {}
    if body.get("series_id"):
        out["series"] = body["series_id"]
    sd = body.get("start_date")
    st = body.get("start_time")
    if sd:
        out["start_datetime"] = _iso_from_local(sd, st or "19:00")
    ed = body.get("end_date") or body.get("start_date")
    et = body.get("end_time")
    if ed and et:
        out["end_datetime"] = _iso_from_local(ed, et)
    if "fb_event_url" in body:
        out["fb_event_url"] = body.get("fb_event_url") or ""
    if "cancelled" in body:
        out["cancelled"] = bool(body.get("cancelled"))
    if body.get("label"):
        out["label"] = body["label"]
    return out


# ----- Bulk generator -----

@app.route("/api/series/generate", methods=["POST"])
def api_generate_occurrences():
    """Bulk-create occurrences for a Series.
    Body: {
      series_id: "gid://...",
      series_title: "Commander Thursdays",
      start_date: "2026-05-28",
      start_time: "19:00",
      end_time: "22:00" (optional),
      day_of_week: 4 (0=Mon..6=Sun, or null to use start_date as-is),
      count: 12,
      step: "weekly" | "monthly"
    }
    """
    body = request.get_json(silent=True) or {}
    series_id = body.get("series_id")
    series_title = (body.get("series_title") or "").strip() or "Event"
    start_date_s = body.get("start_date")
    start_time_s = body.get("start_time") or "19:00"
    end_time_s = body.get("end_time") or ""
    dow = body.get("day_of_week")  # 0..6 or None
    count = int(body.get("count") or 0)
    step = (body.get("step") or "weekly").lower()

    if not series_id or not start_date_s or count <= 0:
        return jsonify({"error": "series_id, start_date, count required"}), 400
    if count > 52:
        return jsonify({"error": "count must be <= 52"}), 400

    try:
        start_d = date.fromisoformat(start_date_s)
    except Exception:
        return jsonify({"error": "start_date must be YYYY-MM-DD"}), 400

    # Snap to chosen day of week if provided
    if isinstance(dow, int) and 0 <= dow <= 6:
        # Python weekday(): Mon=0..Sun=6 (matches our 0=Mon convention)
        delta = (dow - start_d.weekday()) % 7
        start_d = start_d + timedelta(days=delta)

    created = []
    failed = []
    cur = start_d
    for i in range(count):
        date_iso = cur.strftime("%Y-%m-%d")
        label = f"{series_title} — {date_iso}"
        fields = {
            "series": series_id,
            "start_datetime": _iso_from_local(date_iso, start_time_s),
            "label": label,
            "cancelled": False,
        }
        if end_time_s:
            fields["end_datetime"] = _iso_from_local(date_iso, end_time_s)
        try:
            result = sc.create_occurrence(fields)
            created.append({"id": result.get("id"), "date": date_iso})
        except Exception as e:
            logger.warning("generator: failed to create %s — %s", date_iso, e)
            failed.append({"date": date_iso, "error": str(e)})

        # Advance
        if step == "monthly":
            # Add 28 days for simplicity (no calendar-aware monthly for v1)
            cur = cur + timedelta(days=28)
        else:
            cur = cur + timedelta(days=7)

    return jsonify({"ok": True, "created": created, "failed": failed})


# ----- Image upload -----

@app.route("/api/upload", methods=["POST"])
def api_upload_image():
    f = request.files.get("image")
    if not f:
        return jsonify({"error": "image file required (multipart field 'image')"}), 400
    filename = f.filename or "upload.jpg"
    mime = f.mimetype or "image/jpeg"
    data = f.read()
    if not data:
        return jsonify({"error": "empty file"}), 400
    if len(data) > 10 * 1024 * 1024:
        return jsonify({"error": "file too large (max 10MB)"}), 400
    try:
        gid = sc.upload_file_to_shopify(data, filename, mime)
        return jsonify({"ok": True, "file_id": gid})
    except Exception as e:
        logger.exception("image upload failed")
        return jsonify({"error": str(e)}), 500


# ---------- Console HTML ----------

CONSOLE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Events · Common Lands</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
  :root {
    --bg: #0f1014;
    --surface: #1a1b23;
    --surface-2: #22232d;
    --border: #2a2b35;
    --text: #e4e4e7;
    --text-dim: #a1a1aa;
    --text-faint: #71717a;
    --accent: #f59e0b;
    --accent-dim: rgba(245,158,11,0.15);
    --green: #22c55e;
    --red: #ef4444;
    --blue: #3b82f6;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: 'DM Sans', system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
  }
  .header {
    padding: 16px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 16px;
    background: var(--surface);
  }
  .header h1 { font-size: 1.25rem; font-weight: 700; margin: 0; }
  .tabs {
    display: flex;
    gap: 4px;
    border-bottom: 1px solid var(--border);
    padding: 0 24px;
    background: var(--surface);
  }
  .tab {
    padding: 12px 18px;
    cursor: pointer;
    color: var(--text-dim);
    border-bottom: 2px solid transparent;
    font-weight: 600;
    font-size: 14px;
  }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .pane { display: none; padding: 24px; }
  .pane.active { display: block; }
  .toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    margin-bottom: 20px;
    flex-wrap: wrap;
  }
  .btn {
    background: var(--surface-2);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  .btn:hover { background: #2e2f3a; }
  .btn-primary { background: var(--accent); color: #0f1014; border-color: var(--accent); }
  .btn-primary:hover { background: #fbbf24; }
  .btn-ghost { background: transparent; }
  .btn-danger { background: rgba(239,68,68,0.12); color: var(--red); border-color: rgba(239,68,68,0.3); }
  .btn-danger:hover { background: rgba(239,68,68,0.2); }
  .btn-sm { padding: 5px 10px; font-size: 12px; }

  /* Series list */
  .series-list {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 16px;
  }
  .series-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  .series-hero {
    aspect-ratio: 16/9;
    background: var(--surface-2);
    background-size: cover;
    background-position: center;
    position: relative;
  }
  .series-color-strip {
    height: 4px;
  }
  .series-body { padding: 14px 16px; flex: 1; display: flex; flex-direction: column; }
  .series-title { font-size: 17px; font-weight: 700; margin: 0 0 4px; }
  .series-schedule { color: var(--text-dim); font-size: 13px; margin-bottom: 8px; }
  .series-meta { color: var(--text-faint); font-size: 12px; margin-bottom: 10px; }
  .series-status {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .series-status.active { background: rgba(34,197,94,0.15); color: var(--green); }
  .series-status.draft { background: rgba(113,113,122,0.15); color: var(--text-dim); }
  .series-actions { display: flex; gap: 6px; margin-top: auto; }

  /* Modal */
  .modal-overlay {
    position: fixed; inset: 0;
    background: rgba(0,0,0,0.7);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 100;
    padding: 20px;
    overflow-y: auto;
  }
  .modal-overlay.active { display: flex; }
  .modal {
    background: var(--surface);
    border-radius: 16px;
    width: 100%;
    max-width: 640px;
    max-height: 90vh;
    overflow-y: auto;
    border: 1px solid var(--border);
  }
  .modal-header {
    padding: 16px 20px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .modal-header h2 { margin: 0; font-size: 18px; font-weight: 700; }
  .modal-body { padding: 20px; }
  .modal-footer {
    padding: 16px 20px;
    border-top: 1px solid var(--border);
    display: flex;
    justify-content: flex-end;
    gap: 10px;
  }
  .modal-close {
    background: none; border: none; color: var(--text-dim);
    cursor: pointer; font-size: 22px; padding: 0;
  }

  /* Form */
  .form-row { margin-bottom: 16px; }
  .form-row label {
    display: block;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-dim);
    margin-bottom: 6px;
  }
  .form-row input[type=text],
  .form-row input[type=url],
  .form-row input[type=number],
  .form-row input[type=date],
  .form-row input[type=time],
  .form-row select,
  .form-row textarea {
    width: 100%;
    padding: 9px 12px;
    background: var(--surface-2);
    border: 1px solid var(--border);
    color: var(--text);
    border-radius: 8px;
    font-family: inherit;
    font-size: 14px;
  }
  .form-row input:focus,
  .form-row select:focus,
  .form-row textarea:focus {
    outline: none;
    border-color: var(--accent);
  }
  .form-row textarea { resize: vertical; min-height: 80px; }
  .form-row.split { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .form-row.split label { grid-column: span 2; margin-bottom: 0; }
  .form-row input[type=color] {
    width: 60px; height: 40px;
    background: var(--surface-2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 4px;
    cursor: pointer;
  }
  .form-row.checkbox { display: flex; align-items: center; gap: 10px; }
  .form-row.checkbox label { margin: 0; text-transform: none; letter-spacing: 0; font-size: 14px; color: var(--text); font-weight: 500; }
  .form-help { color: var(--text-faint); font-size: 12px; margin-top: 4px; }

  /* Image drop zone */
  .image-drop {
    position: relative;
    border: 2px dashed var(--border);
    border-radius: 10px;
    padding: 20px;
    text-align: center;
    cursor: pointer;
    transition: border-color 0.15s, background 0.15s;
    background: var(--surface-2);
  }
  .image-drop:hover, .image-drop.dragover { border-color: var(--accent); background: rgba(245,158,11,0.05); }
  .image-drop input[type=file] { display: none; }
  .image-drop-preview {
    width: 100%;
    aspect-ratio: 16/9;
    background-size: cover;
    background-position: center;
    border-radius: 8px;
    margin-bottom: 10px;
  }
  .image-drop-text { color: var(--text-dim); font-size: 13px; }

  /* Calendar */
  .cal-toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 16px; }
  .cal-month { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 16px; margin-bottom: 16px; }
  .cal-month-title { font-size: 18px; font-weight: 700; margin: 0 0 14px; }
  .cal-grid {
    display: grid;
    grid-template-columns: repeat(7, minmax(0, 1fr));
    gap: 4px;
  }
  .cal-dow {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-faint);
    text-align: center;
    padding: 6px 0;
  }
  .cal-cell {
    background: #15161d;
    border: 1px solid var(--border);
    border-radius: 8px;
    min-height: 88px;
    padding: 5px;
    display: flex;
    flex-direction: column;
    gap: 3px;
    min-width: 0;
    overflow: hidden;
    position: relative;
    cursor: pointer;
    transition: background 0.12s;
  }
  .cal-cell:hover { background: #1a1b23; }
  .cal-cell.is-other-month { opacity: 0.3; cursor: default; }
  .cal-cell.is-past { opacity: 0.55; }
  .cal-cell.is-today {
    border-color: var(--accent);
    background: rgba(245,158,11,0.05);
  }
  .cal-daynum {
    font-size: 11px;
    font-weight: 600;
    color: var(--text-faint);
  }
  .cal-cell.is-today .cal-daynum { color: var(--accent); }
  .cal-chip {
    display: block;
    padding: 3px 6px;
    border-radius: 5px;
    font-size: 11px;
    font-weight: 600;
    line-height: 1.3;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
    border: 1px solid transparent;
  }
  .cal-chip:hover { filter: brightness(1.25); }
  .cal-chip.cancelled { opacity: 0.5; text-decoration: line-through; }
  .cal-add-hint { display: none; font-size: 10px; color: var(--text-faint); margin-top: auto; }
  .cal-cell:not(.is-other-month):hover .cal-add-hint { display: block; }

  /* Toast */
  #toast {
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 12px 18px;
    border-radius: 8px;
    color: var(--text);
    font-size: 14px;
    z-index: 200;
    display: none;
    box-shadow: 0 4px 20px rgba(0,0,0,0.5);
  }
  #toast.success { border-color: var(--green); color: var(--green); }
  #toast.error { border-color: var(--red); color: var(--red); }

  .empty-state {
    text-align: center;
    color: var(--text-faint);
    padding: 60px 20px;
    font-size: 15px;
  }
  .loading { text-align: center; padding: 40px; color: var(--text-faint); }
  .spinner {
    width: 24px; height: 24px;
    border: 3px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    display: inline-block;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>

<div class="header">
  <h1>Events</h1>
  <div style="margin-left:auto; display:flex; gap:10px;">
    <a class="btn btn-sm" href="https://common-lands.com/pages/events" target="_blank">View public calendar &rarr;</a>
  </div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="series">Event Series</div>
  <div class="tab" data-tab="calendar">Calendar</div>
</div>

<div class="pane active" id="pane-series">
  <div class="toolbar">
    <div style="color:var(--text-dim); font-size:14px;">Define each recurring event program. Each Series gets its own SEO-friendly landing page.</div>
    <button class="btn btn-primary" onclick="openSeriesModal()">+ New Series</button>
  </div>
  <div id="series-list" class="series-list">
    <div class="loading"><div class="spinner"></div></div>
  </div>
</div>

<div class="pane" id="pane-calendar">
  <div class="toolbar">
    <div style="color:var(--text-dim); font-size:14px;">Click any date to add an occurrence. Click an existing chip to edit it.</div>
    <button class="btn" onclick="openGeneratorModal()">Generate occurrences in bulk</button>
  </div>
  <div id="calendar-root">
    <div class="loading"><div class="spinner"></div></div>
  </div>
</div>

<!-- Series modal -->
<div class="modal-overlay" id="series-modal">
  <div class="modal">
    <div class="modal-header">
      <h2 id="series-modal-title">New Series</h2>
      <button class="modal-close" onclick="closeModal('series-modal')">&times;</button>
    </div>
    <div class="modal-body">
      <form id="series-form" onsubmit="return false;">
        <input type="hidden" id="series-id">
        <div class="form-row">
          <label>Title</label>
          <input type="text" id="f-title" placeholder="Commander Thursdays" required>
        </div>
        <div class="form-row split">
          <label style="grid-column: span 2;">Color &amp; Status</label>
          <input type="color" id="f-color" value="#f59e0b">
          <select id="f-status">
            <option value="active">Active (visible)</option>
            <option value="draft">Draft (hidden)</option>
          </select>
        </div>
        <div class="form-row">
          <label>Schedule (human-readable)</label>
          <input type="text" id="f-schedule" placeholder="Thursdays at 7pm">
          <div class="form-help">Shown on the public landing page as the "when" line. Not parsed.</div>
        </div>
        <div class="form-row">
          <label>Entry cost</label>
          <input type="number" id="f-entry-cost" step="0.01" placeholder="0.00">
          <div class="form-help">Leave at 0 for free entry. In dollars (e.g., 5.00).</div>
        </div>
        <div class="form-row">
          <label>Short description</label>
          <textarea id="f-desc-short" placeholder="One or two sentences shown in cards and meta description." maxlength="500"></textarea>
        </div>
        <div class="form-row">
          <label>Long description</label>
          <textarea id="f-desc-long" placeholder="Detailed body for the landing page. Blank line = new paragraph." style="min-height: 140px;"></textarea>
        </div>
        <div class="form-row">
          <label>Hero image</label>
          <div class="image-drop" id="image-drop">
            <input type="file" id="f-image" accept="image/*">
            <div class="image-drop-preview" id="image-preview" style="display:none;"></div>
            <div class="image-drop-text" id="image-text">Click or drop an image (16:9 recommended, max 10MB)</div>
            <input type="hidden" id="f-image-id">
          </div>
        </div>
      </form>
    </div>
    <div class="modal-footer">
      <button class="btn btn-danger" id="btn-delete-series" onclick="deleteSeries()" style="display:none;">Delete</button>
      <button class="btn btn-ghost" onclick="closeModal('series-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="saveSeries()" id="btn-save-series">Save</button>
    </div>
  </div>
</div>

<!-- Occurrence modal -->
<div class="modal-overlay" id="occurrence-modal">
  <div class="modal">
    <div class="modal-header">
      <h2 id="occurrence-modal-title">New Occurrence</h2>
      <button class="modal-close" onclick="closeModal('occurrence-modal')">&times;</button>
    </div>
    <div class="modal-body">
      <form id="occurrence-form" onsubmit="return false;">
        <input type="hidden" id="occ-id">
        <div class="form-row">
          <label>Series</label>
          <select id="occ-series"></select>
        </div>
        <div class="form-row split">
          <label>Date &amp; start time</label>
          <input type="date" id="occ-date">
          <input type="time" id="occ-start-time">
        </div>
        <div class="form-row">
          <label>End time (optional)</label>
          <input type="time" id="occ-end-time">
        </div>
        <div class="form-row">
          <label>Facebook event URL</label>
          <input type="url" id="occ-fb-url" placeholder="https://facebook.com/events/...">
          <div class="form-help">Create the FB Event manually, paste its URL here. Drives the "RSVP on Facebook" button.</div>
        </div>
        <div class="form-row checkbox">
          <input type="checkbox" id="occ-cancelled">
          <label for="occ-cancelled">Cancelled</label>
        </div>
      </form>
    </div>
    <div class="modal-footer">
      <button class="btn btn-danger" id="btn-delete-occ" onclick="deleteOccurrence()" style="display:none;">Delete</button>
      <button class="btn btn-ghost" onclick="closeModal('occurrence-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="saveOccurrence()">Save</button>
    </div>
  </div>
</div>

<!-- Generator modal -->
<div class="modal-overlay" id="generator-modal">
  <div class="modal">
    <div class="modal-header">
      <h2>Generate occurrences</h2>
      <button class="modal-close" onclick="closeModal('generator-modal')">&times;</button>
    </div>
    <div class="modal-body">
      <form id="generator-form" onsubmit="return false;">
        <div class="form-row">
          <label>Series</label>
          <select id="gen-series"></select>
        </div>
        <div class="form-row split">
          <label>Start date &amp; pattern</label>
          <input type="date" id="gen-start-date">
          <select id="gen-step">
            <option value="weekly">Weekly</option>
            <option value="monthly">Monthly (every 4 weeks)</option>
          </select>
        </div>
        <div class="form-row">
          <label>Day of week</label>
          <select id="gen-dow">
            <option value="">(use start date as-is)</option>
            <option value="0">Monday</option>
            <option value="1">Tuesday</option>
            <option value="2">Wednesday</option>
            <option value="3">Thursday</option>
            <option value="4">Friday</option>
            <option value="5">Saturday</option>
            <option value="6">Sunday</option>
          </select>
          <div class="form-help">If set, snaps the start date forward to the next matching weekday.</div>
        </div>
        <div class="form-row split">
          <label>Start time &amp; end time</label>
          <input type="time" id="gen-start-time" value="19:00">
          <input type="time" id="gen-end-time" value="22:00">
        </div>
        <div class="form-row">
          <label>Number of occurrences</label>
          <input type="number" id="gen-count" value="8" min="1" max="52">
        </div>
      </form>
    </div>
    <div class="modal-footer">
      <button class="btn btn-ghost" onclick="closeModal('generator-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="runGenerator()" id="btn-run-gen">Generate</button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
// ---------- State ----------
let state = {
  series: [],
  occurrences: [],
};

function toast(msg, kind) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = kind || '';
  el.style.display = 'block';
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { el.style.display = 'none'; }, 3500);
}

function fmt(num) { return num.toString().padStart(2, '0'); }

function ymdToday() {
  const d = new Date();
  return d.getFullYear() + '-' + fmt(d.getMonth()+1) + '-' + fmt(d.getDate());
}

function timeStr(hhmm) {
  if (!hhmm) return '';
  const [h, m] = hhmm.split(':').map(Number);
  const hh = ((h + 11) % 12) + 1;
  const ap = h < 12 ? 'a' : 'p';
  return m === 0 ? `${hh}${ap}` : `${hh}:${fmt(m)}${ap}`;
}

// ---------- Tabs ----------
document.querySelectorAll('.tab').forEach(t => {
  t.onclick = () => {
    document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
    t.classList.add('active');
    document.getElementById('pane-' + t.dataset.tab).classList.add('active');
    if (t.dataset.tab === 'calendar') { renderCalendar(); }
  };
});

// ---------- Modals ----------
function openModal(id) { document.getElementById(id).classList.add('active'); }
function closeModal(id) { document.getElementById(id).classList.remove('active'); }
document.querySelectorAll('.modal-overlay').forEach(o => {
  o.addEventListener('click', e => { if (e.target === o) o.classList.remove('active'); });
});

// ---------- Series ----------
async function loadSeries() {
  try {
    const r = await fetch('/api/series');
    let d;
    try { d = await r.json(); } catch (parseErr) {
      throw new Error('Server returned non-JSON (status ' + r.status + '). Check Railway logs.');
    }
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
    state.series = d.series || [];
    renderSeriesList();
    populateSeriesSelects();
  } catch (e) {
    document.getElementById('series-list').innerHTML =
      '<div class="empty-state" style="color:var(--red);">Failed to load series: ' +
      escapeHtml(e.message) +
      '<div style="margin-top:12px; font-size:13px; color:var(--text-faint);">Common causes: SHOPIFY_TOKEN / SHOPIFY_STORE env vars missing on Railway, or token lacks read_metaobjects scope.</div></div>';
    toast(e.message, 'error');
  }
}

function renderSeriesList() {
  const root = document.getElementById('series-list');
  if (!state.series.length) {
    root.innerHTML = '<div class="empty-state">No series yet. Click "+ New Series" to create your first one.</div>';
    return;
  }
  root.innerHTML = state.series.map(s => {
    const color = s.color || '#f59e0b';
    const heroStyle = s.hero_image_url
      ? `background-image: url('${s.hero_image_url}');`
      : '';
    return `
      <div class="series-card">
        <div class="series-color-strip" style="background:${color};"></div>
        <div class="series-hero" style="${heroStyle}"></div>
        <div class="series-body">
          <div class="series-title">${escapeHtml(s.title || '(untitled)')}</div>
          <div class="series-schedule">${escapeHtml(s.schedule_description || '')}</div>
          <div class="series-meta">
            <span class="series-status ${s.status || 'draft'}">${s.status || 'draft'}</span>
            ${s.handle ? ` &middot; /pages/events/${escapeHtml(s.handle)}` : ''}
          </div>
          <div class="series-actions">
            <button class="btn btn-sm" onclick='openSeriesModal(${JSON.stringify(s.id)})'>Edit</button>
            ${s.handle ? `<a class="btn btn-sm btn-ghost" target="_blank" href="https://common-lands.com/pages/events/${encodeURIComponent(s.handle)}">View</a>` : ''}
          </div>
        </div>
      </div>
    `;
  }).join('');
}

function openSeriesModal(id) {
  document.getElementById('series-id').value = id || '';
  document.getElementById('btn-delete-series').style.display = id ? '' : 'none';
  document.getElementById('series-modal-title').textContent = id ? 'Edit Series' : 'New Series';
  if (id) {
    const s = state.series.find(x => x.id === id);
    if (!s) { toast('Series not found', 'error'); return; }
    document.getElementById('f-title').value = s.title || '';
    document.getElementById('f-color').value = s.color || '#f59e0b';
    document.getElementById('f-status').value = s.status || 'active';
    document.getElementById('f-schedule').value = s.schedule_description || '';
    document.getElementById('f-entry-cost').value = s.entry_cost || '';
    document.getElementById('f-desc-short').value = s.description_short || '';
    document.getElementById('f-desc-long').value = s.description_long_plain || '';
    document.getElementById('f-image-id').value = s.hero_image_id || '';
    if (s.hero_image_url) {
      const p = document.getElementById('image-preview');
      p.style.backgroundImage = `url('${s.hero_image_url}')`;
      p.style.display = 'block';
      document.getElementById('image-text').textContent = 'Click or drop to replace';
    } else {
      document.getElementById('image-preview').style.display = 'none';
      document.getElementById('image-text').textContent = 'Click or drop an image (16:9 recommended, max 10MB)';
    }
  } else {
    document.getElementById('series-form').reset();
    document.getElementById('f-color').value = '#f59e0b';
    document.getElementById('image-preview').style.display = 'none';
    document.getElementById('f-image-id').value = '';
    document.getElementById('image-text').textContent = 'Click or drop an image (16:9 recommended, max 10MB)';
  }
  openModal('series-modal');
}

async function saveSeries() {
  const id = document.getElementById('series-id').value || null;
  const body = {
    title: document.getElementById('f-title').value.trim(),
    color: document.getElementById('f-color').value,
    status: document.getElementById('f-status').value,
    schedule_description: document.getElementById('f-schedule').value.trim(),
    entry_cost: document.getElementById('f-entry-cost').value.trim(),
    description_short: document.getElementById('f-desc-short').value.trim(),
    description_long_plain: document.getElementById('f-desc-long').value,
    hero_image: document.getElementById('f-image-id').value || null,
  };
  if (!body.title) { toast('Title required', 'error'); return; }
  if (id) body.id = id;
  const btn = document.getElementById('btn-save-series');
  btn.disabled = true; btn.textContent = 'Saving…';
  try {
    const url = id ? '/api/series/save' : '/api/series';
    const r = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'save failed');
    toast(id ? 'Series updated' : 'Series created', 'success');
    closeModal('series-modal');
    await loadSeries();
  } catch (e) {
    toast(e.message, 'error');
  } finally {
    btn.disabled = false; btn.textContent = 'Save';
  }
}

async function deleteSeries() {
  const id = document.getElementById('series-id').value;
  if (!id) return;
  if (!confirm('Delete this series? Its occurrences will become orphaned and stop appearing on the public calendar.')) return;
  try {
    const r = await fetch('/api/series/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'delete failed');
    toast('Series deleted', 'success');
    closeModal('series-modal');
    await loadSeries();
  } catch (e) {
    toast(e.message, 'error');
  }
}

// ---------- Image upload ----------
(function setupImageDrop() {
  const drop = document.getElementById('image-drop');
  const input = document.getElementById('f-image');
  drop.onclick = () => input.click();
  drop.ondragover = e => { e.preventDefault(); drop.classList.add('dragover'); };
  drop.ondragleave = () => drop.classList.remove('dragover');
  drop.ondrop = e => {
    e.preventDefault();
    drop.classList.remove('dragover');
    if (e.dataTransfer.files.length) handleImageFile(e.dataTransfer.files[0]);
  };
  input.onchange = () => { if (input.files.length) handleImageFile(input.files[0]); };
})();

async function handleImageFile(file) {
  const text = document.getElementById('image-text');
  const preview = document.getElementById('image-preview');
  text.textContent = 'Uploading…';
  // Local preview immediately
  const reader = new FileReader();
  reader.onload = e => {
    preview.style.backgroundImage = `url('${e.target.result}')`;
    preview.style.display = 'block';
  };
  reader.readAsDataURL(file);

  const fd = new FormData();
  fd.append('image', file);
  try {
    const r = await fetch('/api/upload', {method: 'POST', body: fd});
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'upload failed');
    document.getElementById('f-image-id').value = d.file_id;
    text.textContent = 'Uploaded ✓ (click to replace)';
  } catch (e) {
    text.textContent = 'Upload failed: ' + e.message;
    toast(e.message, 'error');
  }
}

// ---------- Occurrences ----------
async function loadOccurrences() {
  try {
    const r = await fetch('/api/occurrences');
    let d;
    try { d = await r.json(); } catch (parseErr) {
      throw new Error('Server returned non-JSON (status ' + r.status + ').');
    }
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
    state.occurrences = d.occurrences || [];
  } catch (e) {
    state.occurrences = [];
    toast('Failed to load occurrences: ' + e.message, 'error');
  }
}

function populateSeriesSelects() {
  const opts = state.series
    .filter(s => s.status === 'active' || !s.status)
    .map(s => `<option value="${escapeHtml(s.id)}" data-title="${escapeHtml(s.title)}">${escapeHtml(s.title)}</option>`)
    .join('');
  ['occ-series', 'gen-series'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = '<option value="">— select —</option>' + opts;
  });
}

function openOccurrenceModal(id, prefillDate) {
  document.getElementById('occ-id').value = id || '';
  document.getElementById('btn-delete-occ').style.display = id ? '' : 'none';
  document.getElementById('occurrence-modal-title').textContent = id ? 'Edit Occurrence' : 'New Occurrence';
  if (id) {
    const o = state.occurrences.find(x => x.id === id);
    if (!o) { toast('Occurrence not found', 'error'); return; }
    document.getElementById('occ-series').value = o.series_id || '';
    document.getElementById('occ-date').value = o.start_date;
    document.getElementById('occ-start-time').value = o.start_time;
    document.getElementById('occ-end-time').value = o.end_time || '';
    document.getElementById('occ-fb-url').value = o.fb_event_url || '';
    document.getElementById('occ-cancelled').checked = !!o.cancelled;
  } else {
    document.getElementById('occurrence-form').reset();
    document.getElementById('occ-date').value = prefillDate || ymdToday();
    document.getElementById('occ-start-time').value = '19:00';
    document.getElementById('occ-end-time').value = '22:00';
  }
  openModal('occurrence-modal');
}

async function saveOccurrence() {
  const id = document.getElementById('occ-id').value || null;
  const seriesId = document.getElementById('occ-series').value;
  const date = document.getElementById('occ-date').value;
  if (!seriesId) { toast('Series required', 'error'); return; }
  if (!date) { toast('Date required', 'error'); return; }
  const seriesOpt = document.querySelector(`#occ-series option[value="${CSS.escape(seriesId)}"]`);
  const seriesTitle = seriesOpt ? seriesOpt.dataset.title : 'Event';
  const body = {
    series_id: seriesId,
    start_date: date,
    start_time: document.getElementById('occ-start-time').value || '19:00',
    end_date: date,
    end_time: document.getElementById('occ-end-time').value || '',
    fb_event_url: document.getElementById('occ-fb-url').value.trim(),
    cancelled: document.getElementById('occ-cancelled').checked,
    label: `${seriesTitle} — ${date}`,
  };
  if (id) body.id = id;
  try {
    const url = id ? '/api/occurrences/save' : '/api/occurrences';
    const r = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'save failed');
    toast(id ? 'Occurrence updated' : 'Occurrence created', 'success');
    closeModal('occurrence-modal');
    await loadOccurrences();
    renderCalendar();
  } catch (e) {
    toast(e.message, 'error');
  }
}

async function deleteOccurrence() {
  const id = document.getElementById('occ-id').value;
  if (!id) return;
  if (!confirm('Delete this occurrence?')) return;
  try {
    const r = await fetch('/api/occurrences/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'delete failed');
    toast('Occurrence deleted', 'success');
    closeModal('occurrence-modal');
    await loadOccurrences();
    renderCalendar();
  } catch (e) {
    toast(e.message, 'error');
  }
}

// ---------- Generator ----------
function openGeneratorModal() {
  document.getElementById('generator-form').reset();
  document.getElementById('gen-start-date').value = ymdToday();
  document.getElementById('gen-start-time').value = '19:00';
  document.getElementById('gen-end-time').value = '22:00';
  document.getElementById('gen-count').value = 8;
  document.getElementById('gen-step').value = 'weekly';
  openModal('generator-modal');
}

async function runGenerator() {
  const seriesId = document.getElementById('gen-series').value;
  if (!seriesId) { toast('Series required', 'error'); return; }
  const seriesOpt = document.querySelector(`#gen-series option[value="${CSS.escape(seriesId)}"]`);
  const seriesTitle = seriesOpt ? seriesOpt.dataset.title : 'Event';
  const dow = document.getElementById('gen-dow').value;
  const body = {
    series_id: seriesId,
    series_title: seriesTitle,
    start_date: document.getElementById('gen-start-date').value,
    start_time: document.getElementById('gen-start-time').value || '19:00',
    end_time: document.getElementById('gen-end-time').value || '',
    day_of_week: dow === '' ? null : parseInt(dow, 10),
    count: parseInt(document.getElementById('gen-count').value, 10) || 1,
    step: document.getElementById('gen-step').value || 'weekly',
  };
  const btn = document.getElementById('btn-run-gen');
  btn.disabled = true; btn.textContent = 'Generating…';
  try {
    const r = await fetch('/api/series/generate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'generate failed');
    const okCount = (d.created || []).length;
    const failCount = (d.failed || []).length;
    toast(`Created ${okCount} occurrence${okCount===1?'':'s'}${failCount?` (${failCount} failed)`:''}`, failCount ? 'error' : 'success');
    closeModal('generator-modal');
    await loadOccurrences();
    renderCalendar();
  } catch (e) {
    toast(e.message, 'error');
  } finally {
    btn.disabled = false; btn.textContent = 'Generate';
  }
}

// ---------- Calendar ----------
function renderCalendar() {
  if (!state.series.length && !state.occurrences.length) {
    document.getElementById('calendar-root').innerHTML =
      '<div class="empty-state">Create a Series first, then come back to add dates.</div>';
    return;
  }
  const seriesById = {};
  state.series.forEach(s => { seriesById[s.id] = s; });
  const today = new Date();
  const months = [
    {year: today.getFullYear(), month: today.getMonth()},
  ];
  let nextM = today.getMonth() + 1, nextY = today.getFullYear();
  if (nextM > 11) { nextM = 0; nextY++; }
  months.push({year: nextY, month: nextM});

  let html = '';
  for (const m of months) html += renderMonth(m.year, m.month, seriesById);
  document.getElementById('calendar-root').innerHTML = html;

  // Wire up cell + chip clicks
  document.querySelectorAll('.cal-cell[data-date]').forEach(c => {
    c.onclick = e => {
      if (e.target.classList.contains('cal-chip')) return;
      openOccurrenceModal(null, c.dataset.date);
    };
  });
  document.querySelectorAll('.cal-chip[data-occ]').forEach(c => {
    c.onclick = e => { e.stopPropagation(); openOccurrenceModal(c.dataset.occ); };
  });
}

function renderMonth(year, month, seriesById) {
  const monthName = new Date(year, month, 1).toLocaleString('en-US', {month: 'long', year: 'numeric'});
  const firstDow = new Date(year, month, 1).getDay(); // 0=Sun
  const lastDay = new Date(year, month + 1, 0).getDate();
  const todayYmd = ymdToday();

  // Group occurrences by date
  const byDate = {};
  for (const o of state.occurrences) {
    if (!o.start_date) continue;
    (byDate[o.start_date] = byDate[o.start_date] || []).push(o);
  }

  let cells = '';
  for (let i = 0; i < firstDow; i++) cells += '<div class="cal-cell is-other-month"></div>';
  for (let d = 1; d <= lastDay; d++) {
    const ymd = `${year}-${fmt(month+1)}-${fmt(d)}`;
    const isToday = ymd === todayYmd;
    const isPast = ymd < todayYmd;
    const occs = (byDate[ymd] || []);
    occs.sort((a,b) => (a.start_time || '').localeCompare(b.start_time || ''));
    let chips = '';
    for (const o of occs) {
      const s = seriesById[o.series_id] || {};
      const color = s.color || '#f59e0b';
      const title = s.title || '(unknown)';
      chips += `<a class="cal-chip${o.cancelled?' cancelled':''}" data-occ="${escapeAttr(o.id)}"
        style="background:${color}26; border-color:${color}59; color:${color};"
        title="${escapeAttr(title)} · ${escapeAttr(timeStr(o.start_time))}${o.fb_event_url?' · FB ✓':''}">
        <strong>${escapeHtml(timeStr(o.start_time))}</strong> ${escapeHtml(title)}
      </a>`;
    }
    cells += `<div class="cal-cell${isToday?' is-today':''}${isPast?' is-past':''}" data-date="${ymd}">
      <div class="cal-daynum">${d}</div>
      ${chips}
      <div class="cal-add-hint">+ Add</div>
    </div>`;
  }
  const used = firstDow + lastDay;
  const trailing = (7 - (used % 7)) % 7;
  for (let i = 0; i < trailing; i++) cells += '<div class="cal-cell is-other-month"></div>';

  return `
    <div class="cal-month">
      <div class="cal-month-title">${monthName}</div>
      <div class="cal-grid">
        <div class="cal-dow">Sun</div><div class="cal-dow">Mon</div><div class="cal-dow">Tue</div>
        <div class="cal-dow">Wed</div><div class="cal-dow">Thu</div><div class="cal-dow">Fri</div><div class="cal-dow">Sat</div>
        ${cells}
      </div>
    </div>
  `;
}

// ---------- Utils ----------
function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function escapeAttr(s) {
  return String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
}

// ---------- Boot ----------
(async function init() {
  await loadSeries();
  await loadOccurrences();
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
