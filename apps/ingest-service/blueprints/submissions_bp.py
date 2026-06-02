"""Public "sell to us" form intake + staff review.

Phase A: capture leads from the Shopify /pages/sell-to-us form (name,
contact, a Collectr link and/or an uploaded spreadsheet) into the
sell_submissions table, and give staff a review page. Turning a submission
into an intake session is manual for now — a Collectr link has to be
fetched/extracted and an arbitrary spreadsheet column-mapped, so staff
paste the link or CSV into the New Intake tab themselves.

Auth note: ONLY `/api/sell-to-us` is public (registered in app.py's
register_auth_hooks public_paths). The `/submissions` page and the
`/api/sell-to-us/<id>/...` staff endpoints stay JWT-gated because they are
longer paths that don't exact-match the public entry.
"""
import logging

from flask import Blueprint, request, jsonify, render_template, Response, abort

import db

logger = logging.getLogger("intake.submissions")

bp = Blueprint("submissions", __name__)

# Inline-stored uploads are capped — Collectr/CSV exports are tiny; this is
# just a guard against someone posting a huge file to a public endpoint.
MAX_FILE_BYTES = 8 * 1024 * 1024  # 8 MB
ALLOWED_EXT = (".csv", ".tsv", ".txt", ".xlsx", ".xls", ".numbers", ".ods")
VALID_STATUSES = ("new", "contacted", "converted", "archived")


# ── Public: form submission ──────────────────────────────────────────────
@bp.route("/api/sell-to-us", methods=["POST"])
def submit_sell_to_us():
    """PUBLIC (no auth) — the Shopify storefront form posts multipart here."""
    # Honeypot: bots fill the hidden `website` field. Pretend success, store
    # nothing — never tip off the bot that it was caught.
    if (request.form.get("website") or "").strip():
        return jsonify({"success": True})

    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    collectr_link = (request.form.get("collectr_link") or "").strip()
    message = (request.form.get("message") or "").strip()

    if not name or not email:
        return jsonify({"error": "Name and email are required."}), 400
    if "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"error": "Please enter a valid email address."}), 400

    file = request.files.get("file")
    file_name = file_mime = file_bytes = file_size = None
    if file and file.filename:
        fn = file.filename
        if not fn.lower().endswith(ALLOWED_EXT):
            return jsonify({"error": "Upload a spreadsheet — .csv, .xlsx, .xls, .tsv or .txt."}), 400
        data = file.read()
        if len(data) > MAX_FILE_BYTES:
            return jsonify({"error": "File too large — keep it under 8 MB."}), 400
        if data:
            file_bytes = data
            file_name = fn[:300]
            file_mime = (file.mimetype or "application/octet-stream")[:150]
            file_size = len(data)

    if not collectr_link and not file_bytes:
        return jsonify({"error": "Add a Collectr link or upload a spreadsheet so we can review your collection."}), 400

    # Behind Railway's proxy the real client is the leftmost X-Forwarded-For.
    fwd = request.headers.get("X-Forwarded-For", "")
    client_ip = fwd.split(",")[0].strip() if fwd else request.remote_addr

    try:
        db.execute(
            """
            INSERT INTO sell_submissions
                (name, email, phone, collectr_link, message,
                 file_name, file_mime, file_size, file_bytes,
                 created_ip, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                name[:200], email[:320], phone[:50] or None,
                collectr_link or None, message or None,
                file_name, file_mime, file_size, file_bytes,
                client_ip or None, (request.headers.get("User-Agent") or "")[:500] or None,
            ),
        )
    except Exception as e:
        logger.exception("sell_submissions insert failed")
        return jsonify({"error": "Could not save your submission. Please try again."}), 500

    return jsonify({"success": True})


# ── Staff: review page + API (JWT-gated by register_auth_hooks) ───────────
@bp.route("/submissions", methods=["GET"])
def submissions_page():
    return render_template("sell_submissions.html")


@bp.route("/api/sell-to-us/list", methods=["GET"])
def list_submissions():
    status = (request.args.get("status") or "").strip()
    params = None
    where = ""
    if status in VALID_STATUSES:
        where = "WHERE status = %s"
        params = (status,)

    rows = db.query(
        f"""
        SELECT id, name, email, phone, collectr_link, message,
               file_name, file_mime, file_size,
               status, staff_notes, created_at
        FROM sell_submissions
        {where}
        ORDER BY created_at DESC
        LIMIT 300
        """,
        params,
    )
    out = []
    for r in rows:
        d = dict(r)
        d["id"] = str(d["id"])
        d["created_at"] = d["created_at"].isoformat() if d.get("created_at") else None
        d["has_file"] = bool(d.get("file_name"))
        out.append(d)

    counts = {row["status"]: row["n"] for row in db.query(
        "SELECT status, COUNT(*) AS n FROM sell_submissions GROUP BY status")}
    counts["all"] = sum(counts.values())
    return jsonify({"submissions": out, "counts": counts})


@bp.route("/api/sell-to-us/<sub_id>/file", methods=["GET"])
def download_submission_file(sub_id):
    try:
        row = db.query_one(
            "SELECT file_name, file_mime, file_bytes FROM sell_submissions WHERE id = %s",
            (sub_id,),
        )
    except Exception:
        abort(404)
    if not row or not row.get("file_bytes"):
        abort(404)
    fn = (row.get("file_name") or "submission.csv").replace('"', "")
    return Response(
        bytes(row["file_bytes"]),
        mimetype=row.get("file_mime") or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@bp.route("/api/sell-to-us/<sub_id>/status", methods=["POST"])
def update_submission_status(sub_id):
    data = request.get_json(silent=True) or {}
    new_status = (data.get("status") or "").strip()
    if new_status not in VALID_STATUSES:
        return jsonify({"error": "invalid status"}), 400
    try:
        n = db.execute("UPDATE sell_submissions SET status = %s WHERE id = %s", (new_status, sub_id))
    except Exception:
        return jsonify({"error": "not found"}), 404
    if not n:
        return jsonify({"error": "not found"}), 404
    return jsonify({"success": True, "status": new_status})


@bp.route("/api/sell-to-us/<sub_id>/notes", methods=["POST"])
def update_submission_notes(sub_id):
    data = request.get_json(silent=True) or {}
    notes = (data.get("notes") or "").strip() or None
    try:
        db.execute("UPDATE sell_submissions SET staff_notes = %s WHERE id = %s", (notes, sub_id))
    except Exception:
        return jsonify({"error": "not found"}), 404
    return jsonify({"success": True})
