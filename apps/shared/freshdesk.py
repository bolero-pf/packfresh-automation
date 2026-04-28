"""
Shared Freshdesk API client — ticket search, conversations, replies, canned responses.

Used by screening/ to surface customer verification responses and send templated replies.

Freshdesk API docs: https://developers.freshdesk.com/api/
Auth: HTTP Basic with API key as username, "X" as password.
Rate limits: 200/min (Growth), 400/min (Pro). Returns 429 on throttle.
"""

import os
import time
import base64
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

FRESHDESK_API_KEY = os.environ.get("FRESHDESK_API_KEY", "")
FRESHDESK_DOMAIN = os.environ.get("FRESHDESK_DOMAIN", "")  # e.g. "packfresh.freshdesk.com"
_BASE = f"https://{FRESHDESK_DOMAIN}/api/v2" if FRESHDESK_DOMAIN else ""


class FreshdeskError(RuntimeError):
    pass


def is_configured():
    """Return True if Freshdesk env vars are set."""
    return bool(FRESHDESK_API_KEY and FRESHDESK_DOMAIN)


def _auth_header():
    """HTTP Basic Auth header: API key as username, X as password."""
    token = base64.b64encode(f"{FRESHDESK_API_KEY}:X".encode()).decode()
    return f"Basic {token}"


def _request(method, endpoint, *, json_body=None, params=None, max_tries=3, return_response=False):
    """Low-level HTTP with retry on 429/5xx. Pass return_response=True to get the
    full Response object (needed when callers must inspect headers e.g. Link)."""
    if not is_configured():
        raise FreshdeskError("Freshdesk not configured (missing FRESHDESK_API_KEY or FRESHDESK_DOMAIN)")

    # Allow callers to pass a full URL (e.g. a Link-header next page) instead of an endpoint.
    url = endpoint if endpoint.startswith("http") else f"{_BASE}{endpoint}"
    headers = {
        "Authorization": _auth_header(),
        "Content-Type": "application/json",
    }

    for attempt in range(max_tries):
        try:
            resp = requests.request(method, url, headers=headers, json=json_body, params=params, timeout=15)
        except requests.exceptions.RequestException as e:
            logger.warning("Freshdesk %s %s failed (attempt %d): %s", method, endpoint, attempt + 1, e)
            time.sleep(min(1.0 * (attempt + 1), 3.0))
            continue

        if resp.status_code < 400:
            if return_response:
                return resp
            return resp.json() if resp.text.strip() else {}

        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else 1.0 * (attempt + 1)
            logger.warning("Freshdesk %s %s → %d, retrying in %.1fs", method, endpoint, resp.status_code, wait)
            time.sleep(min(wait, 10.0))
            continue

        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise FreshdeskError(f"Freshdesk {method} {endpoint} → {resp.status_code}: {body}")

    raise FreshdeskError(f"Freshdesk {method} {endpoint} exhausted {max_tries} retries")


def _parse_next_link(link_header):
    """Parse a Link header and return the rel="next" URL, or None."""
    if not link_header:
        return None
    for part in link_header.split(","):
        segs = part.strip().split(";")
        if len(segs) >= 2 and 'rel="next"' in segs[1]:
            return segs[0].strip().strip("<>")
    return None


# ---------------------------------------------------------------------------
# Tickets
# ---------------------------------------------------------------------------

def search_tickets_by_email(email, *, max_pages=3):
    """List tickets where the requester matches the given email, newest first.
    Follows Link-header pagination up to max_pages (300 tickets at per_page=100).

    Returns [] when the email has no matching contact (Freshdesk returns 400 in that case),
    which is the common path for first-time customers who've never opened a ticket.

    Uses /tickets?email= rather than /search/tickets — the search endpoint rejects
    `email` as a filter field, and the list endpoint returns the same shape.
    """
    try:
        resp = _request("GET", "/tickets", params={
            "email": email,
            "per_page": 100,
            "order_by": "updated_at",
            "order_type": "desc",
            "include": "description",
        }, return_response=True)
    except FreshdeskError as e:
        # 400 "no contact matching email" is expected for fresh customers — silent empty.
        if "no contact matching" in str(e).lower():
            return []
        raise

    tickets = resp.json() if resp.text.strip() else []
    if not isinstance(tickets, list):
        return []

    next_url = _parse_next_link(resp.headers.get("Link"))
    pages_fetched = 1
    while next_url and pages_fetched < max_pages:
        try:
            resp = _request("GET", next_url, return_response=True)
        except FreshdeskError as e:
            logger.warning("Freshdesk pagination stopped early for %s: %s", email, e)
            break
        page = resp.json() if resp.text.strip() else []
        if not isinstance(page, list):
            break
        tickets.extend(page)
        next_url = _parse_next_link(resp.headers.get("Link"))
        pages_fetched += 1
    return tickets


def get_ticket(ticket_id, *, include=None):
    """Fetch a single ticket. Returns the `attachments` field by default — the
    list endpoint omits it, so this is how we surface customer-uploaded files
    (PDFs, IDs) on email-source tickets.
    Note: single-ticket GET only accepts include values
    'conversations, requester, company, stats, sla_policy' — NOT 'description'."""
    params = {"include": include} if include else None
    return _request("GET", f"/tickets/{ticket_id}", params=params)


def get_ticket_conversations(ticket_id):
    """Get all conversations (replies) on a ticket. Returns list of conversation dicts."""
    return _request("GET", f"/tickets/{ticket_id}/conversations")


def reply_to_ticket(ticket_id, body_html):
    """Send a reply on a ticket (as agent). body_html is the HTML body."""
    return _request("POST", f"/tickets/{ticket_id}/reply", json_body={"body": body_html})


def resolve_ticket(ticket_id):
    """Set ticket status to Resolved (status=4)."""
    return _request("PUT", f"/tickets/{ticket_id}", json_body={"status": 4})


def reply_and_resolve(ticket_id, body_html):
    """Reply to a ticket and then resolve it. Returns reply result."""
    result = reply_to_ticket(ticket_id, body_html)
    try:
        resolve_ticket(ticket_id)
    except FreshdeskError as e:
        logger.warning("Failed to resolve ticket %s after reply: %s", ticket_id, e)
    return result


# ---------------------------------------------------------------------------
# Canned Responses
# ---------------------------------------------------------------------------

def get_canned_response(response_id):
    """Fetch a single canned response by ID. Returns dict with 'content' (HTML body)."""
    return _request("GET", f"/canned_responses/{response_id}")


def list_canned_response_folders():
    """List all canned response folders. Returns list of folder dicts."""
    return _request("GET", "/canned_response_folders")


def list_canned_responses_in_folder(folder_id):
    """List canned responses in a folder. Returns list of response dicts."""
    return _request("GET", f"/canned_response_folders/{folder_id}/responses")
