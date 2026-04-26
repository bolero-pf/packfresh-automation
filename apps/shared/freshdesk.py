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


def _request(method, endpoint, *, json_body=None, params=None, max_tries=3):
    """Low-level HTTP with retry on 429/5xx."""
    if not is_configured():
        raise FreshdeskError("Freshdesk not configured (missing FRESHDESK_API_KEY or FRESHDESK_DOMAIN)")

    url = f"{_BASE}{endpoint}"
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


# ---------------------------------------------------------------------------
# Tickets
# ---------------------------------------------------------------------------

def search_tickets_by_email(email):
    """List tickets where the requester matches the given email. Returns list of ticket dicts.

    Uses /tickets?email= rather than /search/tickets — the search endpoint rejects
    `email` as a filter field (only requester_id is searchable), and the simpler
    list endpoint returns the same shape we need without a contact-lookup hop.
    """
    data = _request("GET", "/tickets", params={"email": email, "per_page": 30})
    return data if isinstance(data, list) else []


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
