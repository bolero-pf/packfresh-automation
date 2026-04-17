# vip/discord.py
"""
Discord OAuth2 integration for VIP role assignment.
Links Shopify customers to Discord users, auto-syncs VIP tier roles.

Flow:
  1. Customer gets a link (Klaviyo email, site, or console-generated)
  2. Link redirects to Discord OAuth2 ("Login with Discord")
  3. Discord confirms identity, redirects back with code
  4. We exchange code for Discord user ID, store the mapping
  5. Current VIP tier role is assigned immediately
  6. On every future tier change, write_state() calls sync_discord_role()
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import jwt
import requests as http_requests
from flask import Blueprint, request, redirect, jsonify

from db import query_one, execute

logger = logging.getLogger(__name__)

# ---- CONFIG ----
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID")

DISCORD_ROLE_MAP = {
    "VIP1": os.getenv("DISCORD_ROLE_VIP1"),
    "VIP2": os.getenv("DISCORD_ROLE_VIP2"),
    "VIP3": os.getenv("DISCORD_ROLE_VIP3"),
}

JWT_SECRET = os.getenv("ADMIN_JWT_SECRET")
DISCORD_API = "https://discord.com/api/v10"
REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI",
                         "https://vip.pack-fresh.com/discord/callback")
STORE_DISCORD_PAGE = os.getenv("DISCORD_STORE_PAGE",
                               "https://pack-fresh.com/pages/discord")

bp = Blueprint("discord", __name__, url_prefix="/discord")


# ---- LINK TOKENS ----
# Short-lived signed tokens that encode which Shopify customer is linking.
# Passed as OAuth2 `state` param so we know who they are when Discord redirects back.

def _generate_link_token(customer_gid: str) -> str:
    return jwt.encode({
        "customer_gid": customer_gid,
        "exp": datetime.now(timezone.utc) + timedelta(hours=24),
        "purpose": "discord_link",
    }, JWT_SECRET, algorithm="HS256")


def _decode_link_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        if payload.get("purpose") != "discord_link":
            return None
        return payload.get("customer_gid")
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


# ---- OAUTH2 ROUTES ----

@bp.get("/link")
def discord_link():
    """
    Start Discord OAuth2 flow.
    Customer-facing: ?token=<signed_jwt>  (from Klaviyo email or site)
    Console-facing:  ?customer_id=<gid>   (staff generates link for a customer)
    """
    token = request.args.get("token")
    customer_gid = request.args.get("customer_id")

    if customer_gid and not token:
        token = _generate_link_token(customer_gid)

    if not token:
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=missing_params")

    gid = _decode_link_token(token)
    if not gid:
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=expired")

    params = urlencode({
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "identify",
        "state": token,
    })
    return redirect(f"https://discord.com/oauth2/authorize?{params}")


@bp.get("/callback")
def discord_callback():
    """
    Discord OAuth2 callback.
    Exchanges code for access token, fetches Discord user, stores link, assigns roles.
    """
    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        return redirect(f"{STORE_DISCORD_PAGE}?status=cancelled")

    if not code or not state:
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=missing_params")

    # Decode customer GID from state
    customer_gid = _decode_link_token(state)
    if not customer_gid:
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=expired")

    # Exchange code for access token
    try:
        token_resp = http_requests.post(f"{DISCORD_API}/oauth2/token", data={
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        }, headers={"Content-Type": "application/x-www-form-urlencoded"},
           timeout=10)
        token_data = token_resp.json()
    except Exception as e:
        logger.error(f"Discord token exchange failed: {e}")
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=discord_down")

    access_token = token_data.get("access_token")
    if not access_token:
        logger.error(f"Discord token exchange error: {token_data}")
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=auth_failed")

    # Get Discord user info
    try:
        user_resp = http_requests.get(f"{DISCORD_API}/users/@me", headers={
            "Authorization": f"Bearer {access_token}",
        }, timeout=10)
        user_data = user_resp.json()
    except Exception as e:
        logger.error(f"Discord user fetch failed: {e}")
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=discord_down")

    discord_user_id = user_data.get("id")
    discord_username = user_data.get("username", "")
    discord_global_name = user_data.get("global_name", discord_username)

    if not discord_user_id:
        logger.error(f"Discord user data missing id: {user_data}")
        return redirect(f"{STORE_DISCORD_PAGE}?status=error&reason=profile_error")

    # Check if this customer previously had a different Discord user linked —
    # if so, strip VIP roles from the old Discord user.
    old_link = get_discord_link(customer_gid)
    if old_link and old_link["discord_user_id"] != discord_user_id:
        sync_discord_role(customer_gid, "VIP0", discord_user_id=old_link["discord_user_id"])

    # Store the link (one Discord user = one Shopify customer)
    # Remove any existing link for this Discord user first (they may be re-linking
    # to a different Shopify account), then upsert by Shopify customer.
    execute("DELETE FROM discord_links WHERE discord_user_id = %s AND shopify_customer_gid != %s",
            (discord_user_id, customer_gid))
    execute("""
        INSERT INTO discord_links
            (shopify_customer_gid, discord_user_id, discord_username, linked_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (shopify_customer_gid)
        DO UPDATE SET discord_user_id  = EXCLUDED.discord_user_id,
                      discord_username = EXCLUDED.discord_username,
                      linked_at        = EXCLUDED.linked_at
    """, (customer_gid, discord_user_id, discord_global_name or discord_username,
          datetime.now(timezone.utc)))

    # Sync current VIP tier to Discord roles
    from service import get_customer_state, normalize_tier
    state_data = get_customer_state(customer_gid)
    tier = normalize_tier(state_data.get("tier", "VIP0"))

    sync_discord_role(customer_gid, tier, discord_user_id=discord_user_id)

    display = discord_global_name or discord_username
    return redirect(f"{STORE_DISCORD_PAGE}?status=linked&user={display}&tier={tier}")


# ---- ROLE SYNC ----

def sync_discord_role(customer_gid: str, tier: str, *, discord_user_id: str = None) -> bool:
    """
    Sync a customer's VIP tier to their Discord role.
    Removes old VIP roles, adds the correct one.
    Called from write_state() on every tier change.
    Returns True if sync succeeded (or was skipped), False on error.
    """
    if not DISCORD_BOT_TOKEN or not DISCORD_GUILD_ID:
        return True  # Discord not configured — not an error

    if not discord_user_id:
        try:
            row = query_one(
                "SELECT discord_user_id FROM discord_links WHERE shopify_customer_gid = %s",
                (customer_gid,)
            )
        except Exception:
            return True  # DB not available — skip silently
        if not row:
            return True  # Customer hasn't linked Discord
        discord_user_id = row["discord_user_id"]

    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json",
    }

    ok = True
    for role_tier, role_id in DISCORD_ROLE_MAP.items():
        if not role_id:
            continue
        try:
            if role_tier == tier:
                r = http_requests.put(
                    f"{DISCORD_API}/guilds/{DISCORD_GUILD_ID}/members/"
                    f"{discord_user_id}/roles/{role_id}",
                    headers=headers, timeout=10,
                )
                if r.status_code not in (200, 204):
                    logger.error(f"Discord add role {role_tier} failed: "
                                 f"{r.status_code} {r.text[:200]}")
                    ok = False
            else:
                r = http_requests.delete(
                    f"{DISCORD_API}/guilds/{DISCORD_GUILD_ID}/members/"
                    f"{discord_user_id}/roles/{role_id}",
                    headers=headers, timeout=10,
                )
                # 204=removed, 404=didn't have it — both fine
        except Exception as e:
            logger.error(f"Discord role sync error for {role_tier}: {e}")
            ok = False

    return ok


# ---- HELPERS FOR OTHER CODE ----

def get_discord_link(customer_gid: str) -> dict | None:
    """Check if a customer has linked their Discord."""
    try:
        return query_one(
            "SELECT discord_user_id, discord_username, linked_at "
            "FROM discord_links WHERE shopify_customer_gid = %s",
            (customer_gid,)
        )
    except Exception:
        return None


def generate_link_url(customer_gid: str) -> str:
    """Generate a Discord link URL for a customer (for Klaviyo emails, console, etc.)."""
    token = _generate_link_token(customer_gid)
    base_url = REDIRECT_URI.rsplit("/discord/", 1)[0]
    return f"{base_url}/discord/link?token={token}"


# ---- CONSOLE API ENDPOINTS ----

@bp.get("/api/link-url")
def api_generate_link():
    """Generate a Discord link URL for a customer. Staff-only (JWT protected by app)."""
    customer_gid = request.args.get("customer_id")
    if not customer_gid:
        return jsonify({"error": "customer_id required"}), 400
    return jsonify({"url": generate_link_url(customer_gid)})


@bp.get("/api/status")
def api_link_status():
    """Check if a customer has linked Discord. Public (called from storefront JS)."""
    customer_gid = request.args.get("customer_id")
    if not customer_gid:
        resp = jsonify({"error": "customer_id required"})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp, 400
    link = get_discord_link(customer_gid)
    if link:
        resp = jsonify({
            "linked": True,
            "discord_username": link["discord_username"],
            "linked_at": link["linked_at"].isoformat() if link["linked_at"] else None,
        })
    else:
        resp = jsonify({"linked": False})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


