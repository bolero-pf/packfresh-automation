"""
Shared JWT authentication middleware for Pack Fresh staff services.

Issues JWT tokens from the admin service, validates them across all
subdomains via a cookie scoped to .pack-fresh.com.

Usage in a service:
    from auth import require_auth, get_current_user

    @app.before_request
    def check_auth():
        # Skip public paths, health checks, server-to-server API calls
        if request.path in ('/health', '/ping'):
            return
        if request.headers.get('X-Ingest-Api-Key'):
            return  # server-to-server
        return require_auth()
"""

import os
import logging
from functools import wraps
from datetime import datetime, timezone, timedelta

import jwt
from flask import request, redirect, g, jsonify

logger = logging.getLogger(__name__)

JWT_SECRET = os.environ.get("ADMIN_JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_COOKIE_NAME = "pf_auth"
JWT_EXPIRY_HOURS = 24
ADMIN_LOGIN_URL = "https://admin.pack-fresh.com/login"

# Role hierarchy — higher index = more access
ROLE_HIERARCHY = {
    "associate": 0,
    "manager": 1,
    "owner": 2,
}


def create_token(user_id: str, email: str, name: str, role: str) -> str:
    """Create a signed JWT token."""
    payload = {
        "sub": user_id,
        "email": email,
        "name": name,
        "role": role,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict | None:
    """Decode and validate a JWT token. Returns payload or None."""
    if not JWT_SECRET:
        return None
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        logger.debug("JWT expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.debug(f"JWT invalid: {e}")
        return None


def get_current_user() -> dict | None:
    """Get the current authenticated user from the request context."""
    return getattr(g, "user", None)


def require_auth(roles=None):
    """
    Validate JWT cookie. Call from @before_request.

    Args:
        roles: optional list of allowed roles (e.g., ['owner', 'manager']).
               If None, any authenticated user is allowed.

    Returns None if authenticated (Flask continues), or a redirect response.
    """
    token = request.cookies.get(JWT_COOKIE_NAME)
    if not token:
        return _redirect_to_login()

    payload = decode_token(token)
    if not payload:
        return _redirect_to_login()

    user_role = payload.get("role", "associate")

    # Check role access
    if roles:
        if user_role not in roles and user_role != "owner":  # owner always has access
            return jsonify({"error": "Insufficient permissions"}), 403

    # Set user on request context
    g.user = {
        "id": payload.get("sub"),
        "email": payload.get("email"),
        "name": payload.get("name"),
        "role": user_role,
    }
    return None  # authenticated — continue


def _redirect_to_login():
    """Redirect to admin login with return URL."""
    next_url = request.url
    # For API calls, return 401 instead of redirect
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({"error": "Authentication required"}), 401
    return redirect(f"{ADMIN_LOGIN_URL}?next={next_url}")


def set_auth_cookie(response, token: str):
    """Set the JWT cookie on a response, scoped to .pack-fresh.com."""
    response.set_cookie(
        JWT_COOKIE_NAME,
        token,
        max_age=JWT_EXPIRY_HOURS * 3600,
        httponly=True,
        secure=True,
        samesite="Lax",
        domain=".pack-fresh.com",
        path="/",
    )
    return response


def clear_auth_cookie(response):
    """Clear the JWT cookie."""
    response.set_cookie(
        JWT_COOKIE_NAME,
        "",
        max_age=0,
        httponly=True,
        secure=True,
        samesite="Lax",
        domain=".pack-fresh.com",
        path="/",
    )
    return response


ADMIN_BAR_HTML = """
<div id="pf-admin-bar" style="position:sticky;top:0;z-index:9999;background:#141720;border-bottom:1px solid #2a2f42;padding:6px 16px;display:flex;align-items:center;gap:12px;font-family:'DM Sans',sans-serif;font-size:0.78rem;">
  <a href="https://admin.pack-fresh.com" style="color:#4f7df9;text-decoration:none;font-weight:600;">← Console</a>
  <span style="color:#6b7280;">|</span>
  <span style="color:#6b7280;" id="pf-admin-user"></span>
  <a href="https://admin.pack-fresh.com/api/logout" style="color:#6b7280;text-decoration:none;margin-left:auto;font-size:0.72rem;" onclick="document.cookie='pf_auth=;domain=.pack-fresh.com;path=/;max-age=0';">Sign Out</a>
</div>
<script>
try {
  const t = document.cookie.split(';').map(c=>c.trim()).find(c=>c.startsWith('pf_auth='));
  if (t) {
    const p = JSON.parse(atob(t.split('.')[1]));
    const el = document.getElementById('pf-admin-user');
    if (el) el.textContent = p.name + ' (' + p.role + ')';
  }
} catch(e) {}
</script>
"""


def inject_admin_bar(response):
    """Inject the admin navigation bar into HTML responses."""
    if response.content_type and "text/html" in response.content_type:
        data = response.get_data(as_text=True)
        # Insert after <body> tag
        if "<body" in data:
            import re
            data = re.sub(r"(<body[^>]*>)", r"\1" + ADMIN_BAR_HTML, data, count=1)
            response.set_data(data)
    return response
