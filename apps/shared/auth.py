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
from flask import Blueprint, request, redirect, g, jsonify

logger = logging.getLogger(__name__)

JWT_SECRET = os.environ.get("ADMIN_JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_COOKIE_NAME = "pf_auth"
JWT_EXPIRY_HOURS = 24
ADMIN_LOGIN_URL = "https://admin.pack-fresh.com/login"

# Short-lived "manager-walked-up-and-typed-their-PIN" override tokens.
# Sized for one transaction — long enough to fill out an offer, short
# enough that walking away from the kiosk doesn't leave it unlocked.
OVERRIDE_TTL_MINUTES = 10

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


def create_override_token(manager_id: str, manager_name: str, manager_role: str,
                          action: str = "manager_override") -> str:
    """Mint a short-lived JWT representing a manager's PIN-confirmed approval
    of a single privileged action by an associate. The kind=override claim
    keeps these tokens from being usable as login cookies.
    """
    payload = {
        "kind": "override",
        "sub": manager_id,
        "name": manager_name,
        "role": manager_role,
        "action": action,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=OVERRIDE_TTL_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_override_token(token: str, action: str | None = None) -> dict | None:
    """Decode a manager-override token. Returns the payload (with manager
    info) only if it's a valid override token and, when `action` is given,
    matches that specific action label. Returns None otherwise — callers
    should treat None as "associate is not authorized" and reject the
    request.
    """
    if not token or not JWT_SECRET:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    if payload.get("kind") != "override":
        return None
    if action is not None and payload.get("action") != action:
        return None
    return payload


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
  <a href="https://admin.pack-fresh.com/logout" style="color:#6b7280;text-decoration:none;margin-left:auto;font-size:0.72rem;" onclick="document.cookie='pf_auth=;domain=.pack-fresh.com;path=/;max-age=0';">Sign Out</a>
</div>
<script>
try {
  const t = document.cookie.split(';').map(c=>c.trim()).find(c=>c.startsWith('pf_auth='));
  if (t) {
    const p = JSON.parse(atob(t.split('.')[1]));
    window._pfUser = { name: p.name, role: p.role, email: p.email, user_id: p.user_id };
    const el = document.getElementById('pf-admin-user');
    if (el) el.textContent = p.name + ' (' + p.role + ')';
  }
} catch(e) {}
</script>
"""


def register_auth_hooks(app, roles=None, public_paths=('/health', '/ping', '/favicon.ico'),
                        public_prefixes=('/static',), skip_jwt_prefixes=()):
    """
    Register standard JWT auth + admin bar hooks on a Flask app.

    Args:
        app: Flask app
        roles: list of roles to require (e.g. ["owner", "manager"]), or None for any authenticated
        public_paths: exact paths that skip auth
        public_prefixes: path prefixes that skip auth (e.g. ('/static',))
        skip_jwt_prefixes: prefixes where JWT is parsed but not required (webhooks, etc.)
    """
    @app.before_request
    def _check_auth():
        if request.path in public_paths:
            return
        for prefix in (*public_prefixes, '/pf-static'):
            if request.path.startswith(prefix):
                return
        for prefix in skip_jwt_prefixes:
            if request.path.startswith(prefix):
                # Parse JWT if present but don't block
                try:
                    token = request.cookies.get(JWT_COOKIE_NAME, "")
                    if token:
                        payload = decode_token(token)
                        if payload:
                            g.user = payload
                except Exception:
                    pass
                return
        return require_auth(roles=roles)

    # Serve shared static assets (pf_theme.css, pf_ui.js) at /pf-static/
    pf_static = Blueprint(
        "pf_static", __name__,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        static_url_path="/pf-static",
    )
    app.register_blueprint(pf_static)

    @app.after_request
    def _add_admin_bar(response):
        try:
            if get_current_user():
                return inject_admin_bar(response)
        except Exception:
            pass
        return response


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
