# Admin Portal (admin/)
> Unified staff login + command console (admin.pack-fresh.com)

## Key Files
- **app.py** — Flask app: login, dashboard, user management
- **templates/login.html** — Login form
- **templates/dashboard.html** — Command console with app cards
- **templates/users.html** — User management (owner only)
- **migrate_admin_users.py** — Create admin_users table + seed owner
- DB via shared/db.py (no local db.py)

## Authentication System
- JWT token in `pf_auth` cookie, scoped to `.pack-fresh.com` domain
- Shared secret: `ADMIN_JWT_SECRET` env var (must be set on ALL staff services)
- Token contains: user_id, email, name, role, exp (24h)
- `shared/auth.py` provides validation middleware for all services
- Roles: owner (everything), manager (no user mgmt), associate (limited apps)

## Database
- `admin_users` table: id, email, name, password_hash (bcrypt), role, is_active, timestamps

## Key Patterns
- Login issues JWT cookie → all subdomains can read it
- Dashboard refreshes token on each visit (extends expiry)
- API calls return 401 JSON, browser requests redirect to /login
- User management is owner-only
