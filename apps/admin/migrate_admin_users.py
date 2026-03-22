"""
Migration: Create admin_users table and seed the owner account.

Run once against the shared database (safe to re-run).

Usage:
    OWNER_EMAIL=sean@example.com OWNER_PASSWORD=changeme python migrate_admin_users.py
"""

import os
import sys
import bcrypt
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Running admin_users migration...")


def table_exists(name):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (name,))
    return bool(cur.fetchone())


if not table_exists("admin_users"):
    cur.execute("""
        CREATE TABLE admin_users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'associate',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login_at TIMESTAMP
        )
    """)
    print("  Created admin_users table")
else:
    print("  admin_users table already exists")

# Seed owner account if provided and table is empty
cur.execute("SELECT COUNT(*) AS c FROM admin_users")
count = cur.fetchone()["c"]
if count == 0:
    owner_email = os.getenv("OWNER_EMAIL", "").strip()
    owner_pass = os.getenv("OWNER_PASSWORD", "").strip()
    owner_name = os.getenv("OWNER_NAME", "Sean").strip()
    if owner_email and owner_pass:
        hashed = bcrypt.hashpw(owner_pass.encode(), bcrypt.gensalt()).decode()
        cur.execute("""
            INSERT INTO admin_users (email, name, password_hash, role)
            VALUES (%s, %s, %s, 'owner')
        """, (owner_email, owner_name, hashed))
        print(f"  Seeded owner account: {owner_email}")
    else:
        print("  No OWNER_EMAIL/OWNER_PASSWORD set — skipping seed. Add a user manually or re-run with env vars.")

conn.commit()
cur.close()
conn.close()
print("Done.")
