"""
Migration: Audit table for manager-PIN overrides on intake offer percentages.

When an associate submits an offer outside their default range, or a manager
submits one above their 80% cap, a higher-role user types their PIN and the
admin service mints a short-lived `override_token` (see `shared/auth.py`).
The intake-service offer endpoints validate the token and — on success —
write a row here. That gives us:

  - "Who approved this off-policy offer for which associate?"
  - "How often does manager X override?"
  - Per-session forensics if a deal goes sideways.

This is append-only. Failed PIN attempts NEVER touch this table — we don't
want a way to enumerate who has a PIN.

Schema:
  id                       UUID  PK
  session_id               UUID  → intake_sessions(id), nullable for non-session
                                   actions (kept nullable so the same audit
                                   table can be reused for other override
                                   actions later)
  approved_by_user_id      VARCHAR  the manager/owner whose PIN was accepted
  approver_role            VARCHAR  'manager' or 'owner' (sets effective cap)
  approved_for_user_id     VARCHAR  the associate whose request was unlocked
  action                   VARCHAR  matches the override_token's action label
  approved_cash_pct        DECIMAL(5,2)  nullable
  approved_credit_pct      DECIMAL(5,2)  nullable
  created_at               TIMESTAMP

Idempotent — re-running is safe.
Run once: python migrate_session_overrides.py
"""
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

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
    raise RuntimeError("DATABASE_URL not set and not found in .env")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

print("Running session_overrides migration...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS session_overrides (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        session_id UUID REFERENCES intake_sessions(id) ON DELETE SET NULL,
        approved_by_user_id VARCHAR(100) NOT NULL,
        approver_role VARCHAR(20) NOT NULL,
        approved_for_user_id VARCHAR(100),
        action VARCHAR(50) NOT NULL,
        approved_cash_pct DECIMAL(5, 2),
        approved_credit_pct DECIMAL(5, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("  Ensured session_overrides table")

cur.execute("CREATE INDEX IF NOT EXISTS idx_session_overrides_session ON session_overrides(session_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_session_overrides_approver ON session_overrides(approved_by_user_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_session_overrides_for_user ON session_overrides(approved_for_user_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_session_overrides_created ON session_overrides(created_at DESC)")
print("  Ensured indexes")

cur.close()
conn.close()
print("Done.")
