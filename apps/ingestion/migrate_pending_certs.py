"""
Migration: add intake_items.pending_certs JSONB

Persists typed-but-not-yet-pushed cert # + listing price for each graded slab
in the Push tab. Was DOM-only before — a refresh (or Railway redeploy mid-
session) wiped every cert/price the operator had typed for unpushed slabs.

Shape: JSONB array, one entry per remaining slab in the parent item's qty.
    [{"cert": "12345678", "price": 250.00}, null, {"cert": "..."}]
`null` entries are placeholders so slab index stays stable while the operator
fills rows out of order. Successful push removes the entry (array splice),
remaining indices shift down.

Safe to re-run.
"""
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to .env or the environment")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'intake_items' AND column_name = 'pending_certs'
""")
if cur.fetchone():
    print("[skip] pending_certs already exists — nothing to do")
else:
    cur.execute("ALTER TABLE intake_items ADD COLUMN pending_certs JSONB DEFAULT '[]'::jsonb")
    print("[ok] Added intake_items.pending_certs (JSONB, default '[]')")

conn.commit()
cur.close()
conn.close()
print("done.")
