"""
Initialize database schema on Railway PostgreSQL.
Run with: railway run python init_db.py
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    print("Run with: railway run python init_db.py")
    sys.exit(1)

# Read schema
print("Reading schema.sql...")
with open("schema.sql", "r", encoding="utf-8") as f:
    schema_sql = f.read()

print("Connecting to database...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

try:
    print("Executing schema.sql...")
    cur.execute(schema_sql)
    conn.commit()
    print("OK - Schema initialized")

    # Verify tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = cur.fetchall()
    print(f"\nCreated {len(tables)} tables:")
    for (name,) in tables:
        print(f"  - {name}")

except Exception as e:
    conn.rollback()
    print(f"ERROR: {e}")
    sys.exit(1)
finally:
    cur.close()
    conn.close()

print("\nDone! Database ready.")
