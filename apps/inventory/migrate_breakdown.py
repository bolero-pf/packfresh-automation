"""
migrate_breakdown.py — add breakdown_ignore table to inventory DB
Run once: python migrate_breakdown.py
"""
import sys, os

# Load DATABASE_URL from .env if not already in environment
if not os.getenv("DATABASE_URL"):
    for _p in [".env", os.path.join(os.path.dirname(__file__), ".env")]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    os.environ["DATABASE_URL"] = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if os.getenv("DATABASE_URL"):
            break

sys.path.insert(0, os.path.dirname(__file__))
import db

def run():
    db.execute("""
        CREATE TABLE IF NOT EXISTS breakdown_ignore (
            tcgplayer_id   BIGINT PRIMARY KEY,
            product_name   VARCHAR(500),
            ignored_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reason         TEXT
        )
    """)
    print("✓ breakdown_ignore table ready")

    db.execute("""
        CREATE TABLE IF NOT EXISTS breakdown_base_components (
            tcgplayer_id  BIGINT PRIMARY KEY,
            product_name  VARCHAR(500),
            marked_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ breakdown_base_components table ready")

if __name__ == "__main__":
    run()
