"""
migrate_breakdown.py — add breakdown_ignore table to inventory DB
Run once: python migrate_breakdown.py
"""
import sys, os
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

if __name__ == "__main__":
    run()
