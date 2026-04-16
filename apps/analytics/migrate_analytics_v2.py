"""
Migration v2: Analytics overhaul tables.

Creates 6 new tables for the executive analytics engine:
  - scrydex_price_history  (daily market price snapshots)
  - product_taxonomy       (dimensional classification per SKU)
  - customer_orders        (per-customer order log)
  - customer_summary       (rolled-up customer dimension table)
  - daily_business_summary (pre-aggregated daily KPIs)
  - realized_margin        (per-variant per-day margin)

Run once against the shared database (safe to re-run).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../ingestion/.env", "../ingest-service/.env"]:
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

print("Running analytics v2 migration...")


def table_exists(name):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (name,))
    return bool(cur.fetchone())


# ── scrydex_price_history ────────────────────────────────────────────────────

if not table_exists("scrydex_price_history"):
    cur.execute("""
        CREATE TABLE scrydex_price_history (
            id                  SERIAL PRIMARY KEY,
            snapshot_date       DATE NOT NULL,
            scrydex_id          TEXT NOT NULL,
            tcgplayer_id        INTEGER,
            expansion_id        TEXT NOT NULL,
            expansion_name      TEXT,
            product_type        TEXT NOT NULL DEFAULT 'card',
            product_name        TEXT,
            variant             TEXT NOT NULL DEFAULT 'normal',
            condition           TEXT NOT NULL DEFAULT 'NM',
            price_type          TEXT NOT NULL DEFAULT 'raw',
            grade_company       TEXT,
            grade_value         TEXT,
            market_price        NUMERIC(10,2),
            low_price           NUMERIC(10,2)
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX idx_sph_unique ON scrydex_price_history(
            snapshot_date, scrydex_id, variant, condition, price_type,
            COALESCE(grade_company, ''), COALESCE(grade_value, '')
        )
    """)
    cur.execute("CREATE INDEX idx_sph_expansion_date ON scrydex_price_history(expansion_id, snapshot_date)")
    cur.execute("CREATE INDEX idx_sph_tcg_date ON scrydex_price_history(tcgplayer_id, snapshot_date)")
    cur.execute("CREATE INDEX idx_sph_date ON scrydex_price_history(snapshot_date)")
    print("  Created scrydex_price_history table")
else:
    print("  scrydex_price_history already exists")


# ── product_taxonomy ─────────────────────────────────────────────────────────

if not table_exists("product_taxonomy"):
    cur.execute("""
        CREATE TABLE product_taxonomy (
            shopify_variant_id  BIGINT PRIMARY KEY,
            shopify_product_id  BIGINT,
            tcgplayer_id        BIGINT,
            title               TEXT,
            ip                  TEXT,
            product_type        TEXT,
            form_factor         TEXT,
            expansion_id        TEXT,
            set_name            TEXT,
            era                 TEXT,
            rarity              TEXT,
            manual_override     BOOLEAN DEFAULT FALSE,
            classified_at       TIMESTAMPTZ,
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX idx_taxonomy_ip ON product_taxonomy(ip)")
    cur.execute("CREATE INDEX idx_taxonomy_form ON product_taxonomy(form_factor)")
    cur.execute("CREATE INDEX idx_taxonomy_expansion ON product_taxonomy(expansion_id)")
    cur.execute("CREATE INDEX idx_taxonomy_product_id ON product_taxonomy(shopify_product_id)")
    cur.execute("CREATE INDEX idx_taxonomy_tcg ON product_taxonomy(tcgplayer_id) WHERE tcgplayer_id IS NOT NULL")
    print("  Created product_taxonomy table")
else:
    print("  product_taxonomy already exists")


# ── customer_orders ──────────────────────────────────────────────────────────

if not table_exists("customer_orders"):
    cur.execute("""
        CREATE TABLE customer_orders (
            id                  SERIAL PRIMARY KEY,
            customer_id         BIGINT NOT NULL,
            order_id            BIGINT NOT NULL,
            order_gid           TEXT NOT NULL,
            order_name          TEXT,
            order_date          DATE NOT NULL,
            order_total         NUMERIC(10,2) NOT NULL,
            refund_amount       NUMERIC(10,2) DEFAULT 0,
            net_amount          NUMERIC(10,2),
            channel             TEXT,
            fulfillment_status  TEXT,
            created_at_ts       TIMESTAMPTZ,
            fulfilled_at_ts     TIMESTAMPTZ,
            item_count          INTEGER,
            items               JSONB,
            UNIQUE(customer_id, order_id)
        )
    """)
    cur.execute("CREATE INDEX idx_custord_customer ON customer_orders(customer_id)")
    cur.execute("CREATE INDEX idx_custord_date ON customer_orders(order_date)")
    cur.execute("CREATE INDEX idx_custord_order ON customer_orders(order_id)")
    print("  Created customer_orders table")
else:
    print("  customer_orders already exists")


# ── customer_summary ─────────────────────────────────────────────────────────

if not table_exists("customer_summary"):
    cur.execute("""
        CREATE TABLE customer_summary (
            customer_id         BIGINT PRIMARY KEY,
            customer_gid        TEXT NOT NULL,
            email               TEXT,
            first_name          TEXT,
            last_name           TEXT,
            first_order_date    DATE,
            first_order_amount  NUMERIC(10,2),
            cohort_month        TEXT,
            total_orders        INTEGER DEFAULT 0,
            total_spend         NUMERIC(10,2) DEFAULT 0,
            total_refunds       NUMERIC(10,2) DEFAULT 0,
            net_spend           NUMERIC(10,2) DEFAULT 0,
            avg_order_value     NUMERIC(10,2),
            max_order_value     NUMERIC(10,2),
            days_between_orders NUMERIC(6,1),
            last_order_date     DATE,
            last_order_amount   NUMERIC(10,2),
            order_frequency_30d INTEGER DEFAULT 0,
            order_frequency_90d INTEGER DEFAULT 0,
            vip_tier            TEXT,
            channel_mix         JSONB,
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX idx_custsumm_email ON customer_summary(email)")
    cur.execute("CREATE INDEX idx_custsumm_cohort ON customer_summary(cohort_month)")
    cur.execute("CREATE INDEX idx_custsumm_tier ON customer_summary(vip_tier)")
    cur.execute("CREATE INDEX idx_custsumm_spend ON customer_summary(net_spend DESC)")
    cur.execute("CREATE INDEX idx_custsumm_last_order ON customer_summary(last_order_date)")
    print("  Created customer_summary table")
else:
    print("  customer_summary already exists")


# ── daily_business_summary ───────────────────────────────────────────────────

if not table_exists("daily_business_summary"):
    cur.execute("""
        CREATE TABLE daily_business_summary (
            summary_date        DATE PRIMARY KEY,
            total_orders        INTEGER DEFAULT 0,
            total_revenue       NUMERIC(12,2) DEFAULT 0,
            total_refunds       NUMERIC(12,2) DEFAULT 0,
            net_revenue         NUMERIC(12,2) DEFAULT 0,
            unique_customers    INTEGER DEFAULT 0,
            new_customers       INTEGER DEFAULT 0,
            returning_customers INTEGER DEFAULT 0,
            avg_order_value     NUMERIC(10,2),
            total_units_sold    INTEGER DEFAULT 0,
            orders_online       INTEGER DEFAULT 0,
            orders_pos          INTEGER DEFAULT 0,
            revenue_online      NUMERIC(12,2) DEFAULT 0,
            revenue_pos         NUMERIC(12,2) DEFAULT 0,
            intake_sessions     INTEGER DEFAULT 0,
            intake_total_cost   NUMERIC(12,2) DEFAULT 0,
            intake_total_items  INTEGER DEFAULT 0,
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    print("  Created daily_business_summary table")
else:
    print("  daily_business_summary already exists")


# ── realized_margin ──────────────────────────────────────────────────────────

if not table_exists("realized_margin"):
    cur.execute("""
        CREATE TABLE realized_margin (
            id                  SERIAL PRIMARY KEY,
            sale_date           DATE NOT NULL,
            shopify_variant_id  BIGINT NOT NULL,
            shopify_product_id  BIGINT,
            units_sold          INTEGER NOT NULL,
            revenue             NUMERIC(10,2) NOT NULL,
            cogs_at_sale        NUMERIC(10,2),
            market_price_at_sale NUMERIC(10,2),
            gross_margin        NUMERIC(10,2),
            margin_pct          NUMERIC(6,2),
            effective_margin_pct NUMERIC(6,2),
            UNIQUE(sale_date, shopify_variant_id)
        )
    """)
    cur.execute("CREATE INDEX idx_realized_margin_date ON realized_margin(sale_date)")
    cur.execute("CREATE INDEX idx_realized_margin_variant ON realized_margin(shopify_variant_id)")
    cur.execute("CREATE INDEX idx_realized_margin_product ON realized_margin(shopify_product_id)")
    print("  Created realized_margin table")
else:
    print("  realized_margin already exists")


conn.commit()
cur.close()
conn.close()
print("Done.")
