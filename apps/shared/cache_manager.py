"""
shared/cache_manager.py

Self-aware Shopify product cache with configurable table prefix.

Each service that needs a cache instantiates with its own prefix:
    intake/ingestion:  CacheManager(db, shopify, table_prefix="")
        → uses: shopify_product_cache, cache_meta

    inventory:         CacheManager(db, shopify, table_prefix="inventory_")
        → uses: inventory_product_cache, inventory_cache_meta

The inventory cache stores ALL Shopify products (including items without a
TCGPlayer ID, like slabs and accessories).  Intake's cache only stores items
that have a tcgplayer_id metafield — that behaviour is controlled by the
`cache_all_products` flag.

Staleness triggers (any one is sufficient):
  1. Price updater window: cache last refreshed between 03:30–08:30 today
  2. New orders:   latest Shopify order number has advanced — ALWAYS fires
  3. Products updated: latest product updated_at has advanced
     → suppressed for TOOL_PUSH_COOLDOWN_MINUTES after any tool push
       (prevents tool-originated Shopify edits from immediately re-syncing)
  4. Explicit invalidation: call .invalidate(reason)

Tool push cooldown: when your own tools push price/qty to Shopify, they call
  cm.record_tool_push() which sets last_tool_push_at in the meta table.
  The product_updated staleness signal is suppressed for 10 minutes after that,
  so your own edits don't thrash the cache. New orders are NEVER suppressed.
"""

import logging
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

PRICE_UPDATER_START = (3, 30)
PRICE_UPDATER_DONE  = (8, 30)
TOOL_PUSH_COOLDOWN_MINUTES = 10   # suppress product_updated signal after a tool push


class CacheManager:
    def __init__(self, db, shopify_client, *,
                 table_prefix: str = "",
                 cache_all_products: bool = False):
        """
        Args:
            db:                 db module (has .execute, .query_one, .query)
            shopify_client:     ShopifyClient instance
            table_prefix:       Prefix for cache/meta table names.
                                  ""           → shopify_product_cache + cache_meta  (intake)
                                  "inventory_" → inventory_product_cache + inventory_cache_meta
            cache_all_products: If True, cache every product regardless of TCGPlayer ID.
                                If False (default), only cache products that have a tcgplayer_id.
        """
        self.db = db
        self.shopify = shopify_client
        self.table_prefix = table_prefix
        self.cache_all_products = cache_all_products

        self._cache_table = f"{table_prefix}product_cache"
        self._meta_table  = f"{table_prefix}cache_meta"

        self._refresh_lock = threading.Lock()
        self._refresh_in_progress = False
        self._last_refresh_started = None  # UTC timestamp of last refresh start

    # ─── Public API ───────────────────────────────────────────────────────────

    def check_and_refresh_if_stale(self) -> dict:
        """
        Non-blocking staleness check. Fires a background refresh if stale.
        Returns immediately.
        """
        if self.shopify is None:
            return {"stale": False, "reason": "shopify_not_configured"}

        self.ensure_tables()
        meta = self._get_meta()
        if meta is None:
            logger.info(f"[{self._meta_table}] not found — triggering initial refresh")
            self._fire_background_refresh("initial")
            return {"stale": True, "reason": "initial"}

        reasons = self._check_staleness(meta)
        if reasons:
            logger.info(f"[{self._cache_table}] stale ({', '.join(reasons)}) — refreshing")
            self._fire_background_refresh(reasons[0])
            return {"stale": True, "reason": reasons[0], "all_reasons": reasons}

        return {"stale": False, "last_refreshed_at": meta["last_refreshed_at"].isoformat()}

    def record_tool_push(self) -> None:
        """
        Call this whenever your tools push a price or qty change to Shopify.
        Suppresses the product_updated staleness signal for TOOL_PUSH_COOLDOWN_MINUTES
        so your own edits don't immediately trigger a full re-sync.
        New orders are never suppressed.
        """
        try:
            self.db.execute(f"""
                INSERT INTO {self._meta_table} (id, last_refreshed_at, last_tool_push_at)
                VALUES (1, '1970-01-01', CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET last_tool_push_at = CURRENT_TIMESTAMP
            """)
            logger.debug(f"[{self._meta_table}] tool push recorded")
        except Exception as e:
            logger.warning(f"record_tool_push failed: {e}")

    def invalidate(self, reason: str = "explicit") -> None:
        """Mark cache as needing a refresh and fire one immediately."""
        logger.info(f"[{self._cache_table}] invalidated (reason: {reason})")
        self._update_meta(reason)
        self._fire_background_refresh(reason)

    def get_status(self) -> dict:
        """Return cache health info for display in UI."""
        meta = self._get_meta()
        if meta is None:
            return {"status": "never_synced", "last_refreshed_at": None}
        now = datetime.now(timezone.utc)
        age_min = (now - meta["last_refreshed_at"].replace(tzinfo=timezone.utc)).total_seconds() / 60
        return {
            "status":                "ok",
            "last_refreshed_at":     meta["last_refreshed_at"].isoformat(),
            "last_refreshed_reason": meta.get("last_refreshed_reason"),
            "age_minutes":           round(age_min, 1),
            "refresh_in_progress":   self._refresh_in_progress,
        }

    # ─── Table bootstrap ──────────────────────────────────────────────────────

    def ensure_tables(self):
        """Create cache tables if they don't exist yet (idempotent)."""
        try:
            if self.table_prefix == "inventory_":
                # Inventory cache: stores ALL products, adds tags + inventory_item_id
                self.db.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._cache_table} (
                        shopify_product_id  BIGINT NOT NULL,
                        shopify_variant_id  BIGINT NOT NULL,
                        title               VARCHAR(500),
                        handle              VARCHAR(500),
                        status              VARCHAR(50),
                        tags                TEXT,
                        sku                 VARCHAR(200),
                        shopify_price       NUMERIC(10,2),
                        shopify_qty         INTEGER,
                        inventory_item_id   BIGINT,
                        tcgplayer_id        BIGINT,
                        is_damaged          BOOLEAN DEFAULT FALSE,
                        committed           INTEGER DEFAULT 0,
                        last_synced         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (shopify_product_id, shopify_variant_id)
                    )
                """)
                self.db.execute(f"""
                    CREATE TABLE IF NOT EXISTS inventory_overrides (
                        shopify_variant_id  BIGINT PRIMARY KEY,
                        physical_count      INTEGER,
                        notes               TEXT,
                        updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                # Intake/ingestion cache: keyed by tcgplayer_id + variant
                self.db.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._cache_table} (
                        tcgplayer_id        BIGINT NOT NULL,
                        shopify_product_id  BIGINT,
                        shopify_variant_id  BIGINT NOT NULL,
                        title               VARCHAR(500),
                        handle              VARCHAR(500),
                        sku                 VARCHAR(200),
                        shopify_price       NUMERIC(10,2),
                        shopify_qty         INTEGER,
                        status              VARCHAR(50),
                        is_damaged          BOOLEAN DEFAULT FALSE,
                        last_synced         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (tcgplayer_id, shopify_variant_id)
                    )
                """)

            self.db.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._meta_table} (
                    id                      INTEGER PRIMARY KEY DEFAULT 1,
                    last_refreshed_at       TIMESTAMP NOT NULL DEFAULT '1970-01-01',
                    last_refreshed_reason   VARCHAR(200),
                    last_order_number       INTEGER,
                    last_product_updated_at TIMESTAMP,
                    last_tool_push_at       TIMESTAMP
                )
            """)
            # Migrate existing tables — safe to run repeatedly
            self._migrate_columns()
        except Exception as e:
            logger.warning(f"ensure_tables warning: {e}")

    def _migrate_columns(self):
        """Add columns introduced in later versions — idempotent."""
        migrations = []
        if self.table_prefix == "inventory_":
            migrations += [
                f"ALTER TABLE {self._cache_table} ADD COLUMN IF NOT EXISTS committed INTEGER DEFAULT 0",
                f"ALTER TABLE {self._cache_table} ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(10,2)",
                f"ALTER TABLE {self._cache_table} ADD COLUMN IF NOT EXISTS sku VARCHAR(200)",
                f"ALTER TABLE {self._cache_table} ADD COLUMN IF NOT EXISTS image_url TEXT",
                # Barcode mirrors variant.barcode from Shopify so sealed/slab
                # pulls in card_manager can match a physical scan against the
                # barcode set via the inventory bind tool (SKU and barcode
                # diverge for sealed/slab, unlike raw cards where they match).
                f"ALTER TABLE {self._cache_table} ADD COLUMN IF NOT EXISTS barcode VARCHAR(200)",
                f"CREATE INDEX IF NOT EXISTS idx_{self._cache_table}_barcode ON {self._cache_table}(barcode) WHERE barcode IS NOT NULL",
            ]
        migrations += [
            f"ALTER TABLE {self._meta_table} ADD COLUMN IF NOT EXISTS last_tool_push_at TIMESTAMP",
        ]
        for sql in migrations:
            try:
                self.db.execute(sql)
            except Exception as e:
                logger.debug(f"Migration skipped ({e}): {sql[:60]}")

    # ─── Staleness checks ─────────────────────────────────────────────────────

    def _check_staleness(self, meta: dict) -> list[str]:
        reasons = []
        if self._in_price_updater_window(meta["last_refreshed_at"]):
            reasons.append("price_updater_window")
        try:
            signals = self.shopify.get_cache_staleness_signals()
            latest_order   = signals.get("latest_order_number")
            latest_updated = signals.get("latest_product_updated_at")
            if latest_order and meta.get("last_order_number"):
                if int(latest_order) > int(meta["last_order_number"]):
                    reasons.append(f"new_orders (#{meta['last_order_number']} → #{latest_order})")
            if latest_updated and meta.get("last_product_updated_at"):
                shopify_ts = _parse_ts(latest_updated)
                cache_ts   = meta["last_product_updated_at"].replace(tzinfo=timezone.utc)
                if shopify_ts > cache_ts:
                    # Suppress if a tool push happened within the cooldown window
                    if self._in_tool_push_cooldown(meta):
                        logger.debug(f"[{self._cache_table}] product_updated suppressed — tool push cooldown active")
                    else:
                        reasons.append("product_updated")
        except Exception as e:
            logger.warning(f"Staleness signal fetch failed: {e} — treating cache as fresh")
        return reasons

    def _in_price_updater_window(self, last_refreshed_at: datetime) -> bool:
        now = datetime.now()
        today_done  = PRICE_UPDATER_DONE[0] * 60 + PRICE_UPDATER_DONE[1]
        now_minutes = now.hour * 60 + now.minute
        if now_minutes < today_done:
            return False
        lr = last_refreshed_at.astimezone().replace(tzinfo=None) \
             if last_refreshed_at.tzinfo else last_refreshed_at
        today = now.date()
        window_start = datetime(today.year, today.month, today.day,
                                *PRICE_UPDATER_START)
        window_end   = datetime(today.year, today.month, today.day,
                                *PRICE_UPDATER_DONE)
        return window_start <= lr <= window_end

    def _in_tool_push_cooldown(self, meta: dict) -> bool:
        """Return True if a tool push happened within TOOL_PUSH_COOLDOWN_MINUTES."""
        last_push = meta.get("last_tool_push_at")
        if not last_push:
            return False
        try:
            now = datetime.now(timezone.utc)
            if last_push.tzinfo is None:
                last_push = last_push.replace(tzinfo=timezone.utc)
            age_minutes = (now - last_push).total_seconds() / 60
            return age_minutes < TOOL_PUSH_COOLDOWN_MINUTES
        except Exception:
            return False

    # ─── Background refresh ───────────────────────────────────────────────────

    def _fire_background_refresh(self, reason: str) -> None:
        with self._refresh_lock:
            if self._refresh_in_progress:
                logger.info(f"[{self._cache_table}] refresh already running — skipping")
                return
            # Debounce: skip if a refresh started within the last 30 seconds
            now = datetime.now(timezone.utc)
            if self._last_refresh_started and (now - self._last_refresh_started).total_seconds() < 30:
                logger.info(f"[{self._cache_table}] refresh started recently — skipping")
                return
            self._last_refresh_started = now
        t = threading.Thread(target=self._run_refresh, args=(reason,), daemon=True)
        t.start()

    def _run_refresh(self, reason: str) -> None:
        with self._refresh_lock:
            if self._refresh_in_progress:
                return
            self._refresh_in_progress = True
        try:
            logger.info(f"[{self._cache_table}] refresh starting (reason: {reason})")
            start = datetime.now(timezone.utc)
            signals = {}
            try:
                signals = self.shopify.get_cache_staleness_signals()
            except Exception as e:
                logger.warning(f"Could not fetch staleness signals: {e}")

            upserted = 0
            seen_keys: set[tuple] = set()  # (shopify_product_id, shopify_variant_id)

            for page_products, _ in self.shopify.iter_products_pages(batch_size=100):
                rows_to_upsert = (
                    page_products if self.cache_all_products
                    else [p for p in page_products if p.get("tcgplayer_id")]
                )
                for p in rows_to_upsert:
                    self._upsert_product(p)
                    upserted += 1
                    seen_keys.add((str(p["shopify_product_id"]), str(p["variant_id"])))

            # Purge rows for products that no longer exist in Shopify
            if seen_keys and self.table_prefix == "inventory_":
                try:
                    existing = self.db.query(
                        f"SELECT shopify_product_id, shopify_variant_id FROM {self._cache_table}"
                    )
                    stale = [
                        (r["shopify_product_id"], r["shopify_variant_id"])
                        for r in existing
                        if (str(r["shopify_product_id"]), str(r["shopify_variant_id"])) not in seen_keys
                    ]
                    if stale:
                        for pid, vid in stale:
                            self.db.execute(
                                f"DELETE FROM {self._cache_table} "
                                f"WHERE shopify_product_id = %s AND shopify_variant_id = %s",
                                (pid, vid)
                            )
                        logger.info(f"[{self._cache_table}] purged {len(stale)} deleted product(s)")
                except Exception as e:
                    logger.warning(f"[{self._cache_table}] stale row purge failed: {e}")

            if self.table_prefix == "":
                # Intake-specific: backfill sealed_cogs linkage
                try:
                    self.db.execute("""
                        UPDATE sealed_cogs sc SET shopify_product_id = spc.shopify_product_id
                        FROM shopify_product_cache spc
                        WHERE sc.tcgplayer_id = spc.tcgplayer_id
                          AND sc.shopify_product_id IS NULL
                    """)
                except Exception:
                    pass  # table may not exist in all environments

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.info(f"[{self._cache_table}] refresh complete: {upserted} rows in {elapsed:.1f}s")
            self._set_meta(
                reason=reason,
                order_number=signals.get("latest_order_number"),
                product_updated_at=signals.get("latest_product_updated_at"),
            )
        except Exception as e:
            logger.error(f"[{self._cache_table}] refresh failed: {e}", exc_info=True)
        finally:
            self._refresh_in_progress = False

    def _upsert_product(self, p: dict) -> None:
        """Upsert one product row into the appropriate cache table."""
        if self.table_prefix == "inventory_":
            self.db.execute(f"""
                INSERT INTO {self._cache_table}
                    (shopify_product_id, shopify_variant_id, title, handle, status,
                     tags, sku, barcode, shopify_price, shopify_qty, inventory_item_id,
                     tcgplayer_id, is_damaged, committed, unit_cost, image_url, last_synced)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (shopify_product_id, shopify_variant_id) DO UPDATE SET
                    title             = EXCLUDED.title,
                    handle            = EXCLUDED.handle,
                    status            = EXCLUDED.status,
                    tags              = EXCLUDED.tags,
                    sku               = EXCLUDED.sku,
                    barcode           = EXCLUDED.barcode,
                    shopify_price     = EXCLUDED.shopify_price,
                    shopify_qty       = EXCLUDED.shopify_qty,
                    inventory_item_id = EXCLUDED.inventory_item_id,
                    tcgplayer_id      = EXCLUDED.tcgplayer_id,
                    is_damaged        = EXCLUDED.is_damaged,
                    committed         = EXCLUDED.committed,
                    unit_cost         = EXCLUDED.unit_cost,
                    image_url         = EXCLUDED.image_url,
                    last_synced       = CURRENT_TIMESTAMP
            """, (
                p["shopify_product_id"], p["variant_id"],
                p["title"], p["handle"], p.get("status", "ACTIVE"),
                p.get("tags_csv", ""),
                p.get("sku"),
                p.get("barcode"),
                p["shopify_price"], p["shopify_qty"],
                p.get("inventory_item_id"),
                p.get("tcgplayer_id"),
                p.get("is_damaged", False),
                p.get("committed", 0),
                p.get("unit_cost"),
                p.get("image_url"),
            ))
        else:
            # Intake/ingestion schema (keyed by tcgplayer_id)
            self.db.execute(f"""
                INSERT INTO {self._cache_table}
                    (tcgplayer_id, shopify_product_id, shopify_variant_id,
                     title, handle, sku, shopify_price, shopify_qty,
                     status, is_damaged, last_synced)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (tcgplayer_id, shopify_variant_id) DO UPDATE SET
                    title         = EXCLUDED.title,
                    handle        = EXCLUDED.handle,
                    sku           = EXCLUDED.sku,
                    shopify_price = EXCLUDED.shopify_price,
                    shopify_qty   = EXCLUDED.shopify_qty,
                    status        = EXCLUDED.status,
                    is_damaged    = EXCLUDED.is_damaged,
                    last_synced   = CURRENT_TIMESTAMP
            """, (
                p["tcgplayer_id"], p["shopify_product_id"], p["variant_id"],
                p["title"], p["handle"], p.get("sku"),
                p["shopify_price"], p["shopify_qty"],
                p.get("status", "ACTIVE"), p.get("is_damaged", False),
            ))

    # ─── cache_meta helpers ───────────────────────────────────────────────────

    def _get_meta(self) -> dict | None:
        try:
            return self.db.query_one(f"SELECT * FROM {self._meta_table} LIMIT 1")
        except Exception:
            return None

    def _set_meta(self, reason: str, order_number=None, product_updated_at=None):
        try:
            ts = _parse_ts(product_updated_at) if product_updated_at else None
            self.db.execute(f"""
                INSERT INTO {self._meta_table}
                    (id, last_refreshed_at, last_refreshed_reason,
                     last_order_number, last_product_updated_at)
                VALUES (1, CURRENT_TIMESTAMP, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_refreshed_at       = CURRENT_TIMESTAMP,
                    last_refreshed_reason   = EXCLUDED.last_refreshed_reason,
                    last_order_number       = EXCLUDED.last_order_number,
                    last_product_updated_at = EXCLUDED.last_product_updated_at
            """, (reason, order_number, ts))
        except Exception as e:
            logger.warning(f"Failed to update {self._meta_table}: {e}")

    def _update_meta(self, reason: str):
        try:
            self.db.execute(f"""
                INSERT INTO {self._meta_table} (id, last_refreshed_at, last_refreshed_reason)
                VALUES (1, '1970-01-01', %s)
                ON CONFLICT (id) DO UPDATE SET last_refreshed_reason = %s
            """, (f"invalidated:{reason}", f"invalidated:{reason}"))
        except Exception as e:
            logger.warning(f"Failed to mark {self._meta_table} invalidated: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> datetime:
    if not ts_str:
        return None
    ts_str = ts_str.rstrip("Z")
    if "+" in ts_str:
        ts_str = ts_str.split("+")[0]
    return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
