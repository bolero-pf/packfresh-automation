"""
cache_manager.py — Shopify product cache for the inventory service.

Extends the intake CacheManager pattern but caches ALL Shopify products,
not just those with a TCGPlayer ID.  This is critical for inventory because
slabs, accessories, and any item without a TCGPlayer ID must still appear.

Cache table: inventory_product_cache
Meta table:  inventory_cache_meta

These are separate from intake's shopify_product_cache so the two services
can refresh independently without stepping on each other.

Staleness triggers (same as intake):
  1. Price updater window (03:30 – 08:30 today)
  2. New Shopify orders since last refresh
  3. Products updated_at has advanced
  4. Explicit invalidation (after a listing is created/edited here)
"""

import logging
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

PRICE_UPDATER_START = (3, 30)
PRICE_UPDATER_DONE  = (8, 30)


class CacheManager:
    def __init__(self, db, shopify_client):
        self.db = db
        self.shopify = shopify_client
        self._refresh_lock = threading.Lock()
        self._refresh_in_progress = False

    # ─── Public API ───────────────────────────────────────────────────────────

    def check_and_refresh_if_stale(self) -> dict:
        """
        Non-blocking staleness check. Fires a background refresh if stale.
        Returns immediately with staleness info.
        """
        if self.shopify is None:
            return {"stale": False, "reason": "shopify_not_configured"}

        self._ensure_tables()
        meta = self._get_meta()
        if meta is None:
            logger.info("inventory_cache_meta not found — triggering initial refresh")
            self._fire_background_refresh("initial")
            return {"stale": True, "reason": "initial"}

        reasons = self._check_staleness(meta)
        if reasons:
            logger.info(f"Inventory cache stale ({', '.join(reasons)}) — refreshing")
            self._fire_background_refresh(reasons[0])
            return {"stale": True, "reason": reasons[0], "all_reasons": reasons}

        return {"stale": False, "last_refreshed_at": meta["last_refreshed_at"].isoformat()}

    def invalidate(self, reason: str = "explicit") -> None:
        """Mark cache as needing refresh and fire one immediately."""
        logger.info(f"Inventory cache invalidated (reason: {reason})")
        self._update_meta(reason)
        self._fire_background_refresh(reason)

    def get_status(self) -> dict:
        meta = self._get_meta()
        if meta is None:
            return {"status": "never_synced", "last_refreshed_at": None}
        now = datetime.now(timezone.utc)
        age_min = (now - meta["last_refreshed_at"].replace(tzinfo=timezone.utc)).total_seconds() / 60
        return {
            "status": "ok",
            "last_refreshed_at": meta["last_refreshed_at"].isoformat(),
            "last_refreshed_reason": meta.get("last_refreshed_reason"),
            "age_minutes": round(age_min, 1),
            "refresh_in_progress": self._refresh_in_progress,
        }

    # ─── Staleness ────────────────────────────────────────────────────────────

    def _check_staleness(self, meta: dict) -> list[str]:
        reasons = []
        if self._in_price_updater_window(meta["last_refreshed_at"]):
            reasons.append("price_updater_window")
        try:
            signals = self.shopify.get_cache_staleness_signals()
            latest_order = signals.get("latest_order_number")
            latest_updated = signals.get("latest_product_updated_at")
            if latest_order and meta.get("last_order_number"):
                if int(latest_order) > int(meta["last_order_number"]):
                    reasons.append(f"new_orders (#{meta['last_order_number']} → #{latest_order})")
            if latest_updated and meta.get("last_product_updated_at"):
                shopify_ts = _parse_ts(latest_updated)
                cache_ts = meta["last_product_updated_at"].replace(tzinfo=timezone.utc)
                if shopify_ts > cache_ts:
                    reasons.append("product_updated")
        except Exception as e:
            logger.warning(f"Staleness signal fetch failed: {e} — treating cache as fresh")
        return reasons

    def _in_price_updater_window(self, last_refreshed_at: datetime) -> bool:
        now = datetime.now()
        today_done = PRICE_UPDATER_DONE[0] * 60 + PRICE_UPDATER_DONE[1]
        now_minutes = now.hour * 60 + now.minute
        if now_minutes < today_done:
            return False
        if last_refreshed_at.tzinfo is not None:
            lr = last_refreshed_at.astimezone().replace(tzinfo=None)
        else:
            lr = last_refreshed_at
        today = now.date()
        window_start = datetime(today.year, today.month, today.day,
                                PRICE_UPDATER_START[0], PRICE_UPDATER_START[1])
        window_end   = datetime(today.year, today.month, today.day,
                                PRICE_UPDATER_DONE[0],  PRICE_UPDATER_DONE[1])
        return window_start <= lr <= window_end

    # ─── Background refresh ───────────────────────────────────────────────────

    def _fire_background_refresh(self, reason: str) -> None:
        if self._refresh_in_progress:
            logger.info("Inventory cache refresh already in progress — skipping")
            return
        t = threading.Thread(target=self._run_refresh, args=(reason,), daemon=True)
        t.start()

    def _run_refresh(self, reason: str) -> None:
        with self._refresh_lock:
            if self._refresh_in_progress:
                return
            self._refresh_in_progress = True
        try:
            logger.info(f"Inventory cache refresh starting (reason: {reason})")
            start = datetime.now(timezone.utc)
            signals = {}
            try:
                signals = self.shopify.get_cache_staleness_signals()
            except Exception as e:
                logger.warning(f"Could not fetch staleness signals: {e}")

            upserted = 0
            for page_products, _ in self.shopify.iter_products_pages(batch_size=100):
                # Cache ALL products — including those without a TCGPlayer ID
                for p in page_products:
                    self.db.execute("""
                        INSERT INTO inventory_product_cache
                            (shopify_product_id, shopify_variant_id, title, handle, status,
                             tags, shopify_price, shopify_qty, inventory_item_id,
                             tcgplayer_id, is_damaged, last_synced)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (shopify_product_id, shopify_variant_id)
                        DO UPDATE SET
                            title = EXCLUDED.title,
                            handle = EXCLUDED.handle,
                            status = EXCLUDED.status,
                            tags = EXCLUDED.tags,
                            shopify_price = EXCLUDED.shopify_price,
                            shopify_qty = EXCLUDED.shopify_qty,
                            inventory_item_id = EXCLUDED.inventory_item_id,
                            tcgplayer_id = EXCLUDED.tcgplayer_id,
                            is_damaged = EXCLUDED.is_damaged,
                            last_synced = CURRENT_TIMESTAMP
                    """, (
                        p["shopify_product_id"], p["variant_id"],
                        p["title"], p["handle"], p.get("status", "ACTIVE"),
                        p.get("tags_csv", ""),
                        p["shopify_price"], p["shopify_qty"],
                        p.get("inventory_item_id"),
                        p.get("tcgplayer_id"),  # may be None — that's fine
                        p.get("is_damaged", False),
                    ))
                    upserted += 1

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.info(f"Inventory cache refresh complete: {upserted} rows in {elapsed:.1f}s")
            self._set_meta(
                reason=reason,
                order_number=signals.get("latest_order_number"),
                product_updated_at=signals.get("latest_product_updated_at"),
            )
        except Exception as e:
            logger.error(f"Inventory cache refresh failed: {e}", exc_info=True)
        finally:
            self._refresh_in_progress = False

    # ─── Table bootstrap ──────────────────────────────────────────────────────

    def _ensure_tables(self):
        """Create tables if they don't exist yet (idempotent)."""
        try:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS inventory_product_cache (
                    shopify_product_id  BIGINT NOT NULL,
                    shopify_variant_id  BIGINT NOT NULL,
                    title               VARCHAR(500),
                    handle              VARCHAR(500),
                    status              VARCHAR(50),
                    tags                TEXT,
                    shopify_price       NUMERIC(10,2),
                    shopify_qty         INTEGER,
                    inventory_item_id   BIGINT,
                    tcgplayer_id        BIGINT,         -- NULL for slabs/accessories
                    is_damaged          BOOLEAN DEFAULT FALSE,
                    last_synced         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (shopify_product_id, shopify_variant_id)
                )
            """)
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS inventory_meta (
                    id                      INTEGER PRIMARY KEY DEFAULT 1,
                    physical_count          INTEGER,
                    notes                   TEXT,
                    shopify_qty             INTEGER,
                    shopify_price           NUMERIC(10,2),
                    last_edited_at          TIMESTAMP
                )
            """)
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS inventory_cache_meta (
                    id                      INTEGER PRIMARY KEY DEFAULT 1,
                    last_refreshed_at       TIMESTAMP NOT NULL DEFAULT '1970-01-01',
                    last_refreshed_reason   VARCHAR(200),
                    last_order_number       INTEGER,
                    last_product_updated_at TIMESTAMP
                )
            """)
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS inventory_overrides (
                    shopify_variant_id  BIGINT PRIMARY KEY,
                    physical_count      INTEGER,
                    notes               TEXT,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            logger.warning(f"_ensure_tables warning: {e}")

    # ─── cache_meta helpers ───────────────────────────────────────────────────

    def _get_meta(self) -> dict | None:
        try:
            return self.db.query_one("SELECT * FROM inventory_cache_meta LIMIT 1")
        except Exception:
            return None

    def _set_meta(self, reason: str, order_number=None, product_updated_at=None):
        try:
            ts = _parse_ts(product_updated_at) if product_updated_at else None
            self.db.execute("""
                INSERT INTO inventory_cache_meta
                    (id, last_refreshed_at, last_refreshed_reason, last_order_number, last_product_updated_at)
                VALUES (1, CURRENT_TIMESTAMP, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_refreshed_at       = CURRENT_TIMESTAMP,
                    last_refreshed_reason   = EXCLUDED.last_refreshed_reason,
                    last_order_number       = EXCLUDED.last_order_number,
                    last_product_updated_at = EXCLUDED.last_product_updated_at
            """, (reason, order_number, ts))
        except Exception as e:
            logger.warning(f"Failed to update inventory_cache_meta: {e}")

    def _update_meta(self, reason: str):
        try:
            self.db.execute("""
                INSERT INTO inventory_cache_meta (id, last_refreshed_at, last_refreshed_reason)
                VALUES (1, '1970-01-01', %s)
                ON CONFLICT (id) DO UPDATE SET last_refreshed_reason = %s
            """, (f"invalidated:{reason}", f"invalidated:{reason}"))
        except Exception as e:
            logger.warning(f"Failed to mark cache invalidated: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> datetime:
    if not ts_str:
        return None
    ts_str = ts_str.rstrip("Z")
    if "+" in ts_str:
        ts_str = ts_str.split("+")[0]
    return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
