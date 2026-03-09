"""
cache_manager.py — Self-aware Shopify product cache staleness management.

Tracks when the cache was last refreshed and why, and decides when a
background refresh is needed. All staleness decisions live here.

Staleness triggers (any one is sufficient):
  1. Price updater window: last_refreshed_at is between 03:30 and 08:30 today
     → refresh once the clock passes 08:30 (price updater will have finished)
  2. New orders: latest Shopify order number has advanced since last refresh
     → sales have happened, inventory quantities may have changed
  3. Products updated: latest Shopify product updated_at has advanced
     → catches price updates, manual admin edits, new listings, ingest
  4. Explicit invalidation: ingest, inventory edits, or manual trigger
     → immediate background refresh

Usage:
    from cache_manager import CacheManager
    cm = CacheManager(db, shopify_client)

    # On every cache read — fires background refresh if stale, returns immediately
    cm.check_and_refresh_if_stale()

    # After ingest push-live completes
    cm.invalidate("ingest")

    # From API route
    cm.invalidate("manual")
"""

import logging
import threading
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# Price updater runs 03:30–08:00; we refresh after 08:30 to be safe
PRICE_UPDATER_START = (3, 30)   # (hour, minute) local time — 24h
PRICE_UPDATER_DONE  = (8, 30)   # refresh any cache last updated during this window


class CacheManager:
    def __init__(self, db, shopify_client):
        self.db = db
        self.shopify = shopify_client
        self._refresh_lock = threading.Lock()
        self._refresh_in_progress = False

    # ─── Public API ───────────────────────────────────────────────────────────

    def check_and_refresh_if_stale(self) -> dict:
        """
        Check staleness and fire a background refresh if needed.
        Returns immediately — refresh happens in a daemon thread.
        Returns a dict describing the staleness check result.
        """
        if self.shopify is None:
            return {"stale": False, "reason": "shopify_not_configured"}

        meta = self._get_meta()
        if meta is None:
            logger.info("cache_meta not found — triggering initial refresh")
            self._fire_background_refresh("initial")
            return {"stale": True, "reason": "initial"}

        reasons = self._check_staleness(meta)
        if reasons:
            logger.info(f"Cache stale ({', '.join(reasons)}) — triggering background refresh")
            self._fire_background_refresh(reasons[0])
            return {"stale": True, "reason": reasons[0], "all_reasons": reasons}

        return {"stale": False, "last_refreshed_at": meta["last_refreshed_at"].isoformat()}

    def invalidate(self, reason: str = "explicit") -> None:
        """
        Mark the cache as needing a refresh and fire one immediately in background.
        Call this after ingest, price updates, or inventory edits.
        """
        logger.info(f"Cache invalidated (reason: {reason})")
        self._update_meta(invalidated_reason=reason)
        self._fire_background_refresh(reason)

    def get_status(self) -> dict:
        """Return cache health info for display in UI."""
        meta = self._get_meta()
        if meta is None:
            return {"status": "never_synced", "last_refreshed_at": None}

        now = datetime.now(timezone.utc)
        age_minutes = (now - meta["last_refreshed_at"].replace(tzinfo=timezone.utc)).total_seconds() / 60

        return {
            "status": "ok",
            "last_refreshed_at": meta["last_refreshed_at"].isoformat(),
            "last_refreshed_reason": meta.get("last_refreshed_reason"),
            "last_order_number": meta.get("last_order_number"),
            "last_product_updated_at": meta["last_product_updated_at"].isoformat() if meta.get("last_product_updated_at") else None,
            "age_minutes": round(age_minutes, 1),
            "refresh_in_progress": self._refresh_in_progress,
        }

    # ─── Staleness checks ─────────────────────────────────────────────────────

    def _check_staleness(self, meta: dict) -> list[str]:
        """Return list of staleness reasons (empty = cache is fresh)."""
        reasons = []

        # 1. Price updater window check
        if self._in_price_updater_window(meta["last_refreshed_at"]):
            reasons.append("price_updater_window")

        # 2 & 3. Check Shopify for new orders / product updates
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
        """
        Returns True if the cache was last refreshed during the price updater
        window (03:30–08:30 today), meaning it was stale during the run and
        hasn't been refreshed since.
        """
        now = datetime.now()  # local time
        today_start = (PRICE_UPDATER_START[0] * 60 + PRICE_UPDATER_START[1])  # minutes since midnight
        today_done  = (PRICE_UPDATER_DONE[0]  * 60 + PRICE_UPDATER_DONE[1])
        now_minutes = now.hour * 60 + now.minute

        # Only relevant after the window has closed for today
        if now_minutes < today_done:
            return False

        # Convert last_refreshed_at to local naive datetime for comparison
        if last_refreshed_at.tzinfo is not None:
            lr = last_refreshed_at.astimezone().replace(tzinfo=None)
        else:
            lr = last_refreshed_at

        today = now.date()
        window_start = datetime(today.year, today.month, today.day,
                                PRICE_UPDATER_START[0], PRICE_UPDATER_START[1])
        window_end   = datetime(today.year, today.month, today.day,
                                PRICE_UPDATER_DONE[0],  PRICE_UPDATER_DONE[1])

        # Cache was last refreshed during the window → stale
        return window_start <= lr <= window_end

    # ─── Background refresh ───────────────────────────────────────────────────

    def _fire_background_refresh(self, reason: str) -> None:
        """Start a background thread to rebuild the cache. No-ops if one is already running."""
        if self._refresh_in_progress:
            logger.info("Refresh already in progress — skipping duplicate trigger")
            return
        t = threading.Thread(target=self._run_refresh, args=(reason,), daemon=True)
        t.start()

    def _run_refresh(self, reason: str) -> None:
        """Full cache rebuild — runs in background thread."""
        with self._refresh_lock:
            if self._refresh_in_progress:
                return
            self._refresh_in_progress = True

        try:
            logger.info(f"Cache refresh starting (reason: {reason})")
            start = datetime.now(timezone.utc)

            # Snapshot staleness signals before refresh so we can record them
            signals = {}
            try:
                signals = self.shopify.get_cache_staleness_signals()
            except Exception as e:
                logger.warning(f"Could not fetch staleness signals: {e}")

            # Pull all products from Shopify and upsert into cache
            upserted = 0
            for page_products, _ in self.shopify.iter_products_pages(batch_size=100):
                with_tcg = [p for p in page_products if p.get("tcgplayer_id")]
                for p in with_tcg:
                    self.db.execute("""
                        INSERT INTO shopify_product_cache
                            (tcgplayer_id, shopify_product_id, shopify_variant_id,
                             title, handle, sku, shopify_price, shopify_qty, status, is_damaged, last_synced)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (tcgplayer_id, shopify_variant_id)
                        DO UPDATE SET title = EXCLUDED.title, handle = EXCLUDED.handle, sku = EXCLUDED.sku,
                            shopify_price = EXCLUDED.shopify_price, shopify_qty = EXCLUDED.shopify_qty,
                            status = EXCLUDED.status, is_damaged = EXCLUDED.is_damaged,
                            last_synced = CURRENT_TIMESTAMP
                    """, (p["tcgplayer_id"], p["shopify_product_id"], p["variant_id"],
                          p["title"], p["handle"], p.get("sku"), p["shopify_price"], p["shopify_qty"],
                          p.get("status", "ACTIVE"), p.get("is_damaged", False)))
                    upserted += 1

            # Backfill sealed_cogs linkage
            self.db.execute("""
                UPDATE sealed_cogs sc SET shopify_product_id = spc.shopify_product_id
                FROM shopify_product_cache spc
                WHERE sc.tcgplayer_id = spc.tcgplayer_id AND sc.shopify_product_id IS NULL
            """)

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.info(f"Cache refresh complete: {upserted} rows in {elapsed:.1f}s (reason: {reason})")

            # Update meta
            self._set_meta(
                reason=reason,
                order_number=signals.get("latest_order_number"),
                product_updated_at=signals.get("latest_product_updated_at"),
            )

        except Exception as e:
            logger.error(f"Cache refresh failed: {e}")
        finally:
            self._refresh_in_progress = False

    # ─── cache_meta DB helpers ────────────────────────────────────────────────

    def _get_meta(self) -> dict | None:
        try:
            return self.db.query_one("SELECT * FROM cache_meta LIMIT 1")
        except Exception:
            return None

    def _set_meta(self, reason: str, order_number=None, product_updated_at=None) -> None:
        try:
            ts = _parse_ts(product_updated_at) if product_updated_at else None
            self.db.execute("""
                INSERT INTO cache_meta (id, last_refreshed_at, last_refreshed_reason,
                                        last_order_number, last_product_updated_at)
                VALUES (1, CURRENT_TIMESTAMP, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_refreshed_at = CURRENT_TIMESTAMP,
                    last_refreshed_reason = EXCLUDED.last_refreshed_reason,
                    last_order_number = EXCLUDED.last_order_number,
                    last_product_updated_at = EXCLUDED.last_product_updated_at
            """, (reason, order_number, ts))
        except Exception as e:
            logger.warning(f"Failed to update cache_meta: {e}")

    def _update_meta(self, invalidated_reason: str) -> None:
        """Mark cache as needing refresh without rebuilding yet."""
        try:
            self.db.execute("""
                INSERT INTO cache_meta (id, last_refreshed_at, last_refreshed_reason)
                VALUES (1, '1970-01-01', %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_refreshed_reason = %s
            """, (f"invalidated:{invalidated_reason}", f"invalidated:{invalidated_reason}"))
        except Exception as e:
            logger.warning(f"Failed to mark cache invalidated: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO8601 timestamp string to UTC-aware datetime."""
    if not ts_str:
        return None
    ts_str = ts_str.rstrip("Z")
    if "+" in ts_str:
        ts_str = ts_str.split("+")[0]
    dt = datetime.fromisoformat(ts_str)
    return dt.replace(tzinfo=timezone.utc)
