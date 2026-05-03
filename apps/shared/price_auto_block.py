"""Helpers for the price_auto_block table.

The nightly price updaters (raw, slab, sealed) all run an "always raise"
auto-apply policy now: any upward market move flows straight through to
the live price without human review. That's the right default for the
common case but it has a known failure mode — a corrupted catalog
mapping can suggest moving a $10 card to $13,000 and we'd happily push
it. price_auto_block is the escape hatch: a single per-(domain, key)
mute that the updater consults at the top of every run and treats as
action='skip', reason='auto-block'.

Domain / block_key conventions (kept in sync with 020_price_auto_block.sql):
  raw    -> scrydex_id when present, else 'tcg:<tcgplayer_id>'
  slab   -> variant_gid (each slab is a unique Shopify listing)
  sealed -> variant_id (the Shopify numeric variant id, as a string)
"""

from typing import Set


def raw_key(scrydex_id, tcgplayer_id) -> str | None:
    """Canonical block_key for a raw card. scrydex_id wins; tcg fallback
    is namespaced so it can't collide with a real scrydex_id."""
    if scrydex_id:
        return str(scrydex_id)
    if tcgplayer_id:
        return f"tcg:{tcgplayer_id}"
    return None


def load_blocks(db_module, domain: str) -> Set[str]:
    """Return the set of block_keys for the given domain. Cheap query
    intended to run once at the top of an updater pass."""
    rows = db_module.query(
        "SELECT block_key FROM price_auto_block WHERE domain = %s",
        (domain,),
    )
    return {r["block_key"] for r in rows}


def add_block(db_module, *, domain: str, block_key: str,
              label: str | None = None, reason: str | None = None,
              blocked_by: str | None = None) -> bool:
    """Insert a block. Idempotent — returns True if newly inserted,
    False if it already existed."""
    row = db_module.execute_returning(
        """INSERT INTO price_auto_block (domain, block_key, label, reason, blocked_by)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (domain, block_key) DO NOTHING
           RETURNING id""",
        (domain, block_key, label, reason, blocked_by),
    )
    return row is not None


def remove_block(db_module, *, domain: str, block_key: str) -> int:
    """Remove a block. Returns the number of rows deleted (0 or 1)."""
    return db_module.execute(
        "DELETE FROM price_auto_block WHERE domain = %s AND block_key = %s",
        (domain, block_key),
    )


def list_blocks(db_module) -> list[dict]:
    """All blocks across every domain, newest first — for the
    /dashboard/price-blocks page."""
    return db_module.query(
        """SELECT id, domain, block_key, label, reason, blocked_by, blocked_at
             FROM price_auto_block
            ORDER BY blocked_at DESC""")
