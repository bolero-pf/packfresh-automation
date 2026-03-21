# Pack Fresh Monorepo

## Directory Naming (IMPORTANT)
- **`ingest-service/`** = the **intake** service (offers.pack-fresh.com)
- **`ingestion/`** = the **ingest** service (ingest.pack-fresh.com)
- These names are swapped from what you'd expect. Do not confuse them.

## Services
1. **ingest-service/** - Offers intake (offers.pack-fresh.com)
2. **ingestion/** - Data ingestion (ingest.pack-fresh.com)
3. **inventory/** - Inventory + Breakdown Engine (inventory.pack-fresh.com)
4. **price_updater/** - Price sync from TCGPlayer (prices.pack-fresh.com)
5. **price_updater/vip/** - VIP service (prices.pack-fresh.com/vip/)
6. **price_updater/screening/** - Fraud/verification/abuse screening (prices.pack-fresh.com/screening/)
7. **kiosk/** - Raw card browser (kiosk.pack-fresh.com)
8. **card_manager/** - Card admin panel (cardadmin.pack-fresh.com)
9. **card_browser/** - Card browser
10. **frontpage_update/** - Front page randomizer cron job
11. **shared/** - Components shared by intake, ingest, inventory, kiosk, cardadmin
12. **psa_lookup/** - PSA cert lookup/cache
13. **slab_updater/** - Slab inventory updates
14. **pull_list/** - Pull list management
15. **inventory_ui/** - Inventory UI
16. **inventory_value_calc/** - Inventory value calculations
17. **conference_price/** - Conference pricing
18. **drop_updater/** - Drop updates
19. **gtin_updater/** - GTIN updates
20. **sku-updater/** - SKU updates
21. **tag_updater/** - Tag updates
22. **whatnot_sorter/** - Whatnot sorting
23. **southern_inventory/** - Southern inventory
24. **dashboard/** - Dashboard
25. **libs/** - Shared libraries

## Stack
- Python/Flask for backend services
- Deployed on Railway via GitHub
- Shopify GraphQL API for store operations
- Klaviyo for email flows
- Each service has its own Railway deployment with watch paths

## Rules
- Do NOT modify files outside your assigned service directory
- shared/ components require coordination — do not modify without explicit approval
- All Shopify GraphQL goes through vip/service.py's shopify_gql() helper
- Klaviyo integration is in integrations/klaviyo.py
- Environment variables are in Railway, not committed

## Per-Service CLAUDE.md
- Each service directory has its own CLAUDE.md with architecture notes
- When exploring a service, update its CLAUDE.md with stable architectural knowledge (file roles, key patterns, status flows) — NOT things that change frequently like specific variable values or line numbers
- This saves significant token usage in future sessions
