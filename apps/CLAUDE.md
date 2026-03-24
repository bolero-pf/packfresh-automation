# Pack Fresh Monorepo

## Directory Naming (IMPORTANT)
- **`ingest-service/`** = the **intake** service (offers.pack-fresh.com)
- **`ingestion/`** = the **ingest** service (ingest.pack-fresh.com)
- These names are swapped from what you'd expect. Do not confuse them.

## Services (Active)
1. **admin/** - Unified login portal + command console (admin.pack-fresh.com) — JWT auth, user management
2. **ingest-service/** - Offers intake (offers.pack-fresh.com)
3. **ingestion/** - Data ingestion / warehouse processing (ingest.pack-fresh.com)
4. **inventory/** - Inventory + Breakdown Engine (inventory.pack-fresh.com)
5. **price_updater/** - Nightly sealed SKU price sync (prices.pack-fresh.com) — NO LONGER hosts VIP or screening
6. **screening/** - Order fraud detection + verification console (screening.pack-fresh.com)
7. **vip/** - VIP tier management + customer console (vip.pack-fresh.com)
8. **kiosk/** - Customer-facing card browser (kiosk.pack-fresh.com) — public, no auth
9. **card_manager/** - Staff card hold processing (cards.pack-fresh.com)
10. **analytics/** - SKU sell-through velocity metrics (analytics.pack-fresh.com) — daily order ingestion + per-variant velocity
11. **drops/** - Drop planner + scheduling (drops.pack-fresh.com) — weekly + VIP drops, deal candidates
12. **frontpage_update/** - Front page randomizer cron job
13. **shared/** - Components shared by all services (auth, Shopify GQL, Klaviyo, PPT client, breakdown helpers, sku_analytics, etc.)

## Authentication
- **JWT cookie** (`pf_auth`) scoped to `.pack-fresh.com` — issued by admin service, validated by all staff services
- **`shared/auth.py`** provides `require_auth()` middleware + `inject_admin_bar()` for console nav bar
- **`ADMIN_JWT_SECRET`** env var must be set on: admin, intake, ingest, inventory, card_manager, price_updater
- **Roles**: owner (everything), manager (no user mgmt), associate (limited apps)
- **Public**: kiosk (customer-facing, no auth)
- **Webhook auth**: screening + VIP use `X-Flow-Secret` header (Shopify Flows), NOT JWT
- **Legacy**: some services still have HTTP Basic Auth as fallback — `requires_auth` decorator checks `g.user` first

## Shared Components (shared/)
- **auth.py** — JWT cookie validation, admin bar injection, role checking
- **shopify_graphql.py** — Shopify Admin GraphQL client with retry (used by vip, screening)
- **klaviyo.py** — Klaviyo profile upsert with duplicate resolution
- **webhook_verify.py** — X-Flow-Secret validation for Shopify Flow webhooks
- **ppt_client.py** — PokemonPriceTracker API client
- **breakdown_helpers.py** — JIT component price refresh from PPT
- **cache_manager.py** — Shopify product cache with staleness detection
- **storage.py** — Bin assignment for raw card storage

## Stack
- Python/Flask for backend services
- Deployed on Railway via GitHub (`git push origin main`)
- Shopify GraphQL API for store operations (shared/shopify_graphql.py)
- Shopify REST API for listings (card_manager)
- Klaviyo for email flows (shared/klaviyo.py)
- Each service has its own Railway deployment with watch paths
- JWT auth across all staff services via shared cookie

## Rules
- Do NOT modify files outside your assigned service directory
- shared/ components affect ALL services — coordinate changes carefully
- All Shopify GraphQL goes through shared/shopify_graphql.py
- Klaviyo integration is in shared/klaviyo.py — don't change properties without discussing
- Environment variables are in Railway, not committed
- **Never break live services during migration** — two-phase: add new, verify, then remove old

## Per-Service CLAUDE.md
- Each service directory has its own CLAUDE.md with architecture notes
- When exploring a service, update its CLAUDE.md with stable architectural knowledge (file roles, key patterns, status flows) — NOT things that change frequently like specific variable values or line numbers
- This saves significant token usage in future sessions

## Working With Me

### Who I Am
- Former SDET at Microsoft → dev → dev manager → director of software development in analytics
- I think about how things break first, then work backward to the happy path
- I test in production with real orders — there is no test store
- I manage all services across Railway, deploy via `git push origin main`

### How I Communicate
- Direct answers, no hedging, no jargon
- I describe things in **usage patterns** — UX-focused for most services, technical for backend-only services (/vip/, /screening/, price_updater)
- When I bring a "bug list," these are real failures I've hit through actual usage, not theoretical issues
- I spend time up front describing what I want — trust that description

### How to Work With Me
- **If you're confident you understand, just implement, say you're done, and push to main.** No summaries of what you changed.
- **If there's any uncertainty, /plan/ first.** Don't guess and build the wrong thing.
- **Audit all affected services before coding.** The same concept (e.g., "breakdown") means different things at different points in the pipeline. Market value in intake-offers vs store value in intake-store vs margin optimization in inventory. Ask yourself: does this concept appear in other services? Tell me which services you're updating and what each one will do differently.
- **Don't change Klaviyo properties without discussing it first.** I need to know what's emitted.
- **Code must be deployable immediately.** No theoretical, no "you'll need to also do X." Ship it.
- **Don't break existing integrations/flows.** If a change has side effects on other services, flag it before pushing.

### What These Tools Are For
- These services are **operational infrastructure for new hires**, not just for me
- Intake: new hire prices collections, determines margin, queues for approval, makes the offer
- Ingest: when product arrives, we know what made it in, press one button, organize onto shelves
- Inventory/Breakdown: quick lookups, monthly physical inventory, decide what to break down for raw packs
- Screening: prevents fulfillment agent from shipping orders with holds
- VIP: auto-maintains Klaviyo segments so customers get correct discounts and drops
- Price Updater: nightly sealed SKU price sync so we don't lose margin
- Card Manager: staff pulls cards for customer holds, processes accepts/rejects, handles missing cards
- Admin: single login for all tools, role-based access
- Everything must be foolproof enough that someone unfamiliar with the business can use it correctly

### Git / Deploy
- Push directly to main — I don't use branches or PRs
- Railway deploys from GitHub on push, each service has its own watch paths
- If something breaks in prod, walk me through the fix rather than pushing git workflow on me
