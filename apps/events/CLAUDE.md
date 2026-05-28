# Events Service (events/)
> Staff console for event series + occurrence management (events.pack-fresh.com)

## Purpose
Operational UI for managing in-store events (Commander, FNM, Pokemon League, etc.).
Backs a public calendar at `/pages/events` and per-series SEO landing pages at
`/events/<handle>`. Designed primarily for advertising — SEO sitelinks, FB Events,
Google rich results. Discord publisher is deferred.

## Key Files
- **app.py** — Flask app: console UI + API endpoints inline
- **shopify_client.py** — Shopify Admin GraphQL helpers for metaobjects + files
- **Dockerfile / railway.toml** — Standard service deploy

## Metaobjects (source of truth — NO local DB)
Type strings are NOT renamable in Shopify; preserved from initial setup:

| Purpose | API type | Fields |
|---|---|---|
| Series  | `event` | title, color, status (`active`/`draft`), schedule_description, entry_cost (Money), description_short, description_long (rich text), hero_image (File ref) |
| Occurrence | `event_occurence` (**single 'r'**) | series (ref→event), start_datetime, end_datetime, fb_event_url, cancelled, discord_message_id, discord_scheduled_event_id, label |

Storefront Liquid templates in `apps/events/section.*.liquid` consume the same metaobjects.

## API Endpoints
- `GET  /api/series` — list all
- `POST /api/series` — create
- `POST /api/series/save` — update (body must include `id`)
- `POST /api/series/delete` — delete (body must include `id`)
- `GET  /api/occurrences` — list all
- `POST /api/occurrences` — create
- `POST /api/occurrences/save` — update
- `POST /api/occurrences/delete` — delete
- `POST /api/series/generate` — bulk-create N occurrences for a series (weekly/monthly)
- `POST /api/upload` — multipart image upload, returns Shopify MediaImage GID

## Auth
- JWT cookie (owner + manager) via `register_auth_hooks`
- No public routes

## Required ENV
- `SHOPIFY_TOKEN` — Admin API access token (with metaobject + files write scopes)
- `SHOPIFY_STORE` — store domain (e.g., `pack-fresh.myshopify.com`)
- `ADMIN_JWT_SECRET` — shared JWT secret
- `PORT` — gunicorn port (Railway sets this)

## Datetime Conventions
- Store TZ: `America/Chicago` (defined as `STORE_TZ` in app.py)
- Inbound from UI: `YYYY-MM-DD` + `HH:MM` (24h) → server combines into ISO 8601 with offset
- Stored in Shopify as ISO 8601 with offset; rendered server-side back to local components for editing

## Rich Text Storage
- `description_long` is rich text. UI takes plain text with blank-line paragraph breaks.
- `plain_text_to_rich()` converts plain text → Shopify rich text JSON tree.
- `rich_to_plain_text()` round-trips for editing.

## Image Upload Flow
1. UI POSTs multipart to `/api/upload`
2. Backend calls Shopify `stagedUploadsCreate` → presigned target URL
3. Backend POSTs file bytes to target
4. Backend calls `fileCreate` to register as MediaImage
5. Returns the MediaImage GID; UI sets it on `hero_image` when saving Series
