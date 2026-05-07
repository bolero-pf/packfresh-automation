# Pack Fresh — Intake Service

Collection intake management for TCG store operations. Handles sealed product intake via Collectr CSV and raw card intake via manual form entry.

## What This Service Does

1. **Sealed Intake (Collectr CSV)** — Upload CSV export → items listed → staff links each product to a tcgplayer_id → cached for future imports → finalize creates COGS entries with weighted-average cost tracking

2. **Raw Card Intake (Manual Form)** — Staff enters card details + tcgplayer_id → PPT API returns condition-based pricing → calculate offer → finalize creates individual `raw_cards` rows with barcode IDs

3. **PPT Integration** — PokemonPriceTracker API for real-time pricing. Card lookup by tcgplayer_id, sealed product lookup, and fuzzy title matching for auto-suggesting IDs.

## Project Structure

```
intake-service/
├── app.py                  # Flask API routes
├── intake.py               # Core business logic (sessions, mapping, finalization)
├── ppt_client.py           # PokemonPriceTracker API client
├── collectr_parser.py      # Collectr CSV parser (tested against real exports)
├── barcode_gen.py          # Code 128 barcode generation for raw cards
├── db.py                   # Connection pool + query helpers
├── schema.sql              # PostgreSQL schema (all tables, views, triggers)
├── init_db.py              # Schema initializer for Railway
├── requirements.txt        # Python dependencies
├── Procfile                # Railway/Heroku process definition
├── .env.example            # Environment variable template
└── templates/
    └── intake_dashboard.html  # Full dashboard UI
```

## Setup on Railway

### 1. Create Railway project
```bash
railway login
railway init
```

### 2. Add PostgreSQL plugin
In Railway dashboard: Add plugin → PostgreSQL. This auto-sets `DATABASE_URL`.

### 3. Set environment variables
```bash
railway variables set PPT_API_KEY=your_ppt_api_key_here
```

### 4. Initialize database
```bash
railway run python init_db.py
```

### 5. Deploy
```bash
railway up
```

## API Endpoints

### Collectr CSV Flow
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/intake/upload-collectr` | Upload & parse CSV (multipart form) |
| GET | `/api/intake/session/<id>` | Get session details + items |
| POST | `/api/intake/map-item` | Link item to tcgplayer_id |
| POST | `/api/intake/session/<id>/offer` | Lock prices and present offer to customer |

### Raw Card Manual Entry
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/intake/create-session` | Create empty session |
| POST | `/api/intake/add-raw-card` | Add card (auto-prices from PPT) |

### Price provider (Scrydex-first, PPT fallback)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/lookup/card` | Raw card price by tcgplayer_id |
| POST | `/api/lookup/sealed` | Sealed product price by tcgplayer_id |

### Utility
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (DB + PPT status) |

## Key Design Decisions

- **`tcgplayer_id` as universal join key** — Shared across PPT, Shopify metafields, and your database
- **Product mappings are cached** — First time you link "Astral Radiance ETB" → tcgplayer_id 12345, it's remembered for all future imports
- **`shopify_product_id` is nullable** — Gets linked later by a separate Shopify sync process, not during intake
- **Connection pooling** — `psycopg2.ThreadedConnectionPool` instead of per-request connections
- **Barcode format: `PF-YYYYMMDD-XXXXXX`** — Code 128, compatible with Brother QL thermal printers and standard USB scanners. Barcode generation lives in the ingest service (`ingestion/`), not here.

## Schema Notes

Sealed COGS lives on the Shopify variant `cost_per_item` field, maintained by the ingest service at push-live using weighted-average costing:
```
new_avg = (old_qty × old_avg + new_qty × new_unit_cost) / (old_qty + new_qty)
```
Mirrored locally as `inventory_product_cache.unit_cost` via the cache refresh.

Raw cards get individual `cost_basis` values calculated from `offer_price / quantity` and stored on the `raw_cards` row itself (also written by ingest at push-live).
"# retry" 
