# Intake Service (ingest-service/)
> This is the **intake** service despite the directory name. See root CLAUDE.md.

## Key Files
- **app.py** — Flask routes and API endpoints. `/api/intake/sessions` is the main session listing endpoint (~line 589)
- **intake.py** — Business logic for sessions (create, list, update offers, add items)
- **ingestion_app.py** — Ingestion-side logic (push items, partial ingestion)
- **schema.sql** — DB schema
- **templates/intake_dashboard.html** — Main dashboard UI (single-page app with tabs)

## Session Status Flow
```
in_progress → offered → accepted → received → partially_ingested → ingested → finalized
                      → rejected
in_progress → cancelled
```

## Dashboard Tabs (intake_dashboard.html)
- **Sealed Intake** — CSV upload for sealed product
- **Raw Intake** — Individual card entry
- **Active Sessions** — Shows statuses: in_progress, offered, accepted, partially_ingested
- **Completed** — Shows statuses: received, ingested, finalized
- **Cancelled** — Shows cancelled sessions

## Key Patterns
- Session status filtering is driven by the frontend dropdown and default load in `intake_dashboard.html`
- Status badge colors are defined in a `statusColors` JS object in the dashboard template
- `intake.py` has guard clauses that block modifications (offer %, adding items) based on session status
- `list_sessions()` accepts comma-separated status strings for flexible filtering
