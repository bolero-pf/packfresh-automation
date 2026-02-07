# QUICK START GUIDE

## 🚀 Get This Running in 15 Minutes

### Step 1: Set Up Railway Database (5 min)

1. Go to Railway dashboard: https://railway.app
2. Create new project or use existing "Pack Fresh" project
3. Click "+ New" → "Database" → "Add PostgreSQL"
4. Wait for provisioning
5. Copy the `DATABASE_URL` from the PostgreSQL service variables

### Step 2: Initialize Database Schema (2 min)

Option A - Railway CLI:
```bash
railway link [your-project-id]
railway run psql -f schema.sql
```

Option B - Railway Dashboard:
1. Click on PostgreSQL service
2. Go to "Data" tab
3. Click "Query"
4. Copy contents of `schema.sql` and paste
5. Click "Run Query"

### Step 3: Get TCGPlayer API Key (3 min)

1. Go to https://developer.tcgplayer.com/
2. Sign in or create account
3. Create new application
4. Copy API key (Bearer token)

### Step 4: Deploy to Railway (3 min)

```bash
# From this directory
cd /path/to/intake-service

# Initialize git if not already
git init
git add .
git commit -m "Initial intake service"

# Connect to Railway
railway link [your-project-id]

# Create new service
railway up

# Set environment variables in Railway dashboard:
# DATABASE_URL = [from PostgreSQL service]
# TCGPLAYER_API_KEY = [your API key]
# PORT = 5000

# Deploy
git push railway main
```

### Step 5: Set Up Custom Domain (2 min)

1. In Railway dashboard → Your service → Settings
2. Click "Generate Domain" (gives you a .railway.app URL)
3. OR add custom domain: `intake.pack-fresh.com`
4. Update your DNS if using custom domain

### Step 6: Test It! (2 min)

1. Visit your Railway URL
2. You should see "Pack Fresh - Intake System"
3. Try uploading a Collectr CSV:
   - Click "New Intake"
   - Fill in customer name and offer %
   - Upload CSV
   - Should process and show summary

---

## 🔧 Next: Connect to Your JetBrains IDE

1. Open your project in JetBrains IDE
2. Copy these files into your project:
   - `app.py`
   - `schema.sql`
   - `requirements.txt`
   - `templates/intake_dashboard.html`

3. In IDE, you can now:
   - Modify endpoints
   - Add new features
   - Test locally with `python app.py`
   - Ask Claude (me!) to implement specific features

---

## 📝 First Real Test: Import a Collection

### Sample Collectr CSV Format (Sealed):

```csv
Title,Quantity,Market Price
"Pokemon Crimson Invasion Booster Box",5,$89.99
"Pokemon Lost Origin Booster Box",3,$120.00
"Pokemon Scarlet & Violet Base Set Booster Bundle",10,$42.50
```

### Sample Collectr CSV Format (Raw Cards):

```csv
Title,Set,Card Number,Rarity,Condition,Quantity,Market Price
"Charizard VMAX","Champion's Path","074","Secret Rare","NM",1,$250.00
"Pikachu VMAX","Vivid Voltage","188","Secret Rare","LP",1,$45.00
```

### What to Expect:

1. Upload sealed CSV → unmapped products
2. Click "Map" on each product
3. Enter TCGPlayer ID (or search)
4. System fetches current price
5. Click "Finalize"
6. Check `sealed_cogs` table - should see new entries

---

## 🐛 Quick Troubleshooting

**Database connection fails:**
```bash
# Check Railway PostgreSQL is running
railway status

# Verify DATABASE_URL format:
# postgresql://user:password@host:port/database
```

**TCGPlayer API errors:**
- Verify API key is correct (Bearer token, not public key)
- Check rate limits: 1000 req/hr free tier
- Test manually: `curl -H "Authorization: Bearer YOUR_KEY" https://api.tcgplayer.com/v1.39.0/catalog/categories`

**Uploads failing:**
- Check CSV format matches Collectr export
- Verify file size < 10MB
- Check Railway logs: `railway logs`

**Can't find unmapped products:**
- TCGPlayer IDs are specific to condition/printing
- Use TCGPlayer website to find exact product ID
- Check URL: `https://www.tcgplayer.com/product/[ID]`

---

## 🎯 What Works Right Now

✅ Collectr CSV upload (sealed + raw)
✅ TCGPlayer ID mapping (manual + search)
✅ COGS calculation (weighted average)
✅ Session management (active/completed)
✅ Product mapping persistence (learns over time)
✅ Raw card barcode generation
✅ Database audit trails

## 🚧 What Needs Building Next

❌ Shopify integration (creating listings after finalization)
❌ Barcode printing interface
❌ Storage scanning system (PURCHASED → STORED)
❌ Kiosk interface (customer browsing)
❌ POS scanning (PULLED → PENDING_SALE → REMOVED)
❌ Pricing engine (nightly updates)

Once you test the intake system and it's working, we'll build these next!

---

## 💡 Tips for Using Claude in JetBrains

Once this is deployed and working:

1. **In JetBrains IDE**, highlight code blocks and ask me:
   - "Add Shopify API integration to create listings after finalization"
   - "Write a function to print barcode labels as PDF"
   - "Add validation to prevent duplicate product mappings"

2. **Back in this chat**, share:
   - Screenshots of errors
   - Database query results
   - Feature requests with context

3. **In command line**, use me for:
   - Quick scripts: "Write a script to bulk import 100 test cards"
   - Database queries: "Show me all cards purchased but not stored"
   - Deployment: "Help me set up continuous deployment"

---

Ready to deploy? Start with Step 1 above! 🚀
