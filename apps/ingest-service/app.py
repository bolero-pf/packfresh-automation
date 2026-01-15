"""
TCG Store - Intake Service
Handles collection purchases (sealed + raw cards)
"""

import os
import csv
import hashlib
from datetime import datetime
from decimal import Decimal
from io import StringIO
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from flask import Flask, request, jsonify, render_template, redirect, url_for
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import requests

app = Flask(__name__)
CORS(app)

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL')
TCGPLAYER_API_KEY = os.getenv('TCGPLAYER_API_KEY')
TCGPLAYER_API_URL = "https://api.tcgplayer.com/v1.39.0"

# ==========================================
# DATABASE HELPERS
# ==========================================

def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def execute_query(query: str, params: tuple = None, fetch: bool = True):
    """Execute a query and return results"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()

def execute_many(query: str, params_list: List[tuple]):
    """Execute batch insert/update"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, params_list)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()

# ==========================================
# TCGPLAYER API HELPERS
# ==========================================

def get_tcgplayer_price(tcgplayer_id: int, condition: str = 'Near Mint') -> Optional[Decimal]:
    """
    Get current market price from TCGPlayer API
    Returns lowest listing price for the given condition
    """
    try:
        headers = {
            'Authorization': f'Bearer {TCGPLAYER_API_KEY}',
            'Accept': 'application/json'
        }
        
        # Get product pricing
        url = f"{TCGPLAYER_API_URL}/pricing/product/{tcgplayer_id}"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        pricing_data = response.json()
        
        # Find price for matching condition
        for price_entry in pricing_data.get('results', []):
            if price_entry.get('printingCondition', '').lower() == condition.lower():
                return Decimal(str(price_entry.get('lowPrice', 0)))
        
        # Fallback to market price if condition not found
        return Decimal(str(pricing_data.get('results', [{}])[0].get('marketPrice', 0)))
    
    except Exception as e:
        app.logger.error(f"TCGPlayer API error for product {tcgplayer_id}: {e}")
        return None

def search_tcgplayer_product(product_name: str, set_name: str = None) -> List[Dict]:
    """
    Search TCGPlayer for products matching name/set
    Returns list of potential matches
    """
    try:
        headers = {
            'Authorization': f'Bearer {TCGPLAYER_API_KEY}',
            'Accept': 'application/json'
        }
        
        # Build search query
        search_query = product_name
        if set_name:
            search_query = f"{product_name} {set_name}"
        
        url = f"{TCGPLAYER_API_URL}/catalog/products"
        params = {
            'name': search_query,
            'limit': 10
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        results = response.json().get('results', [])
        return [
            {
                'tcgplayer_id': r.get('productId'),
                'name': r.get('name'),
                'set_name': r.get('groupName'),
                'product_type': r.get('productTypeName')
            }
            for r in results
        ]
    
    except Exception as e:
        app.logger.error(f"TCGPlayer search error for '{product_name}': {e}")
        return []

# ==========================================
# PRODUCT MAPPING HELPERS
# ==========================================

def get_product_mapping(collectr_name: str, product_type: str) -> Optional[int]:
    """Check if we have a saved mapping for this Collectr product"""
    query = """
        SELECT tcgplayer_id, use_count 
        FROM product_mappings 
        WHERE collectr_name = %s AND product_type = %s
    """
    results = execute_query(query, (collectr_name, product_type))
    
    if results:
        # Update usage tracking
        update_query = """
            UPDATE product_mappings 
            SET use_count = use_count + 1, last_used = CURRENT_TIMESTAMP
            WHERE collectr_name = %s AND product_type = %s
        """
        execute_query(update_query, (collectr_name, product_type), fetch=False)
        return results[0]['tcgplayer_id']
    
    return None

def save_product_mapping(collectr_name: str, tcgplayer_id: int, product_type: str, 
                        set_name: str = None, card_number: str = None):
    """Save a new product mapping for future imports"""
    query = """
        INSERT INTO product_mappings (collectr_name, tcgplayer_id, product_type, set_name, card_number)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (collectr_name, product_type) 
        DO UPDATE SET 
            tcgplayer_id = EXCLUDED.tcgplayer_id,
            last_used = CURRENT_TIMESTAMP,
            use_count = product_mappings.use_count + 1
    """
    execute_query(query, (collectr_name, tcgplayer_id, product_type, set_name, card_number), fetch=False)

# ==========================================
# COLLECTR CSV PARSER
# ==========================================

def parse_collectr_csv(file_content: str) -> Tuple[List[Dict], str]:
    """
    Parse Collectr CSV export
    Returns (items_list, product_type)
    """
    csv_reader = csv.DictReader(StringIO(file_content))
    items = []
    product_type = None
    
    for row in csv_reader:
        # Detect product type from CSV structure
        if 'Card Number' in row or 'Rarity' in row:
            product_type = 'raw'
            item = {
                'product_name': row.get('Title', '').strip(),
                'card_name': row.get('Title', '').strip(),
                'set_name': row.get('Set', '').strip(),
                'card_number': row.get('Card Number', '').strip(),
                'condition': row.get('Condition', 'NM').strip(),
                'rarity': row.get('Rarity', '').strip(),
                'quantity': int(row.get('Quantity', 1)),
                'market_price': Decimal(row.get('Market Price', '0').replace('$', '').replace(',', ''))
            }
        else:
            product_type = 'sealed'
            item = {
                'product_name': row.get('Title', '').strip(),
                'quantity': int(row.get('Quantity', 1)),
                'market_price': Decimal(row.get('Market Price', '0').replace('$', '').replace(',', ''))
            }
        
        items.append(item)
    
    return items, product_type

# ==========================================
# INTAKE SESSION MANAGEMENT
# ==========================================

def create_intake_session(customer_name: str, session_type: str, 
                         offer_percentage: Decimal, file_name: str = None,
                         file_hash: str = None) -> str:
    """Create a new intake session"""
    session_id = str(uuid4())
    
    query = """
        INSERT INTO intake_sessions 
        (id, customer_name, session_type, offer_percentage, source_file_name, source_file_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_query(query, (session_id, customer_name, session_type, offer_percentage, 
                         file_name, file_hash), fetch=False)
    
    return session_id

def add_intake_items(session_id: str, items: List[Dict]):
    """Add items to an intake session"""
    query = """
        INSERT INTO intake_items 
        (session_id, product_name, tcgplayer_id, product_type, set_name, card_number, 
         condition, rarity, quantity, market_price, offer_price, unit_cost_basis, is_mapped)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    params_list = [
        (
            session_id,
            item['product_name'],
            item.get('tcgplayer_id'),
            item['product_type'],
            item.get('set_name'),
            item.get('card_number'),
            item.get('condition'),
            item.get('rarity'),
            item['quantity'],
            item['market_price'],
            item['offer_price'],
            item['unit_cost_basis'],
            item.get('tcgplayer_id') is not None
        )
        for item in items
    ]
    
    execute_many(query, params_list)

def finalize_intake_session(session_id: str) -> Dict:
    """
    Finalize intake session:
    - For sealed: Create Shopify listings + update COGS
    - For raw: Create raw_cards entries with barcodes
    """
    # Get session and items
    session = execute_query(
        "SELECT * FROM intake_sessions WHERE id = %s",
        (session_id,)
    )[0]
    
    items = execute_query(
        "SELECT * FROM intake_items WHERE session_id = %s",
        (session_id,)
    )
    
    # Check if all items are mapped
    unmapped = [i for i in items if not i['is_mapped']]
    if unmapped:
        return {
            'success': False,
            'error': f'{len(unmapped)} items need TCGPlayer ID mapping'
        }
    
    # Process based on session type
    if session['session_type'] in ['sealed', 'mixed']:
        sealed_items = [i for i in items if i['product_type'] == 'sealed']
        _process_sealed_items(sealed_items, session_id)
    
    if session['session_type'] in ['raw', 'mixed']:
        raw_items = [i for i in items if i['product_type'] == 'raw']
        _process_raw_items(raw_items, session_id)
    
    # Mark session as finalized
    execute_query(
        "UPDATE intake_sessions SET status = 'finalized', finalized_at = CURRENT_TIMESTAMP WHERE id = %s",
        (session_id,),
        fetch=False
    )
    
    return {'success': True, 'session_id': session_id}

def _process_sealed_items(items: List[Dict], session_id: str):
    """Process sealed products: update COGS and create Shopify listings"""
    for item in items:
        # Check if we have existing COGS entry
        existing = execute_query(
            "SELECT * FROM sealed_cogs WHERE tcgplayer_id = %s",
            (item['tcgplayer_id'],)
        )
        
        quantity_delta = item['quantity']
        cost_added = item['offer_price']
        
        if existing:
            # Update weighted average COGS
            old_qty = existing[0]['current_quantity']
            old_total = existing[0]['total_cost']
            
            new_qty = old_qty + quantity_delta
            new_total = old_total + cost_added
            new_avg = new_total / new_qty if new_qty > 0 else Decimal('0')
            
            execute_query("""
                UPDATE sealed_cogs 
                SET current_quantity = %s, total_cost = %s, avg_cogs = %s, 
                    last_updated = CURRENT_TIMESTAMP, last_intake_session_id = %s
                WHERE tcgplayer_id = %s
            """, (new_qty, new_total, new_avg, session_id, item['tcgplayer_id']), fetch=False)
            
            # Log COGS history
            execute_query("""
                INSERT INTO cogs_history 
                (sealed_cogs_id, old_quantity, new_quantity, old_avg_cogs, new_avg_cogs, 
                 quantity_delta, cost_added, intake_session_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (existing[0]['id'], old_qty, new_qty, existing[0]['avg_cogs'], 
                 new_avg, quantity_delta, cost_added, session_id), fetch=False)
        else:
            # Create new COGS entry
            avg_cogs = cost_added / quantity_delta if quantity_delta > 0 else Decimal('0')
            
            execute_query("""
                INSERT INTO sealed_cogs 
                (tcgplayer_id, product_name, current_quantity, total_cost, avg_cogs, last_intake_session_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (item['tcgplayer_id'], item['product_name'], quantity_delta, 
                 cost_added, avg_cogs, session_id), fetch=False)

def _process_raw_items(items: List[Dict], session_id: str):
    """Process raw cards: create inventory entries with barcodes"""
    cards_to_insert = []
    
    for item in items:
        for _ in range(item['quantity']):
            barcode = _generate_barcode()
            cards_to_insert.append({
                'barcode': barcode,
                'tcgplayer_id': item['tcgplayer_id'],
                'card_name': item['product_name'],
                'set_name': item['set_name'],
                'card_number': item['card_number'],
                'condition': item['condition'],
                'rarity': item['rarity'],
                'cost_basis': item['unit_cost_basis'],
                'current_price': item['market_price'],
                'intake_session_id': session_id
            })
    
    # Batch insert
    query = """
        INSERT INTO raw_cards 
        (barcode, tcgplayer_id, card_name, set_name, card_number, condition, rarity,
         cost_basis, current_price, intake_session_id, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PURCHASED')
    """
    
    params_list = [
        (c['barcode'], c['tcgplayer_id'], c['card_name'], c['set_name'], 
         c['card_number'], c['condition'], c['rarity'], c['cost_basis'], 
         c['current_price'], c['intake_session_id'])
        for c in cards_to_insert
    ]
    
    execute_many(query, params_list)

def _generate_barcode() -> str:
    """Generate unique barcode for raw cards"""
    # Format: PF-YYYYMMDD-XXXXXX (PF = Pack Fresh, then date, then random)
    date_str = datetime.now().strftime('%Y%m%d')
    random_suffix = str(uuid4())[:6].upper()
    return f"PF-{date_str}-{random_suffix}"

# ==========================================
# API ENDPOINTS
# ==========================================

@app.route('/')
def index():
    """Main intake dashboard"""
    return render_template('intake_dashboard.html')

@app.route('/api/intake/upload-collectr', methods=['POST'])
def upload_collectr():
    """
    Upload Collectr CSV export
    Creates intake session and parses items
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    customer_name = request.form.get('customer_name', 'Unknown')
    offer_percentage = Decimal(request.form.get('offer_percentage', '75'))
    
    # Read and hash file
    file_content = file.read().decode('utf-8')
    file_hash = hashlib.sha256(file_content.encode()).hexdigest()
    
    # Check for duplicate import
    existing = execute_query(
        "SELECT id FROM intake_sessions WHERE source_file_hash = %s",
        (file_hash,)
    )
    if existing:
        return jsonify({'error': 'This file has already been imported'}), 400
    
    # Parse CSV
    try:
        items, product_type = parse_collectr_csv(file_content)
    except Exception as e:
        return jsonify({'error': f'Failed to parse CSV: {str(e)}'}), 400
    
    # Create session
    session_id = create_intake_session(
        customer_name, product_type, offer_percentage,
        file.filename, file_hash
    )
    
    # Calculate offer prices and check for existing mappings
    processed_items = []
    for item in items:
        offer_price = item['market_price'] * (offer_percentage / Decimal('100'))
        unit_cost_basis = offer_price / item['quantity']
        
        # Check for existing TCGPlayer mapping
        tcgplayer_id = get_product_mapping(item['product_name'], product_type)
        
        processed_items.append({
            **item,
            'product_type': product_type,
            'offer_price': offer_price,
            'unit_cost_basis': unit_cost_basis,
            'tcgplayer_id': tcgplayer_id
        })
    
    # Add items to session
    add_intake_items(session_id, processed_items)
    
    # Calculate totals
    total_market_value = sum(i['market_price'] * i['quantity'] for i in items)
    total_offer = sum(i['offer_price'] for i in processed_items)
    
    # Update session totals
    execute_query("""
        UPDATE intake_sessions 
        SET total_market_value = %s, total_offer_amount = %s
        WHERE id = %s
    """, (total_market_value, total_offer, session_id), fetch=False)
    
    return jsonify({
        'success': True,
        'session_id': session_id,
        'item_count': len(items),
        'total_market_value': float(total_market_value),
        'total_offer': float(total_offer),
        'unmapped_count': sum(1 for i in processed_items if not i['tcgplayer_id'])
    })

@app.route('/api/intake/session/<session_id>', methods=['GET'])
def get_intake_session(session_id):
    """Get intake session details and items"""
    session = execute_query(
        "SELECT * FROM intake_session_summary WHERE id = %s",
        (session_id,)
    )
    
    if not session:
        return jsonify({'error': 'Session not found'}), 404
    
    items = execute_query(
        "SELECT * FROM intake_items WHERE session_id = %s ORDER BY is_mapped ASC, product_name",
        (session_id,)
    )
    
    return jsonify({
        'session': dict(session[0]),
        'items': [dict(i) for i in items]
    })

@app.route('/api/intake/map-product', methods=['POST'])
def map_product():
    """
    Map an intake item to a TCGPlayer ID
    Can be manual ID entry or selection from search results
    """
    data = request.json
    item_id = data.get('item_id')
    tcgplayer_id = data.get('tcgplayer_id')
    
    if not item_id or not tcgplayer_id:
        return jsonify({'error': 'item_id and tcgplayer_id required'}), 400
    
    # Get the item
    item = execute_query(
        "SELECT * FROM intake_items WHERE id = %s",
        (item_id,)
    )[0]
    
    # Update item with TCGPlayer ID and re-fetch price
    market_price = get_tcgplayer_price(tcgplayer_id, item.get('condition', 'Near Mint'))
    
    if market_price is None:
        return jsonify({'error': 'Failed to fetch TCGPlayer price'}), 500
    
    # Recalculate offer price
    session = execute_query(
        "SELECT offer_percentage FROM intake_sessions WHERE id = %s",
        (item['session_id'],)
    )[0]
    
    offer_price = market_price * (session['offer_percentage'] / Decimal('100'))
    unit_cost_basis = offer_price / item['quantity']
    
    # Update item
    execute_query("""
        UPDATE intake_items 
        SET tcgplayer_id = %s, market_price = %s, offer_price = %s, 
            unit_cost_basis = %s, is_mapped = TRUE
        WHERE id = %s
    """, (tcgplayer_id, market_price, offer_price, unit_cost_basis, item_id), fetch=False)
    
    # Save mapping for future use
    save_product_mapping(
        item['product_name'], tcgplayer_id, item['product_type'],
        item.get('set_name'), item.get('card_number')
    )
    
    return jsonify({'success': True, 'updated_price': float(market_price)})

@app.route('/api/intake/search-tcgplayer', methods=['POST'])
def search_tcgplayer():
    """Search TCGPlayer for product matches"""
    data = request.json
    product_name = data.get('product_name')
    set_name = data.get('set_name')
    
    if not product_name:
        return jsonify({'error': 'product_name required'}), 400
    
    results = search_tcgplayer_product(product_name, set_name)
    return jsonify({'results': results})

@app.route('/api/intake/finalize/<session_id>', methods=['POST'])
def finalize_session(session_id):
    """Finalize an intake session"""
    result = finalize_intake_session(session_id)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 400

@app.route('/api/intake/sessions', methods=['GET'])
def list_sessions():
    """List all intake sessions"""
    status_filter = request.args.get('status', 'in_progress')
    
    sessions = execute_query("""
        SELECT * FROM intake_session_summary 
        WHERE status = %s
        ORDER BY created_at DESC
        LIMIT 50
    """, (status_filter,))
    
    return jsonify({'sessions': [dict(s) for s in sessions]})

# ==========================================
# HEALTH CHECK
# ==========================================

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        execute_query("SELECT 1")
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
