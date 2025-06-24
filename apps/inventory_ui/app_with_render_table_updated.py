import pandas as pd
from urllib.parse import urlparse
import io
import json
import shopify
import sys, os
import flask
import requests
from flask import Flask, render_template_string, request, render_template, redirect, flash, get_flashed_messages, send_file
from dotenv import load_dotenv
from flask import request, Response
from functools import wraps
from sqlalchemy import create_engine

load_dotenv()
print("Flask version:", flask.__version__)
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = "something-super-secret-and-unique"
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
USERNAME = os.environ.get("INVENTORY_USER")
PASSWORD = os.environ.get("INVENTORY_PASS")
LOCATION_ID = os.environ.get("LOCATION_ID")
API_VERSION = "2023-07"
VISIBLE_COLUMNS = [
    "name",
    "total amount (4/1)",
    "rc_qty",
    "rc_price",
    "shopify_qty",
    "shopify_price",
    "shopify_value",
    "rc_value",  # if you want to show the rc_value as well
    "notes"
]
COLUMN_RENAMES = {
    "name": "Name",
    "total amount (4/1)": "Total Inventory",
    "rc_qty": "RC Qty",
    "rc_price": "RC Price",
    "shopify_qty": "Shopify Qty",
    "shopify_price": "Shopify Price",
    "rare_find_id" : "RC ID",
    "rc value": "RC Value",
    "shopify value": "Shopify Value",
    "notes": "Notes",
    "__key__": "__key__",
}
NAV_BUTTONS = [
    {"label": "⬅ Back to Inventory", "href": "/", "class": "btn-outline-secondary", "key": "back"},
    {"label": "📦 Unpublished", "href": "/unpublished", "class": "btn-outline-warning", "key": "unpublished"},
    {"label": "🛍️ Only on Shopify", "href": "/only_shopify", "class": "btn-outline-info", "key": "only_shopify"},
    {"label": "🧪 Only on RC", "href": "/only_rc", "class": "btn-outline-primary", "key": "only_rc"},
    {"label": "🕵️‍♂️ Untouched", "href": "/untouched", "class": "btn-outline-dark", "key": "untouched"},
    {"label": "📤 Export CSV", "href": "/export_csv", "class": "btn-outline-success", "key": "export"},
    {"label": "🔁 Sync Rare Candy", "href": "/sync_rc", "class": "btn-outline-primary", "key": "sync_rc"},
    {"label": "🔁 Sync Shopify", "href": "/sync_shopify", "class": "btn-outline-success", "key": "sync_shopify"},
]
def working_dir_path(filename):
    if getattr(sys, 'frozen', False):
        return os.path.join(os.path.dirname(sys.executable), filename)
    return os.path.abspath(filename)

inventory_path = os.path.join(os.getcwd(), "InventoryFinal.csv")

BEARER_TOKEN = os.environ.get("RC_BEARER")
def check_auth(u, p):
    return u == USERNAME and p == PASSWORD

def authenticate():
    return Response(
        'Unauthorized', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# Load inventory and export files
def parse_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() == "true"
    return False

def get_bearer_token():
    return BEARER_TOKEN

def load_inventory_fallback():
    engine = create_engine("sqlite:////data/inventory.db")

    if os.path.exists("/data/inventory.db"):
        print("📂 Loading inventory from SQLite...")
        df = pd.read_sql("SELECT * FROM inventory", engine)
    else:
        print("📂 SQLite not found — loading from CSV and seeding database...")
        df = pd.read_csv(inventory_path)
        df.to_sql("inventory", engine, if_exists="replace", index=False)

    # Run your cleanup regardless of source
    # Force ID columns to int where possible (avoids float-style `.0` problems)
    for id_col in ["variant_id", "inventory_item_id", "shopify_inventory_id", "rare_find_id"]:
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors="coerce").astype("Int64")
    df = df.drop(columns=["unnamed: 10"], errors="ignore")
    df["touched_rc"] = False
    df["touched_shopify"] = False
    for col in ["rare_find_id", "pending_rc_update", "inventory_item_id", "rc_qty", "rc_price", "variant_id", "pending_shopify_update", "shopify_inventory_id"]:
        if col not in df.columns:
            df[col] = None
    df.columns = df.columns.str.strip().str.lower()
    df["__key__"] = df["name"].apply(normalize_name)
    return df

def save_inventory_to_db(df):
    engine = create_engine("sqlite:////data/inventory.db")
    df.to_sql("inventory", engine, if_exists="replace", index=False)


def fetch_rc_inventory_via_graphql(bearer_token):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }

    graphql_url = "https://api.rarecandy.com/graphql"
    page = 1
    page_size = 50
    all_items = []

    query = """
    query RareFindCatalogV2($input: StoreRareFindSearchInput!) {
      me {
        store {
          rareFindCatalogV2(input: $input) {
            data {
              id
              shareUrl
              isVisible
              isArchived
              inventoryItems {
                id
                name
                __typename
              }
              product {
                name
                price
                quantity
              }
            }
            resultsInfo {
              totalCount
              page
              pageSize
            }
          }
        }
      }
    }
    """

    while True:
        variables = {
            "input": {
                "query": "",
                "page": page,
                "pageSize": page_size,
                "isArchived": False
            }
        }

        response = requests.post(graphql_url, headers=headers, json={"query": query, "variables": variables})
        print("📡 Status Code:", response.status_code)
        print("🔽 Raw Text:", response.text[:300])
        response.raise_for_status()
        data = response.json()

        products = data["data"]["me"]["store"]["rareFindCatalogV2"]["data"]
        if not products:
            break

        for item in products:
            if item["isArchived"]:
                continue
            product = item["product"]
            name = product.get("name")
            slug = extract_slug_from_share_url(item.get("shareUrl") or "")
            #print(name)
            if name is None or pd.isna(name) or name.strip() == "" or name.strip().lower() == "nan":
                continue  # Skip rows with an invalid or missing product name
            inventory_item_id = None
            inv_items = item.get("inventoryItems") or item.get("inventoryitems")
            if inv_items and isinstance(inv_items, list) and len(inv_items) > 0:
                inventory_item_id = inv_items[0].get("id")
            all_items.append({
                "name": product["name"],
                "quantity listed": product["quantity"],
                "current price": product["price"],
                "rare_find_id": item["id"],  # ← th
                "inventory_item_id": inventory_item_id,  # the underlying InventoryItem id
                "is published": item.get("isVisible", False),
                "share_url": item.get("shareUrl"),
                "slug" : slug
            })
        page += 1
        info = data["data"]["me"]["store"]["rareFindCatalogV2"]["resultsInfo"]
        if page > (info["totalCount"] // page_size) + 1:
            break

    df = pd.DataFrame(all_items)
    return df
def normalize_name(name):
    if not name or pd.isna(name):
        return ""
    return str(name).strip().lower()
def load_inventory(csv_path):
    df = pd.read_csv(csv_path).drop(columns=["unnamed: 10"], errors="ignore")
    df["touched_rc"] = False
    df["touched_shopify"] = False
    for col in ["rare_find_id", "pending_rc_update", "inventory_item_id", "rc_qty", "rc_price", "variant_id", "pending_shopify_update","shopify_inventory_id"]:
        if col not in df.columns:
            df[col] = None
    df.columns = df.columns.str.strip().str.lower()
    df["__key__"] = df["name"].apply(normalize_name)
    return df
def fetch_export_data(bearer_token):
    export_df = fetch_rc_inventory_via_graphql(bearer_token)
    if export_df is None or not isinstance(export_df, pd.DataFrame):
        raise RuntimeError("❌ fetch_rc_inventory_via_graphql() returned None or invalid type")
    export_df.columns = export_df.columns.str.strip().str.lower()
    export_df["name"] = export_df["name"].str.strip()
    # Filter out rows with missing or invalid names:
    export_df = export_df[export_df["name"].notna() & (export_df["name"].str.lower() != "nan") & (export_df["name"].str.strip() != "")]
    export_df["__key__"] = export_df["name"].apply(normalize_name)
    return export_df


def merge_inventory(inventory_df, export_df):
    # Keep only the needed columns from export_df.
    merge_cols = ["__key__", "name", "rare_find_id", "inventory_item_id", "quantity listed", "current price", "slug"]
    export_df = export_df[merge_cols]

    # Filter out export rows with missing or invalid __key__ or name.
    export_df = export_df[
        export_df["__key__"].notna() &
        (export_df["__key__"].str.strip().str.lower() != "nan") &
        export_df["name"].notna() &
        (export_df["name"].str.strip() != "")
    ]
    # Clean up any prior merge artifacts to prevent suffix collisions
    merge_artifact_prefixes = ["name_new", "rare_find_id_new", "inventory_item_id_new", "rare_find_id_x",
                               "rare_find_id_y", "inventory_item_id_x", "inventory_item_id_y"]
    inventory_df = inventory_df.drop(columns=[col for col in inventory_df.columns if
                                              any(col.startswith(prefix) for prefix in merge_artifact_prefixes)],
                                     errors="ignore")

    export_df = export_df.rename(columns={
        "name": "name_new",
        "rare_find_id": "rare_find_id_new",
        "inventory_item_id": "inventory_item_id_new",
        "slug" : "slug_new"
    })
    # After performing the merge (with indicator)
    merged_df = pd.merge(
        inventory_df,
        export_df,
        on="__key__",
        how="outer",
        indicator=True
    )

    merged_df["rare_find_id"] = merged_df["rare_find_id_new"].combine_first(merged_df["rare_find_id"])
    merged_df["inventory_item_id"] = merged_df["inventory_item_id_new"].combine_first(merged_df["inventory_item_id"])

    merged_df["rc_qty"] = merged_df["quantity listed"]
    merged_df["rc_price"] = merged_df["current price"]

    if "name_new" in merged_df.columns:
        merged_df["name"] = merged_df["name"].combine_first(merged_df["name_new"])

    merged_df.drop(
        columns=["name_new", "rare_find_id_new", "inventory_item_id_new", "quantity listed", "current price", "_merge"],
        inplace=True, errors="ignore")

    # Optionally, remove any rows with an invalid name.
    merged_df = merged_df[
        merged_df["name"].notna() &
        (merged_df["name"].str.strip().str.lower() != "nan") &
        (merged_df["name"].str.strip() != "")
        ]

    # Optionally, de-duplicate on __key__.
    merged_df = merged_df.drop_duplicates(subset="__key__", keep="last")

    # Count new and updated rows (if needed)
    updated_count = len(merged_df[merged_df["rare_find_id"].notna()])  # or use the merge indicator before dropping it
    # (You can adjust these counts as needed based on your logic.)
    print(f"DEBUG: In merge_inventory, updated_count: {updated_count}, added_count: (calculation here)")

    return merged_df

def save_inventory(inventory_df, csv_path):
    inventory_df.to_csv(csv_path, index=False)

def do_usercontext_query(session, bearer_token):
    graphql_url = "https://api.rarecandy.com/graphql"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    query = """
    query UserContextQuery {
      me {
        id
        username
        store {
          id
          name
          __typename
        }
        __typename
      }
    }
    """
    response = session.post(graphql_url, headers=headers, json={"query": query})
    try:
        response.raise_for_status()
    except Exception as err:
        print("Response text:", response.text)
    data = response.json()
    print("DEBUG: UserContextQuery response:", data)
    # If it returns a 'store' object with ID=117, that sets context in the session/cookies.
def get_inventory_item(session,bearer_token, inventory_item_id):
    graphql_url = "https://api.rarecandy.com/graphql"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    query = """
    query GetInventoryItem($inventoryItemId: Int) {
      inventoryItem(id: $inventoryItemId) {
        id
        name
        description
        suggestedDescription
        sku
        analysisKey
        company
        grade
        gradeCode
        gradeLabel
        certNumber
        images {
          blurHash
          desktop
          id
          landscapeUrl
          landscapeUrlHD
          mobile
          original
          portraitUrl
          portraitUrlHD
          squareUrl
          squareUrlHD
          weight
          __typename
        }
        quantity
        quantityAvailable
        provenanceSource
        consignorName
        costBasis
        commissionRate
        price
        estimatedSalePrice
        tags
        customTags
        notes
        weightOunces
        widthInches
        lengthInches
        heightInches
        marketPriceInsights {
          source {
            logo {
              id
              mobile
              __typename
            }
            name
            url
            __typename
          }
          spread {
            highPrice
            lowPrice
            marketPrice
            __typename
          }
          __typename
        }
        __typename
      }
    }
    """
    variables = {"inventoryItemId": inventory_item_id}
    response = session.post(graphql_url, headers=headers, json={"query": query, "variables": variables})
    try:
        response.raise_for_status()
    except Exception as err:
        print("Response text:", response.text)
    data = response.json()
    print("DEBUG: Full response from GetInventoryItem:", json.dumps(data, indent=2))
    inventory_item = data.get("data", {}).get("inventoryItem")
    if inventory_item is None:
        raise Exception(f"Inventory item with id {inventory_item_id} not found. Response: {data}")
    return inventory_item

def extract_slug_from_share_url(url: str) -> str:
    path = urlparse(url).path
    return path.strip('/').split('/')[-1]

def update_rc_listing(bearer_token, inventory_item_id, rare_find_id, name, rc_qty=None, rc_price=None,
                      rc_description=None, rc_slug=None, rc_tags=None, rc_shipping=None):
    with requests.Session() as s:
        do_usercontext_query(s, bearer_token)
        current_item = get_inventory_item(s, bearer_token, inventory_item_id)

    # Build the payload using the current item as a base
    payload = {
        "name": name,
        "description": current_item["suggestedDescription"],
        "price": current_item["price"] if current_item["price"] is not None else 0,
        "quantity": current_item["quantity"] if current_item["quantity"] is not None else 0,
        "slug": current_item.get("slug") or "",
        # Convert current_item["tags"] to a comma-separated string for keywords:
        "keywords": ",".join(current_item["tags"]) if current_item.get("tags") else "",
        # Use current_item["tags"] (or fallback to customTags if available) for tags:
        "tags": current_item["tags"] if current_item.get("tags") else (
            current_item["customTags"] if current_item.get("customTags") is not None else []),
        "limitPerPerson": None,
        "preorderReleaseDate": None,
        "quantityWarningThreshold": 5,
        "showScarcity": True,
        "isVisible": True,
        "shippingHandlingNorthAmerica": rc_shipping.get("na") if rc_shipping and "na" in rc_shipping else 10.99,
        "shippingHandlingSouthAmerica": rc_shipping.get("sa") if rc_shipping and "sa" in rc_shipping else 29.99,
        "shippingHandlingEurope": rc_shipping.get("eu") if rc_shipping and "eu" in rc_shipping else 29.99,
        "shippingHandlingAsia": rc_shipping.get("asia") if rc_shipping and "asia" in rc_shipping else 29.99,
        "shippingHandlingAustralia": rc_shipping.get("aus") if rc_shipping and "aus" in rc_shipping else 29.99,
        "shippingHandlingAfrica": rc_shipping.get("africa") if rc_shipping and "africa" in rc_shipping else 29.99,
    }
    # Override with new values if provided
    if rc_qty is not None:
        payload["quantity"] = int(rc_qty)
    if rc_price is not None:
        payload["price"] = float(rc_price)
    if rc_description is not None:
        payload["description"] = rc_description
    if rc_slug is not None:
        print(rc_slug)
        payload["slug"] = rc_slug
    if rc_tags is not None:
        payload["tags"] = rc_tags

    graphql_url = "https://api.rarecandy.com/graphql"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    mutation = """
    mutation UpdateRarefind($rareFindId: Int!, $input: UpdateRareFindInput!) {
      updateRareFind(rareFindId: $rareFindId, input: $input) {
        id
        product { name price quantity }
      }
    }
    """
    variables = {"rareFindId": rare_find_id, "input": payload}
    response = requests.post(graphql_url, headers=headers, json={"query": mutation, "variables": variables})
    try:
        response.raise_for_status()
    except Exception as err:
        print("Update error response:", response.text)
        raise
    print(f"✅ Updated Rare Candy listing '{payload['name']}' with new values: quantity={payload['quantity']}, price={payload['price']}")


def render_inventory_table(filtered_df, title="Inventory Viewer", search_term="", show_columns=None,
                           hidden_buttons=None):
    # Make a copy of filtered_df and, if show_columns is provided, narrow down the columns
    df = filtered_df.copy()
    if show_columns:
        df = df[[col for col in show_columns if col in df.columns]]  # <- change filtered_df to df

    # Only calculate derived values if columns exist in our filtered df:
    if "shopify_qty" in df.columns and "shopify_price" in df.columns:
        df["shopify_value"] = (df["shopify_qty"].fillna(0) * df["shopify_price"].fillna(0)).round(2)

    if "rc_qty" in df.columns and "rc_price" in df.columns:
        df["rc_value"] = (df["rc_qty"].fillna(0) * df["rc_price"].fillna(0)).round(2)

    # Now drop __key__ if it exists:
    table_df = df.drop(columns=["__key__"], errors="ignore")
    if "rare_find_id" not in table_df.columns and "rare_find_id" in df.columns:
        table_df["rare_find_id"] = df["rare_find_id"]

    html_rows = []
    for i, row in table_df.iterrows():
        checkbox = f"<td><input type='checkbox' name='merge_ids' value='{i}'></td>"
        cells = ""
        for col in table_df.columns:
            if col.lower() == "name":
                cells += f"<td style='white-space: nowrap; max-width: 400px'>{row[col]}</td>"
            elif col.lower() in ["rc_value", "shopify_value"]:
                val = row.get(col)
                display_val = "" if pd.isna(val) else f"{val:,.2f}"
                cells += f"<td>{display_val}</td>"
            else:
                val = row.get(col)
                display_val = "" if pd.isna(val) else (
                    str(int(val)) if isinstance(val, float) and val.is_integer() else str(val))
                cell_name = f"cell_{i}_{col}"
                cells += f"<td><input type='text' name='{cell_name}' value='{display_val}' class='form-control form-control-sm'></td>"

        html_rows.append(f"<tr>{checkbox}{cells}</tr>")

    column_headers = ''.join(f"<th>{COLUMN_RENAMES.get(col, col)}</th>" for col in table_df.columns)
    if hidden_buttons is None:
        visible_buttons = NAV_BUTTONS
    else:
        visible_buttons = [btn for btn in NAV_BUTTONS if btn["key"] not in hidden_buttons]
    buttons_html = "".join(
        f'<a href="{btn["href"]}" class="btn {btn["class"]}">{btn["label"]}</a>'
        for btn in visible_buttons)

    total_row = ""
    if "shopify_value" in table_df.columns or "rc_value" in table_df.columns:
        total_row_cells = ['<td><strong>Total</strong></td>']
        for col in table_df.columns:
            if col.lower() == "shopify_value":
                total = table_df["shopify_value"].sum()
                total_row_cells.append(f"<td><strong>{total:,.2f}</strong></td>")
            elif col.lower() == "rc_value":
                total = table_df["rc_value"].sum()
                total_row_cells.append(f"<td><strong>{total:,.2f}</strong></td>")
            else:
                total_row_cells.append("<td></td>")
        total_row = f"<tr>{''.join(total_row_cells)}</tr>"

    add_item_form = f"""
        <div class="input-group mb-3 w-50 mt-4">
            <input type="text" name="new_name" placeholder="New Item Name" class="form-control" required autocomplete="off">
            <input type="text" name="new_qty" placeholder="Quantity" class="form-control" required autocomplete="off">
            <button name="add" value="1" class="btn btn-secondary">➕ Add New Item</button>
        </div
"""
    table_html = f"""
        <table class="table table-bordered table-sm">
            <thead style="position: sticky; top: 0; background-color: #dfa260;">
                <tr>
                    <th>Select</th>
                    {column_headers}
                </tr>
            </thead>
            <tbody>
                {''.join(html_rows)}
                {total_row}
            </tbody>
        </table>
    """

    return render_template_string("""
        <html>
        <head>
            <title>{{ title }}</title>

            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                body {
                    font-family: 'Segoe UI', sans-serif;
                    background-color: #fcf7e1;
                    color: #2a361c;
                    padding: 40px;
                }
                h1 {
                    color: #616d39;
                }
                table {
                    border-collapse: collapse;
                    width: 100%;
                    font-size: 14px;
                    margin-top: 20px;
                }
                th, td {
                    border: 1px solid #ba6b29;
                    padding: 8px;
                    text-align: left;
                }
                th {
                    background-color: #dfa260;
                    color: #000;
                    position: sticky;
                    top: 0;
                    z-index: 1;
                }
                tr:nth-child(even) {
                    background-color: #fff9ef;
                }
                tr:hover {
                    background-color: #ffeacc;
                }
                input[type=text] {
                    width: 80px;
                    padding: 4px;
                }
                button {
                    background-color: #616d39;
                    color: white;
                    padding: 8px 12px;
                    border: none;
                    cursor: pointer;
                    font-weight: bold;
                }
                button:hover {
                    background-color: #2a361c;
                }
            </style>
            <script>
                document.addEventListener("DOMContentLoaded", function () {
                    const scrollY = sessionStorage.getItem("scrollY");
                    if (scrollY !== null) {
                        window.scrollTo(0, parseInt(scrollY));
                        sessionStorage.removeItem("scrollY");
                    }
                    document.querySelectorAll("form").forEach(form => {
                        form.addEventListener("submit", function () {
                            sessionStorage.setItem("scrollY", window.scrollY);
                        });
                    });
                    // ⬇️ Target only the inventory table's inputs
                    const tableInputs = document.querySelectorAll("table input[type='text']");
                    tableInputs.forEach(input => {
                        input.addEventListener("keydown", function(e) {
                            if (e.key === "Enter") {
                                e.preventDefault();
                                sessionStorage.setItem("scrollY", window.scrollY);
                                // Find the nearest Save button and click it
                                const saveBtn = document.querySelector("button[name='save']");
                                if (saveBtn) saveBtn.click();
                            }
                        });
                    });
                });
            </script>
        </head>
        <body class="p-4">
            <h1>{{ title }}</h1>
            {% with messages = get_flashed_messages(with_categories=true) %}
              {% if messages %}
                <div class="alert alert-success alert-dismissible fade show" role="alert">
                  {{ messages[0][1] }}
                  <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                </div>
              {% endif %}
            {% endwith %}
            <div class="mb-3 d-flex flex-wrap gap-2">
              {{ buttons_html|safe }}
            </div>
            <!-- Search Form -->
            <form method="get" class="mb-3">
                <input name="q" placeholder="Search by name" class="form-control w-50 d-inline" value="{{ request.args.get('q', '') }}">
                <button class="btn btn-primary">Search</button>
            </form>
            <!-- Add Item Form -->
            <form method="post" action="" class="mb-3">
                <div class="input-group w-50">
                    <input type="text" name="new_name" placeholder="New Item Name" class="form-control" required>
                    <input type="text" name="new_qty" placeholder="Quantity" class="form-control">
                    <button name="add" value="1" class="btn btn-secondary">➕ Add New Item</button>
                </div>
            </form>
            <!-- Table and Actions Form -->
            <form method="post">
                <div class="d-flex justify-content-start gap-3 mb-3">
                    <button formaction="/merge_preview?q={{ request.args.get('q', '') }}" class="btn btn-warning">🧬 Merge Selected</button>
                    <button name="delete" value="1" class="btn btn-danger">🗑️ Delete Selected</button>
                </div>
                <div class="mt-4">{{ table|safe }}</div>
                <button name="save" value="1" class="btn btn-success mt-3">💾 Save Changes</button>
            </form>
        </body>
        </html>
    """, table=table_html, title=title, buttons_html=buttons_html, add_item_form=add_item_form)


def update_variant_price(variant_id, new_price):
    """
    Update the price of a Shopify variant using the REST API via ShopifyAPI.
    variant_id should be a string that uniquely identifies the variant.
    """
    shopify.ShopifyResource.clear_session()
    session = shopify.Session(SHOPIFY_STORE, API_VERSION, SHOPIFY_TOKEN)
    shopify.ShopifyResource.activate_session(session)
    try:
        cleaned_variant_id = int(variant_id)
        variant = shopify.Variant.find(cleaned_variant_id)
        #print(f"DEBUG: Current price for variant {variant_id} is {variant.price}")
        variant.price = "{:.2f}".format(new_price)
        success = variant.save()
        #print(f"DEBUG: Updated variant {variant_id} price to {variant.price}: {success}")
        return variant.attributes  # or variant.to_dict() if you prefer a dict
    except Exception as e:
        print(f"❌ Failed to update variant price for variant {variant_id}: {e}")
        raise
def get_current_inventory_level(global_inventory_item_id, location_id):
    # Build a query to fetch inventoryLevels as a sub-field of inventoryItem.
    query = """
       query GetInventoryItemLevels($id: ID!) {{
         inventoryItem(id: $id) {{
           inventoryLevels(first: 1, query: "location_id:'{0}'") {{
             edges {{
               node {{
                 onHand
               }}
             }}
           }}
         }}
       }}
       """.format(location_id)
    print("DEBUG: GetInventoryItemLevels query (repr):", repr(query))

    variables = {"id": global_inventory_item_id}

    print("DEBUG: GetInventoryItemLevels query (repr):", repr(query))
    print("DEBUG: Variables:", variables)

    url = f"https://{SHOPIFY_STORE}/admin/api/2023-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    }

    try:
        response = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=10)
    except Exception as e:
        print("DEBUG: Exception during POST:", e)
        raise

    print("DEBUG: Raw response text:")
    print(response.text)

    response.raise_for_status()

    data = response.json()
    print("DEBUG: JSON data from GetInventoryItemLevels query:")
    print(data)

    try:
        available = data["data"]["inventoryItem"]["inventoryLevels"]["edges"][0]["node"]["available"]
        print(f"DEBUG: Current available for {global_inventory_item_id} at {location_id} is {available}")
        return int(available)
    except Exception as e:
        print(f"DEBUG: Could not get available level: {e}")
        return 0


def update_inventory_level(inventory_item_id, location_id, new_available):
    """
    Update the inventory level using Shopify's REST API endpoint for inventory_levels/set.json.

    Parameters:
        inventory_item_id (int): The numeric ID of the inventory item.
        location_id (int): The numeric ID of the location.
        new_available (int): The desired new available quantity.
    Returns:
        The JSON response from Shopify.
    """
    url = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/inventory_levels/set.json"
    data = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
        "available": int(new_available)
    }
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    }
    #print("DEBUG: Updating inventory level via REST with payload:")
    #print(data)
    response = requests.post(url, json=data, headers=headers)
    #print("DEBUG: Response status code:", response.status_code)
    #print("DEBUG: Raw response text:")
    #print(response.text)
    response.raise_for_status()
    return response.json()

def update_shopify_item(variant_id, inventory_item_id, new_price=None, new_quantity=None):
    """
    Update a Shopify variant.
    - Updates the price using your existing update_variant_price function.
    - Updates the inventory quantity using update_inventory_level.

    Only calls the respective mutation if the corresponding value is provided.
    """
    results = {}
    # Update price if new_price is provided.
    if new_price is not None:
        #print(f"DEBUG: Updating price for variant {variant_id} to {new_price}")
        # Call your existing update_variant_price function.
        price_result = update_variant_price(variant_id, new_price)
        results['price'] = price_result

    # Update quantity if new_quantity is provided.
    if new_quantity is not None:
        #print(
        #    f"DEBUG: Updating quantity for inventory item {inventory_item_id} to {new_quantity} at location {LOCATION_ID}")
        qty_result = update_inventory_level(int(inventory_item_id), int(LOCATION_ID), int(new_quantity))
        results['quantity'] = qty_result

    return results
@app.route("/commit_merge", methods=["POST"])
@requires_auth
def commit_merge():
    global inventory_df

    idx1 = int(request.form["row1_index"])
    idx2 = int(request.form["row2_index"])
    keep = request.form.get("keep", "left")
    search = request.form.get("q", "")

    if keep == "left":
        survivor = inventory_df.loc[idx1].copy()
    else:
        survivor = inventory_df.loc[idx2].copy()

    # Drop both original rows
    inventory_df.drop(index=[idx1, idx2], inplace=True)

    # Reappend the one we kept
    survivor_df = pd.DataFrame([survivor], columns=inventory_df.columns)
    inventory_df = pd.concat([inventory_df, survivor_df], ignore_index=True)

    # Save and redirect
    save_inventory_to_db(inventory_df)
    flash("✅ Merged items by choosing one version.", "success")
    return redirect(f"/?q={search}")

@app.route("/export_csv")
@requires_auth
def export_csv():
    global inventory_df
    buffer = io.StringIO()
    inventory_df.to_csv(buffer, index=False)
    buffer.seek(0)
    return send_file(
        io.BytesIO(buffer.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="InventoryExport.csv"
    )

def rc_sync_logic():
    global inventory_df
    print("📂 Attempting to load inventory from:", inventory_path)
    print("📦 Initial inventory_df shape:", inventory_df.shape if inventory_df is not None else "None")
    print("📦 Columns:", inventory_df.columns.tolist() if inventory_df is not None else "None")

    # Ensure essential columns exist
    for col in ["rare_find_id", "inventory_item_id"]:
        if col not in inventory_df.columns:
            inventory_df[col] = None

    # Fetch export data from Rare Candy
    export_df = fetch_export_data(BEARER_TOKEN)
    print("DEBUG: Raw export data:\n", export_df[["name", "quantity listed"]].head(20))

    # Prepare published + unpublished items
    published_df = export_df[export_df["is published"] == True]
    unpublished_df = export_df[~export_df["__key__"].isin(published_df["__key__"])]
    combined_df = pd.concat([published_df, unpublished_df], ignore_index=True)

    # Deduplicate to retain preferred row per __key__
    filtered_export = combined_df.sort_values(
        by=["__key__", "quantity listed"], ascending=[True, False]
    ).drop_duplicates(subset="__key__", keep="first")

    # Select relevant fields, rename name → name_new to avoid suffix clashes
    # Select relevant fields
    filtered_export = filtered_export[
        ["__key__", "name", "rare_find_id", "inventory_item_id", "quantity listed", "current price", "slug"]]

    # Rename all conflicting columns
    filtered_export = filtered_export.rename(columns={
        "name": "name_new",
        "rare_find_id": "rare_find_id_new",
        "inventory_item_id": "inventory_item_id_new",
        "slug" : "slug_new"
    })

    # Drop stale merge columns from inventory_df that would conflict
    inventory_df = inventory_df.drop(columns=[
        "name_new", "rare_find_id_new", "inventory_item_id_new"
    ], errors="ignore")

    # Ensure __key__ is present
    inventory_df["__key__"] = inventory_df["name"].apply(normalize_name)

    # Merge export into inventory
    merged_df = pd.merge(
        inventory_df,
        filtered_export,
        on="__key__",
        how="outer",
        indicator=True
    )

    # Use new data if available
    if "slug_new" in merged_df.columns:
        merged_df["slug"] = merged_df["slug_new"].combine_first(merged_df.get("slug", pd.Series(index=merged_df.index, dtype="object")))
    else:
        print("⚠️ slug_new column not found in merged_df — likely due to merge mismatch")
        merged_df["slug"] = merged_df.get("slug", "")
    if "share_url" in merged_df.columns:
        merged_df["slug"] = merged_df["slug"].fillna(
            merged_df["share_url"].apply(lambda url: extract_slug_from_share_url(url) if pd.notna(url) else ""))
    if "rare_find_id_new" in merged_df.columns:
        merged_df["rare_find_id"] = merged_df["rare_find_id"].combine_first(merged_df["rare_find_id_new"])
        merged_df["inventory_item_id"] = merged_df["inventory_item_id"].combine_first(
            merged_df["inventory_item_id_new"])
    merged_df["name"] = merged_df.get("name", pd.Series(dtype=str)).combine_first(merged_df.get("name_new"))
    merged_df["rc_qty"] = merged_df["quantity listed"]
    merged_df["rc_price"] = merged_df["current price"]

    # Count update types before cleanup
    updated_count = len(merged_df[merged_df["_merge"] == "both"])
    added_count = len(merged_df[merged_df["_merge"] == "right_only"])
    merged_df["touched_rc"] = False  # initialize it

    # Set touched_rc = True for any row that came from export
    merged_df.loc[merged_df["_merge"].isin(["both", "right_only"]), "touched_rc"] = True
    # Drop temporary columns
    merged_df.drop(columns=[
        "name_new", "rare_find_id_new", "inventory_item_id_new", "slug_new",
        "quantity listed", "current price", "_merge",
    ], inplace=True, errors="ignore")

    # Replace global inventory
    inventory_df = merged_df
    save_inventory_to_db(inventory_df)

    print(f"✅ Rare Candy sync complete: {updated_count} updated, {added_count} added")
    flash(f"Rare Candy sync complete: {updated_count} updated, {added_count} added.", "success")
def process_inventory_save(filtered_df):
    global inventory_df
    updates = request.form.to_dict()

    for i, row in filtered_df.iterrows():
        orig_idx = row.get("__orig_idx__")
        for col in filtered_df.columns:
            if col in ["__orig_idx__", "pending_rc_update", "pending_shopify_update"]:
                continue

            key = f"cell_{i}_{col}"
            if key not in updates:
                continue

            val = updates[key].strip()
            current_val = inventory_df.at[orig_idx, col]

            if val == "":
                inventory_df.at[orig_idx, col] = None
            elif col.lower() in ["rc_qty", "shopify_qty", "total amount (4/1)"]:
                try:
                    new_val = int(float(val))
                    if pd.isna(current_val) or int(current_val) != new_val:
                        inventory_df.at[orig_idx, col] = new_val
                        if col.lower() == "rc_qty":
                            inventory_df.at[orig_idx, "pending_rc_update"] = True
                        elif col.lower() == "shopify_qty":
                            inventory_df.at[orig_idx, "pending_shopify_update"] = True
                except ValueError:
                    inventory_df.at[orig_idx, col] = None
            elif col.lower() in ["rc_price", "shopify_price"]:
                try:
                    new_val = round(float(val), 2)
                    if pd.isna(current_val) or float(current_val) != new_val:
                        inventory_df.at[orig_idx, col] = new_val
                        if col.lower() == "rc_price":
                            inventory_df.at[orig_idx, "pending_rc_update"] = True
                        elif col.lower() == "shopify_price":
                            inventory_df.at[orig_idx, "pending_shopify_update"] = True
                except ValueError:
                    inventory_df.at[orig_idx, col] = None
            else:
                inventory_df.at[orig_idx, col] = val

    inventory_df["pending_rc_update"] = inventory_df["pending_rc_update"].apply(parse_bool)
    inventory_df["pending_shopify_update"] = inventory_df["pending_shopify_update"].apply(parse_bool)

    save_inventory_to_db(inventory_df)
    flash("💾 Changes saved successfully!", "success")
@app.route("/sync_rc")
@requires_auth
def sync_rc():
    rc_sync_logic()
    return redirect("/")

def shopify_sync_logic():
    global inventory_df
    shopify.ShopifyResource.clear_session()
    session = shopify.Session(SHOPIFY_STORE, API_VERSION, SHOPIFY_TOKEN)
    shopify.ShopifyResource.activate_session(session)

    updated = 0
    added = 0
    shopify_items = []

    # Fetch products with pagination.
    products = shopify.Product.find(limit=250)
    while products:
        # For each product, iterate over its variants.
        for product in products:
            for variant in product.variants:
                # Create a display name by combining product title with variant title if not default.
                if variant.title != "Default Title":
                    name = f"{product.title} - {variant.title}".strip()
                else:
                    name = product.title.strip()

                # Collect the fields from the variant.
                # Check that the variant object has inventory_item_id in its attributes.
                variant_data = variant.to_dict()
                print(
                    f"[DEBUG] Variant ID {variant.id} → inventory_item_id: {variant_data.get('inventory_item_id')} (type: {type(variant_data.get('inventory_item_id'))})")

                shopify_inventory_id = variant_data.get("inventory_item_id")
                shopify_qty = variant_data.get("inventory_quantity")
                shopify_price = variant_data.get("price")
                # Get the numeric variant id from the global id.
                if isinstance(variant.id, str):
                    variant_id = int(variant.id.split("/")[-1])
                else:
                    variant_id = variant.id

                shopify_items.append({
                    "name": name,
                    "shopify_price": float(shopify_price) if shopify_price else None,
                    "shopify_qty": int(shopify_qty) if shopify_qty is not None else None,
                    "variant_id": variant_id,
                    "shopify_inventory_id": int(shopify_inventory_id) if shopify_inventory_id else None
                })

        # Check if there is a next page.
        if products.has_next_page():
            products = products.next_page()
        else:
            break

    # Report duplicate names (case‑insensitive)
    seen = set()
    duplicates = set()
    for item in shopify_items:
        key = item["name"].strip().lower()
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    if duplicates:
        print("⚠️ Duplicate Shopify item names detected:")
        for name in duplicates:
            print(f" - {name}")

    # Merge or update local inventory_df based on matching product name.
    for item in shopify_items:
        name = item["name"]
        price = item.get("shopify_price")
        qty = item.get("shopify_qty")
        variant_id = item.get("variant_id")
        shopify_inventory_id = item.get("shopify_inventory_id")

        # Perform a case-insensitive match.
        match = inventory_df[inventory_df["name"].str.lower() == name.strip().lower()]
        if not match.empty:
            idx = match.index[0]
            if price is not None:
                inventory_df.at[idx, "shopify_price"] = float(price)
                inventory_df.at[idx, "touched_shopify"] = True
            if qty is not None:
                inventory_df.at[idx, "shopify_qty"] = int(qty)
                inventory_df.at[idx, "touched_shopify"] = True
            if variant_id is not None:
                inventory_df.at[idx, "variant_id"] = int(variant_id)
                inventory_df.at[idx, "touched_shopify"] = True
            if shopify_inventory_id is not None:
                inventory_df.at[idx, "shopify_inventory_id"] = int(shopify_inventory_id)
                inventory_df.at[idx, "touched_shopify"] = True
            updated += 1
        else:
            # Add new row if not found
            new_row = {
                "name": name,
                "__key__": normalize_name(name),
                "shopify_price": float(price) if price is not None else None,
                "shopify_qty": int(qty) if qty is not None else None,
                "variant_id": int(variant_id) if variant_id is not None else None,
                "shopify_inventory_id": int(shopify_inventory_id) if shopify_inventory_id is not None else None,
                "rc_qty": None,
                "rc_price": None,
                "total amount (4/1)": None
            }
            inventory_df = pd.concat([inventory_df, pd.DataFrame([new_row])], ignore_index=True)
            added += 1
    save_inventory_to_db(inventory_df)
    print(f"✅ Shopify Sync Complete — {updated} updated, {added} added")

@app.route("/sync_shopify")
@requires_auth
def sync_shopify():
    shopify_sync_logic()
    return redirect("/")
@app.route("/merge_preview", methods=["GET", "POST"])
@requires_auth
def preview_merge():

    search = request.args.get("q", "")
    merge_ids = request.form.getlist("merge_ids")
    if len(merge_ids) != 2:
        return "❌ Please select exactly 2 rows to merge.", 400

    idx1, idx2 = map(int, merge_ids)
    row1 = inventory_df.loc[idx1]
    row2 = inventory_df.loc[idx2]

    return render_template(
        "merge_preview.html",
        row1=row1,
        row2=row2,
        row1_index=idx1,
        row2_index=idx2,
        search=search
    )
@app.route("/untouched", methods=["GET", "POST"])
@requires_auth
def view_untouched():
    if "save" in request.form:
        return index()
    df = inventory_df.copy()
    df = df[
        (df["touched_rc"] == False) &
        (df["touched_shopify"] == False) &
        (df["total amount (4/1)"].fillna(0) > 0)    ]

    if "save" in request.form:
        process_inventory_save(df)
        return redirect("/untouched")
    df = df[["name", "total amount (4/1)", "rc_qty", "rc_price", "shopify_qty", "shopify_price", "notes"]]
    return render_inventory_table(df, "🕳️ Untouched After Sync")
def sanitize_boolean(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        val = val.strip().lower()
        if val in ["true", "1"]:
            return True
        elif val in ["false", "0"]:
            return False
    return pd.NA
# Flask route
@app.route("/", methods=["GET", "POST"])
@requires_auth
def index():
    global inventory_df
    search = request.values.get("q", "").lower()
    save_requested = "save" in request.form
    inventory_df["__orig_idx__"] = inventory_df.index
    essential_cols = ["__orig_idx__","pending_rc_update", "rare_find_id", "inventory_item_id", "pending_shopify_update","shopify_inventory_id"]
    columns_to_keep = [col for col in inventory_df.columns if not col.startswith("touched_")]

    # Add essential columns to the actual DataFrame if missing
    for col in essential_cols:
        if col not in inventory_df.columns:
            inventory_df[col] = None
        if col not in columns_to_keep:
            columns_to_keep.append(col)

    # Apply filters
    filtered_df = inventory_df.copy()
    # Add new item if requested
    if "add" in request.form:
        new_name = request.form.get("new_name", "").strip()
        new_qty = request.form.get("new_qty", "").strip()

        if new_name:
            try:
                new_qty_val = int(float(new_qty)) if new_qty else None
            except ValueError:
                new_qty_val = None

            new_row = {
                "name": new_name,
                "__key__": normalize_name(new_name),
                "total amount (4/1)": new_qty_val,
                "rc_qty": None,
                "rc_price": None,
                "shopify_qty": None,
                "shopify_price": None,
                "variant_id": None,
                "rare_find_id": None,
                "inventory_item_id": None,
                "pending_rc_update": False,
                "pending_shopify_update": False,
                "shopify_inventory_id": None,
                "notes": "",
            }

            inventory_df = pd.concat([inventory_df, pd.DataFrame([new_row])], ignore_index=True)
            save_inventory_to_db(inventory_df)
            flash(f"➕ Added item '{new_name}'", "success")
            search = request.args.get("q", "")
            return redirect(f"/?q={search}")
    if search:
        filtered_df = filtered_df[
            filtered_df["name"].astype(str).str.lower().str.contains(search)
        ]

    filtered_df = filtered_df[columns_to_keep]
    inventory_df["pending_rc_update"] = inventory_df.get("pending_rc_update", pd.Series([False] * len(inventory_df)))
    inventory_df["pending_rc_update"] = inventory_df["pending_rc_update"].astype(object)
    inventory_df["pending_shopify_update"] = inventory_df.get("pending_shopify_update", pd.Series([False] * len(inventory_df)))
    inventory_df["pending_shopify_update"] = inventory_df["pending_shopify_update"].astype(object)


    # SYNC ACTIONS
    if "sync_rc" in request.form:
        return redirect("/sync_rc")

    if "sync_shopify" in request.form:
        return redirect("/sync_shopify")

    # MERGE & DELETE
    if "delete" in request.form:
        ids_to_delete = request.form.getlist("merge_ids")
        inventory_df.drop(index=[int(i) for i in ids_to_delete], inplace=True)
        inventory_df.reset_index(drop=True, inplace=True)
        save_inventory_to_db(inventory_df)
        flash(f"🗑️ Deleted {len(ids_to_delete)} rows.", "success")
        search = request.args.get("q", "")
        return redirect(f"/?q={search}")

    # SAVE
    if save_requested:
        #print("🔥 Save triggered!")
        updates = request.form.to_dict()
        #print("[DEBUG] About to iterate, filtered_df.shape:", filtered_df.shape)
        for i, row in filtered_df.iterrows():
            orig_idx = row.get("__orig_idx__")
            #print(f"[DEBUG] Row {i}, orig_idx={orig_idx}")
            for col in filtered_df.columns:
                # Skip these columns that should not be updated from form inputs
                if col in ["__orig_idx__", "pending_rc_update", "pending_shopify_update"]:
                    continue

                key = f"cell_{i}_{col}"
                if key not in updates:
                    continue

                val = updates[key].strip()
                current_val = inventory_df.at[orig_idx, col]

                if val == "":
                    inventory_df.at[orig_idx, col] = None
                elif col.lower() in ["rc_qty", "shopify_qty", "total amount (4/1)"]:
                    try:
                        new_val = int(float(val))
                        if pd.isna(current_val) or int(current_val) != new_val:
                            inventory_df.at[orig_idx, col] = new_val
                            if col.lower() == "rc_qty":
                                inventory_df.at[orig_idx, "pending_rc_update"] = True
                            elif col.lower() == "shopify_qty":
                                inventory_df.at[orig_idx, "pending_shopify_update"] = True
                                print(f" -> Setting pending_shopify_update to True for row {orig_idx}")

                    except ValueError:
                        inventory_df.at[orig_idx, col] = None
                elif col.lower() in ["rc_price", "shopify_price"]:
                    try:
                        new_val = round(float(val), 2)
                        if pd.isna(current_val) or float(current_val) != new_val:
                            inventory_df.at[orig_idx, col] = new_val
                            if col.lower() == "rc_price":
                                inventory_df.at[orig_idx, "pending_rc_update"] = True
                            elif col.lower() == "shopify_price":
                                inventory_df.at[orig_idx, "pending_shopify_update"] = True
                                print(f" -> Setting pending_shopify_update to True for row {orig_idx}")

                    except ValueError:
                        inventory_df.at[orig_idx, col] = None
                else:
                    inventory_df.at[orig_idx, col] = val


        inventory_df["pending_rc_update"] = inventory_df["pending_rc_update"].apply(parse_bool)
        inventory_df["pending_shopify_update"] = inventory_df["pending_shopify_update"].apply(parse_bool)

        pending_rows = inventory_df.loc[inventory_df["pending_rc_update"] == True]
        if "slug" not in inventory_df.columns:
            if "share_url" in inventory_df.columns:
                inventory_df["slug"] = inventory_df["share_url"].apply(
                    lambda url: extract_slug_from_share_url(url) if pd.notna(url) else ""
                )
            else:
                inventory_df["slug"] = ""
        #print("Pending rows:")
        #print(inventory_df.loc[inventory_df["pending_rc_update"] == True][
        #          ["name", "pending_rc_update", "rare_find_id", "inventory_item_id"]])
        for idx, row in pending_rows.iterrows():
            if pd.notna(row.get("rare_find_id")):
                print(f"🛠️ Updating RC: {row['name']} (ID {row['inventory_item_id']})")
                rc_slug = row.get('slug')
                print(f"Here's the slug {rc_slug}")
                try:
                    if isinstance(rc_slug, float) and pd.isna(rc_slug):
                        raise ValueError(f"❌ Slug is NaN — this should never happen. Debug this row immediately.")
                    update_rc_listing(
                        bearer_token=BEARER_TOKEN,
                        inventory_item_id=int(row["inventory_item_id"]),
                        rare_find_id=int(row["rare_find_id"]),
                        name=str(row["name"]),
                        rc_price=row.get("rc_price"),
                        rc_qty=int(row.get("rc_qty")),
                        rc_slug=rc_slug
                    )
                    inventory_df.at[idx, "pending_rc_update"] = False
                except Exception as e:
                    print(f"❌ Failed to update {row['name']} on Rare Candy: {e}")
        pending_rows = inventory_df.loc[inventory_df["pending_shopify_update"] == True]
        print("Pending rows:")
        print(inventory_df.loc[inventory_df["pending_shopify_update"] == True][
                  ["name", "pending_shopify_update", "variant_id", "shopify_inventory_id"]])
        for idx, row in pending_rows.iterrows():
            if pd.notna(row.get("variant_id")):
                print(f"🛠️ Updating Shopify: {row['name']} (ID {row['variant_id']})")
                try:
                    print(f"📦 {row['name']}")
                    print(f"  variant_id: {row['variant_id']} ({type(row['variant_id'])})")
                    print(
                        f"  shopify_inventory_id: {row['shopify_inventory_id']} ({type(row['shopify_inventory_id'])})")
                    print(f"  shopify_qty: {row['shopify_qty']} ({type(row['shopify_qty'])})")
                    print(f"  shopify_price: {row['shopify_price']} ({type(row['shopify_price'])})")
                    update_shopify_item(
                        variant_id=int(row["variant_id"]),
                        inventory_item_id = int(row["shopify_inventory_id"]),
                        new_price=row["shopify_price"],
                        new_quantity=int(row["shopify_qty"])
                    )
                    inventory_df.at[idx, "pending_shopify_update"] = False
                except Exception as e:
                    print(f"❌ Failed to update {row['name']} on Shopify: {e}")

        save_inventory_to_db(inventory_df)
        flash("💾 Changes saved successfully!", "success")
    filtered_df = inventory_df.copy()
    if search:
        filtered_df = filtered_df[
            filtered_df["name"].astype(str).str.lower().str.contains(search)
        ]
    filtered_df = filtered_df[[col for col in filtered_df.columns if not col.startswith("touched_")]]
    return render_inventory_table(filtered_df, "📋 Inventory Viewer", hidden_buttons=["back"], show_columns=VISIBLE_COLUMNS)
@app.route("/only_rc", methods=["GET", "POST"])
@requires_auth
def only_rc():
    df = inventory_df.copy()
    filtered = df[
        (df["rc_qty"].fillna(0) > 0) &
        ((df["shopify_qty"].isna()) | (df["shopify_qty"] == 0))
    ]
    if "save" in request.form:
        process_inventory_save(filtered)
        return redirect("/only_rc")
    filtered = filtered[["name", "total amount (4/1)", "rc_qty", "rc_price", "shopify_qty", "shopify_price", "notes"]]
    return render_inventory_table(filtered, "🛒 Only Published on Rare Candy")
@app.route("/only_shopify", methods=["GET", "POST"])
@requires_auth
def only_shopify():
    df = inventory_df.copy()
    filtered = df[
        (df["shopify_qty"].fillna(0) > 0) &
        ((df["rc_qty"].isna()) | (df["rc_qty"] == 0))
    ]
    if "save" in request.form:
        process_inventory_save(filtered)
        return redirect("/only_shopify")
    filtered = filtered[["name", "total amount (4/1)", "rc_qty", "rc_price", "shopify_qty", "shopify_price", "notes"]]
    return render_inventory_table(filtered, "🛍️ Only Published on Shopify")
@app.route("/unpublished", methods=["GET", "POST"])
@requires_auth
def unpublished():
    df = inventory_df.copy()

    unpublished_df = df[
        (df["total amount (4/1)"].fillna(0) > 0) &
        ((df["shopify_qty"].fillna(0) == 0) & (df["rc_qty"].fillna(0) == 0))
    ]
    if "save" in request.form:
        process_inventory_save(unpublished_df)
        return redirect("/unpublished")
    # Keep only necessary columns for this view
    cols_to_show = ["name", "total amount (4/1)", "notes"]
    unpublished_df = unpublished_df[cols_to_show]

    # Reuse styled, sortable, sticky header table rendering
    return render_inventory_table(unpublished_df, "📦 Unpublished Inventory")
def filter_and_deduplicate_export(export_df):
    # Assume "is published" is a boolean and use it to sort, with True first.
    export_df = export_df.sort_values(by=["__key__", "is published"], ascending=[True, False])
    return export_df.drop_duplicates(subset="__key__", keep="first")

with app.app_context():
    try:
        inventory_df = load_inventory_fallback()
        #rc_sync_logic()
        #shopify_sync_logic()
        print("✅ Initial sync complete.")
    except Exception as e:
        print(f"❌ Failed to load inventory at startup: {e}")
        inventory_df = pd.DataFrame()

if __name__ == "__main__":
    # Only run this block in local development
    with app.app_context():
        with app.test_request_context():
            try:
                inventory_df = load_inventory_fallback()
                #rc_sync_logic()
                #shopify_sync_logic()
                print("Initial sync complete.")
            except Exception as e:
                print("Error during initial sync:", e)

    # Dev server only — Gunicorn will not trigger this
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
