import pandas as pd
import math

# Load your input CSV file
df = pd.read_csv(".venv/Scripts/InventoryFinal.csv")

# Stepwise pricing function
def calculate_conference_price(price):
    if pd.isnull(price):
        return 0
    try:
        price = float(price)
    except:
        return 0

    if 0 < price < 20:
        return math.ceil(price * 1.05)
    elif 20 < price < 100:
        return math.ceil(price * 1.05 / 5) * 5
    elif 100 < price < 200:
        return math.ceil(price * 1.03 / 10) * 10
    elif 200 < price < 1000:
        return math.ceil(price * 1.02 / 25) * 25
    elif price > 1000:
        return math.ceil(price * 1.01 / 100) * 100
    else:
        return 0

# Filter only 'DC' rows
dc_rows = df[df['notes'] == 'DC'].copy()

# Apply conference pricing
dc_rows['conference_price'] = dc_rows['shopify_price'].apply(calculate_conference_price)

# Select output columns
output_df = dc_rows[['name', 'conference_price', 'shopify_price']]

# Export to CSV
output_df.to_csv("conference_pricing_output.csv", index=False)
print("✅ Done. Output saved as conference_pricing_output.csv")
