import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    PPT_API_KEY     = os.getenv("PPT_API_KEY")
    SHOPIFY_STORE   = os.getenv("SHOPIFY_STORE")
    SHOPIFY_TOKEN   = os.getenv("SHOPIFY_TOKEN")
    API_VERSION     = os.getenv("SHOPIFY_API_VERSION", "2024-07")
    DEBUG           = os.getenv("DEBUG", "false").lower() == "true"
    PRICING_TARGET  = os.getenv("PRICING_TARGET", "shopify")  # legacy|shopify
    MAX_PAGE_SIZE   = int(os.getenv("MAX_PAGE_SIZE", "50"))   # server-side grid
