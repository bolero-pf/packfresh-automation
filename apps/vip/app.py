"""
vip — vip.pack-fresh.com
VIP tier management: rolling spend, tier transitions, Klaviyo sync, Shopify tags.
"""

import os
import logging
from flask import Flask

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

from routes import bp as vip_bp
app.register_blueprint(vip_bp)


@app.route("/")
def index():
    return {"service": "vip", "status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
