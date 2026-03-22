"""
screening — screening.pack-fresh.com
Order screening microservice: fraud detection, verification, combine shipping, signature.
"""

import os
import logging
from flask import Flask

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

from routes import bp as screening_bp
app.register_blueprint(screening_bp)


@app.route("/")
def index():
    return {"service": "screening", "status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
