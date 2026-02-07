from flask import Flask, redirect, url_for
from .config import Config

# ADD THESE:
from .cli import register_cli
from .adapters.shopify_client import ShopifyClient
from .blueprints.inventory import bp as inventory_bp
from .blueprints.actions import bp as actions_bp
from .Views.slabs_reports import bp_slabs_reports
import os, secrets

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config.setdefault("SLABS_OUT_ROOT", "out")
    app.config.setdefault("ALLOW_PRICE_WRITES", True)  # safety off by default

    # add this:
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY") or secrets.token_hex(32)

    # create Shopify client within an app context
    with app.app_context():
        app.config["SHOPIFY_CLIENT"] = ShopifyClient()

    app.register_blueprint(inventory_bp, url_prefix="/inventory")
    app.register_blueprint(actions_bp,   url_prefix="/actions")
    app.register_blueprint(bp_slabs_reports)
    register_cli(app)
    return app

    @app.get("/")
    def root():
        return redirect(url_for("inventory.index"))

    return app

app = create_app()
