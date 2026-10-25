"""Regenerate openapi.yaml from the app: KIMICLAW_TOKEN=x python tools_freeze_openapi.py"""
import yaml

from gateway.main import app

with open("openapi.yaml", "w") as f:
    yaml.safe_dump(app.openapi(), f, sort_keys=False, allow_unicode=True)
