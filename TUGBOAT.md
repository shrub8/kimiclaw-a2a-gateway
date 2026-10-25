# For Tugboat: test commands (CI workflow is yours; none is committed here)

Python 3.13. No secrets or network access needed; tests use a fake `openclaw` (tests/fakebin) and a local mock n8n receiver.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests -q          # characterization + contract tests, ~35 s
```

Contract check only: `python -m pytest tests/test_contract.py -q`
If routes or schemas change on purpose: `KIMICLAW_TOKEN=x python tools_freeze_openapi.py` and commit openapi.yaml.

Golden replay (parity against fixtures recorded from original main; lives outside the repo for now at
/workspace/kimiclaw/fixtures, may be vendored later):
```bash
python fixtures/golden.py replay-local --app-dir .     # expect "[default] 24 passed" and "[n8n_basic] 2 passed"
```
Docker smoke: `docker build -t kimiclaw-gw .` (image needs openclaw added to pass /readyz).
