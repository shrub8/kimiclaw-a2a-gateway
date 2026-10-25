#!/usr/bin/env bash
# Full local/CI check: pytest (characterization + contract) and golden replay vs. original main.
set -euo pipefail
cd "$(dirname "$0")"
PY=python
[ -x .venv/bin/python ] && PY=.venv/bin/python
rc=0
"$PY" -m pytest tests -q -p no:cacheprovider 2>&1 | tail -3 || rc=1
out=$("$PY" tests/golden/golden.py replay-local --app-dir . 2>/dev/null) || rc=1
echo "$out" | grep -E "FAIL|passed" || true
echo "$out" | grep -q "FAIL" && rc=1
exit $rc
