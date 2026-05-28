#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

API_BASE_URL="${API_BASE_URL:-https://tk-api.void52.site}"
FRONTEND_URL="${FRONTEND_URL:-https://tk-ai-dashboard.pages.dev}"
READINESS_JSON="${READINESS_JSON:-/tmp/tk_landing_readiness.json}"
BRIEF_JSON="${BRIEF_JSON:-/tmp/tk_landing_brief_smoke.json}"

section() {
    printf '\n== %s ==\n' "$1"
}

section "Static validation"
scripts/validate_cloud.sh

section "Local compose status"
if docker compose version >/dev/null 2>&1; then
    docker compose ps
elif sudo -n docker compose version >/dev/null 2>&1; then
    sudo docker compose ps
else
    echo "DOCKER_STATUS_SKIPPED"
fi

section "Load operator token"
if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi
if [[ -z "${OPERATOR_TOKEN:-}" ]]; then
    echo "OPERATOR_TOKEN_MISSING" >&2
    exit 1
fi

section "Public health"
curl -fsS "$API_BASE_URL/api/health" >/tmp/tk_health.json
python3 - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path('/tmp/tk_health.json').read_text())
print('HEALTH', payload.get('status'), payload.get('storage', {}).get('backend'), 'auth=', payload.get('operator_auth_enabled'))
PY

section "Protected readiness"
curl -fsS \
    -H "Authorization: Bearer ${OPERATOR_TOKEN}" \
    "$API_BASE_URL/api/readiness" > "$READINESS_JSON"
python3 - <<'PY'
import json
import os
from pathlib import Path
path = Path(os.environ.get('READINESS_JSON', '/tmp/tk_landing_readiness.json'))
payload = json.loads(path.read_text())
summary = payload.get('summary', {})
print('STATUS', payload.get('status'), 'demo=', payload.get('ready_for_demo'), 'pilot=', payload.get('ready_for_pilot'))
print('SUMMARY', 'products=', summary.get('products'), 'diagnosed=', summary.get('diagnosed_products'), 'coverage=', summary.get('diagnosis_coverage'), 'evidence=', summary.get('evidence_items'), 'critical=', summary.get('critical_products'))
for check in payload.get('checks', []):
    print(f"CHECK {check.get('status'):>4} | {check.get('name')} | {check.get('message')}")
if payload.get('next_actions'):
    print('NEXT_ACTIONS')
    for idx, action in enumerate(payload['next_actions'], 1):
        print(f"{idx}. {action}")
if payload.get('status') == 'fail':
    raise SystemExit(2)
PY

section "Brief smoke test"
python3 - <<'PY'
import json
import os
import time
import urllib.request
from pathlib import Path
base = os.environ.get('API_BASE_URL', 'https://tk-api.void52.site')
token = os.environ['OPERATOR_TOKEN']
headers = {'Authorization': 'Bearer ' + token}
req = urllib.request.Request(base + '/api/products', headers=headers, method='GET')
with urllib.request.urlopen(req, timeout=20) as response:
    products = json.loads(response.read().decode('utf-8'))
product_id = next(iter(products), '')
if not product_id:
    raise SystemExit('NO_PRODUCT_FOR_BRIEF_SMOKE')
body = json.dumps({'brief_type': 'supply_chain', 'product_id': product_id, 'products': products, 'executive_report': {}}).encode('utf-8')
req = urllib.request.Request(
    base + '/api/generate-executive-brief',
    data=body,
    headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
    method='POST',
)
started = time.time()
with urllib.request.urlopen(req, timeout=20) as response:
    payload = json.loads(response.read().decode('utf-8'))
elapsed = time.time() - started
Path(os.environ.get('BRIEF_JSON', '/tmp/tk_landing_brief_smoke.json')).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
print('BRIEF', response.status, payload.get('schema'), payload.get('source'), payload.get('brief_label'), len(payload.get('brief_text', '')), f'{elapsed:.2f}s')
if payload.get('schema') != 'tk_action_brief_v2' or not payload.get('brief_text'):
    raise SystemExit('BRIEF_SMOKE_FAILED')
PY

section "Frontend markers"
html="$(curl -fsSL -H 'Cache-Control: no-cache' "$FRONTEND_URL?v=$(git rev-parse --short HEAD)")"
for marker in login-screen ai-executive-report data-executive-brief section-details; do
    if [[ "$html" != *"$marker"* ]]; then
        echo "FRONTEND_MARKER_MISSING $marker" >&2
        exit 1
    fi
    echo "FRONTEND_MARKER_OK $marker"
done

section "Git state"
git status -sb

echo "LANDING_READINESS_OK"
