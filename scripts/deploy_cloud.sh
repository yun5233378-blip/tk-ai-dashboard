#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PAGES_PROJECT_NAME="${PAGES_PROJECT_NAME:-tk-ai-dashboard}"
PAGES_DEPLOY_DIR="${PAGES_DEPLOY_DIR:-.pages-deploy}"
PRIMARY_FRONTEND_URL="${PRIMARY_FRONTEND_URL:-https://tk-api.void52.site}"
PAGES_FRONTEND_URL="${PAGES_FRONTEND_URL:-https://tk-ai-dashboard.pages.dev}"

retry_command() {
    local label="$1"
    shift
    local attempt
    for attempt in 1 2 3 4 5 6; do
        if "$@"; then
            return 0
        fi
        echo "RETRY ${label} attempt ${attempt}/6" >&2
        sleep 3
    done
    echo "FAILED ${label}" >&2
    return 1
}

require_frontend_marker() {
    local url="$1"
    local label="$2"
    local html
    html="$(curl -fsSL -H 'Cache-Control: no-cache' "${url}?v=$(git rev-parse --short HEAD)")"
    if [[ "$html" != *"section-details"* ]] || [[ "$html" != *"btn-refresh-executive-report"* ]] || [[ "$html" != *"section-market"* ]]; then
        echo "FRONTEND_MARKER_MISSING ${label} ${url}" >&2
        return 1
    fi
    echo "FRONTEND_MARKER_OK ${label} ${url}"
}

scripts/validate_cloud.sh

mkdir -p "$PAGES_DEPLOY_DIR"
cp index.html "$PAGES_DEPLOY_DIR/index.html"
if [[ -f tools/platform_login_helper.py ]]; then
    mkdir -p "$PAGES_DEPLOY_DIR/tools"
    cp tools/platform_login_helper.py "$PAGES_DEPLOY_DIR/tools/platform_login_helper.py"
fi

echo "== Git status =="
git status -sb

echo "== Docker rebuild =="
if docker compose version >/dev/null 2>&1; then
    docker compose up -d --build
    docker compose ps
else
    sudo docker compose up -d --build
    sudo docker compose ps
fi

echo "== Public health =="
retry_command "api_health" curl -fsS https://tk-api.void52.site/api/health
echo

echo "== Primary frontend =="
retry_command "primary_frontend" require_frontend_marker "$PRIMARY_FRONTEND_URL" "primary"

if command -v npx >/dev/null 2>&1 && npx --yes wrangler@3 whoami >/dev/null 2>&1; then
    echo "== Cloudflare Pages deploy =="
    npx --yes wrangler@3 pages deploy "$PAGES_DEPLOY_DIR" \
        --project-name "$PAGES_PROJECT_NAME" \
        --branch main \
        --commit-hash "$(git rev-parse HEAD)" \
        --commit-message "$(git log -1 --pretty=%s | tr ' ' '_')" \
        --commit-dirty=true \
        --skip-caching
    retry_command "pages_frontend" require_frontend_marker "$PAGES_FRONTEND_URL" "pages"
else
    echo "PAGES_DEPLOY_SKIPPED Wrangler login not available. Run: npx wrangler@3 login" >&2
fi

echo "DEPLOY_CLOUD_OK"
