#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

scripts/validate_cloud.sh

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
curl -fsS https://tk-api.void52.site/api/health
echo

echo "DEPLOY_CLOUD_OK"
