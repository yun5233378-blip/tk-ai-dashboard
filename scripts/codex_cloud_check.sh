#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Locale =="
locale | sed -n '1,8p'

echo "== Git =="
git status -sb

echo "== Line ending / whitespace =="
git diff --check

echo "== Compile =="
python3 -m py_compile server.py ai_diagnose.py

echo "== Static validation =="
scripts/validate_cloud.sh

echo "CODEX_CLOUD_CHECK_OK"
