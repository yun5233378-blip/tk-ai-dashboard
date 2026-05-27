#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== Python compile =="
python3 -m py_compile server.py ai_diagnose.py

echo "== HTML JS parse =="
node <<'JS'
const fs = require("fs");
for (const file of ["index.html", "TK_AI_ECommerce_Dashboard.html"]) {
    const html = fs.readFileSync(file, "utf8");
    const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(match => match[1]);
    if (scripts.length === 0) {
        throw new Error(`${file}: no script tags found`);
    }
    new Function(scripts[scripts.length - 1]);
    console.log("JS_PARSE_OK", file, scripts.length);
}
JS

echo "== Static artifact checks =="
python3 <<'PY'
from pathlib import Path
import hashlib
import zipfile

paths = [
    Path("index.html"),
    Path("TK_AI_ECommerce_Dashboard.html"),
    Path("server.py"),
    Path("ai_diagnose.py"),
]
for path in paths:
    text = path.read_text(encoding="utf-8")
    if "\u00a0" in text:
        raise SystemExit(f"NBSP_FOUND {path}")
    print(f"NBSP_OK {path}")

index_hash = hashlib.sha256(Path("index.html").read_bytes()).hexdigest()
dashboard_hash = hashlib.sha256(Path("TK_AI_ECommerce_Dashboard.html").read_bytes()).hexdigest()
with zipfile.ZipFile("cloudflare-pages-platinum-dashboard.zip") as zf:
    zip_hash = hashlib.sha256(zf.read("index.html")).hexdigest()
if not (index_hash == dashboard_hash == zip_hash):
    raise SystemExit("HTML_ZIP_HASH_MISMATCH")

html = Path("index.html").read_text(encoding="utf-8")
required = [
    "section-intelligence",
    "section-evidence",
    "renderEvidenceAuditCenter",
    "Evidence Audit Center",
]
missing = [item for item in required if item not in html]
if missing:
    raise SystemExit(f"MISSING_HTML_MARKERS {missing}")

print("STATIC_CHECK_OK", index_hash)
PY

echo "== Docker compose syntax =="
if docker compose version >/dev/null 2>&1; then
    docker compose config >/dev/null
elif sudo -n docker compose version >/dev/null 2>&1; then
    sudo docker compose config >/dev/null
else
    echo "DOCKER_COMPOSE_CHECK_SKIPPED requires sudo password or docker group re-login"
fi

echo "VALIDATE_CLOUD_OK"
