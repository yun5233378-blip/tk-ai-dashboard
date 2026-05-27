# Cloud Development Workflow

This repository is now developed directly on the cloud server. Treat `/opt/tk-ai` as the source of truth.

## Standard Loop

1. SSH into the server.

```bash
ssh ubuntu@43.156.180.164
cd /opt/tk-ai
```

2. Confirm the repository is clean and up to date.

```bash
git status -sb
git pull --ff-only
```

3. Make changes in `/opt/tk-ai`. Keep runtime secrets and data out of Git.

4. Validate before committing.

```bash
python3 -m py_compile server.py ai_diagnose.py
python3 - <<'PY'
from pathlib import Path
html = Path("index.html").read_text(encoding="utf-8")
assert "section-evidence" in html or "section-intelligence" in html
print("HTML_STATIC_CHECK_OK")
PY
```

5. Commit and push.

```bash
git add .
git commit -m "Describe the change"
git push
```

6. Rebuild and verify the cloud service.

```bash
sudo docker compose up -d --build
sudo docker compose ps
curl -fsS https://tk-api.void52.site/api/health
```

## Rollback

```bash
cd /opt/tk-ai
git log --oneline -5
git revert <commit>
sudo docker compose up -d --build
```

## Do Not Commit

- `.env`
- `operator-token.txt`
- `diagnosed_products.json`
- `raw_comments.json`
- `competitor_vs_reports.json`
- `admin_audit_logs.json`
- `backups/`

## Remote

GitHub remote: `git@github.com:yun5233378-blip/tk-ai-dashboard.git`
