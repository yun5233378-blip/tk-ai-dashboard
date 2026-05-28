# Cloud Development Workflow

This repository is developed directly on the cloud server. Treat `/opt/tk-ai` as the source of truth.

## Cloud Entrypoints

- Stable app/API host: `https://tk-api.void52.site`
- Cloudflare Pages app host: `https://tk-ai-dashboard.pages.dev`
- Legacy custom host: `https://dashboard.void52.site`

`dashboard.void52.site` may still serve old Cloudflare edge cache until the cache is purged or the custom domain is bound to the new `tk-ai-dashboard` Pages project. For product verification, prefer `tk-api.void52.site` and `tk-ai-dashboard.pages.dev`.

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
scripts/validate_cloud.sh
```

5. Commit and push.

```bash
git add .
git commit -m "Describe the change"
git push
```

6. Deploy the full cloud stack.

```bash
scripts/deploy_cloud.sh
```

The deploy script now performs all of this in one pass:

- Python and HTML static validation.
- Docker rebuild/restart for FastAPI, Redis, and Caddy.
- Public `/api/health` check.
- Frontend marker check on `https://tk-api.void52.site`.
- Cloudflare Pages deploy to `tk-ai-dashboard` when Wrangler is logged in.
- Frontend marker check on `https://tk-ai-dashboard.pages.dev`.

## Cloudflare Notes

Wrangler OAuth is stored under the server user's `~/.config/.wrangler`. If Pages deploy is skipped, re-login from a local PowerShell session with port forwarding:

```powershell
ssh -L 8976:localhost:8976 tk-ai-cloud
```

Then run on the server:

```bash
cd /opt/tk-ai
npx wrangler@3 login
npx wrangler@3 whoami
npx wrangler@3 pages project list
```

The current Pages project is `tk-ai-dashboard`. If `dashboard.void52.site` needs to become the primary frontend host, bind it as a custom domain to that Pages project or purge the existing cached host in Cloudflare.

## Rollback

```bash
cd /opt/tk-ai
git log --oneline -5
git revert <commit>
scripts/deploy_cloud.sh
```

## Do Not Commit

- `.env`
- `operator-token.txt`
- `diagnosed_products.json`
- `raw_comments.json`
- `competitor_vs_reports.json`
- `admin_audit_logs.json`
- `backups/`
- `.pages-deploy/`

## Remote

GitHub remote: `git@github.com:yun5233378-blip/tk-ai-dashboard.git`

## Codex Remote Execution Rules

These rules prevent Windows PowerShell, SSH quoting, CRLF, and UTF-8 corruption during cloud development.

1. Use a persistent remote shell for non-trivial work.

```bash
ssh tk-ai-cloud
cd /opt/tk-ai
export LANG=C.UTF-8 LC_ALL=C.UTF-8
```

2. Do not send multiline edit scripts through one-off local commands like `ssh tk-ai-cloud "..."` from PowerShell.

3. For complex edits, create and run the script or patch on the server side:

```bash
cat > /tmp/change.py <<'PY'
# remote-only script content
PY
python3 /tmp/change.py
```

or:

```bash
git apply <<'PATCH'
# unified diff
PATCH
```

4. Keep one-off SSH commands ASCII-only and short. Use them for reads and simple checks only.

5. Before any deploy, run:

```bash
git status -sb
git diff --check
scripts/validate_cloud.sh
```

6. Deploy only through:

```bash
scripts/deploy_cloud.sh
```

7. If a failed experiment leaves partial edits, clean only Codex-owned uncommitted files after checking status:

```bash
git status -sb
git diff -- <file>
git restore <file>
```
