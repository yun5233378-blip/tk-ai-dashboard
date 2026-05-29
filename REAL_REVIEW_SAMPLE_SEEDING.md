# Review Sample Seeding

This project has two evidence seeding modes:

- `scripts/seed_current_market_signals.py`: current 2026 public discussion
  signals. Use this for demos, pilots and product decisions.
- `scripts/seed_real_review_samples.py`: historical Amazon Reviews 2023 public
  corpus. Use this only for algorithm regression tests and same-category method
  validation.

Do not present the Amazon Reviews 2023 seed as current market evidence.

## Current 2026 Market Signals

The current seeder imports curated 2026 public discussion/news signals through
`/api/import-comments-pipeline`, so evidence ledger, sentiment, TOP complaints
and executive reports still use the normal application lifecycle.

Current test groups:

- 2026 手机壳与磁吸保护套
- 2026 USB-C 快充线
- 2026 手机屏幕保护膜

Run:

```bash
cd /opt/tk-ai
python3 scripts/seed_current_market_signals.py --dry-run --skip-historical-check
python3 scripts/seed_current_market_signals.py --skip-historical-check
scripts/landing_readiness.sh
```

`OPERATOR_TOKEN` must be available in `.env` or the environment.

The current seeder intentionally does not fall back to old corpora. If fresh
public signals are unavailable, it should fail instead of silently using stale
data.

## Historical Regression Corpus

`scripts/seed_real_review_samples.py` can still stream a tiny, keyword-filtered
slice from the public Amazon Reviews 2023 corpus and import it through
`/api/import-comments-pipeline`. It does not download the full dataset.

### Source

- Dataset: Amazon Reviews 2023 by McAuley Lab
- Category used now: `Cell_Phones_and_Accessories`
- Public landing page: https://amazon-reviews-2023.github.io/
- Raw files streamed by the script:
- `raw/review_categories/Cell_Phones_and_Accessories.jsonl.gz`
- `raw/meta_categories/meta_Cell_Phones_and_Accessories.jsonl.gz`

### Historical Test Groups

- 手机壳防摔保护套
- 快充数据线
- 手机钢化膜

The historical seeding flow first selects matching product ASINs from metadata,
then selects low-rating or explicit-problem reviews for those ASINs.

Run only for regression:

```bash
cd /opt/tk-ai
python3 scripts/seed_real_review_samples.py --dry-run --per-target 8
python3 scripts/seed_real_review_samples.py --per-target 24
scripts/landing_readiness.sh
```

## Boundary

These samples/signals are public evidence for method validation and
same-category pain-point testing. They are not TikTok Shop sales data, GMV,
market share or platform-internal ranking data.
