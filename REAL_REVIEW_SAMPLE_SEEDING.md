# Real Review Sample Seeding

This project can seed same-category public review samples into the existing
diagnosis pipeline with `scripts/seed_real_review_samples.py`.

## Source

- Dataset: Amazon Reviews 2023 by McAuley Lab
- Category used now: `Cell_Phones_and_Accessories`
- Public landing page: https://amazon-reviews-2023.github.io/
- Raw files streamed by the script:
  - `raw/review_categories/Cell_Phones_and_Accessories.jsonl.gz`
  - `raw/meta_categories/meta_Cell_Phones_and_Accessories.jsonl.gz`

The script streams a small filtered slice. It does not download or commit the
full dataset.

## Current Test Groups

- 手机壳防摔保护套
- 快充数据线
- 手机钢化膜

The seeding flow first selects matching product ASINs from metadata, then
selects low-rating or explicit-problem reviews for those ASINs. The selected
comments are imported through `/api/import-comments-pipeline`, so evidence
ledger, sentiment, TOP complaints and executive reports still use the normal
application lifecycle.

## Run

```bash
cd /opt/tk-ai
python3 scripts/seed_real_review_samples.py --dry-run --per-target 8
python3 scripts/seed_real_review_samples.py --per-target 24
scripts/landing_readiness.sh
```

`OPERATOR_TOKEN` must be available in `.env` or the environment.

## Boundary

These samples are public review evidence for method validation and same-category
pain-point testing. They are not TikTok Shop sales data, GMV, market share or
platform-internal ranking data.
