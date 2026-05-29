#!/usr/bin/env python3
"""Seed same-category public review samples through the existing diagnosis API.

The script streams a tiny, keyword-filtered slice from the public Amazon Reviews
2023 corpus and imports it through /api/import-comments-pipeline. It does not
download the full dataset and it does not bypass the app's normal evidence
ledger generation path.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable, List


DEFAULT_API_BASE_URL = "https://tk-api.void52.site"
DEFAULT_DATASET_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/"
    "review_categories/Cell_Phones_and_Accessories.jsonl.gz"
)
DEFAULT_META_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/"
    "meta_categories/meta_Cell_Phones_and_Accessories.jsonl.gz"
)
DATASET_REFERENCE = "https://amazon-reviews-2023.github.io/"
USER_AGENT = "TK-AI-Evidence-Seeder/1.0 (+https://tk-api.void52.site)"


@dataclass(frozen=True)
class SampleTarget:
    product_id: str
    product_name: str
    product_terms: List[str]
    product_exclude_terms: List[str]
    required_terms: List[str]
    exclude_terms: List[str]
    source_platform: str = "amazon_reviews_2023"


TARGETS = [
    SampleTarget(
        product_id="amazon_phone_case_gap",
        product_name="手机壳防摔保护套",
        product_terms=["case", "cover"],
        product_exclude_terms=["charger", "charging station", "cable", "cord", "screen protector", "tempered glass"],
        exclude_terms=["charging station", "charger", "cable", "screen protector", "tempered glass"],
        required_terms=[
            "color",
            "picture",
            "photo",
            "expect",
            "cheap",
            "quality",
            "crack",
            "break",
            "broken",
            "fit",
            "tight",
            "scratch",
            "package",
            "shipping",
        ],
    ),
    SampleTarget(
        product_id="amazon_charging_cable_gap",
        product_name="快充数据线",
        product_terms=["cable", "charger", "charging", "usb", "cord"],
        product_exclude_terms=["case", "cover", "protector", "screen", "glass", "mount", "holder", "stand", "dock"],
        exclude_terms=["case", "cover", "screen protector", "tempered glass"],
        required_terms=[
            "connection",
            "connect",
            "slow",
            "failed",
            "stopped",
            "broken",
            "cheap",
            "quality",
            "durability",
            "package",
            "shipping",
        ],
    ),
    SampleTarget(
        product_id="amazon_screen_protector_gap",
        product_name="手机钢化膜",
        product_terms=["screen protector", "tempered glass", "glass screen", "protector film"],
        product_exclude_terms=["charger", "charging station", "cable", "cord", "phone case", "cover case"],
        exclude_terms=["charger", "charging station", "cable", "cord", "phone case"],
        required_terms=[
            "install",
            "installation",
            "instructions",
            "guide",
            "bubble",
            "crack",
            "cracked",
            "fit",
            "edge",
            "package",
            "damaged",
        ],
    ),
]


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip().strip("\"").strip("'")


def normalize_text(value: Any) -> str:
    text = re.sub(r"<br\s*/?>", " ", str(value or ""), flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def text_matches(text: str, target: SampleTarget) -> bool:
    value = text.lower()
    return (
        any(term in value for term in target.product_terms)
        and any(term in value for term in target.required_terms)
        and not any(term in value for term in target.exclude_terms)
    )


def has_negative_signal(text: str, rating: float) -> bool:
    value = text.lower()
    negative_terms = [
        "not worth",
        "broke",
        "broken",
        "failed",
        "stopped",
        "cheap",
        "poor",
        "bad",
        "disappointed",
        "returned",
        "return",
        "does not",
        "doesn't",
        "wouldn't",
        "won't",
        "cannot",
        "can't",
        "scratch",
        "crack",
        "damaged",
        "bubble",
        "hard to install",
        "slow",
    ]
    return rating <= 3.0 or any(term in value for term in negative_terms)


def iter_dataset_reviews(dataset_url: str) -> Iterable[dict[str, Any]]:
    request = urllib.request.Request(dataset_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        with gzip.GzipFile(fileobj=response) as gzipped:
            for raw_line in gzipped:
                if not raw_line:
                    continue
                yield json.loads(raw_line)


def iter_meta_records(meta_url: str) -> Iterable[dict[str, Any]]:
    request = urllib.request.Request(meta_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        with gzip.GzipFile(fileobj=response) as gzipped:
            for raw_line in gzipped:
                if not raw_line:
                    continue
                yield json.loads(raw_line)


def product_text(meta: dict[str, Any]) -> str:
    parts = [
        normalize_text(meta.get("title")),
        normalize_text(" ".join(str(item) for item in meta.get("features") or [])),
        normalize_text(" ".join(str(item) for item in meta.get("description") or [])),
        normalize_text(" ".join(str(item) for item in meta.get("categories") or [])),
    ]
    return " ".join(part for part in parts if part).lower()


def build_target_asin_map(meta_url: str, asin_limit: int, scan_limit: int) -> dict[str, set[str]]:
    asin_map: dict[str, set[str]] = {target.product_id: set() for target in TARGETS}
    scanned = 0
    for meta in iter_meta_records(meta_url):
        scanned += 1
        asin = str(meta.get("parent_asin") or "").strip()
        if not asin:
            continue
        text = product_text(meta)
        for target in TARGETS:
            if len(asin_map[target.product_id]) >= asin_limit:
                continue
            if any(term in text for term in target.product_terms) and not any(term in text for term in target.product_exclude_terms):
                asin_map[target.product_id].add(asin)
                break
        if all(len(values) >= asin_limit for values in asin_map.values()):
            break
        if scanned >= scan_limit:
            break

    missing = {key: asin_limit - len(values) for key, values in asin_map.items() if len(values) < asin_limit}
    if missing:
        raise RuntimeError(f"Not enough target ASINs after scanning {scanned} meta rows: {missing}")

    print(f"META_SCAN_OK rows={scanned} asin_limit={asin_limit}")
    for target in TARGETS:
        print(f"META_TARGET {target.product_id} asins={len(asin_map[target.product_id])}")
    return asin_map


def collect_samples(
    dataset_url: str,
    meta_url: str,
    per_target: int,
    asin_limit: int,
    meta_scan_limit: int,
    review_scan_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    asin_map = build_target_asin_map(meta_url, asin_limit, meta_scan_limit)
    samples: dict[str, list[dict[str, Any]]] = {target.product_id: [] for target in TARGETS}
    seen: set[str] = set()
    scanned = 0

    for item in iter_dataset_reviews(dataset_url):
        scanned += 1
        title = normalize_text(item.get("title"))
        body = normalize_text(item.get("text"))
        text = f"{title}. {body}".strip(". ")
        if len(text) < 24:
            continue

        lowered = text.lower()
        if lowered in seen:
            continue

        rating = float(item.get("rating") or 0)
        if rating > 3.0 or not has_negative_signal(text, rating):
            continue

        parent_asin = str(item.get("parent_asin") or "")
        for target in TARGETS:
            if len(samples[target.product_id]) >= per_target:
                continue
            if parent_asin not in asin_map[target.product_id]:
                continue
            if not text_matches(text, target):
                continue
            seen.add(lowered)
            samples[target.product_id].append({
                "text": text[:500],
                "rating": rating,
                "helpful_vote": int(item.get("helpful_vote") or 0),
                "parent_asin": parent_asin,
            })
            break

        if all(len(values) >= per_target for values in samples.values()):
            break
        if scanned >= review_scan_limit:
            break

    missing = {key: per_target - len(values) for key, values in samples.items() if len(values) < per_target}
    if missing:
        raise RuntimeError(f"Not enough matching reviews after scanning {scanned} rows: {missing}")

    print(f"DATASET_SCAN_OK rows={scanned} per_target={per_target}")
    return samples


def http_json(url: str, payload: dict[str, Any], token: str, timeout: int = 240) -> tuple[int, dict[str, Any]]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"detail": body}
        return exc.code, parsed


def import_target(api_base_url: str, token: str, target: SampleTarget, reviews: list[dict[str, Any]]) -> None:
    comments = [item["text"] for item in reviews]
    source_url = (
        f"{DATASET_REFERENCE}#Cell_Phones_and_Accessories/"
        f"{target.product_id}/sample={len(comments)}"
    )
    payload = {
        "comments": comments,
        "source_platform": target.source_platform,
        "source_url": source_url,
        "product_id": target.product_id,
        "product_name": target.product_name,
        "limit": len(comments),
    }
    status, body = http_json(f"{api_base_url.rstrip('/')}/api/import-comments-pipeline", payload, token)
    if status >= 300:
        raise RuntimeError(f"IMPORT_FAILED {target.product_id} status={status} body={body}")
    print(
        "IMPORT_OK",
        target.product_id,
        "comments=",
        body.get("raw_comment_count"),
        "source=",
        body.get("source_type"),
    )


def get_readiness(api_base_url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{api_base_url.rstrip('/')}/api/readiness",
        headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed public same-category review samples into TK AI.")
    parser.add_argument("--api-base-url", default=os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL))
    parser.add_argument("--dataset-url", default=os.getenv("AMAZON_2023_CELL_DATASET_URL", DEFAULT_DATASET_URL))
    parser.add_argument("--meta-url", default=os.getenv("AMAZON_2023_CELL_META_URL", DEFAULT_META_URL))
    parser.add_argument("--per-target", type=int, default=36)
    parser.add_argument("--asin-limit", type=int, default=240)
    parser.add_argument("--meta-scan-limit", type=int, default=60000)
    parser.add_argument("--review-scan-limit", type=int, default=240000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    token = os.getenv("OPERATOR_TOKEN", "").strip()
    if not token:
        raise SystemExit("OPERATOR_TOKEN is required in .env or environment.")

    started = time.time()
    samples = collect_samples(
        args.dataset_url,
        args.meta_url,
        args.per_target,
        args.asin_limit,
        args.meta_scan_limit,
        args.review_scan_limit,
    )
    for target in TARGETS:
        reviews = samples[target.product_id]
        print(f"SAMPLE_READY {target.product_id} {target.product_name} count={len(reviews)}")
        if args.dry_run:
            for item in reviews[:2]:
                print("  -", item["text"][:160])
            continue
        import_target(args.api_base_url, token, target, reviews)

    if not args.dry_run:
        readiness = get_readiness(args.api_base_url, token)
        summary = readiness.get("summary", {})
        print(
            "READINESS",
            readiness.get("status"),
            "products=",
            summary.get("products"),
            "diagnosed=",
            summary.get("diagnosed_products"),
            "evidence_coverage=",
            summary.get("evidence_coverage"),
            "evidence_items=",
            summary.get("evidence_items"),
        )
    print(f"DONE elapsed={time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
