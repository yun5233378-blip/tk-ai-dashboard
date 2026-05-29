#!/usr/bin/env python3
"""Seed 2026 public market discussion signals into the normal diagnosis API.

This script is intentionally curated and strict: it uses only current public
discussion/news signals with explicit dates and URLs. It does not fall back to
the historical Amazon Reviews 2023 corpus.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, List


DEFAULT_API_BASE_URL = "https://tk-api.void52.site"
USER_AGENT = "TK-AI-Current-Market-Signals/1.0 (+https://tk-api.void52.site)"
HISTORICAL_PRODUCT_IDS = [
    "amazon_phone_case_gap",
    "amazon_charging_cable_gap",
    "amazon_screen_protector_gap",
]


@dataclass(frozen=True)
class MarketSignal:
    date: str
    source: str
    url: str
    text: str


@dataclass(frozen=True)
class SignalTarget:
    product_id: str
    product_name: str
    source_url: str
    signals: List[MarketSignal]


TARGETS = [
    SignalTarget(
        product_id="current_phone_case_2026",
        product_name="2026 手机壳与磁吸保护套",
        source_url="https://www.reddit.com/r/Spigen/comments/1slrls2/major_design_flaw_iPhone_17_pro_classic_ls/",
        signals=[
            MarketSignal(
                "2026-04-15",
                "Reddit r/Spigen",
                "https://www.reddit.com/r/Spigen/comments/1slrls2/major_design_flaw_iPhone_17_pro_classic_ls/",
                "MagSafe phone case discussion reports a loose fit and the case partially dislodging during low-height drops, creating a protection and fit risk.",
            ),
            MarketSignal(
                "2026-03-11",
                "Reddit r/Mous",
                "https://www.reddit.com/r/Mous/comments/1rqsd2o/very_bad_case_for_s26_ultra_with_a_major_design/",
                "S26 Ultra case discussion flags a design flaw around camera protection, with expected chips, scratches, or cracked lens risk if the case takes a bump.",
            ),
            MarketSignal(
                "2026-03-07",
                "Reddit r/samsunggalaxy",
                "https://www.reddit.com/r/samsunggalaxy/comments/1rnmh1g/s26_ultra_mag_safe_case_that_actually_works/",
                "S26 Ultra discussion reports that MagSafe wallets, battery packs, and phone grips do not attach well because camera bezel height blocks centered contact.",
            ),
            MarketSignal(
                "2026-05-03",
                "Reddit r/Spigen",
                "https://www.reddit.com/r/Spigen/comments/1t2ok87/s26_spigen_magfit_unusable_magsafe/",
                "A Galaxy S26 MagFit case discussion reports MagSafe function disappointment because Apple-oriented magnetic accessories do not behave as expected on the non-Apple phone.",
            ),
            MarketSignal(
                "2026-02-02",
                "Smartphone Board",
                "https://www.smartphoneboard.com/google-forum/top-rated-transparent-cases-that-do-not-turn-yellow-77641/",
                "Transparent case buyers discuss yellowing, TPU UV degradation, material choice, and the tradeoff that harder polycarbonate can stay clear but crack more easily.",
            ),
            MarketSignal(
                "2026-05-26",
                "Reddit r/iphone",
                "https://www.reddit.com/r/iphone/comments/1tnudpx/any_clear_iphone_cases_that_actually_dont_yellow/",
                "Clear case discussion says TPU sides yellow while hard polycarbonate backs remain clear, making material mix and anti-yellowing claims a live buyer concern.",
            ),
            MarketSignal(
                "2026-03-17",
                "ScienceInsights",
                "https://scienceinsights.org/why-do-phone-cases-turn-yellow-causes-and-fixes/",
                "Phone case material guide explains UV-driven yellowing and warns that peroxide cleaning can weaken polymers, making cases brittle enough to crack or crumble.",
            ),
        ],
    ),
    SignalTarget(
        product_id="current_usb_c_cable_2026",
        product_name="2026 USB-C 快充线",
        source_url="https://www.reddit.com/r/techsupport/comments/1s4r514/usbc_to_usbc_cable_breaking_after_a_few_months/",
        signals=[
            MarketSignal(
                "2026-03-27",
                "Reddit r/techsupport",
                "https://www.reddit.com/r/techsupport/comments/1s4r514/usbc_to_usbc_cable_breaking_after_a_few_months/",
                "USB-C cable discussion reports cables breaking after only a few months while borrowed cables still work, pointing to durability and connector quality risk.",
            ),
            MarketSignal(
                "2026-03-19",
                "Reddit r/UsbCHardware",
                "https://www.reddit.com/r/UsbCHardware/comments/1ry22xf/usbc_cable_connector_durability/",
                "USB-C connector durability discussion reports broken cables that still connect but drop charging current to very low levels, creating slow charging complaints.",
            ),
            MarketSignal(
                "2026-03-29",
                "Reddit r/UsbCHardware",
                "https://www.reddit.com/r/UsbCHardware/comments/1s6mofq/can_anyone_explain_why_my_cord_is_doing_this/",
                "USB-C buyer discussion describes a newly purchased cable showing abnormal behavior, with responses pointing to resistance and communication problems.",
            ),
            MarketSignal(
                "2026-02-16",
                "Framework Community",
                "https://community.frame.work/t/solved-charging-cable-split-and-frayed/19056?page=4",
                "Framework charging cable thread discusses split and frayed outer insulation, exposed internal wires, and whether replacement cables will survive repeated use.",
            ),
            MarketSignal(
                "2026-02-20",
                "TechTimes",
                "https://www.techtimes.com/articles/314742/20260220/7-warning-signs-your-charging-cable-destroying-your-phone.htm",
                "Charging cable safety article lists frayed wires, loose or wobbly connection, distorted connectors, and unstable charging as warning signs that can damage a device.",
            ),
            MarketSignal(
                "2026-05-14",
                "Rugged Ratings",
                "https://www.ruggedratings.com/most-durable-usb-c-cable",
                "Durable USB-C cable guide describes recurring buyer replacement patterns around frayed connectors, lost data speeds, and cables that stop charging at full wattage.",
            ),
            MarketSignal(
                "2026-01-13",
                "Anker",
                "https://www.anker.com/story/cables/iphone-charging-cable-not-working",
                "Charging cable troubleshooting guide identifies wear, exposed wires, bending marks near connectors, and unstable power as common failure modes.",
            ),
        ],
    ),
    SignalTarget(
        product_id="current_screen_protector_2026",
        product_name="2026 手机屏幕保护膜",
        source_url="https://www.reddit.com/r/RedMagic/comments/1rv3otk/screen_protector_issues/",
        signals=[
            MarketSignal(
                "2026-03-16",
                "Reddit r/RedMagic",
                "https://www.reddit.com/r/RedMagic/comments/1rv3otk/screen_protector_issues/",
                "Tempered glass protector discussion reports installation following instructions but bubbles and dust remained, and removal made the problem worse.",
            ),
            MarketSignal(
                "2026-05-27",
                "Reddit r/galaxys26ultra",
                "https://www.reddit.com/r/galaxys26ultra/comments/1s8h013/these_screen_protectors_really_suck_what_you_using/",
                "Galaxy S26 Ultra screen protector discussion reports protectors cracking twice within two days, raising material durability and edge strength concerns.",
            ),
            MarketSignal(
                "2026-05-21",
                "Reddit r/galaxys26ultra",
                "https://www.reddit.com/r/galaxys26ultra/comments/1t7tcqq/tempered_glass_screen_protectors_quality/",
                "Tempered glass quality discussion reports repeated protector replacement and edge weak points that may cause cracking with thin cases.",
            ),
            MarketSignal(
                "2026-04-27",
                "Reddit r/samsunggalaxy",
                "https://www.reddit.com/r/samsunggalaxy/comments/1s2lswe/beware_the_s26_ultra_screen_cracked/",
                "S26 Ultra discussion reports a screen crack after a low-height fall even with a clear magnetic case and anti-reflective screen protector.",
            ),
            MarketSignal(
                "2026-03-05",
                "Smartphone Board",
                "https://www.smartphoneboard.com/samsung-forum/which-screen-protector-allows-for-smooth-fingerprint-scanning-on-s26-ultra-83721/",
                "S26 Ultra buyer discussion reports a random tempered glass protector making the ultrasonic fingerprint scanner fail repeatedly despite re-registering thumbs.",
            ),
            MarketSignal(
                "2026-02-10",
                "Computer Forums",
                "https://computerforums.net/threads/applying-a-screen-protector.2356/",
                "Screen protector installation discussion asks how to apply a protector without air bubbles, reinforcing bubble-free installation as a current buyer concern.",
            ),
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


def request_json(url: str, token: str, method: str = "GET", payload: dict[str, Any] | None = None, timeout: int = 240) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
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


def delete_historical_products(api_base_url: str, token: str) -> None:
    status, products = request_json(f"{api_base_url.rstrip('/')}/api/products", token)
    if status >= 300:
        raise RuntimeError(f"PRODUCTS_FETCH_FAILED status={status} body={products}")
    if not isinstance(products, dict):
        raise RuntimeError("PRODUCTS_FETCH_FAILED products payload is not an object")

    removed = []
    for product_id in HISTORICAL_PRODUCT_IDS:
        if product_id in products:
            del products[product_id]
            removed.append(product_id)
    if not removed:
        print("HISTORICAL_SAMPLE_REMOVE skipped none_present")
        return

    # There is no public bulk replace endpoint, so this script intentionally
    # delegates active cleanup to the server-side helper command in the runbook.
    raise RuntimeError(
        "Historical samples are present. Run the documented docker cleanup command "
        f"before seeding current signals: {', '.join(removed)}"
    )


def import_target(api_base_url: str, token: str, target: SignalTarget) -> None:
    comments = [
        f"[{signal.date} | {signal.source}] {signal.text} Source: {signal.url}"
        for signal in target.signals
    ]
    payload = {
        "comments": comments,
        "source_platform": "current_public_discussion_2026",
        "source_url": target.source_url,
        "product_id": target.product_id,
        "product_name": target.product_name,
        "limit": len(comments),
    }
    status, body = request_json(
        f"{api_base_url.rstrip('/')}/api/import-comments-pipeline",
        token,
        method="POST",
        payload=payload,
    )
    if status >= 300:
        raise RuntimeError(f"IMPORT_FAILED {target.product_id} status={status} body={body}")
    print("CURRENT_IMPORT_OK", target.product_id, "comments=", body.get("raw_comment_count"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed 2026 public discussion signals into TK AI.")
    parser.add_argument("--api-base-url", default=os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-historical-check", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    token = os.getenv("OPERATOR_TOKEN", "").strip()
    if not token:
        raise SystemExit("OPERATOR_TOKEN is required in .env or environment.")

    if not args.skip_historical_check:
        delete_historical_products(args.api_base_url, token)

    for target in TARGETS:
        print("CURRENT_SAMPLE_READY", target.product_id, target.product_name, "signals=", len(target.signals))
        for signal in target.signals:
            print(" ", signal.date, signal.source, signal.url)
        if not args.dry_run:
            import_target(args.api_base_url, token, target)

    if not args.dry_run:
        status, readiness = request_json(f"{args.api_base_url.rstrip('/')}/api/readiness", token)
        if status >= 300:
            raise RuntimeError(f"READINESS_FAILED status={status} body={readiness}")
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
