#!/usr/bin/env python3
"""Local visible-comment capture helper for TK-AI.

The helper opens the target platform link on the operator's own computer,
lets the operator log in / open the comment panel, captures visible comment-like
text, and uploads the text to the TK-AI cloud diagnosis pipeline.

It does not bypass CAPTCHA, paywalls, or access controls. The operator must have
normal access to the page in the visible browser window.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

import requests
from playwright.sync_api import sync_playwright


MIN_COMMENT_LENGTH = 4
MAX_COMMENT_LENGTH = 500

NOISE_PATTERNS = [
    r"^login$",
    r"^follow$",
    r"^share$",
    r"^comment$",
    r"^for you$",
    r"^following$",
    r"^like$",
    r"^\d+$",
    r"^\d+(\.\d+)?[wkm万千]?$",
    r"^(home|discover|search|profile|message|upload)$",
    r"^(douyin|tiktok|xiaohongshu|red note)$",
    r"^(打开|登录|关注|推荐|首页|搜索|消息|我|评论|分享|点赞|收藏|转发)$",
    r"^(抖音|小红书|复制链接|打开看看|相关搜索)$",
]


def normalize_platform(value: str) -> str:
    platform = (value or "").strip().lower()
    aliases = {
        "xhs": "xiaohongshu",
        "red": "xiaohongshu",
        "xiaohongshu": "xiaohongshu",
        "douyin": "douyin",
    }
    platform = aliases.get(platform, platform)
    if platform not in {"douyin", "xiaohongshu"}:
        raise SystemExit("Unsupported platform. Use douyin or xiaohongshu.")
    return platform


def normalize_comment_text(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"^(comment|user|buyer|评论|用户|买家)[:：\s]+", "", text, flags=re.I).strip()
    return text[:MAX_COMMENT_LENGTH].strip()


def looks_like_comment(text: str) -> bool:
    if len(text) < MIN_COMMENT_LENGTH or len(text) > MAX_COMMENT_LENGTH:
        return False
    lowered = text.lower().strip()
    for pattern in NOISE_PATTERNS:
        if re.match(pattern, lowered, flags=re.I):
            return False
    has_word_signal = bool(re.search(r"[\u4e00-\u9fffA-Za-z]", text))
    if not has_word_signal:
        return False
    return True


def dedupe(values: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = normalize_comment_text(value)
        key = text.lower()
        if not looks_like_comment(text) or key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def collect_visible_text(page: Any, limit: int) -> list[str]:
    candidates: list[str] = []
    selectors = [
        '[data-e2e*="comment"]',
        '[class*="comment"]',
        '[class*="Comment"]',
        '[class*="reply"]',
        '[class*="Reply"]',
        'div[role="listitem"]',
        "p",
        "span",
        "div",
    ]
    for selector in selectors:
        try:
            elements = page.query_selector_all(selector)
        except Exception:
            continue
        for element in elements[:500]:
            try:
                text = element.inner_text(timeout=500)
            except Exception:
                continue
            if text:
                candidates.extend(line.strip() for line in text.splitlines() if line.strip())

    try:
        body_text = page.locator("body").inner_text(timeout=3000)
        candidates.extend(line.strip() for line in body_text.splitlines() if line.strip())
    except Exception:
        pass

    return dedupe(candidates, limit)


def open_and_capture(platform: str, url: str, limit: int, scroll_rounds: int) -> list[str]:
    print("")
    print("A Chromium window will open on this computer.")
    print("Log in normally if the platform asks. Open the video/note comment panel.")
    print("Scroll a few comments into view, then return here and press Enter.")
    print("")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1365, "height": 900},
            locale="zh-CN",
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        input("Press Enter after comments are visible in the browser...")
        for _ in range(max(1, scroll_rounds)):
            page.mouse.wheel(0, 900)
            page.wait_for_timeout(900)
        comments = collect_visible_text(page, limit)
        if not comments:
            print("No visible comment-like text captured. Keep the browser open, open comments, scroll, then press Enter again.")
            input("Press Enter to retry capture...")
            for _ in range(max(1, scroll_rounds)):
                page.mouse.wheel(0, 900)
                page.wait_for_timeout(900)
            comments = collect_visible_text(page, limit)
        browser.close()
    return comments


def upload_comments(
    api_base: str,
    helper_token: str,
    platform: str,
    source_url: str,
    comments: list[str],
    limit: int,
) -> dict[str, Any]:
    endpoint = api_base.rstrip("/") + "/api/local-capture-helper/upload"
    payload = {
        "helper_token": helper_token,
        "platform": platform,
        "source_url": source_url,
        "comments": comments,
        "limit": min(limit, len(comments)),
    }
    response = requests.post(endpoint, json=payload, timeout=120)
    try:
        data = response.json()
    except Exception:
        data = {"detail": response.text}
    if response.status_code >= 400:
        raise RuntimeError(data.get("detail") or f"Upload failed with HTTP {response.status_code}")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="TK-AI local visible-comment capture helper")
    parser.add_argument("--platform", required=True, help="douyin or xiaohongshu")
    parser.add_argument("--url", required=True, help="Video or note URL to open locally")
    parser.add_argument("--api", default="https://tk-api.void52.site", help="TK-AI API base URL")
    parser.add_argument("--helper-token", required=True, help="Short-lived helper token generated by admin panel")
    parser.add_argument("--limit", type=int, default=100, help="Maximum comments to capture")
    parser.add_argument("--scroll-rounds", type=int, default=8, help="Scroll rounds before extraction")
    args = parser.parse_args()

    platform = normalize_platform(args.platform)
    comments = open_and_capture(platform, args.url, args.limit, args.scroll_rounds)
    if not comments:
        print("No valid comments captured. Make sure comments are visible before pressing Enter.")
        return 2

    print("")
    print(f"Captured {len(comments)} visible comment candidates.")
    print("Sample:")
    for idx, item in enumerate(comments[:5], 1):
        print(f"{idx}. {item}")
    print("")
    confirm = input("Upload these comments to TK-AI for diagnosis? [Y/n] ").strip().lower()
    if confirm in {"n", "no"}:
        print("Cancelled before upload.")
        return 130

    result = upload_comments(args.api, args.helper_token, platform, args.url, comments, args.limit)
    print("")
    print("Upload and diagnosis complete.")
    print(f"Product: {result.get('product_name')}")
    print(f"Valid comments: {result.get('raw_comment_count')}")
    print(f"Message: {result.get('message')}")
    print("Return to the dashboard and refresh the diagnosis view.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"Helper failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
