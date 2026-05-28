#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多源评论采集适配器。

设计目标：
1. 复用现有 raw_comments.json -> ai_diagnose.py 诊断链路。
2. 支持抖音、小红书等国内讨论源的合规采集入口。
3. 不绕过登录、验证码、反爬或平台访问控制；如页面不可公开读取，则要求人工导入评论文本。

支持输入：
- https://www.douyin.com/... / https://v.douyin.com/...
- https://www.xiaohongshu.com/... / https://xhslink.com/...
- comments://评论1%0A评论2 或直接传入多行评论文本
- file:///path/to/comments.txt 或本地文本文件路径
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, List
from urllib.parse import unquote, urlparse

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

PUBLIC_PAGE_TIMEOUT = 12
MIN_COMMENT_LENGTH = 4
MAX_COMMENT_LENGTH = 500

PLATFORM_HINTS = {
    "douyin": ["douyin.com", "v.douyin.com", "iesdouyin.com"],
    "xiaohongshu": ["xiaohongshu.com", "xhslink.com"],
    "manual": [],
}


def detect_platform(source: str) -> str:
    value = (source or "").strip().lower()
    if value.startswith("comments://") or "\n" in source:
        return "manual"
    parsed = urlparse(value)
    host = parsed.hostname or ""
    if not host and Path(source).exists():
        return "manual"
    for platform, hosts in PLATFORM_HINTS.items():
        if any(item in host for item in hosts):
            return platform
    return "public_web"


def normalize_comment_text(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"^(评论|用户|买家|note|comment)[:：]\s*", "", text, flags=re.I)
    return text[:MAX_COMMENT_LENGTH].strip()


def dedupe_comments(comments: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen = set()
    cleaned: List[Dict[str, Any]] = []
    for item in comments:
        text = normalize_comment_text(str(item.get("comment_text", "")))
        if len(text) < MIN_COMMENT_LENGTH:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        item = dict(item)
        item["comment_text"] = text
        cleaned.append(item)
        if len(cleaned) >= limit:
            break
    return cleaned


def make_comment(text: str, platform: str, index: int, source_url: str = "") -> Dict[str, Any]:
    return {
        "username": f"{platform}_source_{index}",
        "comment_text": normalize_comment_text(text),
        "like_count": 0,
        "publish_time": "",
        "source_platform": platform,
        "source_url": source_url,
        "collection_method": "multi_source_adapter",
    }


def read_manual_comments(source: str, platform: str, limit: int) -> List[Dict[str, Any]]:
    raw = source
    if source.startswith("comments://"):
        raw = unquote(source[len("comments://"):])
    elif source.startswith("file://"):
        raw = Path(unquote(urlparse(source).path)).read_text(encoding="utf-8")
    elif Path(source).exists():
        raw = Path(source).read_text(encoding="utf-8")

    lines: List[str] = []
    for line in raw.splitlines():
        value = line.strip()
        if not value:
            continue
        if value.startswith(('-', '*', '•')):
            value = value[1:].strip()
        lines.append(value)

    comments = [make_comment(line, platform, idx + 1) for idx, line in enumerate(lines)]
    return dedupe_comments(comments, limit)


def extract_public_text_candidates(html: str) -> List[str]:
    candidates: List[str] = []
    patterns = [
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        r'"desc"\s*:\s*"([^"]{8,500})"',
        r'"description"\s*:\s*"([^"]{8,500})"',
        r'"title"\s*:\s*"([^"]{8,300})"',
        r'<title[^>]*>(.*?)</title>',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.I | re.S):
            value = re.sub(r'<[^>]+>', ' ', match.group(1))
            value = value.encode('utf-8', errors='ignore').decode('unicode_escape', errors='ignore') if '\\u' in value else value
            value = normalize_comment_text(value)
            if value:
                candidates.append(value)
    return candidates


def fetch_public_page(source: str) -> str:
    if requests is None:
        raise RuntimeError("requests is not installed")
    response = requests.get(
        source,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"},
        timeout=PUBLIC_PAGE_TIMEOUT,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def build_public_page_comments(source: str, platform: str, limit: int) -> List[Dict[str, Any]]:
    html = fetch_public_page(source)
    candidates = extract_public_text_candidates(html)
    comments = [make_comment(value, platform, idx + 1, source) for idx, value in enumerate(candidates)]
    return dedupe_comments(comments, limit)


def platform_guidance(platform: str) -> str:
    if platform == "douyin":
        return "抖音公开页通常限制评论读取；请导出或复制评论文本后用 comments:// 或文本文件导入。"
    if platform == "xiaohongshu":
        return "小红书公开页通常限制评论读取；请导出或复制笔记评论文本后用 comments:// 或文本文件导入。"
    return "该平台暂未接入专用公开评论 API，请使用 comments:// 或文本文件导入评论。"


def collect_comments(source: str, limit: int) -> tuple[str, List[Dict[str, Any]]]:
    platform = detect_platform(source)
    if platform == "manual":
        comments = read_manual_comments(source, platform, limit)
        return platform, comments

    try:
        comments = build_public_page_comments(source, platform, limit)
        if comments:
            print(f"{platform} public page extracted {len(comments)} text signals.")
            return platform, comments
    except Exception as exc:
        print(f"{platform} public page extraction unavailable: {exc}")

    raise RuntimeError(platform_guidance(platform))


def write_comments(path: Path, platform: str, source: str, comments: List[Dict[str, Any]]) -> None:
    payload = {
        "schema": "tk_multi_source_comments_v1",
        "source_platform": platform,
        "source_url": source if source.startswith("http") else "manual_import",
        "collection_method": "public_page_or_manual_import",
        "collected_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "comments": comments,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="TK AI 多源评论采集适配器")
    parser.add_argument("source", help="平台链接、comments:// 多行评论、file:// 文本文件或本地文本文件")
    parser.add_argument("--limit", type=int, default=100, help="最多输出评论数")
    parser.add_argument("--output", default="raw_comments.json", help="输出 JSON 文件路径")
    args = parser.parse_args()

    try:
        platform, comments = collect_comments(args.source, args.limit)
        if not comments:
            raise RuntimeError(platform_guidance(platform))
        output_path = Path(args.output)
        write_comments(output_path, platform, args.source, comments)
        print(f"{platform} adapter saved {len(comments)} comments/signals to {output_path}")
        return 0
    except Exception as exc:
        print(f"多源采集失败：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
