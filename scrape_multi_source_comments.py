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
import os
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

PLATFORM_COOKIE_DOMAINS = {
    "douyin": [".douyin.com", ".iesdouyin.com"],
    "xiaohongshu": [".xiaohongshu.com", ".xhslink.com"],
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


def load_platform_session_env(platform: str) -> Dict[str, Any]:
    raw_value = os.getenv("TK_PLATFORM_SESSION_JSON", "").strip()
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    if str(payload.get("platform") or "").strip().lower() != platform:
        return {}
    return payload


def platform_cookie_domains(platform: str, source: str) -> List[str]:
    domains = list(PLATFORM_COOKIE_DOMAINS.get(platform, []))
    host = urlparse(source).hostname or ""
    if host and host not in domains:
        domains.append(host)
    return list(dict.fromkeys(domains))


def normalize_cookie_entry(item: Dict[str, Any], source: str, platform: str) -> Dict[str, Any] | None:
    name = str(item.get("name") or "").strip()
    value = str(item.get("value") or "")
    if not name:
        return None
    cookie: Dict[str, Any] = {
        "name": name,
        "value": value,
        "path": str(item.get("path") or "/"),
    }
    domain = str(item.get("domain") or "").strip()
    url = str(item.get("url") or "").strip()
    if domain:
        cookie["domain"] = domain
    elif url:
        cookie["url"] = url
    else:
        cookie["url"] = source

    for key in ("httpOnly", "secure"):
        if key in item:
            cookie[key] = bool(item.get(key))
    if "expires" in item:
        try:
            cookie["expires"] = int(float(item["expires"]))
        except Exception:
            pass
    same_site = str(item.get("sameSite") or "").strip().capitalize()
    if same_site in {"Strict", "Lax", "None"}:
        cookie["sameSite"] = same_site
    return cookie


def parse_cookie_header_text(raw_cookie: str, source: str, platform: str) -> List[Dict[str, Any]]:
    value = re.sub(r"^cookie\s*:\s*", "", raw_cookie.strip(), flags=re.I)
    parts = [part.strip() for part in value.split(";") if "=" in part]
    domains = platform_cookie_domains(platform, source)
    cookies: List[Dict[str, Any]] = []
    for part in parts:
        name, cookie_value = part.split("=", 1)
        name = name.strip()
        if not name or name.lower() in {"path", "domain", "expires", "max-age", "samesite", "secure", "httponly"}:
            continue
        for domain in domains:
            cookies.append({
                "name": name,
                "value": cookie_value.strip(),
                "domain": domain,
                "path": "/",
            })
    return cookies


def parse_platform_cookies(raw_cookie: str, source: str, platform: str) -> List[Dict[str, Any]]:
    value = (raw_cookie or "").strip()
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict) and isinstance(parsed.get("cookies"), list):
            parsed = parsed["cookies"]
        elif isinstance(parsed, dict) and isinstance(parsed.get("cookie"), str):
            return parse_cookie_header_text(parsed["cookie"], source, platform)
        elif isinstance(parsed, dict) and isinstance(parsed.get("cookies"), str):
            return parse_cookie_header_text(parsed["cookies"], source, platform)
        if isinstance(parsed, list):
            cookies = [
                normalize_cookie_entry(item, source, platform)
                for item in parsed
                if isinstance(item, dict)
            ]
            return [cookie for cookie in cookies if cookie]
    except json.JSONDecodeError:
        pass
    return parse_cookie_header_text(value, source, platform)


def add_platform_session_cookies(context: Any, source: str, platform: str, session: Dict[str, Any]) -> int:
    cookies = parse_platform_cookies(str(session.get("cookies") or ""), source, platform)
    if not cookies:
        return 0
    context.add_cookies(cookies)
    return len(cookies)


def is_probable_comment_text(text: str) -> bool:
    value = normalize_comment_text(text)
    if len(value) < MIN_COMMENT_LENGTH or len(value) > 220:
        return False
    lowered = value.lower()
    blocked_terms = [
        "login",
        "sign in",
        "download",
        "copyright",
        "privacy",
        "cookie",
        "javascript",
        "app store",
        "google play",
        "douyin",
        "xiaohongshu",
        "打开app",
        "登录",
        "注册",
        "下载",
        "隐私",
        "用户协议",
    ]
    if any(term in lowered for term in blocked_terms):
        return False
    if re.search(r"https?://|www\.", lowered):
        return False
    return True


def collect_visible_text_candidates(page: Any, limit: int) -> List[str]:
    selectors = [
        '[data-e2e*="comment"]',
        '[class*="comment"]',
        '[class*="Comment"]',
        '[class*="note-content"]',
        '[class*="content"]',
        '[class*="reply"]',
        '[class*="Reply"]',
        'div[role="listitem"]',
        'span',
        'p',
    ]
    candidates: List[str] = []
    for selector in selectors:
        try:
            elements = page.query_selector_all(selector)
        except Exception:
            continue
        for element in elements[:240]:
            if len(candidates) >= limit * 4:
                break
            try:
                raw_text = element.inner_text(timeout=1000)
            except Exception:
                continue
            for line in str(raw_text or "").splitlines():
                value = normalize_comment_text(line)
                if is_probable_comment_text(value):
                    candidates.append(value)
        if len(candidates) >= limit:
            break
    return candidates


def build_browser_visible_comments(source: str, platform: str, limit: int) -> List[Dict[str, Any]]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"{platform} browser comment probe unavailable: {exc}")
        return []

    session = load_platform_session_env(platform)
    user_agent = str(session.get("user_agent") or USER_AGENT).strip() or USER_AGENT
    comments: List[Dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1365, "height": 900},
            locale="zh-CN",
            user_agent=user_agent,
        )
        cookie_count = 0
        if session:
            cookie_count = add_platform_session_cookies(context, source, platform, session)
            print(f"{platform} session injected {cookie_count} browser cookies.")
        page = context.new_page()
        page.set_default_timeout(12000)
        try:
            page.goto(source, wait_until="domcontentloaded", timeout=30000)
            for _ in range(5):
                page.mouse.wheel(0, 900)
                time.sleep(1.2)
            candidates = collect_visible_text_candidates(page, limit)
            comments = [
                make_comment(value, platform, idx + 1, source)
                for idx, value in enumerate(candidates)
            ]
        finally:
            browser.close()
    comments = dedupe_comments(comments, limit)
    for comment in comments:
        comment["collection_method"] = "browser_visible_comment_probe_with_session" if session else "browser_visible_comment_probe"
        if cookie_count:
            comment["session_cookie_count"] = cookie_count
    return comments


def platform_guidance(platform: str, has_session: bool = False) -> str:
    if platform == "douyin":
        if has_session:
            return "抖音链接已注入保存的浏览器登录态，但仍未读到可分析评论；请检查 Cookie 是否过期、账号是否能打开该视频评论区，或平台是否触发验证。"
        return "抖音链接已尝试浏览器直抓，但公开页未暴露可读评论；请先在后台保存抖音采集登录态/Cookie 后重试。"
    if platform == "xiaohongshu":
        if has_session:
            return "小红书链接已注入保存的浏览器登录态，但仍未读到可分析评论；请检查 Cookie 是否过期、账号是否能打开该笔记评论区，或平台是否触发验证。"
        return "小红书链接已尝试浏览器直抓，但公开页未暴露可读评论；请先在后台保存小红书采集登录态/Cookie 后重试。"
    return "该平台暂未接入专用公开评论 API，请使用 comments:// 或文本文件导入评论。"


def collect_comments(source: str, limit: int) -> tuple[str, List[Dict[str, Any]]]:
    platform = detect_platform(source)
    if platform == "manual":
        comments = read_manual_comments(source, platform, limit)
        return platform, comments

    session = load_platform_session_env(platform) if platform in {"douyin", "xiaohongshu"} else {}
    if platform in {"douyin", "xiaohongshu"}:
        try:
            comments = build_browser_visible_comments(source, platform, limit)
            if comments:
                print(f"{platform} browser probe extracted {len(comments)} visible comments.")
                return platform, comments
        except Exception as exc:
            print(f"{platform} browser comment probe unavailable: {exc}")

    try:
        comments = build_public_page_comments(source, platform, limit)
        if comments and (platform not in {"douyin", "xiaohongshu"} or len(comments) >= 2):
            print(f"{platform} public page extracted {len(comments)} text signals.")
            return platform, comments
    except Exception as exc:
        print(f"{platform} public page extraction unavailable: {exc}")

    raise RuntimeError(platform_guidance(platform, bool(session)))


def write_comments(path: Path, platform: str, source: str, comments: List[Dict[str, Any]]) -> None:
    collection_method = str(comments[0].get("collection_method") or "public_page_or_manual_import") if comments else "public_page_or_manual_import"
    payload = {
        "schema": "tk_multi_source_comments_v1",
        "source_platform": platform,
        "source_url": source if source.startswith("http") else "manual_import",
        "collection_method": collection_method,
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
            raise RuntimeError(platform_guidance(platform, bool(load_platform_session_env(platform))))
        output_path = Path(args.output)
        write_comments(output_path, platform, args.source, comments)
        print(f"{platform} adapter saved {len(comments)} comments/signals to {output_path}")
        return 0
    except Exception as exc:
        print(f"多源采集失败：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
