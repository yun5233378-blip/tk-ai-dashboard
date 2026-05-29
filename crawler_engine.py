#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Crawler strategy, friction classification, and artifact capture.

Inspired by Lyx3314844-03/superspider (MIT License, Copyright (c) 2026).
This module implements a small TK-AI specific subset: platform presets,
compliant access-friction reports, crawler selection, and failure artifacts.
It does not attempt CAPTCHA bypass or unauthorized access.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
ARTIFACT_ROOT = BASE_DIR / "artifacts" / "crawler_runs"

SHARE_URL_PATTERN = re.compile(r"https?://[A-Za-z0-9\-._~:/?#@!$&()*+,;=%]+", re.I)
TRAILING_URL_PUNCTUATION = "，。！？；：、,.!?;:)）】]》>\"'"


def extract_first_url(value: str) -> str:
    """Extract the first real URL from a copied platform share sentence."""
    text = str(value or "").strip()
    match = SHARE_URL_PATTERN.search(text)
    if not match:
        return text
    return match.group(0).strip().rstrip(TRAILING_URL_PUNCTUATION)


@dataclass(frozen=True)
class BrowserPreset:
    platform: str
    label: str
    site_family: str
    crawler_type: str
    allowed_domains: List[str]
    runner_order: List[str] = field(default_factory=lambda: ["browser", "authorized-session-replay", "public-meta"])
    viewport_width: int = 1440
    viewport_height: int = 1400
    wait_rounds: int = 2
    scroll_rounds: int = 5
    capture: List[str] = field(default_factory=lambda: ["html", "screenshot", "network", "friction-report"])
    selectors: List[str] = field(default_factory=list)
    stop_conditions: List[str] = field(default_factory=lambda: [
        "explicit-access-denied",
        "captcha-or-risk-control",
        "no-new-comment-candidates-after-scroll",
    ])

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccessFrictionReport:
    level: str
    signals: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    retry_after_seconds: Optional[int] = None
    should_upgrade_to_browser: bool = False
    requires_human_access: bool = False
    challenge_handoff: Dict[str, Any] = field(default_factory=dict)
    capability_plan: Dict[str, Any] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        return self.level in {"medium", "high"}

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["blocked"] = self.blocked
        return payload


PRESETS: Dict[str, BrowserPreset] = {
    "douyin": BrowserPreset(
        platform="douyin",
        label="抖音",
        site_family="douyin-shop",
        crawler_type="hydrated_spa_login_session",
        allowed_domains=["douyin.com", "v.douyin.com", "iesdouyin.com", "jinritemai.com"],
        selectors=[
            '[data-e2e*="comment"]',
            '[class*="comment"]',
            '[class*="Comment"]',
            '[class*="reply"]',
            'div[role="listitem"]',
            "span",
            "p",
        ],
    ),
    "xiaohongshu": BrowserPreset(
        platform="xiaohongshu",
        label="小红书",
        site_family="xiaohongshu",
        crawler_type="hydrated_spa_login_session",
        allowed_domains=["xiaohongshu.com", "xhslink.com"],
        selectors=[
            '[class*="comment"]',
            '[class*="Comment"]',
            '[class*="note-content"]',
            '[class*="reply"]',
            'div[role="listitem"]',
            "span",
            "p",
        ],
    ),
    "public_web": BrowserPreset(
        platform="public_web",
        label="公开网页",
        site_family="generic",
        crawler_type="public_meta_or_hydrated_page",
        allowed_domains=[],
        runner_order=["http", "browser"],
        selectors=["article", "main", "p", "span"],
        scroll_rounds=2,
    ),
}


def detect_platform_family(url: str) -> str:
    host = (urlparse(extract_first_url(url)).hostname or "").lower()
    if any(domain in host for domain in PRESETS["douyin"].allowed_domains):
        return "douyin"
    if any(domain in host for domain in PRESETS["xiaohongshu"].allowed_domains):
        return "xiaohongshu"
    return "public_web"


def get_crawler_preset(platform_or_url: str) -> BrowserPreset:
    key = (platform_or_url or "").strip().lower()
    if key not in PRESETS:
        key = detect_platform_family(platform_or_url)
    return PRESETS.get(key, PRESETS["public_web"])


def select_crawler_strategy(url: str, has_authorized_session: bool = False) -> Dict[str, Any]:
    preset = get_crawler_preset(url)
    runner_order = list(preset.runner_order)
    if has_authorized_session and "authorized-session-replay" in runner_order:
        runner_order.remove("authorized-session-replay")
        runner_order.insert(0, "authorized-session-replay")
    return {
        "scenario": "authenticated_session" if has_authorized_session else "public_probe_then_session",
        "crawler_type": preset.crawler_type,
        "recommended_runner": runner_order[0],
        "runner_order": runner_order,
        "site_family": preset.site_family,
        "risk_level": "high" if preset.platform in {"douyin", "xiaohongshu"} else "medium",
        "capabilities": ["browser_rendering", "scroll_automation", "session_cookies", "artifact_capture"],
        "strategy_hints": [
            "start-with-browser-rendering-for-hydrated-spa",
            "reuse-only-authorized-session",
            "capture-html-screenshot-network-on-failure",
        ],
        "job_template": preset.to_dict(),
        "fallback_plan": [
            "try visible DOM comment candidates",
            "inspect public meta/bootstrap text",
            "if auth or challenge appears, pause for local login helper",
        ],
        "stop_conditions": list(preset.stop_conditions),
        "confidence": 0.82 if preset.platform in {"douyin", "xiaohongshu"} else 0.68,
        "source": "superspider-inspired-tk-crawler-engine-v1",
    }


def analyze_access_friction(
    html: str = "",
    status_code: int = 200,
    headers: Optional[Mapping[str, str]] = None,
    url: str = "",
) -> AccessFrictionReport:
    normalized_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    haystack = "\n".join([url, html, "\n".join(f"{k}: {v}" for k, v in normalized_headers.items())]).lower()
    signals: List[str] = []

    if status_code in {401, 403}:
        signals.append("auth-or-forbidden")
    if status_code == 429:
        signals.append("rate-limited")
    if status_code in {503, 520, 521, 522}:
        signals.append("temporary-gateway-or-challenge")

    keyword_groups: Dict[str, Iterable[str]] = {
        "captcha": ("captcha", "recaptcha", "hcaptcha", "turnstile", "验证码", "验证一下"),
        "slider-captcha": ("geetest", "aliyuncaptcha", "tencentcaptcha", "滑块", "拖动滑块"),
        "auth-required": ("login", "sign in", "登录", "扫码登录", "手机号登录", "请先登录"),
        "risk-control": ("risk control", "环境异常", "访问异常", "操作频繁", "账号存在风险", "安全验证"),
        "request-blocked": ("access denied", "request blocked", "拒绝访问", "禁止访问", "blocked"),
        "managed-browser-challenge": ("checking your browser", "please enable javascript", "cf-chl"),
        "waf-vendor": ("cloudflare", "akamai", "datadome", "perimeterx", "bytedance", "dun.163"),
        "js-signature": ("x-bogus", "a_bogus", "mstoken", "_signature", "__webpack_require__", "webpackchunk"),
        "fingerprint-required": ("navigator.webdriver", "canvas fingerprint", "webgl", "deviceid", "sec-ch-ua"),
    }
    for signal, patterns in keyword_groups.items():
        if any(pattern.lower() in haystack for pattern in patterns):
            signals.append(signal)

    html_lower = (html or "").lower()
    if status_code == 200 and html and len(html.strip()) < 800 and (
        "<script" in html_lower or "enable javascript" in html_lower or "window.location" in html_lower
    ):
        signals.append("empty-or-script-shell")
    if "retry-after" in normalized_headers:
        signals.append("retry-after")
    if any(header in normalized_headers for header in ("cf-ray", "x-datadome", "x-akamai-transformed")):
        signals.append("waf-vendor")

    signals = _dedupe(signals)
    level = _friction_level(status_code, signals)
    retry_after = _parse_retry_after(normalized_headers.get("retry-after"))
    actions = _recommended_actions(signals, retry_after)
    requires_human = any(signal in signals for signal in ("captcha", "slider-captcha", "auth-required", "risk-control"))

    return AccessFrictionReport(
        level=level,
        signals=signals,
        recommended_actions=actions,
        retry_after_seconds=retry_after,
        should_upgrade_to_browser=any(signal in signals for signal in (
            "managed-browser-challenge",
            "waf-vendor",
            "js-signature",
            "fingerprint-required",
            "empty-or-script-shell",
            "auth-required",
        )),
        requires_human_access=requires_human,
        challenge_handoff={
            "required": requires_human,
            "method": "local-browser-login-helper" if requires_human else "none",
            "resume": "after-authorized-session-is-uploaded" if requires_human else "automatic",
        },
        capability_plan={
            "mode": "maximum-compliant",
            "transport_order": ["browser-render", "authorized-session-replay", "public-meta"],
            "throttle": {
                "concurrency": 1 if level in {"medium", "high"} else 2,
                "crawl_delay_seconds": retry_after or (30 if level == "high" else 5 if level == "medium" else 1),
                "honor_retry_after": True,
            },
            "artifacts": ["html", "screenshot", "network-summary", "friction-report"],
            "retry_budget": 0 if "request-blocked" in signals else (1 if level == "high" else 2),
            "stop_conditions": ["explicit-access-denied", "captcha-or-risk-control", "missing-authorized-session"],
        },
    )


def save_crawl_artifacts(
    platform: str,
    url: str,
    html: str = "",
    screenshot: bytes | None = None,
    network: List[Dict[str, Any]] | None = None,
    friction_report: AccessFrictionReport | Dict[str, Any] | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    safe_platform = re.sub(r"[^a-zA-Z0-9_-]+", "_", platform or "unknown")[:40]
    run_id = f"{timestamp}-{safe_platform}-{digest}"
    run_dir = ARTIFACT_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    saved: Dict[str, str] = {}
    if html:
        (run_dir / "page.html").write_text(html, encoding="utf-8", errors="replace")
        saved["html"] = str(run_dir / "page.html")
    if screenshot:
        (run_dir / "screenshot.png").write_bytes(screenshot)
        saved["screenshot"] = str(run_dir / "screenshot.png")
    if network is not None:
        (run_dir / "network_summary.json").write_text(
            json.dumps(network[-120:], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        saved["network"] = str(run_dir / "network_summary.json")
    if friction_report is not None:
        friction_payload = friction_report.to_dict() if hasattr(friction_report, "to_dict") else friction_report
        (run_dir / "friction_report.json").write_text(
            json.dumps(friction_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        saved["friction_report"] = str(run_dir / "friction_report.json")

    manifest = {
        "schema": "tk_crawler_artifact_v1",
        "run_id": run_id,
        "platform": platform,
        "url_hash": digest,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "files": saved,
        "metadata": metadata or {},
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "run_id": run_id,
        "directory": str(run_dir),
        "files": saved,
        "manifest": str(run_dir / "manifest.json"),
    }


def _friction_level(status_code: int, signals: List[str]) -> str:
    if any(signal in signals for signal in ("captcha", "slider-captcha", "auth-required", "risk-control", "request-blocked")):
        return "high"
    if status_code in {401, 403, 429}:
        return "high"
    if any(signal in signals for signal in ("managed-browser-challenge", "waf-vendor", "js-signature", "fingerprint-required", "empty-or-script-shell")):
        return "medium"
    if signals:
        return "low"
    return "none"


def _recommended_actions(signals: List[str], retry_after: Optional[int]) -> List[str]:
    actions: List[str] = []
    if retry_after is not None or "rate-limited" in signals:
        actions.extend(["honor-retry-after", "reduce-concurrency", "increase-crawl-delay"])
    if any(signal in signals for signal in ("managed-browser-challenge", "waf-vendor", "empty-or-script-shell")):
        actions.extend(["render-with-browser", "persist-session-state", "capture-html-screenshot-network"])
    if any(signal in signals for signal in ("js-signature", "fingerprint-required")):
        actions.extend(["capture-devtools-network", "replay-authorized-session-only"])
    if any(signal in signals for signal in ("captcha", "slider-captcha", "auth-required", "risk-control")):
        actions.extend(["pause-for-human-access", "use-local-login-helper"])
    if "request-blocked" in signals:
        actions.append("stop-or-seek-site-permission")
    actions.append("respect-platform-rules")
    return _dedupe(actions)


def _dedupe(items: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _parse_retry_after(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    value = str(value).strip()
    if value.isdigit():
        return max(0, int(value))
    return None
