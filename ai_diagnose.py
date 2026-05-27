# -*- coding: utf-8 -*-
"""
TK 跨境电商评论 AI 诊断脚本

功能说明：
1. 读取本地 raw_comments.json 原始评论文件。
2. 使用 OpenAI 官方 SDK，通过 sub2api 中转服务调用 GPT 模型。
3. 对评论进行情感倾向、负面客诉标签和供应链改进建议分析。
4. 输出前端看板可直接消费的 diagnosed_products.json。

安装依赖：
    python -m pip install -U openai

环境变量：
    OPENAI_API_KEY      必填，sub2api 分发的 API Key
    OPENAI_BASE_URL     可选，默认 https://api.void52.site/v1
    OPENAI_MODEL_NAME   可选，默认 gpt-5.5

运行示例：
    $env:OPENAI_API_KEY="你的 sub2api Key"
    $env:OPENAI_BASE_URL="https://api.void52.site/v1"
    $env:OPENAI_MODEL_NAME="gpt-5.5"
    python .\ai_diagnose.py
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, RateLimitError


DEFAULT_INPUT = "raw_comments.json"
DEFAULT_OUTPUT = "diagnosed_products.json"
DEFAULT_PRODUCT_ID = "apparel"
DEFAULT_PRODUCT_NAME = "AI识别商品"
DEFAULT_OPENAI_BASE_URL = "https://api.void52.site/v1"
DEFAULT_MODEL = "gpt-5.5"
DEFAULT_OPENAI_API_STYLE = "auto"
DEFAULT_TIMEOUT_SECONDS = 120

# 最多 5 次指数退避重试：首次失败后依次等待 1s、2s、4s、8s、16s。
BACKOFF_DELAYS = [1, 2, 4, 8, 16]

# 当模型输出缺失、异常，或 API 完全不可用时使用的安全兜底标签。
FALLBACK_KEYWORD_LABELS = ["起球严重", "尺码偏小", "掉色", "线头多", "物流慢"]
FALLBACK_KEYWORD_COUNTS = [10, 8, 6, 4, 2]
FALLBACK_SENTIMENT = [15, 20, 65]


SYSTEM_PROMPT = """
你是“TK跨境电商高级产品体验官 (CPO)”，擅长从 TikTok 评论、TikTok Shop 商品评价和跨境电商用户反馈中识别产品体验问题、供应链缺陷和售后风险。

你的任务：
1. 合并分析用户输入的所有评论，不要逐条输出。
2. 提取情感分布比例：正面%、中性%、负面%，三者必须为整数且总和必须等于 100。
3. 提取负面评论中的 TOP 5 客诉标签，例如“尺码偏小”“面料起球”“拉链易坏”“掉色”“物流慢”等。
4. 为每个负面标签给出它在负面评论中的提及频次，必须是整数。
5. 给出一段精简且一针见血的“AI 诊断结论及供应链改进建议”，200 字以内。

输出要求：
- 只输出一个合法 JSON 对象，不要 Markdown，不要代码块，不要解释文字。
- JSON 字段必须严格如下：
{
  "product_id": "apparel",
  "product_name": "潮流运动鞋",
  "sentiment": [15, 20, 65],
  "labels": ["正面 (15%)", "中性 (20%)", "负面 (65%)"],
  "keywords": [10, 8, 6, 4, 2],
  "keywordLabels": ["起球严重", "尺码偏小", "掉色", "线头多", "物流慢"],
  "insight": "🤖 AI 诊断结论：目前该产品的主要问题集中在..."
}

字段规则：
- product_id 必须使用用户输入的值。
- product_name 可以基于评论内容自适应更新。如果用户输入的是“AI识别商品”“待识别商品”这类占位名，请根据评论中出现的 shoes、sneakers、boots、dress、bag 等产品信号，输出一个简洁准确的中文商品名。
- sentiment 顺序固定为：[正面百分比, 中性百分比, 负面百分比]。
- labels 必须和 sentiment 完全匹配，格式固定为：“正面 (x%)”“中性 (x%)”“负面 (x%)”。
- keywords 必须是 5 个整数，表示 TOP 5 负面标签的提及频次。
- keywordLabels 必须是 5 个字符串，且顺序与 keywords 一一对应。
- insight 必须以“🤖 AI 诊断结论：”开头，语气专业、直接、可执行。
"""


def read_comments(input_path: Path) -> List[Dict[str, Any]]:
    """
    读取 raw_comments.json，并只保留核心分析字段。

    支持两种输入结构：
    1. [{"username": "...", "comment_text": "..."}]
    2. {"comments": [{"username": "...", "comment_text": "..."}]}
    """
    if not input_path.exists():
        raise FileNotFoundError(f"未找到原始评论文件：{input_path.resolve()}")

    try:
        raw_data = json.loads(input_path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        raw_data = json.loads(input_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"评论文件不是合法 JSON：{input_path.resolve()}，错误：{exc}") from exc

    if isinstance(raw_data, list):
        raw_comments = raw_data
    elif isinstance(raw_data, dict) and isinstance(raw_data.get("comments"), list):
        raw_comments = raw_data["comments"]
    else:
        raise ValueError("raw_comments.json 结构不符合预期，应为评论数组或包含 comments 数组的对象。")

    comments: List[Dict[str, Any]] = []
    for item in raw_comments:
        if not isinstance(item, dict):
            continue

        comment_text = str(item.get("comment_text", "")).strip()
        if not comment_text:
            continue

        comments.append(
            {
                "username": str(item.get("username", "")).strip(),
                "comment_text": comment_text,
                "publish_time": str(item.get("publish_time", "")).strip(),
                "like_count": item.get("like_count", 0),
            }
        )

    if not comments:
        raise ValueError("raw_comments.json 中没有可分析的有效评论。")

    return comments


def build_user_prompt(product_id: str, product_name: str, comments: List[Dict[str, Any]]) -> str:
    """构造用户提示词，把所有评论压缩成一个整体诊断任务。"""
    payload = {
        "product_id": product_id,
        "product_name": product_name,
        "comment_count": len(comments),
        "comments": comments,
    }
    return (
        "请基于以下 TikTok/TikTok Shop 评论数据输出严格符合前端数据契约的 JSON。"
        "如果评论来自 YouTube 测试源，也请把它当作本地联调用的电商评论样本处理。\n"
        "注意：请合并分析全部评论，不要逐条复述。\n\n"
        f"{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )


def build_openai_client(api_key: str, base_url: str, timeout_seconds: int) -> OpenAI:
    """初始化 OpenAI 官方 SDK 客户端，base_url 指向 sub2api 中转端点。"""
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，请先设置 sub2api API Key。")

    # Cloudflare / Tunnel 有时会拦截 OpenAI Python SDK 默认 User-Agent。
    # 这里显式覆盖成朴素 UA，仍然使用官方 OpenAI SDK，不改变业务调用方式。
    return OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=timeout_seconds,
        default_headers={
            "User-Agent": "curl/8.5.0",
            "Accept": "application/json",
        },
    )


def extract_chat_response_text(response: Any) -> str:
    """从 OpenAI Chat Completions 响应中提取 JSON 字符串。"""
    choices = getattr(response, "choices", None) or []
    if not choices:
        raise ValueError("OpenAI 响应中没有 choices。")

    message = getattr(choices[0], "message", None)
    content = getattr(message, "content", None)
    if not content:
        raise ValueError("OpenAI 响应内容为空。")

    return str(content).strip()


def extract_responses_text(response: Any) -> str:
    """从 OpenAI Responses API 响应中提取文本。"""
    output_text = getattr(response, "output_text", None)
    if output_text:
        return str(output_text).strip()

    parts: List[str] = []
    for output_item in getattr(response, "output", []) or []:
        for content_item in getattr(output_item, "content", []) or []:
            text = getattr(content_item, "text", None)
            if text:
                parts.append(str(text))

    merged = "".join(parts).strip()
    if not merged:
        raise ValueError("OpenAI Responses API 响应内容为空。")

    return merged


def parse_json_response(text: str) -> Dict[str, Any]:
    """解析模型返回的 JSON 字符串，并温和清理偶发的代码块包裹。"""
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"模型未返回合法 JSON：{exc}；原始返回：{text[:800]}") from exc

    if not isinstance(data, dict):
        raise ValueError("模型返回的 JSON 顶层不是对象。")

    return data


def is_retryable_error(exc: Exception) -> bool:
    """判断是否属于适合指数退避重试的网络、超时、限流或服务端错误。"""
    if isinstance(exc, (APIConnectionError, APITimeoutError, RateLimitError, TimeoutError, ConnectionError)):
        return True

    if isinstance(exc, APIStatusError):
        status_code = exc.status_code
        return status_code in {408, 409, 425, 429} or status_code >= 500

    # JSON 解析失败也重试一次完整请求，因为上游网关偶尔可能返回非模型内容。
    if isinstance(exc, ValueError):
        return True

    return False


def resolve_api_styles(api_style: str) -> List[str]:
    """
    解析调用风格。

    auto 模式尽量贴近 Codex 新式调用路径：先尝试 Responses API，再尝试 Chat Completions。
    *_text 是兼容兜底：不发送 JSON 模式参数，只依赖提示词和本地 JSON 解析。
    """
    normalized = (api_style or DEFAULT_OPENAI_API_STYLE).strip().lower()
    mapping = {
        "auto": ["responses_json", "chat_json", "responses_text", "chat_text"],
        "responses": ["responses_json"],
        "responses_json": ["responses_json"],
        "responses_text": ["responses_text"],
        "chat": ["chat_json"],
        "chat_json": ["chat_json"],
        "chat_text": ["chat_text"],
    }
    if normalized not in mapping:
        raise ValueError(f"不支持的 OPENAI_API_STYLE：{api_style}")
    return mapping[normalized]


def call_openai_once(
    client: OpenAI,
    model_name: str,
    prompt: str,
    timeout_seconds: int,
    api_style: str,
) -> Dict[str, Any]:
    """按指定路径调用一次 OpenAI 兼容接口。"""
    if api_style == "responses_json":
        response = client.responses.create(
            model=model_name,
            instructions=SYSTEM_PROMPT,
            input=prompt,
            temperature=0.15,
            text={"format": {"type": "json_object"}},
            timeout=timeout_seconds,
        )
        return parse_json_response(extract_responses_text(response))

    if api_style == "responses_text":
        response = client.responses.create(
            model=model_name,
            instructions=SYSTEM_PROMPT,
            input=prompt,
            temperature=0.15,
            timeout=timeout_seconds,
        )
        return parse_json_response(extract_responses_text(response))

    if api_style == "chat_json":
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.15,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        return parse_json_response(extract_chat_response_text(response))

    if api_style == "chat_text":
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.15,
            timeout=timeout_seconds,
        )
        return parse_json_response(extract_chat_response_text(response))

    raise ValueError(f"未知 OpenAI 调用路径：{api_style}")


def call_openai_style_with_retry(
    client: OpenAI,
    model_name: str,
    prompt: str,
    timeout_seconds: int,
    api_style: str,
) -> Dict[str, Any]:
    """
    针对单一接口路径执行指数退避重试。

    chat_json 路径会发送 response_format={"type": "json_object"}。
    responses_json 路径会发送 text={"format": {"type": "json_object"}}。
    """
    last_error: Exception | None = None
    total_attempts = len(BACKOFF_DELAYS) + 1

    for attempt in range(1, total_attempts + 1):
        try:
            return call_openai_once(client, model_name, prompt, timeout_seconds, api_style)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            last_error = exc
            should_retry = attempt < total_attempts and is_retryable_error(exc)

            if not should_retry:
                break

            delay = BACKOFF_DELAYS[attempt - 1]
            print(
                f"第 {attempt}/{total_attempts} 次 {api_style} 调用失败，{delay}s 后重试。原因：{exc}",
                file=sys.stderr,
            )
            time.sleep(delay)

    raise RuntimeError(f"{api_style} 调用失败。最后错误：{last_error}")


def call_openai_with_retry(
    client: OpenAI,
    model_name: str,
    prompt: str,
    timeout_seconds: int,
    api_style: str = DEFAULT_OPENAI_API_STYLE,
) -> Dict[str, Any]:
    """按配置顺序尝试 Responses API 与 Chat Completions，并保留指数退避能力。"""
    last_error: Exception | None = None
    for style in resolve_api_styles(api_style):
        try:
            print(f"正在尝试 OpenAI 调用路径：{style}")
            return call_openai_style_with_retry(client, model_name, prompt, timeout_seconds, style)
        except Exception as exc:
            last_error = exc
            print(f"{style} 路径不可用，准备尝试下一个可选路径。原因：{exc}", file=sys.stderr)

    raise RuntimeError(f"OpenAI/sub2api 所有调用路径均失败，已触发安全降级。最后错误：{last_error}")


def number_from_any(value: Any, default: float = 0.0) -> float:
    """把 int、float、'65%'、'1.2k'、'3万' 等值尽量转成数字。"""
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().lower().replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return default

    number = float(match.group(0))
    if "k" in text:
        number *= 1000
    elif "w" in text or "万" in text:
        number *= 10000
    return number


def normalize_percentages(values: Any) -> List[int]:
    """把情感比例校准为 3 个整数，并确保总和绝对等于 100。"""
    if not isinstance(values, list):
        values = []

    numbers = [max(0.0, number_from_any(value)) for value in values[:3]]
    while len(numbers) < 3:
        numbers.append(0.0)

    total = sum(numbers)
    if total <= 0:
        return [0, 100, 0]

    scaled = [value / total * 100 for value in numbers]
    rounded = [int(round(value)) for value in scaled]
    diff = 100 - sum(rounded)

    # 把四舍五入产生的差值补到占比最大的类别上，保证图表数据稳定。
    max_index = max(range(3), key=lambda index: rounded[index])
    rounded[max_index] += diff

    return [max(0, value) for value in rounded]


def parse_keyword_frequency(value: Any) -> int | None:
    """把模型返回的频次解析成正整数；无法确认是数字时返回 None。"""
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip().lower().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None

        number = float(match.group(0))
        if "k" in text:
            number *= 1000
        elif "w" in text or "万" in text:
            number *= 10000

    if number <= 0:
        return None

    rounded = int(round(number))
    if rounded <= 0:
        return None
    return rounded


def normalize_top5_pairs(raw_keywords: Any, raw_labels: Any) -> tuple[List[int], List[str]]:
    """
    成对归一化负面标签与频次，避免 keywords 和 keywordLabels 语义错位。

    模型偶尔会漏掉某个频次或标签。如果分别补齐两个数组，看板虽然能渲染，
    但“尺码偏小”可能对应到“物流慢”的频次。这里按相同下标先组成 Pair，
    再统一过滤、去重、排序和补位，保证最终两个数组始终一一对应。
    """
    keywords = raw_keywords if isinstance(raw_keywords, list) else []
    labels = raw_labels if isinstance(raw_labels, list) else []
    pairs_by_label: Dict[str, tuple[str, int]] = {}

    for index, raw_label in enumerate(labels):
        label = str(raw_label).strip()
        if not label or index >= len(keywords):
            continue

        count = parse_keyword_frequency(keywords[index])
        if count is None:
            continue

        normalized_label = re.sub(r"\s+", " ", label).lower()
        existing = pairs_by_label.get(normalized_label)
        if existing is None or count > existing[1]:
            pairs_by_label[normalized_label] = (label, count)

    pairs = sorted(pairs_by_label.values(), key=lambda item: item[1], reverse=True)[:5]
    used_labels = {re.sub(r"\s+", " ", label).lower() for label, _ in pairs}

    for fallback_label, fallback_count in zip(FALLBACK_KEYWORD_LABELS, FALLBACK_KEYWORD_COUNTS):
        if len(pairs) >= 5:
            break

        normalized_label = re.sub(r"\s+", " ", fallback_label).lower()
        if normalized_label in used_labels:
            continue

        pairs.append((fallback_label, fallback_count))
        used_labels.add(normalized_label)

    return [count for _, count in pairs[:5]], [label for label, _ in pairs[:5]]


def trim_insight(text: Any, max_chars: int = 200) -> str:
    """控制诊断结论长度，并保证固定 Emoji 前缀。"""
    prefix = "🤖 AI 诊断结论："
    insight = str(text or "").strip()

    if not insight:
        insight = f"{prefix}当前评论样本信号不足，建议继续补充近 7-14 天评价后再判断核心客诉。"
    elif not insight.startswith(prefix):
        insight = prefix + insight

    if len(insight) > max_chars:
        insight = insight[: max_chars - 1].rstrip("，。；;、 ") + "…"

    return insight


def normalize_contract(
    raw_report: Dict[str, Any],
    product_id: str,
    product_name: str,
) -> Dict[str, Any]:
    """把模型输出强制整理成前端 Demo 所需的数据契约。"""
    sentiment = normalize_percentages(raw_report.get("sentiment"))
    keywords, keyword_labels = normalize_top5_pairs(
        raw_report.get("keywords"),
        raw_report.get("keywordLabels"),
    )
    resolved_product_name = str(raw_report.get("product_name", "")).strip() or product_name

    return {
        "product_id": product_id,
        "product_name": resolved_product_name,
        "sentiment": sentiment,
        "labels": [
            f"正面 ({sentiment[0]}%)",
            f"中性 ({sentiment[1]}%)",
            f"负面 ({sentiment[2]}%)",
        ],
        "keywords": keywords,
        "keywordLabels": keyword_labels,
        "insight": trim_insight(raw_report.get("insight")),
    }


def count_keyword_mentions(comments: List[Dict[str, Any]]) -> List[tuple[str, int]]:
    """API 不可用时，用本地规则生成一份较可信的负面标签频次。"""
    patterns = [
        ("起球严重", ["pilling", "pill", "lint ball", "lint", "起球"]),
        ("尺码偏小", ["too small", "tight", "xs", "size chart", "one size up", "尺码偏小", "偏小"]),
        ("材质单薄", ["thin", "transparent", "see through", "squat", "fabric", "单薄", "透"]),
        ("掉色", ["fade", "fading", "color runs", "discolor", "掉色", "褪色"]),
        ("线头多", ["thread", "stitch", "seam", "loose", "线头", "做工"]),
        ("物流慢", ["shipping", "delivery", "delay", "late", "物流", "延迟"]),
        ("包装破损", ["package", "packaging", "broken box", "damaged", "包装"]),
    ]

    combined_counts: List[tuple[str, int]] = []
    for label, terms in patterns:
        count = 0
        for comment in comments:
            text = str(comment.get("comment_text", "")).lower()
            if any(term.lower() in text for term in terms):
                count += 1
        combined_counts.append((label, count))

    combined_counts.sort(key=lambda item: item[1], reverse=True)
    top5 = combined_counts[:5]

    if all(count == 0 for _, count in top5):
        return list(zip(FALLBACK_KEYWORD_LABELS, FALLBACK_KEYWORD_COUNTS))

    padded = top5[:]
    for label, count in zip(FALLBACK_KEYWORD_LABELS, FALLBACK_KEYWORD_COUNTS):
        if len(padded) >= 5:
            break
        if label not in [item[0] for item in padded]:
            padded.append((label, count))

    return padded[:5]


def estimate_sentiment_from_comments(comments: List[Dict[str, Any]]) -> List[int]:
    """API 不可用时，用轻量关键词规则估算正、中、负情感比例。"""
    positive_terms = [
        "good",
        "great",
        "love",
        "perfect",
        "soft",
        "beautiful",
        "comfortable",
        "满意",
        "喜欢",
        "好穿",
    ]
    negative_terms = [
        "bad",
        "disappointed",
        "horrible",
        "too small",
        "tight",
        "pilling",
        "thin",
        "return",
        "差",
        "失望",
        "起球",
        "偏小",
    ]

    positive = 0
    neutral = 0
    negative = 0

    for comment in comments:
        text = str(comment.get("comment_text", "")).lower()
        positive_hits = sum(1 for term in positive_terms if term.lower() in text)
        negative_hits = sum(1 for term in negative_terms if term.lower() in text)

        if negative_hits > positive_hits:
            negative += 1
        elif positive_hits > negative_hits:
            positive += 1
        else:
            neutral += 1

    return normalize_percentages([positive, neutral, negative])


def build_fallback_report(
    product_id: str,
    product_name: str,
    comments: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """当 OpenAI/sub2api 不可用时，写入高保真本地降级模板。"""
    sentiment = estimate_sentiment_from_comments(comments) if comments else FALLBACK_SENTIMENT
    keyword_pairs = count_keyword_mentions(comments) if comments else list(zip(FALLBACK_KEYWORD_LABELS, FALLBACK_KEYWORD_COUNTS))
    keyword_labels = [label for label, _ in keyword_pairs]
    keywords = [count for _, count in keyword_pairs]

    main_issue = keyword_labels[0] if keyword_labels else "核心客诉"
    second_issue = keyword_labels[1] if len(keyword_labels) > 1 else "供应链稳定性"
    insight = (
        f"🤖 AI 诊断结论：当前样本显示主要风险集中在“{main_issue}”和“{second_issue}”。"
        "建议优先复核对应供应商批次、尺码/材质标准和详情页承诺，并小批量验证后再恢复放量。"
    )

    return normalize_contract(
        {
            "sentiment": sentiment,
            "keywords": keywords,
            "keywordLabels": keyword_labels,
            "insight": insight,
        },
        product_id,
        product_name,
    )


def write_report(output_path: Path, report: Dict[str, Any]) -> None:
    """写出 diagnosed_products.json，显式使用 UTF-8。"""
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def diagnose_comments(
    comments: List[Dict[str, Any]],
    product_id: str,
    product_name: str,
    api_key: str,
    base_url: str,
    model_name: str,
    timeout_seconds: int,
    api_style: str = DEFAULT_OPENAI_API_STYLE,
) -> Dict[str, Any]:
    """高层封装：输入评论，输出清洗后的前端契约 JSON。"""
    prompt = build_user_prompt(product_id, product_name, comments)
    client = build_openai_client(api_key, base_url, timeout_seconds)
    raw_report = call_openai_with_retry(client, model_name, prompt, timeout_seconds, api_style)
    return normalize_contract(raw_report, product_id, product_name)


def parse_args() -> argparse.Namespace:
    """解析命令行参数；默认值优先来自环境变量。"""
    parser = argparse.ArgumentParser(description="使用 OpenAI SDK + sub2api 诊断 TikTok 评论舆情。")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="原始评论 JSON 文件路径。")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="诊断报告 JSON 输出路径。")
    parser.add_argument("--product-id", default=DEFAULT_PRODUCT_ID, help="前端产品 ID。")
    parser.add_argument("--product-name", default=DEFAULT_PRODUCT_NAME, help="前端产品名称。")
    parser.add_argument(
        "--base-url",
        default=os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL),
        help="OpenAI 兼容接口 base_url，默认读取 OPENAI_BASE_URL。",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL),
        help="模型名称，默认读取 OPENAI_MODEL_NAME。",
    )
    parser.add_argument(
        "--api-style",
        default=os.getenv("OPENAI_API_STYLE", DEFAULT_OPENAI_API_STYLE),
        choices=["auto", "responses", "responses_json", "responses_text", "chat", "chat_json", "chat_text"],
        help="OpenAI 调用路径。auto 会优先尝试 Responses API，再尝试 Chat Completions。",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="单次 API 请求超时时间，单位秒。")
    parser.add_argument(
        "--disable-fallback",
        action="store_true",
        help="禁用 API 失败后的本地 mock 降级写入，主要用于调试。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        input_path = Path(args.input)
        output_path = Path(args.output)
        comments = read_comments(input_path)
        api_key = os.getenv("OPENAI_API_KEY", "").strip()

        print(f"已读取 {len(comments)} 条评论，准备调用模型：{args.model}，调用路径：{args.api_style}")

        try:
            report = diagnose_comments(
                comments=comments,
                product_id=args.product_id,
                product_name=args.product_name,
                api_key=api_key,
                base_url=args.base_url,
                model_name=args.model,
                timeout_seconds=args.timeout,
                api_style=args.api_style,
            )
        except Exception as exc:
            if args.disable_fallback:
                raise

            print(
                f"OpenAI/sub2api 暂不可用，已启用本地高保真 mock 降级。原因：{exc}",
                file=sys.stderr,
            )
            report = build_fallback_report(args.product_id, args.product_name, comments)

        write_report(output_path, report)
        print(f"诊断完成，已保存：{output_path.resolve()}")
        return 0

    except Exception as exc:
        print(f"处理失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
