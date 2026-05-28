# -*- coding: utf-8 -*-
"""
TK 跨境电商 AI 看板本地后端中枢

本文件是 FastAPI 微服务入口，负责把前端看板、评论爬虫和 GPT 诊断脚本串成
一条稳定的本地流水线。

核心升级：
1. POST /api/run-pipeline 收到 URL 后，先识别来源，再分流到不同爬虫脚本。
2. YouTube 链接只会进入 scrape_youtube_comments.py。
3. TikTok 链接只会进入 scrape_tiktok_comments.py。
4. 无法识别的链接默认降级走 YouTube 通道，并在控制台打印警告。
5. 外部脚本通过异步子进程执行，实时穿透 stdout/stderr 到前端终端日志。

启动方式：
    python .\\server.py
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
import copy
import hashlib
import hmac
import json
import math
import os
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# =========================
# 基础路径与运行参数
# =========================

BASE_DIR = Path(__file__).resolve().parent


def load_local_env_file() -> None:
    """Load simple KEY=VALUE pairs from .env before reading runtime settings."""
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        lines = env_path.read_text(encoding="utf-8-sig").splitlines()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("\"").strip("'")
        os.environ[key] = value


load_local_env_file()
RAW_COMMENTS_PATH = BASE_DIR / "raw_comments.json"
DIAGNOSED_PRODUCTS_PATH = BASE_DIR / "diagnosed_products.json"
TEMP_DIAGNOSED_PRODUCT_PATH = BASE_DIR / "_diagnosed_product_tmp.json"
COMPETITOR_VS_REPORTS_PATH = BASE_DIR / "competitor_vs_reports.json"
ADMIN_AUDIT_LOGS_PATH = BASE_DIR / "admin_audit_logs.json"
ALERT_DEDUP_PATH = BASE_DIR / "alert_dedup.json"
RADAR_HISTORY_PATH = BASE_DIR / "radar_history.json"

REDIS_URL = os.getenv("REDIS_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
PRODUCTS_STORE_KEY = os.getenv("PRODUCTS_STORE_KEY", "tk_ai:diagnosed_products")
VS_REPORTS_STORE_KEY = os.getenv("VS_REPORTS_STORE_KEY", "tk_ai:competitor_vs_reports")
ADMIN_AUDIT_STORE_KEY = os.getenv("ADMIN_AUDIT_STORE_KEY", "tk_ai:admin_audit_logs")
ALERT_DEDUP_STORE_KEY = os.getenv("ALERT_DEDUP_STORE_KEY", "tk_ai:alert_dedup")
RADAR_HISTORY_STORE_KEY = os.getenv("RADAR_HISTORY_STORE_KEY", "tk_ai:radar_history")
CORS_EXTRA_ORIGINS = [
    item.strip()
    for item in os.getenv("CORS_EXTRA_ORIGINS", "").split(",")
    if item.strip()
]
ALLOW_LOCAL_CORS = os.getenv("ALLOW_LOCAL_CORS", "0").strip() == "1"
ENABLE_DEMO_PRODUCTS = os.getenv("ENABLE_DEMO_PRODUCTS", "0").strip() == "1"
OPERATOR_TOKEN = os.getenv("OPERATOR_TOKEN", secrets.token_urlsafe(48)).strip()
OPERATOR_USERNAME = os.getenv("OPERATOR_USERNAME", "admin").strip() or "admin"
OPERATOR_PASSWORD = os.getenv("OPERATOR_PASSWORD", "你的登录密码").strip()
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_WEBHOOK_TOKEN = os.getenv("ALERT_WEBHOOK_TOKEN", "").strip()
ALERT_WEBHOOK_TIMEOUT_SECONDS = int(os.getenv("ALERT_WEBHOOK_TIMEOUT_SECONDS", "8"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "3600"))
SECURITY_RATE_LIMIT_ENABLED = os.getenv("SECURITY_RATE_LIMIT_ENABLED", "1").strip() == "1"
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "600"))
RATE_LIMIT_MUTATION_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MUTATION_MAX_REQUESTS", "30"))
_STORAGE_REDIS_CLIENT: Any | None = None

SCRAPE_TIKTOK_SCRIPT = BASE_DIR / "scrape_tiktok_comments.py"
SCRAPE_YOUTUBE_SCRIPT = BASE_DIR / "scrape_youtube_comments.py"
AI_DIAGNOSE_SCRIPT = BASE_DIR / "ai_diagnose.py"

DEFAULT_LIMIT = 100
CRAWLER_TIMEOUT_SECONDS = 180
DIAGNOSE_TIMEOUT_SECONDS = 180
RADAR_PATROL_INTERVAL_SECONDS = int(os.getenv("RADAR_PATROL_INTERVAL_SECONDS", "43200"))
RADAR_PATROL_STARTUP_DELAY_SECONDS = int(os.getenv("RADAR_PATROL_STARTUP_DELAY_SECONDS", "60"))
RADAR_NEGATIVE_SPIKE_THRESHOLD = 15
RADAR_SCORE_CRITICAL_THRESHOLD = 60
RADAR_HISTORY_MAX_POINTS = int(os.getenv("RADAR_HISTORY_MAX_POINTS", "96"))
RADAR_EWMA_ALPHA = float(os.getenv("RADAR_EWMA_ALPHA", "0.35"))
RADAR_Z_THRESHOLD = float(os.getenv("RADAR_Z_THRESHOLD", "2.0"))
RADAR_MIN_HISTORY_POINTS = int(os.getenv("RADAR_MIN_HISTORY_POINTS", "3"))
RADAR_MIN_SAMPLE_SIZE = int(os.getenv("RADAR_MIN_SAMPLE_SIZE", "20"))
RADAR_EWMA_DRIFT_MIN_DELTA = int(os.getenv("RADAR_EWMA_DRIFT_MIN_DELTA", "8"))
RADAR_MODEL_VERSION = "ewma_baseline_v1"

DEFAULT_PRODUCT_KEY = "apparel"
DEFAULT_PRODUCT_ID = "apparel"
DEFAULT_PRODUCT_NAME = "爆款瑜伽裤"
AUTO_PRODUCT_NAME = "AI识别商品"

# 用异步锁保护本地 raw_comments.json / diagnosed_products.json，避免多个请求并发覆盖。
PIPELINE_LOCK = asyncio.Lock()
RADAR_LOCK = asyncio.Lock()
RADAR_TASK: asyncio.Task[Any] | None = None
RADAR_LAST_RUN_AT = ""

# 前端终端日志使用内存队列即可满足本地演示和联调需求。
# 每条日志在写入时统一补上 [HH:MM:SS] 时间戳，前端只需要按行展示。
LOG_QUEUE: List[str] = []
LOG_LOCK = threading.Lock()
MAX_LOG_LINES = 500
RATE_LIMIT_BUCKETS: Dict[str, deque[float]] = defaultdict(deque)
RATE_LIMIT_LOCK = threading.Lock()


# =========================
# 前端默认展示数据
# =========================

DEFAULT_PRODUCTS: Dict[str, Dict[str, Any]] = {
    "apparel": {
        "product_id": "apparel",
        "product_name": "爆款瑜伽裤",
        "score": 62,
        "sentiment": [15, 20, 65],
        "labels": ["正面 (15%)", "中性 (20%)", "负面 (65%)"],
        "keywords": [85, 72, 45, 30, 25],
        "keywordLabels": ["起球严重", "尺码偏小", "颜色不符", "材质单薄", "物流延迟"],
        "insight": (
            "🤖 AI 诊断结论：爆款瑜伽裤的核心症结在于“面料起球”和“美区尺码适配不准”。"
            "建议将尺码规格全面微调，并由供应链升级采用免磨抗静电复合织物，提升高端质感。"
        ),
        "direction": "改善面料抗起球，调整美区尺码",
        "action": "暂停投放并进行升级调款",
        "radar_status": "normal",
        "source_url": "https://www.youtube.com/watch?v=v0K8E8K-W5s",
        "url": "https://www.youtube.com/watch?v=v0K8E8K-W5s",
    },
    "electronics": {
        "product_id": "electronics",
        "product_name": "智能迷你投影仪",
        "score": 78,
        "sentiment": [40, 15, 45],
        "labels": ["正面 (40%)", "中性 (15%)", "负面 (45%)"],
        "keywords": [92, 65, 40, 20, 15],
        "keywordLabels": ["蓝牙连接失败", "说明书缺页", "开机散热快", "扬声器破音", "外包装受损"],
        "insight": (
            "🤖 AI 诊断结论：由于主板蓝牙固件批次兼容性差导致退货率飙升。"
            "建议技术供应链升级固件驱动，同时针对美国本土消费者，制作全英文的新手上路指引短视频放在首图位置。"
        ),
        "direction": "更新主板蓝牙芯片固件，优化英文辅导说明",
        "action": "库存返工升级中",
        "radar_status": "normal",
        "source_url": "https://www.youtube.com/watch?v=coD5vKzH_O4",
        "url": "https://www.youtube.com/watch?v=coD5vKzH_O4",
    },
    "home": {
        "product_id": "home",
        "product_name": "车载香薰加湿器",
        "score": 95,
        "sentiment": [82, 10, 8],
        "labels": ["正面 (82%)", "中性 (10%)", "负面 (8%)"],
        "keywords": [12, 8, 5, 3, 2],
        "keywordLabels": ["香味易蒸发", "容量显小", "外观有划痕", "线缆偏短", "雾量不稳定"],
        "insight": (
            "🤖 AI 诊断结论：该加湿器在设计和质量控制上已经达到美区顶级水准，几乎零核心质量纠纷。"
            "建议扩大推广预算，同时为提升高客单价，配合开发新香氛 SKU，形成精装礼品套盒。"
        ),
        "direction": "工艺高度合格，扩大香氛香型矩阵",
        "action": "扩大推广预算进行流量放大",
        "radar_status": "normal",
        "source_url": "",
        "url": "",
    },
}


# =========================
# 双通道路由配置
# =========================

@dataclass(frozen=True)
class ChannelConfig:
    """描述一条爬虫和诊断通道。"""

    source_type: str
    script_path: Path
    product_key: str
    product_id: str
    product_name: str
    display_name: str


ASPECT_PRIORS: List[Dict[str, Any]] = [
    {
        "id": "fit_size",
        "label": "Fit / size accuracy",
        "terms": ["尺码", "尺寸", "偏小", "偏大", "size", "fit", "tight", "small", "one size up"],
        "gap_type": "Fit Localization Gap",
        "baseline_negative_rate": 7,
    },
    {
        "id": "material_quality",
        "label": "Material / durability",
        "terms": ["材质", "面料", "起球", "质量", "耐用", "material", "fabric", "quality", "pilling", "thin"],
        "gap_type": "Material Upgrade Gap",
        "baseline_negative_rate": 9,
    },
    {
        "id": "packaging_logistics",
        "label": "Packaging / delivery trust",
        "terms": ["包装", "破损", "物流", "外包装", "盒", "shipping", "package", "delivery", "box", "damaged"],
        "gap_type": "Packaging Trust Gap",
        "baseline_negative_rate": 6,
    },
    {
        "id": "setup_onboarding",
        "label": "Setup / onboarding",
        "terms": ["说明", "说明书", "教程", "安装", "蓝牙", "连接", "manual", "guide", "setup", "connect", "pairing"],
        "gap_type": "Education & Onboarding Gap",
        "baseline_negative_rate": 8,
    },
    {
        "id": "listing_accuracy",
        "label": "Listing / expectation accuracy",
        "terms": ["颜色", "色差", "图片", "描述", "listing", "picture", "photo", "color", "colour", "expectation"],
        "gap_type": "Listing Accuracy Gap",
        "baseline_negative_rate": 5,
    },
]

PUBLIC_BENCHMARK_REFERENCE = {
    "name": "Amazon Reviews 2023 public review corpus",
    "url": "https://cseweb.ucsd.edu/~jmcauley/datasets.html#amazon_reviews",
    "note": "Static category priors used for lift estimates; not a live category scrape.",
}

ALGORITHM_REFERENCE_LIBRARY: Dict[str, Dict[str, str]] = {
    "absa": {
        "name": "PyABSA aspect based sentiment analysis",
        "url": "https://github.com/yangheng95/PyABSA",
        "role": "将评论拆成属性级痛点，支撑材质、尺码、包装等可执行归因。",
    },
    "review_mining": {
        "name": "Hu & Liu customer review mining",
        "url": "https://www.cs.uic.edu/~liub/publications/kdd04-revSummary.pdf",
        "role": "评论挖掘与观点摘要的经典方法基础。",
    },
    "association_rules": {
        "name": "mlxtend association rules",
        "url": "https://rasbt.github.io/mlxtend/user_guide/frequent_patterns/association_rules/",
        "role": "用 support / confidence / lift 判断痛点和负面结果是否稳定共现。",
    },
    "wilson": {
        "name": "Wilson score interval",
        "url": "https://www.evanmiller.org/how-not-to-sort-by-average-rating.html",
        "role": "样本少时用下界抑制虚高，保护证据可信度。",
    },
    "nab": {
        "name": "Numenta Anomaly Benchmark",
        "url": "https://github.com/numenta/NAB",
        "role": "流式异常检测和预警评分的公开基准。",
    },
    "river": {
        "name": "River online machine learning",
        "url": "https://github.com/online-ml/river",
        "role": "Python 流式/在线学习项目，提供数据流场景下的异常检测参考。",
    },
    "ewma_spc": {
        "name": "NIST EWMA control chart",
        "url": "https://www.itl.nist.gov/div898/handbook/pmc/section3/pmc314.htm",
        "role": "统计过程控制中用指数加权移动平均捕捉小幅持续漂移的经典方法。",
    },
    "amazon_reviews_2023": {
        "name": "Amazon Reviews 2023",
        "url": "https://amazon-reviews-2023.github.io/",
        "role": "公开评论语料用于类目基线和评价分布先验。",
    },
    "mcauley": {
        "name": "McAuley Lab public review datasets",
        "url": "https://cseweb.ucsd.edu/~jmcauley/datasets.html",
        "role": "电商评论数据集和推荐/评论挖掘研究入口。",
    },
    "evidently": {
        "name": "Evidently AI monitoring reports",
        "url": "https://github.com/evidentlyai/evidently",
        "role": "用监控报告、数据漂移和质量指标组织可复盘的模型/业务信号。",
    },
    "ragas": {
        "name": "RAGAS evaluation framework",
        "url": "https://github.com/explodinggradients/ragas",
        "role": "生成式报告需要基于上下文证据并降低幻觉风险的评估思路。",
    },
    "recbole": {
        "name": "RecBole recommender systems",
        "url": "https://github.com/RUCAIBox/RecBole",
        "role": "推荐系统实验框架，可作为后续选品排序和候选池评估的开源参照。",
    },
}


def reference_source(key: str) -> Dict[str, str]:
    item = ALGORITHM_REFERENCE_LIBRARY[key]
    return {"name": item["name"], "url": item["url"]}


CHANNELS: Dict[str, ChannelConfig] = {
    "youtube": ChannelConfig(
        source_type="youtube",
        script_path=SCRAPE_YOUTUBE_SCRIPT,
        product_key="",
        product_id="",
        product_name="",
        display_name="YouTube 评论源",
    ),
    "tiktok": ChannelConfig(
        source_type="tiktok",
        script_path=SCRAPE_TIKTOK_SCRIPT,
        product_key="",
        product_id="",
        product_name="",
        display_name="TikTok 评论源",
    ),
}


# =========================
# 请求与内部异常模型
# =========================

class PipelineRequest(BaseModel):
    """前端 POST /api/run-pipeline 的请求体。"""

    url: str = Field(..., min_length=8, description="TikTok 或 YouTube 视频链接")
    product_id: str | None = Field(None, description="本次诊断要写入的产品 ID")
    product_name: str | None = Field(None, description="本次诊断要写入的产品名称")
    limit: int = Field(DEFAULT_LIMIT, ge=1, le=500, description="本次最多抓取评论数")


class AddProductRequest(BaseModel):
    """前端 POST /api/add-product 的请求体。"""

    product_id: str = Field(..., min_length=2, description="新增监控商品 ID")
    product_name: str = Field(..., min_length=1, description="新增监控商品名称")
    url: str = Field("", description="该商品默认诊断链接，可为空")


class LoginRequest(BaseModel):
    """前端账号密码登录请求体。"""

    username: str = Field(..., min_length=1, description="运营账号")
    password: str = Field(..., min_length=1, description="运营密码")


class VsPipelineRequest(BaseModel):
    """前端 POST /api/run-vs-pipeline 的请求体。"""

    product_ids: List[str] = Field(..., description="需要横向 PK 的商品 ID 数组")


class AppealRequest(BaseModel):
    """前端 POST /api/generate-appeal 的请求体。"""

    product_id: str = Field(..., min_length=1, description="需要生成英文申诉信的商品 ID")


class BriefRequest(BaseModel):
    """前端 POST /api/generate-brief 的请求体。"""

    product_id: str = Field(..., min_length=1, description="需要生成采购 Brief 的商品 ID")
    factory_name: str = Field(..., min_length=1, description="目标 1688 工厂名称")


class AdminRestoreRequest(BaseModel):
    """前端 POST /api/admin/restore-data 的请求体。"""

    products: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="要恢复的商品诊断大盘")
    vs_reports: List[Dict[str, Any]] = Field(default_factory=list, description="要恢复的竞品 PK 历史报告")


class ExecutiveReportRequest(BaseModel):
    """前端 POST /api/executive-report 的请求体。"""

    products: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="前端当前可见商品快照")
    force_model: bool = Field(False, description="是否强制尝试模型生成")


class ExecutiveBriefRequest(BaseModel):
    """前端 POST /api/generate-executive-brief 的请求体。"""

    brief_type: str = Field(..., min_length=1, description="brief 类型：supply_chain / appeal / selection")
    product_id: str = Field("", description="可选的商品 ID；为空时由后端选择最高优先级商品")
    products: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="前端当前可见商品快照")
    executive_report: Dict[str, Any] = Field(default_factory=dict, description="当前综合经营报告载荷")
    force_model: bool = Field(False, description="是否强制尝试模型生成；默认关闭以保障导出速度")


class PipelineRuntimeError(Exception):
    """把流水线内部错误携带为可转成 HTTPException 的结构。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# =========================
# JSON 文件读写工具
# =========================

def atomic_write_json(path: Path, data: Any) -> None:
    """以临时文件替换方式写 JSON，降低中断导致文件损坏的概率。"""
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def load_json_file(path: Path, default: Any) -> Any:
    """读取 JSON 文件；不存在或损坏时返回默认值的深拷贝。"""
    if not path.exists():
        return copy.deepcopy(default)

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return copy.deepcopy(default)


def get_storage_backend_name() -> str:
    """返回当前数据持久化后端；云端优先，本地 JSON 仅用于开发回退。"""
    if REDIS_URL:
        return "redis"
    if DATABASE_URL:
        return "postgres"
    return "local_json"


def get_redis_client() -> Any:
    """懒加载 Redis 客户端，避免未配置云缓存时引入额外启动依赖。"""
    global _STORAGE_REDIS_CLIENT
    if _STORAGE_REDIS_CLIENT is None:
        import redis

        _STORAGE_REDIS_CLIENT = redis.from_url(REDIS_URL, decode_responses=True)
    return _STORAGE_REDIS_CLIENT


def ensure_postgres_table(conn: Any) -> None:
    """初始化 Postgres JSONB KV 表，用主键约束保障同一数据节点原子覆盖。"""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tk_ai_kv_store (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def parse_json_payload(payload: Any, default: Any) -> Any:
    """统一清洗 Redis 字符串与 Postgres JSONB 返回值，异常时回落到默认结构。"""
    if payload is None:
        return copy.deepcopy(default)
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return copy.deepcopy(default)
    return copy.deepcopy(payload)


def load_postgres_json(key: str, default: Any) -> Any:
    """从云端 Postgres JSONB KV 表读取业务数据。"""
    import psycopg

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_postgres_table(conn)
        row = conn.execute(
            "SELECT value FROM tk_ai_kv_store WHERE key = %s",
            (key,),
        ).fetchone()
        if row is None:
            return copy.deepcopy(default)
        return parse_json_payload(row[0], default)


def save_postgres_json(key: str, data: Any) -> None:
    """把业务数据以 JSONB 形式写入 Postgres，依赖 UPSERT 避免并发插入冲突。"""
    import psycopg

    payload = json.dumps(data, ensure_ascii=False)
    with psycopg.connect(DATABASE_URL) as conn:
        ensure_postgres_table(conn)
        conn.execute(
            """
            INSERT INTO tk_ai_kv_store(key, value, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """,
            (key, payload),
        )


def load_storage_json(key: str, local_path: Path, default: Any) -> Any:
    """按环境变量选择云端存储；配置云数据库后不会读取本地业务 JSON 文件。"""
    backend = get_storage_backend_name()
    if backend == "redis":
        return parse_json_payload(get_redis_client().get(key), default)
    if backend == "postgres":
        return load_postgres_json(key, default)
    return load_json_file(local_path, default)


def save_storage_json(key: str, local_path: Path, data: Any) -> None:
    """按环境变量选择写入目标；配置云数据库后不会写入本地业务 JSON 文件。"""
    backend = get_storage_backend_name()
    if backend == "redis":
        get_redis_client().set(key, json.dumps(data, ensure_ascii=False))
        return
    if backend == "postgres":
        save_postgres_json(key, data)
        return
    atomic_write_json(local_path, data)


def looks_like_single_product(data: Any) -> bool:
    """判断 JSON 顶层是否是 ai_diagnose.py 输出的单个产品报告。"""
    return isinstance(data, dict) and "sentiment" in data and "keywordLabels" in data


def product_key_from_report(report: Dict[str, Any]) -> str:
    """从单产品报告里推断前端产品 key；未知商品保留自己的动态 product_id。"""
    product_id = str(report.get("product_id", "")).strip()
    if product_id:
        return product_id
    return DEFAULT_PRODUCT_KEY


def ensure_products_shape(data: Any) -> Dict[str, Dict[str, Any]]:
    """确保产品字典结构稳定；生产环境默认不再自动注入演示商品。"""
    products = copy.deepcopy(DEFAULT_PRODUCTS) if ENABLE_DEMO_PRODUCTS else {}

    if looks_like_single_product(data):
        key = product_key_from_report(data)
        merged = copy.deepcopy(products.get(key, {}))
        merged.update(enrich_product_for_dashboard(data))
        products[key] = merged
        return products

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                product_key = str(key)
                merged = copy.deepcopy(products.get(product_key, {}))
                merged.update(enrich_product_for_dashboard(value))
                products[product_key] = merged

    return products


def ensure_products_file() -> Dict[str, Dict[str, Any]]:
    """服务启动时初始化诊断文件，防止前端首次加载报错。"""
    products = ensure_products_shape(
        load_storage_json(PRODUCTS_STORE_KEY, DIAGNOSED_PRODUCTS_PATH, {})
    )
    save_storage_json(PRODUCTS_STORE_KEY, DIAGNOSED_PRODUCTS_PATH, products)
    return products


def load_products() -> Dict[str, Dict[str, Any]]:
    """读取并修复前端产品字典。"""
    return ensure_products_file()


def save_products(products: Dict[str, Dict[str, Any]]) -> None:
    """保存完整产品字典。"""
    save_storage_json(PRODUCTS_STORE_KEY, DIAGNOSED_PRODUCTS_PATH, products)


def load_vs_reports() -> List[Dict[str, Any]]:
    """读取竞品 PK 历史报告，异常结构自动回落为空列表。"""
    reports = load_storage_json(VS_REPORTS_STORE_KEY, COMPETITOR_VS_REPORTS_PATH, [])
    if isinstance(reports, list):
        return [item for item in reports if isinstance(item, dict)]
    return []


def save_vs_reports(reports: List[Dict[str, Any]]) -> None:
    """保存最近 50 条竞品 PK 历史报告。"""
    save_storage_json(VS_REPORTS_STORE_KEY, COMPETITOR_VS_REPORTS_PATH, reports[-50:])


def load_radar_history() -> Dict[str, List[Dict[str, Any]]]:
    """读取 24H 雷达历史快照，云端优先，结构异常时自动清洗。"""
    history = load_storage_json(RADAR_HISTORY_STORE_KEY, RADAR_HISTORY_PATH, {})
    if not isinstance(history, dict):
        return {}

    cleaned: Dict[str, List[Dict[str, Any]]] = {}
    for product_key, points in history.items():
        if not isinstance(points, list):
            continue
        valid_points = [point for point in points if isinstance(point, dict)]
        cleaned[str(product_key)] = valid_points[-RADAR_HISTORY_MAX_POINTS:]
    return cleaned


def save_radar_history(history: Dict[str, List[Dict[str, Any]]]) -> None:
    """保存雷达历史，单商品只保留最近 RADAR_HISTORY_MAX_POINTS 个快照。"""
    trimmed = {
        str(product_key): [point for point in points if isinstance(point, dict)][-RADAR_HISTORY_MAX_POINTS:]
        for product_key, points in history.items()
        if isinstance(points, list)
    }
    save_storage_json(RADAR_HISTORY_STORE_KEY, RADAR_HISTORY_PATH, trimmed)


def build_pending_product(product_id: str, product_name: str, url: str = "") -> Dict[str, Any]:
    """为新注册商品创建待诊断占位节点，供前端安全渲染空状态。"""
    return {
        "product_id": product_id,
        "product_name": product_name,
        "score": 100,
        "sentiment": [],
        "labels": [],
        "keywords": [],
        "keywordLabels": [],
        "insight": "等待抓取数据中，请输入评论链接并启动 AI 诊断。",
        "direction": "等待首轮诊断",
        "action": "待抓取评论",
        "pending": True,
        "radar_status": "normal",
        "source_url": url,
        "url": url,
    }


# =========================
# 产品报告补齐逻辑
# =========================

def calculate_score(sentiment: Any) -> int:
    """根据情感分布估算产品健康分。sentiment 顺序为 [正面, 中性, 负面]。"""
    try:
        positive = int(sentiment[0])
        negative = int(sentiment[2])
    except (TypeError, ValueError, IndexError):
        return 60

    score = round(100 - negative * 0.6 + positive * 0.1)
    return max(1, min(98, score))


def infer_direction(keyword_labels: Any) -> str:
    """从 TOP 负面标签生成供应链优化方向。"""
    if not isinstance(keyword_labels, list) or not keyword_labels:
        return "补充样本后定位核心客诉"

    first = str(keyword_labels[0]).strip() or "核心客诉"
    second = str(keyword_labels[1]).strip() if len(keyword_labels) > 1 else ""
    if second:
        return f"优先处理{first}，同步排查{second}"
    return f"优先处理{first}"


def infer_action(sentiment: Any) -> str:
    """根据负面比例生成运营动作建议。"""
    try:
        negative = int(sentiment[2])
    except (TypeError, ValueError, IndexError):
        negative = 50

    if negative >= 60:
        return "暂停投放并进行升级调款"
    if negative >= 40:
        return "库存返工升级中"
    if negative >= 20:
        return "小批量验证并优化详情页"
    return "扩大推广预算进行流量放大"


def infer_factory_category(product: Dict[str, Any]) -> str:
    """根据商品名和客诉标签推断更适合的 1688 源头工厂类型。"""
    text = " ".join([
        str(product.get("product_name", "")),
        " ".join(str(item) for item in product.get("keywordLabels", []) if item),
        str(product.get("insight", "")),
    ]).lower()
    if any(token in text for token in ["鞋", "shoe", "sneaker", "sole", "尺码"]):
        return "footwear"
    if any(token in text for token in ["瑜伽裤", "面料", "起球", "服饰", "fabric", "pilling", "knit"]):
        return "apparel"
    if any(token in text for token in ["投影", "蓝牙", "插头", "电", "塑胶", "五金", "connector"]):
        return "electronics"
    if any(token in text for token in ["包装", "纸箱", "破损", "箱", "物流"]):
        return "packaging"
    return "general"


FACTORY_LIBRARY: Dict[str, List[Dict[str, Any]]] = {
    "apparel": [
        {
            "factory_name": "义乌市锦纬针织服装源头厂",
            "category": "针织运动服饰 / 抗起球面料",
            "advantage": "擅长锦氨高弹面料、抗起球整理和小单快反打样。",
            "scores": [92, 81, 86, 78],
        },
        {
            "factory_name": "东莞市恒纤功能面料制衣厂",
            "category": "瑜伽裤 / 功能压缩服",
            "advantage": "可提供 ISO 12945-2 抗起球测试报告和色牢度改良方案。",
            "scores": [88, 84, 80, 86],
        },
        {
            "factory_name": "绍兴柯桥云织供应链工厂",
            "category": "功能面料 / 柔性供应链",
            "advantage": "面料现货丰富，适合快速替换问题批次并控制采购成本。",
            "scores": [84, 91, 82, 74],
        },
    ],
    "footwear": [
        {
            "factory_name": "晋江云步运动鞋源头工厂",
            "category": "运动鞋 / EVA 鞋底",
            "advantage": "擅长鞋底回弹、楦型修正和美码半码体系适配。",
            "scores": [90, 82, 84, 80],
        },
        {
            "factory_name": "温州星迈鞋业制造厂",
            "category": "休闲鞋 / 复古板鞋",
            "advantage": "可做鞋盒抗压升级、鞋面耐折与胶水牢度改善。",
            "scores": [86, 88, 82, 76],
        },
        {
            "factory_name": "东莞路驰鞋材科技厂",
            "category": "鞋底鞋材 / 模具开发",
            "advantage": "材料研发能力强，适合解决鞋底偏硬和断裂问题。",
            "scores": [93, 76, 78, 84],
        },
    ],
    "electronics": [
        {
            "factory_name": "东莞市启航塑胶五金电子厂",
            "category": "3C 外壳 / 插头连接件",
            "advantage": "擅长插拔寿命、跌落结构补强和阻燃材料替换。",
            "scores": [89, 80, 84, 88],
        },
        {
            "factory_name": "深圳市蓝芯智能电子源头厂",
            "category": "蓝牙模组 / 小家电主板",
            "advantage": "固件迭代快，可提供 EMC 和老化测试配套记录。",
            "scores": [91, 78, 82, 90],
        },
        {
            "factory_name": "惠州科塑精密模具厂",
            "category": "塑胶模具 / 包装结构",
            "advantage": "擅长结构件加筋、卡扣寿命和包装防震内托优化。",
            "scores": [86, 85, 88, 80],
        },
    ],
    "packaging": [
        {
            "factory_name": "义乌安递包装制品厂",
            "category": "跨境电商纸箱 / 缓冲包材",
            "advantage": "支持 ISTA 1A 跌落测试方案和加厚双瓦楞纸箱定制。",
            "scores": [86, 90, 88, 78],
        },
        {
            "factory_name": "东莞固盾环保包装厂",
            "category": "防震内托 / 蜂窝纸箱",
            "advantage": "适合解决海外尾程挤压、包装破损和开箱差评。",
            "scores": [90, 82, 84, 84],
        },
        {
            "factory_name": "宁波海仓包材供应链",
            "category": "出口包装 / 海外仓包材",
            "advantage": "熟悉美区尾程物流包材标准，交付周期稳定。",
            "scores": [84, 86, 92, 76],
        },
    ],
    "general": [
        {
            "factory_name": "义乌跨境优品柔性供应链工厂",
            "category": "跨境百货 / 快反打样",
            "advantage": "适合快速验证改良款，支持小单混批和包装升级。",
            "scores": [82, 88, 86, 74],
        },
        {
            "factory_name": "东莞质造供应链协同工厂",
            "category": "综合制造 / 品控改良",
            "advantage": "擅长把差评问题转成 QC 检验项和包材补强方案。",
            "scores": [88, 80, 82, 82],
        },
        {
            "factory_name": "宁波出口电商源头联盟工厂",
            "category": "跨境出口 / 稳定交付",
            "advantage": "熟悉海外仓补货节奏，适合中等规模批量迭代。",
            "scores": [84, 84, 90, 78],
        },
    ],
}


def calculate_factory_score(scores: List[int]) -> int:
    """供应商多维加权打分：质量 45%、成本 25%、交付 20%、资质 10%。"""
    weights = [0.45, 0.25, 0.20, 0.10]
    normalized = [safe_int(score, 0) for score in scores[:4]]
    while len(normalized) < 4:
        normalized.append(0)
    return round(sum(score * weight for score, weight in zip(normalized, weights)))


def build_recommended_factories(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    """为 critical 商品挂载 2-3 家 1688 源头工厂推荐数据。"""
    category = infer_factory_category(product)
    factories = FACTORY_LIBRARY.get(category, FACTORY_LIBRARY["general"])
    result = []
    for factory in factories[:3]:
        scores = [safe_int(score, 0) for score in factory["scores"]]
        result.append({
            "factory_name": factory["factory_name"],
            "category": factory["category"],
            "advantage": factory["advantage"],
            "scores": scores,
            "score_labels": ["质量", "成本", "交付", "资质"],
            "weighted_score": calculate_factory_score(scores),
        })
    return result


def clamp_percent(value: Any) -> int:
    return max(0, min(100, safe_int(value, 0)))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def wilson_lower_bound(successes: int, total: int, z: float = 1.96) -> float:
    """Return Wilson lower bound for a binomial proportion."""
    if total <= 0:
        return 0.0
    phat = max(0.0, min(1.0, successes / total))
    denominator = 1 + (z * z / total)
    centre = phat + (z * z / (2 * total))
    margin = z * ((phat * (1 - phat) + (z * z / (4 * total))) / total) ** 0.5
    return max(0.0, (centre - margin) / denominator)


def build_metric_lineage_entry(
    metric_id: str,
    label: str,
    value: Any,
    formula: str,
    basis: str,
    sample_size: int,
    confidence: float,
    sources: List[Dict[str, str]],
    degradation: str = "",
) -> Dict[str, Any]:
    return {
        "metric_id": metric_id,
        "label": label,
        "value": value,
        "formula": formula,
        "basis": basis,
        "sample_size": max(0, safe_int(sample_size, 0)),
        "confidence": round(max(0.0, min(1.0, safe_float(confidence))), 3),
        "sources": sources,
        "degradation": degradation or "样本量满足当前轻量判断要求。",
    }


def build_health_score_lineage(score: int, sentiment: List[Any], aspect_terms: List[Dict[str, Any]], evidence_count: int) -> Dict[str, Any]:
    negative_ratio = safe_int(sentiment[2] if len(sentiment) > 2 else 0, 0)
    critical_aspect = max([safe_float(item.get("mention_rate"), 0.0) for item in aspect_terms if isinstance(item, dict)] or [0.0])
    sample_size = max(evidence_count, sum(safe_int(item.get("frequency"), 0) for item in aspect_terms if isinstance(item, dict)))
    confidence = min(0.92, 0.35 + min(sample_size, 80) / 140)
    degradation = "样本不足，健康分已按评论证据量降权解释。" if sample_size < 8 else "情感比例与方面级痛点共同支撑。"
    return build_metric_lineage_entry(
        "health_score",
        "情感健康分",
        score,
        "100 - 负面率惩罚 - 核心痛点惩罚 - 近期波动惩罚 + 证据置信加成",
        f"负面率 {negative_ratio}%，最高痛点提及率 {round(critical_aspect)}%。",
        sample_size,
        confidence,
        [
            reference_source("review_mining"),
            reference_source("absa"),
        ],
        degradation,
    )


def build_evidence_trust_breakdown(comment_count: int, evidence_count: int, confidence: float, source_count: int = 1) -> Dict[str, Any]:
    total = max(comment_count, evidence_count, 0)
    if total <= 0:
        total = evidence_count
    positive_hits = max(0, min(total, round(total * max(0.0, min(1.0, confidence))))) if total > 0 else 0
    wilson = wilson_lower_bound(positive_hits, total) if total > 0 else 0.0
    coverage = min(1.0, total / 60) if total > 0 else 0.0
    source_coverage = min(1.0, max(1, source_count) / 3) if total > 0 else 0.0
    final_score = round((0.65 * wilson + 0.25 * coverage + 0.10 * source_coverage) * 100)
    degradation = "样本不足，已使用 Wilson 下界抑制虚高。" if total < 20 else "样本覆盖达到轻量判断要求。"
    return {
        "score": max(0, min(100, final_score)),
        "wilson_lower": round(wilson, 3),
        "coverage": round(coverage, 3),
        "source_coverage": round(source_coverage, 3),
        "positive_hits": positive_hits,
        "sample_size": total,
        "degradation": degradation,
    }


def build_gap_score_lineage(gap_score: int, metrics: Dict[str, Any], sample_size: int) -> Dict[str, Any]:
    confidence = min(0.9, 0.32 + min(sample_size, 80) / 130)
    degradation = "机会分样本不足，仅作为后台观察信号。" if sample_size < 12 else "机会分由痛点强度、可修复性和证据置信共同支撑。"
    basis = (
        f"需求热度 {safe_int(metrics.get('demandHeat'), 0)}，痛点密度 {safe_int(metrics.get('painDensity'), 0)}，"
        f"可修复性 {safe_int(metrics.get('fixability'), 0)}，证据置信 {safe_int(metrics.get('evidenceConfidence'), 0)}。"
    )
    return build_metric_lineage_entry(
        "market_gap_score",
        "机会分",
        gap_score,
        "需求热度*0.35 + 痛点强度*0.30 + 可修复性*0.20 + 证据置信*0.15",
        basis,
        sample_size,
        confidence,
        [
            reference_source("association_rules"),
            reference_source("amazon_reviews_2023"),
        ],
        degradation,
    )


def build_association_lineage(market_gap: Dict[str, Any]) -> Dict[str, Any]:
    association = market_gap.get("association", {}) if isinstance(market_gap.get("association"), dict) else {}
    sample_size = safe_int(association.get("sample_count"), 0)
    confidence = min(0.9, 0.30 + min(sample_size, 80) / 120)
    basis = (
        f"support {safe_float(association.get('support'), 0.0):.3f}，"
        f"confidence {safe_float(association.get('confidence'), 0.0):.3f}，"
        f"lift {safe_float(association.get('lift'), 0.0):.2f}。"
    )
    degradation = "关联样本不足，当前仅作为后台辅助证据。" if sample_size < 12 else "痛点和负面结果存在可解释共现关系。"
    return build_metric_lineage_entry(
        "association_signal",
        "关联规则信号",
        round(safe_float(association.get("lift"), 0.0), 2),
        "support = 痛点命中样本/总样本；confidence = 痛点命中中的负面比例；lift = confidence/基线负面率",
        basis,
        sample_size,
        confidence,
        [
            reference_source("association_rules"),
            reference_source("mcauley"),
        ],
        degradation,
    )



def build_sps_lineage(product: Dict[str, Any], evidence_count: int) -> Dict[str, Any]:
    sentiment = product.get("sentiment") if isinstance(product.get("sentiment"), list) else [0, 100, 0]
    negative = safe_int(sentiment[2] if len(sentiment) > 2 else 0, 0)
    positive = safe_int(sentiment[0] if len(sentiment) > 0 else 0, 0)
    score = safe_int(product.get("score"), calculate_score(sentiment))
    keyword_values = product.get("keywords") if isinstance(product.get("keywords"), list) else []
    sample_size = max(evidence_count, sum(max(0, safe_int(value, 0)) for value in keyword_values))
    confidence = min(0.86, 0.34 + min(sample_size, 80) / 150)
    return build_metric_lineage_entry(
        "sps_half_life",
        "SPS 修复预测",
        score,
        "预测分 = 100 - 修复后负面率*0.68 + 新增好评率*0.08；近期差评权重 W(t)=0.7^floor(t/15)",
        f"当前正面率 {positive}%，负面率 {negative}%，基础健康分 {score}。",
        sample_size,
        confidence,
        [
            reference_source("wilson"),
            reference_source("nab"),
        ],
        "SPS 平台内部权重不可完全公开，当前使用可解释半衰近似模型。",
    )


def build_algorithm_support_pack(product: Dict[str, Any], market_gap: Dict[str, Any]) -> Dict[str, Any]:
    lineage = product.get("metric_lineage") if isinstance(product.get("metric_lineage"), dict) else {}
    evidence = product.get("evidence_ledger") if isinstance(product.get("evidence_ledger"), dict) else {}
    aspect_terms = product.get("aspect_terms") if isinstance(product.get("aspect_terms"), list) else []
    association = market_gap.get("association") if isinstance(market_gap.get("association"), dict) else {}
    top_aspect = aspect_terms[0] if aspect_terms and isinstance(aspect_terms[0], dict) else {}
    evidence_count = safe_int(evidence.get("evidence_count"), 0)
    sample_size = max(
        evidence_count,
        safe_int(evidence.get("comment_count"), 0),
        safe_int(association.get("sample_count"), 0),
        safe_int(top_aspect.get("frequency"), 0),
    )
    lineage_items = [entry for entry in lineage.values() if isinstance(entry, dict)]
    avg_confidence = round(
        sum(safe_float(entry.get("confidence"), 0.0) for entry in lineage_items) / len(lineage_items),
        3,
    ) if lineage_items else 0.0
    top_pain = str(market_gap.get("top_pain") or top_aspect.get("raw_label") or top_aspect.get("aspect") or "核心痛点待补样本")
    gap_score = safe_int(market_gap.get("gap_score"), 0)
    trust_score = safe_int(evidence.get("evidence_trust_score"), 0)
    lift = safe_float(association.get("lift"), safe_float(top_aspect.get("benchmark_lift"), 0.0))
    support = safe_float(association.get("support"), 0.0)
    action = str(product.get("action") or "等待决策")

    if sample_size <= 0:
        verdict = "当前缺少真实评论样本，算法仅保留框架，不输出强结论。"
    elif avg_confidence < 0.45:
        verdict = f"样本仍偏薄，{top_pain} 只能作为观察信号；建议继续抓取评论后再决定投放。"
    elif gap_score >= 70 and trust_score >= 55:
        verdict = f"{top_pain} 已形成可解释机会：痛点强、证据可信度可用，可进入小批量打样或详情页验证。"
    else:
        verdict = f"{top_pain} 有信号但未到强机会阈值，建议维持后台监控并按 {action} 执行。"

    return {
        "schema": "tk_algorithm_support_v1",
        "verdict": verdict,
        "sample_size": sample_size,
        "average_confidence": avg_confidence,
        "primary_methods": [
            {
                "method_id": "absa",
                "label": "方面级情感分析",
                "why_it_matters": "把评论拆成尺码、材质、包装、说明等可改造痛点，而不是只看总分。",
                "source": reference_source("absa"),
            },
            {
                "method_id": "association_rules",
                "label": "关联规则",
                "why_it_matters": "用 support、confidence、lift 判断某个痛点是否和差评结果稳定共现。",
                "source": reference_source("association_rules"),
            },
            {
                "method_id": "wilson",
                "label": "证据可信度下界",
                "why_it_matters": "样本少时主动压低可信度，避免小样本误判。",
                "source": reference_source("wilson"),
            },
            {
                "method_id": "stream_anomaly",
                "label": "流式异常预警",
                "why_it_matters": "用历史基线、EWMA 和样本量降级识别持续负面漂移。",
                "source": reference_source("nab"),
            },
        ],
        "observations": [
            f"核心痛点：{top_pain}",
            f"机会分：{gap_score}/100",
            f"证据可信度：{trust_score}/100",
            f"关联强度：support {support:.3f}，lift {lift:.2f}",
        ],
        "limitations": [
            "公开类目基线来自静态评论语料，不等同于实时全网市场份额。",
            "历史商品如缺少逐条评论样例，会降级使用关键词频次兼容证据。",
            "SPS 修复曲线是可解释半衰近似，最终以平台真实考核口径为准。",
        ],
        "references": list(ALGORITHM_REFERENCE_LIBRARY.values()),
    }


def build_radar_lineage(evaluated: Dict[str, Any], latest_score: int, sample_size: int = 0) -> Dict[str, Any]:
    delta = safe_int(evaluated.get("radar_negative_delta"), 0)
    latest_negative = safe_float(evaluated.get("radar_latest_negative_ratio"), 0.0)
    baseline = safe_float(evaluated.get("radar_baseline_negative_ratio"), latest_negative)
    ewma = safe_float(evaluated.get("radar_ewma_negative_ratio"), baseline)
    z_like = safe_float(evaluated.get("radar_z_like_delta"), 0.0)
    history_points = safe_int(evaluated.get("radar_history_points"), 0)
    confidence = min(0.9, 0.28 + min(sample_size, 80) / 160 + min(history_points, 12) / 40)
    degradation = str(evaluated.get("radar_degradation") or "历史窗口和样本量满足当前轻量预警要求。")
    return build_metric_lineage_entry(
        "radar_risk",
        "雷达预警",
        evaluated.get("radar_status", "normal"),
        "兼容阈值 + EWMA 基线漂移；z≈(最新负面率-历史基线)/二项比例标准误。",
        (
            f"最新负面率 {latest_negative:.1f}%，历史基线 {baseline:.1f}%，EWMA {ewma:.1f}%，"
            f"快照变化 {delta} 个百分点，z≈{z_like:.2f}，健康分 {latest_score}。"
        ),
        sample_size,
        confidence,
        [
            reference_source("nab"),
            reference_source("river"),
            reference_source("ewma_spc"),
        ],
        degradation,
    )


def match_aspect_prior(label: str) -> Dict[str, Any]:
    normalized = str(label or "").lower()
    for prior in ASPECT_PRIORS:
        if any(term.lower() in normalized for term in prior["terms"]):
            return prior
    return {
        "id": f"keyword_{hashlib.sha1(normalized.encode('utf-8')).hexdigest()[:8]}",
        "label": str(label or "Unclassified pain point"),
        "terms": [],
        "gap_type": "Differentiation Gap",
        "baseline_negative_rate": 8,
    }


def build_compatible_aspect_terms(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    existing = product.get("aspect_terms")
    if isinstance(existing, list) and existing:
        return [item for item in existing[:6] if isinstance(item, dict)]

    ledger = product.get("evidence_ledger")
    if isinstance(ledger, dict) and isinstance(ledger.get("aspect_terms"), list) and ledger["aspect_terms"]:
        return [item for item in ledger["aspect_terms"][:6] if isinstance(item, dict)]

    labels = product.get("keywordLabels") if isinstance(product.get("keywordLabels"), list) else []
    values = product.get("keywords") if isinstance(product.get("keywords"), list) else []
    total_mentions = sum(max(0, safe_int(value, 0)) for value in values) or 1
    aspect_terms: List[Dict[str, Any]] = []
    used_ids: set[str] = set()

    for index, label in enumerate(labels[:6]):
        count = max(0, safe_int(values[index] if index < len(values) else 0, 0))
        if count <= 0:
            continue
        prior = match_aspect_prior(str(label))
        aspect_id = str(prior["id"])
        if aspect_id in used_ids:
            continue
        used_ids.add(aspect_id)
        mention_rate = clamp_percent(round(count / total_mentions * 100))
        baseline = max(1, safe_int(prior.get("baseline_negative_rate"), 8))
        confidence = min(0.82, round(0.36 + min(count, 20) * 0.018, 2))
        aspect_terms.append({
            "aspect_id": aspect_id,
            "aspect": prior.get("label") or str(label),
            "raw_label": str(label),
            "gap_type": prior.get("gap_type", "Differentiation Gap"),
            "polarity": "negative",
            "frequency": count,
            "mention_rate": mention_rate,
            "baseline_negative_rate": baseline,
            "benchmark_lift": round(mention_rate / baseline, 1),
            "confidence": confidence,
            "examples": [],
            "method": "keyword_frequency_compat",
        })

    aspect_terms.sort(key=lambda item: (float(item.get("benchmark_lift", 0)), int(item.get("frequency", 0))), reverse=True)
    return aspect_terms[:6]


def build_compatible_evidence_ledger(product: Dict[str, Any]) -> Dict[str, Any]:
    aspect_terms = build_compatible_aspect_terms(product)
    existing = product.get("evidence_ledger")
    ledger = copy.deepcopy(existing) if isinstance(existing, dict) else {}

    sentiment = product.get("sentiment") if isinstance(product.get("sentiment"), list) else [0, 100, 0]
    confidence_values = [safe_float(item.get("confidence"), 0.0) for item in aspect_terms if isinstance(item, dict)]
    confidence = round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else 0.35
    max_lift = max([safe_float(item.get("benchmark_lift"), 0.0) for item in aspect_terms if isinstance(item, dict)] or [0])
    top_aspect = aspect_terms[0] if aspect_terms else {}
    derived_evidence_count = sum(len(item.get("examples", [])) for item in aspect_terms if isinstance(item, dict))
    if derived_evidence_count <= 0:
        derived_evidence_count = sum(safe_int(item.get("frequency"), 0) for item in aspect_terms if isinstance(item, dict))
    comment_count = safe_int(ledger.get("comment_count"), 0)
    source_count = safe_int(ledger.get("source_count"), 1)
    evidence_count = safe_int(ledger.get("evidence_count"), derived_evidence_count)
    trust_breakdown = build_evidence_trust_breakdown(comment_count, evidence_count, confidence, source_count)

    ledger.setdefault("schema", "tk_absa_evidence_v1")
    ledger.setdefault("method", "ABSA-inspired aspect mining + keyword fallback")
    ledger["comment_count"] = comment_count
    ledger["evidence_count"] = evidence_count
    ledger["source_count"] = max(1, source_count)
    ledger.setdefault("sample_window", ledger.get("sample_window") or "stored_product_snapshot")
    ledger["confidence"] = confidence
    ledger["evidence_trust_score"] = trust_breakdown["score"]
    ledger["evidence_trust_breakdown"] = trust_breakdown
    ledger.setdefault("top_aspect", top_aspect.get("aspect", product.get("keywordLabels", [""])[0] if product.get("keywordLabels") else ""))
    ledger.setdefault("top_gap_type", top_aspect.get("gap_type", "Differentiation Gap"))
    ledger["aspect_terms"] = aspect_terms
    ledger["market_benchmark"] = {
        "reference": PUBLIC_BENCHMARK_REFERENCE,
        "negative_rate": safe_int(sentiment[2] if len(sentiment) > 2 else 0, 0),
        "top_aspect_lift": max_lift,
        "baseline_note": "Lift compares observed aspect mention rate with static public-review priors.",
    }
    ledger["metric_lineage"] = {
        "evidence_trust": build_metric_lineage_entry(
            "evidence_trust",
            "证据可信度",
            trust_breakdown["score"],
            "0.65*Wilson下界 + 0.25*样本覆盖 + 0.10*来源覆盖",
            f"Wilson 下界 {trust_breakdown['wilson_lower']}，样本覆盖 {trust_breakdown['coverage']}，来源覆盖 {trust_breakdown['source_coverage']}。",
            trust_breakdown["sample_size"],
            confidence,
            [
                reference_source("wilson"),
                reference_source("amazon_reviews_2023"),
            ],
            trust_breakdown["degradation"],
        ),
        "aspect_lift": build_metric_lineage_entry(
            "aspect_lift",
            "痛点基线偏离",
            max_lift,
            "当前痛点提及率 / 公开评论类目基线负面率",
            f"Top aspect: {top_aspect.get('raw_label') or top_aspect.get('aspect') or 'N/A'}。",
            evidence_count,
            confidence,
            [
                reference_source("association_rules"),
                reference_source("mcauley"),
            ],
            "静态公开评论先验，非实时全网类目爬取。",
        ),
    }
    return ledger



def infer_market_gap_type_from_text(text: str, fixability: int = 50) -> str:
    normalized = str(text or "").lower()
    if any(term in normalized for term in ["尺码", "尺寸", "size", "fit", "偏小", "偏大"]):
        return "Fit Localization Gap"
    if any(term in normalized for term in ["材质", "面料", "质量", "起球", "material", "fabric", "pilling"]):
        return "Material Upgrade Gap"
    if any(term in normalized for term in ["包装", "物流", "破损", "shipping", "package", "delivery", "box"]):
        return "Packaging Trust Gap"
    if any(term in normalized for term in ["说明", "教程", "安装", "连接", "manual", "setup", "pairing"]):
        return "Education & Onboarding Gap"
    if any(term in normalized for term in ["颜色", "色差", "图片", "描述", "listing", "photo", "color"]):
        return "Listing Accuracy Gap"
    if fixability >= 72:
        return "Supply Chain Fix Gap"
    return "Differentiation Gap"


def build_market_gap_snapshot(product_key: str, product: Dict[str, Any]) -> Dict[str, Any]:
    sentiment = product.get("sentiment") if isinstance(product.get("sentiment"), list) else [0, 100, 0]
    negative = clamp_percent(sentiment[2] if len(sentiment) > 2 else 0)
    positive = clamp_percent(sentiment[0] if len(sentiment) > 0 else 0)
    score = clamp_percent(product.get("score", calculate_score(sentiment)))
    keywords = product.get("keywords") if isinstance(product.get("keywords"), list) else []
    keyword_total = sum(max(0, safe_int(value, 0)) for value in keywords)
    top_keyword = max([safe_int(value, 0) for value in keywords[:1]] or [0])
    concentration = round((top_keyword / keyword_total) * 100) if keyword_total > 0 else 0
    factories = product.get("recommended_factories") if isinstance(product.get("recommended_factories"), list) else []
    radar_delta = safe_int(product.get("radar_negative_delta"), 0)
    evidence = product.get("evidence_ledger") if isinstance(product.get("evidence_ledger"), dict) else {}
    aspect_terms = product.get("aspect_terms") if isinstance(product.get("aspect_terms"), list) else []
    top_aspect = aspect_terms[0] if aspect_terms and isinstance(aspect_terms[0], dict) else {}
    evidence_confidence = clamp_percent(safe_float(evidence.get("confidence"), safe_float(top_aspect.get("confidence"), 0.0)) * 100)
    benchmark_lift = max(0.0, safe_float(top_aspect.get("benchmark_lift"), safe_float(evidence.get("market_benchmark", {}).get("top_aspect_lift") if isinstance(evidence.get("market_benchmark"), dict) else 0.0, 0.0)))
    lift_score = clamp_percent(min(benchmark_lift, 4.0) * 25)
    evidence_count = safe_int(evidence.get("evidence_count"), 0)
    comment_count = safe_int(evidence.get("comment_count"), 0)
    frequency = safe_int(top_aspect.get("frequency"), 0)
    aspect_mention_rate = safe_float(top_aspect.get("mention_rate"), 0.0)
    sample_count = max(comment_count, evidence_count, keyword_total, frequency)
    baseline_negative = max(1.0, safe_float(top_aspect.get("baseline_negative_rate"), 8.0)) / 100
    support = round((frequency / max(sample_count, 1)), 4) if sample_count > 0 else 0.0
    confidence_rule = round(min(1.0, max(0.0, (negative / 100) * (aspect_mention_rate / 100 if aspect_mention_rate > 0 else support))), 4)
    if support > 0 and confidence_rule <= 0:
        confidence_rule = round(min(1.0, negative / 100), 4)
    association_lift = round((confidence_rule / baseline_negative), 2) if baseline_negative > 0 and confidence_rule > 0 else round(benchmark_lift, 2)
    association_lift = max(0.0, association_lift)

    evidence_depth = clamp_percent((evidence_count * 18) + min(comment_count, 120) * 0.22 + frequency * 0.55)
    demand_heat = clamp_percent(negative * 0.85 + min(keyword_total, 100) * 0.15 + aspect_mention_rate * 0.65 + lift_score * 0.32 + max(0, radar_delta) * 1.4)
    pain_density = clamp_percent(concentration * 0.4 + negative * 0.28 + aspect_mention_rate * 0.8 + lift_score * 0.22)
    is_critical = product.get("radar_status") == "critical"
    fixability = clamp_percent(
        68 + min(22, len(factories) * 8) + (6 if is_critical else 0)
        if factories
        else 44 + max(0, 100 - score) * 0.24
    )
    risk_drag = clamp_percent(negative * 0.75 + max(0, 72 - score) * 0.62 + (18 if is_critical else 0))
    timing_window = clamp_percent(42 + max(0, radar_delta) * 1.8 + (18 if is_critical else 0) + max(0, negative - positive) * 0.25)
    gap_score = clamp_percent(
        demand_heat * 0.22
        + pain_density * 0.18
        + lift_score * 0.16
        + evidence_confidence * 0.14
        + evidence_depth * 0.10
        + fixability * 0.12
        + timing_window * 0.08
    )
    metrics = {
        "demandHeat": demand_heat,
        "painDensity": pain_density,
        "fixability": fixability,
        "riskDrag": risk_drag,
        "timingWindow": timing_window,
        "evidenceConfidence": evidence_confidence,
        "liftScore": lift_score,
        "evidenceDepth": evidence_depth,
    }
    top_pain = str(top_aspect.get("raw_label") or top_aspect.get("aspect") or (product.get("keywordLabels", [""])[0] if product.get("keywordLabels") else "需求认知不清"))
    gap_type = str(top_aspect.get("gap_type") or infer_market_gap_type_from_text(f"{top_pain} {product.get('direction', '')} {product.get('insight', '')}", fixability))
    priority = "P0 · 立即打样" if gap_score >= 78 else ("P1 · 本周验证" if gap_score >= 64 else "P2 · 观察池")
    evidence_line = (
        f"ABSA 证据显示“{top_pain}”相对公开类目基线约 {benchmark_lift:.1f}x"
        if benchmark_lift > 0
        else f"ABSA 证据显示“{top_pain}”是当前最高频属性级痛点"
    )
    thesis = f"{gap_type}：{evidence_line}，置信度 {evidence_confidence}%，当前负面率 {negative}%"
    if radar_delta > 0:
        thesis += f"，近 24H 抬升 {radar_delta}%"
    thesis += "。建议以“规避该痛点”的微创新版本切入，并用评论样例验证详情页卖点。"

    market_gap = {
        "schema": "tk_market_gap_v1",
        "product_key": product_key,
        "gap_score": gap_score,
        "gapScore": gap_score,
        "gap_type": gap_type,
        "gapType": gap_type,
        "priority": priority,
        "top_pain": top_pain,
        "topPain": top_pain,
        "sample_count": sample_count,
        "keyword_total": keyword_total,
        "negative": negative,
        "positive": positive,
        "score": score,
        "metrics": metrics,
        "association": {
            "support": support,
            "confidence": confidence_rule,
            "lift": association_lift,
            "sample_count": sample_count,
            "baseline_negative_rate": round(baseline_negative, 4),
            "aspect_frequency": frequency,
        },
        "benchmark_lift": benchmark_lift,
        "benchmarkLift": benchmark_lift,
        "thesis": thesis,
        "method": "server_association_weighted_gap_v1",
    }
    market_gap["metric_lineage"] = {
        "market_gap_score": build_gap_score_lineage(gap_score, metrics, sample_count),
        "association_signal": build_association_lineage(market_gap),
    }
    return market_gap

def enrich_product_for_dashboard(product: Dict[str, Any]) -> Dict[str, Any]:
    """补齐 score、direction、action 等前端表格字段。"""
    enriched = dict(product)
    sentiment = enriched.get("sentiment", [0, 100, 0])
    keyword_labels = enriched.get("keywordLabels", [])

    enriched.setdefault("score", calculate_score(sentiment))
    enriched.setdefault("direction", infer_direction(keyword_labels))
    enriched.setdefault("action", infer_action(sentiment))
    enriched.setdefault("radar_status", "normal")
    if enriched.get("radar_status") == "critical":
        enriched["recommended_factories"] = build_recommended_factories(enriched)
    else:
        enriched.setdefault("recommended_factories", [])
    enriched["aspect_terms"] = build_compatible_aspect_terms(enriched)
    enriched["evidence_ledger"] = build_compatible_evidence_ledger(enriched)
    enriched["market_benchmark"] = enriched["evidence_ledger"]["market_benchmark"]
    evidence_count = safe_int(enriched["evidence_ledger"].get("evidence_count"), 0)
    enriched["metric_lineage"] = dict(enriched.get("metric_lineage") or {})
    enriched["metric_lineage"]["health_score"] = build_health_score_lineage(
        safe_int(enriched.get("score"), calculate_score(sentiment)),
        sentiment if isinstance(sentiment, list) else [0, 100, 0],
        enriched["aspect_terms"],
        evidence_count,
    )
    enriched["metric_lineage"].update(enriched["evidence_ledger"].get("metric_lineage", {}))
    enriched["metric_lineage"]["sps_half_life"] = build_sps_lineage(enriched, evidence_count)
    enriched["market_gap"] = build_market_gap_snapshot(str(enriched.get("product_id") or enriched.get("product_key") or "product"), enriched)
    enriched["metric_lineage"].update(enriched["market_gap"].get("metric_lineage", {}))
    enriched["algorithm_support"] = build_algorithm_support_pack(enriched, enriched["market_gap"])
    enriched["market_gap"]["algorithm_support"] = enriched["algorithm_support"]
    enriched["market_gap"]["metric_lineage"] = {
        **enriched["market_gap"].get("metric_lineage", {}),
        **{key: value for key, value in enriched["metric_lineage"].items() if key in ("health_score", "evidence_trust", "aspect_lift", "radar_risk", "sps_half_life")},
    }
    return enriched


# =========================
# 24 小时舆情雷达巡检逻辑
# =========================

def get_product_source_url(product: Dict[str, Any]) -> str:
    """统一读取商品监控链接，兼容 source_url 与 url 两种字段。"""
    return str(product.get("source_url") or product.get("url") or "").strip()


def current_timestamp() -> str:
    """返回适合写入 JSON 与前端展示的本地时间戳。"""
    return time.strftime("%Y-%m-%d %H:%M:%S")


def load_admin_audit_logs() -> List[Dict[str, Any]]:
    """读取后台操作审计日志。"""
    logs = load_storage_json(ADMIN_AUDIT_STORE_KEY, ADMIN_AUDIT_LOGS_PATH, [])
    if isinstance(logs, list):
        return [item for item in logs if isinstance(item, dict)]
    return []


def save_admin_audit_logs(logs: List[Dict[str, Any]]) -> None:
    """保存最近 300 条后台操作审计日志。"""
    save_storage_json(ADMIN_AUDIT_STORE_KEY, ADMIN_AUDIT_LOGS_PATH, logs[-300:])


def append_admin_audit(action: str, detail: str, extra: Dict[str, Any] | None = None) -> None:
    """记录关键运营动作，便于排查误操作和数据恢复。"""
    try:
        logs = load_admin_audit_logs()
        logs.append({
            "timestamp": current_timestamp(),
            "action": action,
            "detail": detail,
            "extra": extra or {},
        })
        save_admin_audit_logs(logs)
    except Exception as exc:
        append_log(f"审计日志写入失败：{exc}")


def is_alert_webhook_enabled() -> bool:
    """判断是否已配置外部 Webhook 报警通道。"""
    return bool(ALERT_WEBHOOK_URL)


def load_alert_dedup_state() -> Dict[str, Any]:
    """读取告警去重状态，避免同一商品短时间重复推送。"""
    state = load_storage_json(ALERT_DEDUP_STORE_KEY, ALERT_DEDUP_PATH, {})
    return state if isinstance(state, dict) else {}


def save_alert_dedup_state(state: Dict[str, Any]) -> None:
    """保存告警去重状态。"""
    save_storage_json(ALERT_DEDUP_STORE_KEY, ALERT_DEDUP_PATH, state)


def build_alert_payload(
    product_key: str,
    product: Dict[str, Any],
    event_type: str = "radar_critical",
) -> Dict[str, Any]:
    """统一构造发送到外部通知平台的 JSON 载荷。"""
    product_name = str(product.get("product_name") or product_key)
    reason = str(product.get("radar_alert_reason") or "负面反馈异常波动")
    score = safe_int(product.get("score"), calculate_score(product.get("sentiment", [])))
    negative_ratio = extract_negative_ratio(product)
    dashboard_url = os.getenv("DASHBOARD_URL", "https://dashboard.void52.site")
    return {
        "event_type": event_type,
        "severity": "critical",
        "timestamp": current_timestamp(),
        "product_key": product_key,
        "product_id": product.get("product_id", product_key),
        "product_name": product_name,
        "score": score,
        "negative_ratio": negative_ratio,
        "reason": reason,
        "source_url": get_product_source_url(product),
        "dashboard_url": dashboard_url,
        "title": f"🚨 TK 舆情雷达红线：{product_name}",
        "text": (
            f"检测到商品 [{product_name}] 触发舆情红线：{reason}。"
            f" 当前健康分 {score}/100，负面占比 {negative_ratio}%。"
        ),
    }


def should_send_alert(dedup_key: str) -> bool:
    """根据冷却窗口判断是否允许发送本次告警。"""
    state = load_alert_dedup_state()
    now = int(time.time())
    last_sent_at = safe_int(state.get(dedup_key), 0)
    if last_sent_at and now - last_sent_at < ALERT_COOLDOWN_SECONDS:
        return False
    state[dedup_key] = now
    save_alert_dedup_state(state)
    return True


def detect_alert_webhook_provider() -> str:
    """根据 URL 自动识别常见 Webhook 平台，避免在环境变量中暴露额外配置。"""
    parsed = urlparse(ALERT_WEBHOOK_URL)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if "open.feishu.cn" in host and "/bot/" in path:
        return "feishu"
    return "generic"


def format_feishu_alert_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """把内部告警载荷转成飞书自定义机器人 text 消息结构。"""
    text = (
        f"{payload.get('title', '🚨 TK 舆情雷达红线')}\n\n"
        f"{payload.get('text', '')}\n"
        f"商品 ID：{payload.get('product_id', payload.get('product_key', ''))}\n"
        f"触发原因：{payload.get('reason', '')}\n"
        f"监控链接：{payload.get('source_url', '') or '未填写'}\n"
        f"看板地址：{payload.get('dashboard_url', 'https://dashboard.void52.site')}\n"
        f"触发时间：{payload.get('timestamp', current_timestamp())}"
    )
    return {
        "msg_type": "text",
        "content": {
            "text": text,
        },
    }


def format_alert_webhook_payload(payload: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    """返回平台类型与最终发送载荷。"""
    provider = detect_alert_webhook_provider()
    if provider == "feishu":
        return provider, format_feishu_alert_payload(payload)
    return provider, payload


def post_alert_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    """向外部 Webhook 推送 JSON 告警。"""
    if not is_alert_webhook_enabled():
        return {
            "sent": False,
            "reason": "ALERT_WEBHOOK_URL 未配置",
        }

    provider, outgoing_payload = format_alert_webhook_payload(payload)
    body = json.dumps(outgoing_payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "tk-ai-saas-alert-bot/1.0",
    }
    if ALERT_WEBHOOK_TOKEN:
        headers["Authorization"] = f"Bearer {ALERT_WEBHOOK_TOKEN}"

    request = urllib.request.Request(
        ALERT_WEBHOOK_URL,
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=ALERT_WEBHOOK_TIMEOUT_SECONDS) as response:
            response_body = response.read(500).decode("utf-8", errors="replace")
            return {
                "sent": 200 <= response.status < 300,
                "status_code": response.status,
                "provider": provider,
                "response_sample": response_body,
            }
    except urllib.error.HTTPError as exc:
        return {
            "sent": False,
            "status_code": exc.code,
            "provider": provider,
            "reason": exc.read(500).decode("utf-8", errors="replace"),
        }
    except Exception as exc:
        return {
            "sent": False,
            "provider": provider,
            "reason": str(exc),
        }


def send_radar_alert(product_key: str, product: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    """发送雷达告警；默认按商品和原因做冷却去重。"""
    payload = build_alert_payload(product_key, product)
    dedup_key = f"{payload['event_type']}:{product_key}:{payload['reason']}"
    if not force and not should_send_alert(dedup_key):
        return {
            "sent": False,
            "skipped": True,
            "reason": f"冷却窗口内已发送，{ALERT_COOLDOWN_SECONDS} 秒内不重复推送",
        }

    result = post_alert_webhook(payload)
    append_admin_audit(
        "send_alert",
        f"雷达告警推送：{payload['product_name']}，结果：{'成功' if result.get('sent') else '未发送/失败'}。",
        {"alert_result": result, "product_key": product_key},
    )
    return result


def build_alert_status() -> Dict[str, Any]:
    """返回报警通道配置状态，不暴露任何密钥。"""
    return {
        "status": "success",
        "enabled": is_alert_webhook_enabled(),
        "webhook_configured": bool(ALERT_WEBHOOK_URL),
        "token_configured": bool(ALERT_WEBHOOK_TOKEN),
        "provider": detect_alert_webhook_provider() if ALERT_WEBHOOK_URL else "",
        "cooldown_seconds": ALERT_COOLDOWN_SECONDS,
        "timeout_seconds": ALERT_WEBHOOK_TIMEOUT_SECONDS,
        "dedup_entries": len(load_alert_dedup_state()),
    }


def extract_radar_sample_size(product: Dict[str, Any], explicit_sample_size: int = 0) -> int:
    """估算本次雷达判断可用样本量，优先使用爬虫真实评论数。"""
    if explicit_sample_size > 0:
        return explicit_sample_size

    evidence = product.get("evidence_ledger") if isinstance(product.get("evidence_ledger"), dict) else {}
    keywords = product.get("keywords") if isinstance(product.get("keywords"), list) else []
    aspect_terms = product.get("aspect_terms") if isinstance(product.get("aspect_terms"), list) else []
    return max(
        safe_int(product.get("radar_sample_size"), 0),
        safe_int(evidence.get("comment_count"), 0),
        safe_int(evidence.get("evidence_count"), 0),
        sum(max(0, safe_int(value, 0)) for value in keywords),
        sum(max(0, safe_int(item.get("frequency"), 0)) for item in aspect_terms if isinstance(item, dict)),
    )


def build_radar_history_snapshot(
    product_key: str,
    product: Dict[str, Any],
    sample_size: int,
    source: str,
) -> Dict[str, Any]:
    """把一次诊断结果压缩成可长期保存的雷达时间序列点。"""
    sentiment = product.get("sentiment") if isinstance(product.get("sentiment"), list) else []
    return {
        "timestamp": current_timestamp(),
        "product_key": product_key,
        "product_id": product.get("product_id", product_key),
        "negative_ratio": extract_negative_ratio(product),
        "score": safe_int(product.get("score"), calculate_score(sentiment)),
        "sample_size": max(0, sample_size),
        "source": source or "pipeline",
    }


def calculate_ewma(values: List[float], alpha: float = RADAR_EWMA_ALPHA) -> float:
    """计算指数加权移动平均，用于在线漂移基线。"""
    cleaned = [safe_float(value) for value in values]
    if not cleaned:
        return 0.0
    alpha = max(0.01, min(1.0, alpha))
    ewma = cleaned[0]
    for value in cleaned[1:]:
        ewma = alpha * value + (1 - alpha) * ewma
    return ewma


def build_radar_drift_model(
    history_points: List[Dict[str, Any]],
    latest_negative: int,
    latest_score: int,
    sample_size: int,
) -> Dict[str, Any]:
    """基于历史负面率构造 EWMA 与二项近似漂移判断。"""
    prior_values = [
        safe_float(point.get("negative_ratio"), 0.0)
        for point in history_points
        if isinstance(point, dict)
    ]
    prior_values = [value for value in prior_values if 0 <= value <= 100]
    history_count = len(prior_values)
    baseline = sum(prior_values) / history_count if history_count else float(latest_negative)
    ewma = calculate_ewma(prior_values, RADAR_EWMA_ALPHA) if prior_values else baseline

    baseline_rate = max(0.01, min(0.99, baseline / 100))
    latest_rate = max(0.0, min(1.0, latest_negative / 100))
    effective_sample = max(1, sample_size)
    standard_error = math.sqrt(max(baseline_rate * (1 - baseline_rate), 0.0001) / effective_sample)
    z_like_delta = (latest_rate - baseline_rate) / standard_error if standard_error > 0 else 0.0
    drift_delta = latest_negative - ewma
    enough_history = history_count >= RADAR_MIN_HISTORY_POINTS
    enough_sample = sample_size >= RADAR_MIN_SAMPLE_SIZE
    ewma_breakdown = (
        enough_history
        and enough_sample
        and z_like_delta >= RADAR_Z_THRESHOLD
        and drift_delta >= RADAR_EWMA_DRIFT_MIN_DELTA
    )

    degradation_parts = []
    if not enough_history:
        degradation_parts.append(f"历史点仅 {history_count} 个，EWMA 漂移仅作观察。")
    if not enough_sample:
        degradation_parts.append(f"样本 {sample_size} 条低于 {RADAR_MIN_SAMPLE_SIZE} 条，已降低预警置信。")
    if not degradation_parts:
        degradation_parts.append("历史窗口与样本量满足当前轻量漂移判断要求。")

    return {
        "model": RADAR_MODEL_VERSION,
        "history_points": history_count,
        "baseline_negative_ratio": round(baseline, 1),
        "ewma_negative_ratio": round(ewma, 1),
        "z_like_delta": round(z_like_delta, 3),
        "drift_delta": round(drift_delta, 1),
        "ewma_breakdown": ewma_breakdown,
        "enough_history": enough_history,
        "enough_sample": enough_sample,
        "degradation": " ".join(degradation_parts),
    }


def append_radar_history(
    product_key: str,
    product: Dict[str, Any],
    sample_size: int,
    source: str,
) -> Dict[str, Any]:
    """追加当前快照并返回最新历史点。"""
    history = load_radar_history()
    points = history.get(product_key, [])
    snapshot = build_radar_history_snapshot(product_key, product, sample_size, source)
    points.append(snapshot)
    history[product_key] = points[-RADAR_HISTORY_MAX_POINTS:]
    save_radar_history(history)
    return snapshot


def send_test_alert() -> Dict[str, Any]:
    """发送一条不会污染商品数据的模拟告警。"""
    demo_product = {
        "product_id": "alert_test",
        "product_name": "Webhook 告警测试商品",
        "score": 51,
        "sentiment": [20, 20, 60],
        "radar_alert_reason": "Webhook 通道连通性测试",
        "source_url": "https://dashboard.void52.site",
    }
    result = send_radar_alert("alert_test", demo_product, force=True)
    append_log(f"告警通道测试完成：{'成功' if result.get('sent') else result.get('reason', '未发送')}。")
    return {
        "status": "success" if result.get("sent") else "failed",
        "alert_result": result,
        "alert_status": build_alert_status(),
    }


def apply_radar_evaluation(
    product_key: str,
    product: Dict[str, Any],
    previous_product: Dict[str, Any] | None = None,
    emit_alert: bool = False,
    sample_size: int = 0,
    history_source: str = "pipeline",
) -> Dict[str, Any]:
    """
    写入 24H 雷达状态。

    兼容旧红线规则，并叠加基于历史窗口的 EWMA 漂移检测：
    - 最新负面评价占比比上一轮增加 15 个百分点以上；
    - 或 AI 健康分跌破 60 分；
    - 或历史样本足够时，负面率相对 EWMA 基线出现显著漂移。
    """
    evaluated = dict(product)
    latest_negative = extract_negative_ratio(evaluated)
    latest_score = safe_int(evaluated.get("score"), calculate_score(evaluated.get("sentiment", [])))
    resolved_sample_size = extract_radar_sample_size(evaluated, sample_size)
    previous_negative = latest_negative

    if previous_product:
        previous_negative = extract_negative_ratio(previous_product)

    history = load_radar_history()
    history_points = history.get(product_key, [])
    drift_model = build_radar_drift_model(
        history_points,
        latest_negative,
        latest_score,
        resolved_sample_size,
    )

    negative_delta = latest_negative - previous_negative
    score_breakdown = latest_score < RADAR_SCORE_CRITICAL_THRESHOLD
    spike_breakdown = negative_delta >= RADAR_NEGATIVE_SPIKE_THRESHOLD
    ewma_breakdown = bool(drift_model.get("ewma_breakdown"))
    is_critical = spike_breakdown or score_breakdown or ewma_breakdown

    evaluated["radar_status"] = "critical" if is_critical else "normal"
    evaluated["radar_model"] = drift_model["model"]
    evaluated["radar_previous_negative_ratio"] = previous_negative
    evaluated["radar_latest_negative_ratio"] = latest_negative
    evaluated["radar_negative_delta"] = negative_delta
    evaluated["radar_history_points"] = drift_model["history_points"]
    evaluated["radar_baseline_negative_ratio"] = drift_model["baseline_negative_ratio"]
    evaluated["radar_ewma_negative_ratio"] = drift_model["ewma_negative_ratio"]
    evaluated["radar_z_like_delta"] = drift_model["z_like_delta"]
    evaluated["radar_drift_delta"] = drift_model["drift_delta"]
    evaluated["radar_sample_size"] = resolved_sample_size
    evaluated["radar_degradation"] = drift_model["degradation"]
    evaluated["radar_last_checked_at"] = current_timestamp()
    evaluated["radar_alert_reason"] = ""
    evaluated["metric_lineage"] = dict(evaluated.get("metric_lineage") or {})
    evaluated["metric_lineage"]["radar_risk"] = build_radar_lineage(evaluated, latest_score, resolved_sample_size)

    append_radar_history(product_key, evaluated, resolved_sample_size, history_source)

    if is_critical:
        reasons = []
        if spike_breakdown:
            reasons.append(f"负面占比上升 {negative_delta} 个百分点")
        if score_breakdown:
            reasons.append(f"健康分跌破 {RADAR_SCORE_CRITICAL_THRESHOLD} 分")
        if ewma_breakdown:
            reasons.append(
                f"相对历史基线漂移 {drift_model['drift_delta']} 个百分点，z≈{drift_model['z_like_delta']}"
            )
        evaluated["radar_alert_reason"] = "；".join(reasons)
        if emit_alert:
            product_name = str(evaluated.get("product_name") or product_key)
            append_log(
                f"🚨 [雷达预警]: 检测到商品 [{product_name}] 舆情出现重大质量波动，"
                "SPS 负面占比暴增！已自动拉响红线警报。"
            )
            alert_result = send_radar_alert(product_key, evaluated)
            if alert_result.get("sent"):
                append_log(f"📣 [报警通知]: 商品 [{product_name}] 红线告警已推送到外部 Webhook。")
            elif alert_result.get("skipped"):
                append_log(f"📣 [报警通知]: 商品 [{product_name}] 告警处于冷却窗口，已跳过重复推送。")
            else:
                append_log(f"📣 [报警通知]: Webhook 未发送，原因：{alert_result.get('reason', '未配置或失败')}。")

    return evaluated


def product_has_sentiment_snapshot(product: Dict[str, Any]) -> bool:
    sentiment = product.get("sentiment")
    return isinstance(sentiment, list) and len(sentiment) >= 3


def ensure_radar_fields_for_products(
    products: Dict[str, Dict[str, Any]],
) -> tuple[Dict[str, Dict[str, Any]], bool]:
    """为旧存量商品补齐新雷达字段；只在缺字段时执行一次，不重复污染历史。"""
    migrated: Dict[str, Dict[str, Any]] = {}
    changed = False
    for product_key, product in products.items():
        if not isinstance(product, dict):
            continue
        lineage = product.get("metric_lineage") if isinstance(product.get("metric_lineage"), dict) else {}
        missing_radar_model = not product.get("radar_model") or "radar_risk" not in lineage
        if missing_radar_model and not product.get("pending") and product_has_sentiment_snapshot(product):
            migrated[product_key] = apply_radar_evaluation(
                product_key,
                product,
                product,
                emit_alert=False,
                sample_size=extract_radar_sample_size(product),
                history_source="migration",
            )
            changed = True
        else:
            migrated[product_key] = product
    return migrated, changed


def build_radar_channel(product_key: str, product: Dict[str, Any], url: str) -> ChannelConfig:
    """把已注册商品转换成一次雷达巡检可复用的运行时通道。"""
    base_channel = detect_channel(url)
    product_id = str(product.get("product_id") or product_key).strip() or product_key
    product_name = str(product.get("product_name") or product_id).strip() or AUTO_PRODUCT_NAME
    return ChannelConfig(
        source_type=base_channel.source_type,
        script_path=base_channel.script_path,
        product_key=product_key,
        product_id=product_id,
        product_name=product_name,
        display_name=f"Radar -> {base_channel.source_type} / {product_id} / {product_name}",
    )


async def run_single_radar_check(product_key: str, previous_product: Dict[str, Any]) -> Dict[str, Any]:
    """对单个商品执行静默抓取、AI 诊断、NRR 红线评估与持久化。"""
    url = get_product_source_url(previous_product)
    if not url:
        return {
            "product_id": product_key,
            "status": "skipped",
            "reason": "未配置监控链接",
        }

    channel = build_radar_channel(product_key, previous_product, url)
    comments = await run_crawler_async(channel, url, DEFAULT_LIMIT)
    report = await run_ai_diagnose_async(channel)
    diagnosed_product = merge_report_into_products(
        channel,
        report,
        source_url=url,
        radar_previous_product=previous_product,
        emit_radar_alert=True,
        radar_sample_size=len(comments),
        radar_history_source="radar_patrol",
    )

    return {
        "product_id": product_key,
        "product_name": diagnosed_product.get("product_name", product_key),
        "status": diagnosed_product.get("radar_status", "normal"),
        "negative_ratio": diagnosed_product.get("radar_latest_negative_ratio", 0),
        "negative_delta": diagnosed_product.get("radar_negative_delta", 0),
        "baseline_negative_ratio": diagnosed_product.get("radar_baseline_negative_ratio", 0),
        "ewma_negative_ratio": diagnosed_product.get("radar_ewma_negative_ratio", 0),
        "z_like_delta": diagnosed_product.get("radar_z_like_delta", 0),
        "raw_comment_count": len(comments),
    }


async def run_radar_patrol_once(trigger: str = "manual") -> Dict[str, Any]:
    """执行一轮全库商品舆情雷达巡检。"""
    global RADAR_LAST_RUN_AT

    if RADAR_LOCK.locked():
        raise PipelineRuntimeError(409, "雷达巡检正在运行，请稍后再试。")
    if PIPELINE_LOCK.locked():
        raise PipelineRuntimeError(409, "诊断流水线正在运行，雷达巡检稍后再试。")

    async with RADAR_LOCK:
        async with PIPELINE_LOCK:
            products = load_products()
            targets = [
                (product_key, product)
                for product_key, product in products.items()
                if get_product_source_url(product)
            ]

            RADAR_LAST_RUN_AT = current_timestamp()
            append_log(f"📡 [雷达巡检]: 已启动 {trigger} 巡检，本轮目标商品 {len(targets)} 个。")
            results: List[Dict[str, Any]] = []

            for product_key, product in targets:
                try:
                    results.append(await run_single_radar_check(product_key, product))
                except Exception as exc:
                    append_log(f"📡 [雷达巡检]: 商品 {product_key} 巡检失败，原因：{exc}")
                    results.append({
                        "product_id": product_key,
                        "status": "failed",
                        "reason": str(exc),
                    })

            critical_count = sum(1 for item in results if item.get("status") == "critical")
            append_log(f"📡 [雷达巡检]: 本轮完成，触发红线 {critical_count} 个。")
            return {
                "status": "success",
                "trigger": trigger,
                "checked_count": len(results),
                "critical_count": critical_count,
                "last_run_at": RADAR_LAST_RUN_AT,
                "results": results,
            }


async def auto_radar_patrol_loop() -> None:
    """服务启动后常驻后台的全天候舆情雷达巡检协程。"""
    await asyncio.sleep(max(0, RADAR_PATROL_STARTUP_DELAY_SECONDS))
    while True:
        try:
            await run_radar_patrol_once(trigger="auto")
        except PipelineRuntimeError as exc:
            append_log(f"📡 [雷达巡检]: 本轮自动巡检跳过，原因：{exc.detail}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            append_log(f"📡 [雷达巡检]: 后台协程异常但已自愈继续，原因：{exc}")

        await asyncio.sleep(max(60, RADAR_PATROL_INTERVAL_SECONDS))


# =========================
# 竞品横向 VS 诊断逻辑
# =========================

def safe_int(value: Any, default: int = 0) -> int:
    """把模型或 JSON 中可能混入的字符串数值安全转换为整数。"""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def extract_negative_ratio(product: Dict[str, Any]) -> int:
    """读取产品负面情绪比例，sentiment 固定使用 [正面, 中性, 负面]。"""
    sentiment = product.get("sentiment", [])
    if isinstance(sentiment, list) and len(sentiment) >= 3:
        return max(0, min(100, safe_int(sentiment[2], 0)))
    return 0


def build_vs_snapshot(product_key: str, product: Dict[str, Any]) -> Dict[str, Any]:
    """把完整产品诊断记录压缩成模型和前端都需要的横向对比快照。"""
    enriched = enrich_product_for_dashboard(product)
    score = max(0, min(100, safe_int(enriched.get("score"), 0)))
    negative_ratio = extract_negative_ratio(enriched)
    keyword_labels = enriched.get("keywordLabels", [])
    keywords = enriched.get("keywords", [])
    evidence = enriched.get("evidence_ledger") if isinstance(enriched.get("evidence_ledger"), dict) else {}
    aspect_terms = enriched.get("aspect_terms") if isinstance(enriched.get("aspect_terms"), list) else []
    top_aspect = aspect_terms[0] if aspect_terms and isinstance(aspect_terms[0], dict) else {}

    top_complaints: List[Dict[str, Any]] = []
    if isinstance(keyword_labels, list):
        for idx, label in enumerate(keyword_labels[:5]):
            count = 0
            if isinstance(keywords, list) and idx < len(keywords):
                count = safe_int(keywords[idx], 0)
            top_complaints.append({
                "label": str(label),
                "count": count,
            })

    return {
        "product_key": product_key,
        "product_id": str(enriched.get("product_id") or product_key),
        "product_name": str(enriched.get("product_name") or product_key),
        "score": score,
        "negative_ratio": negative_ratio,
        "positive_ratio": safe_int(enriched.get("sentiment", [0, 0, 0])[0], 0)
        if isinstance(enriched.get("sentiment"), list) and len(enriched.get("sentiment", [])) >= 1
        else 0,
        "neutral_ratio": safe_int(enriched.get("sentiment", [0, 0, 0])[1], 0)
        if isinstance(enriched.get("sentiment"), list) and len(enriched.get("sentiment", [])) >= 2
        else 0,
        "top_complaints": top_complaints,
        "direction": str(enriched.get("direction") or "等待诊断生成优化方向"),
        "action": str(enriched.get("action") or "待诊断"),
        "insight": str(enriched.get("insight") or ""),
        "pending": bool(enriched.get("pending")),
        "top_aspect": top_aspect.get("raw_label") or top_aspect.get("aspect") or "",
        "benchmark_lift": top_aspect.get("benchmark_lift", 0),
        "evidence_confidence": evidence.get("confidence", top_aspect.get("confidence", 0)),
        "evidence_trust_score": evidence.get("evidence_trust_score", 0),
        "market_gap": enriched.get("market_gap", {}),
        "metric_lineage": enriched.get("metric_lineage", {}),
    }


def format_vs_report(raw_report: Dict[str, Any]) -> str:
    """把模型返回的结构化字段拼成前端可直接展示的策略书正文。"""
    title = str(raw_report.get("report_title") or "竞品差异化选品套利策略书")
    summary = str(raw_report.get("summary") or "").strip()
    recommendations = raw_report.get("recommendations", [])
    avoid_risks = raw_report.get("avoid_risks", [])

    sections = [f"《{title}》"]
    if summary:
        sections.append(summary)

    if isinstance(recommendations, list) and recommendations:
        sections.append("套利机会：")
        sections.extend([f"{idx + 1}. {item}" for idx, item in enumerate(recommendations)])

    if isinstance(avoid_risks, list) and avoid_risks:
        sections.append("避坑清单：")
        sections.extend([f"{idx + 1}. {item}" for idx, item in enumerate(avoid_risks)])

    return "\n".join(sections)


def build_rule_based_vs_report(snapshots: List[Dict[str, Any]]) -> Dict[str, Any]:
    """OpenAI 不可用时，根据现有产品指标生成稳定可解释的横向策略书。"""
    sorted_by_score = sorted(snapshots, key=lambda item: item["score"], reverse=True)
    sorted_by_risk = sorted(snapshots, key=lambda item: item["negative_ratio"], reverse=True)
    winner = sorted_by_score[0]
    riskiest = sorted_by_risk[0]
    weakest = sorted_by_score[-1]

    repeated_complaints: Dict[str, int] = {}
    for item in snapshots:
        for complaint in item["top_complaints"]:
            label = complaint["label"]
            repeated_complaints[label] = repeated_complaints.get(label, 0) + safe_int(complaint["count"], 0)

    common_pain = "、".join(
        label for label, _count in sorted(repeated_complaints.items(), key=lambda pair: pair[1], reverse=True)[:3]
    ) or "核心客诉样本不足"

    raw_report = {
        "report_title": "竞品差异化选品套利策略书",
        "summary": (
            f"本轮横向 PK 中，{winner['product_name']} 以 {winner['score']} 分处于相对优势；"
            f"{riskiest['product_name']} 的负面声量最高，达到 {riskiest['negative_ratio']}%。"
            f"跨品类共同痛点集中在 {common_pain}，适合作为新品详情页承诺和供应链验货标准。"
        ),
        "recommendations": [
            f"以 {winner['product_name']} 的高分卖点作为详情页锚点，提炼可复制的体验承诺。",
            f"针对 {weakest['product_name']} 暴露的短板做反向选品：优先开发能解决“{common_pain}”的改良款。",
            "投放测试时把健康分高、负面率低的商品作为流量入口，把高风险商品转入小预算验证池。",
        ],
        "avoid_risks": [
            f"避免继续放大 {riskiest['product_name']}，除非其 TOP 客诉已被供应链闭环修复。",
            "不要只看单品高分，需同步观察负面情绪占比和 TOP 客诉是否集中在不可逆缺陷。",
        ],
    }
    raw_report["report"] = format_vs_report(raw_report)
    return raw_report


def build_vs_prompt(snapshots: List[Dict[str, Any]]) -> str:
    """构造 GPT-5.5 横向交叉比对提示词。"""
    compact_payload = json.dumps(snapshots, ensure_ascii=False, indent=2)
    return f"""
你是“TK跨境电商竞品套利策略官”，擅长把多款竞品的评论诊断指标横向交叉比对，输出可执行的选品套利和避坑方案。

请基于以下商品诊断快照，比较健康分、负面情绪比例、TOP 客诉标签、供应链方向和投放动作：
{compact_payload}

输出要求：
1. 只输出合法 JSON 对象，不要 Markdown，不要代码块。
2. 必须包含字段：
{{
  "report_title": "竞品差异化选品套利策略书",
  "summary": "一段 120-180 字的横向结论",
  "recommendations": ["3 条套利机会，每条不超过 60 字"],
  "avoid_risks": ["2-3 条避坑建议，每条不超过 60 字"]
}}
3. 结论必须点名具体商品名称，不能泛泛而谈。
4. 重点回答：谁适合放量、谁需要修复、哪个客诉可被新品反向套利。
""".strip()


def call_vs_model(snapshots: List[Dict[str, Any]]) -> Dict[str, Any]:
    """调用 OpenAI/sub2api 的 gpt-5.5 进行横向 VS 报告生成。"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，已切换本地规则兜底。")

    from ai_diagnose import (  # 延迟导入，避免后端启动时被可选依赖阻塞。
        DEFAULT_MODEL,
        DEFAULT_OPENAI_API_STYLE,
        DEFAULT_OPENAI_BASE_URL,
        DEFAULT_TIMEOUT_SECONDS,
        build_openai_client,
        call_openai_with_retry,
    )

    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model_name = os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL)
    api_style = os.getenv("OPENAI_API_STYLE", DEFAULT_OPENAI_API_STYLE)
    timeout_seconds = safe_int(os.getenv("OPENAI_TIMEOUT_SECONDS"), DEFAULT_TIMEOUT_SECONDS)
    client = build_openai_client(api_key, base_url, timeout_seconds)
    prompt = build_vs_prompt(snapshots)
    raw_report = call_openai_with_retry(client, model_name, prompt, timeout_seconds, api_style)
    if not isinstance(raw_report, dict):
        raise RuntimeError("VS 模型返回结果不是 JSON 对象。")

    raw_report.setdefault("report_title", "竞品差异化选品套利策略书")
    raw_report.setdefault("summary", "")
    raw_report.setdefault("recommendations", [])
    raw_report.setdefault("avoid_risks", [])
    raw_report["report"] = format_vs_report(raw_report)
    return raw_report


def persist_vs_report(report_payload: Dict[str, Any]) -> None:
    """把最新 VS 诊断结果追加保存到独立报告文件，避免污染产品字典结构。"""
    history = load_vs_reports()
    history.append(report_payload)
    save_vs_reports(history)


def run_vs_pipeline(payload: VsPipelineRequest) -> Dict[str, Any]:
    """读取多商品指标并生成横向竞品 PK 报告。"""
    requested_ids = []
    for raw_id in payload.product_ids:
        product_id = str(raw_id).strip()
        if product_id and product_id not in requested_ids:
            requested_ids.append(product_id)

    if len(requested_ids) < 2 or len(requested_ids) > 3:
        raise PipelineRuntimeError(400, "请选择 2-3 个商品进行横向 PK。")

    products = load_products()
    missing_ids = [product_id for product_id in requested_ids if product_id not in products]
    if missing_ids:
        raise PipelineRuntimeError(404, f"以下商品不存在：{', '.join(missing_ids)}")

    snapshots = [build_vs_snapshot(product_id, products[product_id]) for product_id in requested_ids]
    if any(item["pending"] for item in snapshots):
        pending_names = [item["product_name"] for item in snapshots if item["pending"]]
        raise PipelineRuntimeError(400, f"以下商品尚未完成单品诊断，暂不能 PK：{', '.join(pending_names)}")

    append_log(f"开始执行竞品横向 PK：{' vs '.join(item['product_name'] for item in snapshots)}。")
    model_used = os.getenv("OPENAI_MODEL_NAME", "gpt-5.5")
    try:
        raw_report = call_vs_model(snapshots)
        report_source = "openai"
        append_log("竞品横向 PK 已完成 GPT-5.5 合并诊断。")
    except Exception as exc:
        raw_report = build_rule_based_vs_report(snapshots)
        report_source = "local_fallback"
        append_log(f"竞品横向 PK 已启用本地规则兜底：{exc}")

    report_id = hashlib.sha1(
        f"{time.time()}:{','.join(requested_ids)}".encode("utf-8")
    ).hexdigest()[:12]
    generated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    response_payload = {
        "status": "success",
        "report_id": report_id,
        "generated_at": generated_at,
        "product_ids": requested_ids,
        "selected_products": snapshots,
        "chart_labels": ["AI 健康分", "负面情绪占比"],
        "chart_datasets": [
            {
                "product_id": item["product_id"],
                "product_name": item["product_name"],
                "data": [item["score"], item["negative_ratio"]],
                "score": item["score"],
                "negative_ratio": item["negative_ratio"],
            }
            for item in snapshots
        ],
        "report": raw_report["report"],
        "structured_report": raw_report,
        "model_used": model_used,
        "report_source": report_source,
    }
    persist_vs_report(response_payload)
    return response_payload


def build_admin_export_payload() -> Dict[str, Any]:
    """打包当前云端业务数据，供运营手动下载备份。"""
    products = load_products()
    vs_reports = load_vs_reports()
    audit_logs = load_admin_audit_logs()
    return {
        "schema": "tk-ai-saas-backup-v1",
        "exported_at": current_timestamp(),
        "storage_backend": get_storage_backend_name(),
        "products_count": len(products),
        "vs_reports_count": len(vs_reports),
        "products": products,
        "vs_reports": vs_reports,
        "audit_logs": audit_logs[-100:],
    }


def restore_admin_backup(payload: AdminRestoreRequest) -> Dict[str, Any]:
    """用上传备份替换当前云端产品与 VS 报告数据。"""
    products = ensure_products_shape(payload.products)
    vs_reports = [item for item in payload.vs_reports if isinstance(item, dict)]
    save_products(products)
    save_vs_reports(vs_reports)
    append_admin_audit(
        "restore_data",
        f"恢复备份完成：商品 {len(products)} 个，VS 报告 {len(vs_reports[-50:])} 条。",
    )
    append_log(f"管理备份恢复完成：商品 {len(products)} 个，VS 报告 {len(vs_reports[-50:])} 条。")
    return {
        "status": "success",
        "restored_at": current_timestamp(),
        "products_count": len(products),
        "vs_reports_count": len(vs_reports[-50:]),
    }




# =========================
# AI 综合经营报告生成逻辑
# =========================

def is_product_diagnosed(product: Dict[str, Any]) -> bool:
    """判断商品是否已经完成真实诊断，过滤待抓取占位节点。"""
    if product.get("pending") is True:
        return False
    sentiment = product.get("sentiment", [])
    if not isinstance(sentiment, list) or len(sentiment) < 3:
        return False
    score = safe_int(product.get("score"), 100)
    return not (score >= 100 and safe_int(sentiment[1], 0) >= 100)


def product_display_name(product_key: str, product: Dict[str, Any]) -> str:
    return str(product.get("product_name") or product.get("product_id") or product_key)


def get_product_top_pain(product: Dict[str, Any]) -> str:
    aspect_terms = product.get("aspect_terms") if isinstance(product.get("aspect_terms"), list) else []
    if aspect_terms and isinstance(aspect_terms[0], dict):
        return str(aspect_terms[0].get("raw_label") or aspect_terms[0].get("aspect") or "核心痛点待确认")
    labels = product.get("keywordLabels") if isinstance(product.get("keywordLabels"), list) else []
    return str(labels[0]) if labels else "核心痛点待确认"


def build_executive_scorecard(
    total_products: int,
    diagnosed_count: int,
    average_score: int,
    critical_count: int,
    evidence_trust: int,
    evidence_items: int,
) -> List[Dict[str, Any]]:
    """首页报告只读取四个核心指标，复杂计算留在后台依据包。"""
    coverage = round((diagnosed_count / total_products) * 100) if total_products else 0
    return [
        {
            "metric_id": "health_score",
            "label": "整体健康分",
            "value": average_score if diagnosed_count else None,
            "unit": "/100",
            "status": "good" if average_score >= 85 else ("watch" if average_score >= 70 else "risk"),
            "explain": "用已诊断 SKU 的情感健康分均值衡量盘面质量。",
        },
        {
            "metric_id": "risk_products",
            "label": "风险商品",
            "value": critical_count,
            "unit": "个",
            "status": "risk" if critical_count else "good",
            "explain": "触发雷达红线或低分阈值的商品数量。",
        },
        {
            "metric_id": "evidence_trust",
            "label": "证据可信度",
            "value": evidence_trust if diagnosed_count else None,
            "unit": "%",
            "status": "good" if evidence_trust >= 65 else ("watch" if evidence_trust >= 40 else "thin"),
            "explain": "按 Wilson 下界、样本覆盖和来源覆盖折算。",
        },
        {
            "metric_id": "diagnostic_coverage",
            "label": "诊断覆盖率",
            "value": coverage,
            "unit": "%",
            "status": "good" if coverage >= 80 else ("watch" if coverage >= 40 else "thin"),
            "explain": "已完成真实评论诊断的商品占比。",
        },
    ]


def build_executive_method_lineage(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """汇总首页四个指标背后的方法依据，供后台折叠展示。"""
    metric_lineage = snapshot.get("metric_lineage") if isinstance(snapshot.get("metric_lineage"), dict) else {}
    scorecard = snapshot.get("scorecard") if isinstance(snapshot.get("scorecard"), list) else []
    lineage: Dict[str, Dict[str, Any]] = {}

    health_lineage = metric_lineage.get("health_score") if isinstance(metric_lineage.get("health_score"), dict) else {}
    lineage["health_score"] = {
        "label": "整体健康分",
        "formula": "已诊断 SKU 情感健康分均值；单品分由负面率、核心痛点和证据置信共同解释。",
        "basis": f"当前已诊断 {snapshot.get('diagnosed_count', 0)} 个商品，平均分 {snapshot.get('average_score', 0)}。",
        "sources": health_lineage.get("sources") or [reference_source("review_mining"), reference_source("absa")],
        "degradation": health_lineage.get("degradation") or "未诊断商品不会进入均值，避免占位数据污染结论。",
    }
    radar_lineage = metric_lineage.get("radar_risk") if isinstance(metric_lineage.get("radar_risk"), dict) else {}
    lineage["risk_products"] = {
        "label": "风险商品",
        "formula": "雷达红线商品数 + 低健康分商品优先级排序。",
        "basis": f"红线 {snapshot.get('critical_count', 0)} 个；Top 风险池 {len(snapshot.get('top_risks', []))} 个。",
        "sources": radar_lineage.get("sources") or [reference_source("nab"), reference_source("ewma_spc")],
        "degradation": radar_lineage.get("degradation") or "历史点不足时保留阈值规则，漂移模型只作观察。",
    }
    evidence_lineage = metric_lineage.get("evidence_trust") if isinstance(metric_lineage.get("evidence_trust"), dict) else {}
    lineage["evidence_trust"] = {
        "label": "证据可信度",
        "formula": "Wilson 下界、样本覆盖、来源覆盖加权。",
        "basis": f"证据条目 {snapshot.get('evidence_items', 0)}，综合可信度 {snapshot.get('evidence_trust', 0)}%。",
        "sources": evidence_lineage.get("sources") or [reference_source("wilson"), reference_source("ragas")],
        "degradation": evidence_lineage.get("degradation") or "样本薄时主动降低结论强度，防止 GPT 报告过度自信。",
    }
    coverage_item = next((item for item in scorecard if item.get("metric_id") == "diagnostic_coverage"), {})
    lineage["diagnostic_coverage"] = {
        "label": "诊断覆盖率",
        "formula": "已诊断商品数 / 全部监控商品数。",
        "basis": f"{snapshot.get('diagnosed_count', 0)} / {snapshot.get('total_products', 0)} 个商品已完成诊断。",
        "sources": [reference_source("evidently")],
        "degradation": coverage_item.get("explain") or "覆盖不足时，报告优先提示补样本而非强行给经营结论。",
    }
    return lineage


def build_executive_report_snapshot(products: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """压缩全盘商品指标，只保留首页经营报告需要的核心信号。"""
    diagnosed_items = [
        (key, enrich_product_for_dashboard(value))
        for key, value in products.items()
        if isinstance(value, dict) and is_product_diagnosed(value)
    ]
    total_products = len(products)
    diagnosed_count = len(diagnosed_items)
    average_score = round(sum(safe_int(item.get("score"), 0) for _, item in diagnosed_items) / diagnosed_count) if diagnosed_count else 0
    critical_items = [(key, item) for key, item in diagnosed_items if item.get("radar_status") == "critical"]

    evidence_confidences: List[float] = []
    evidence_items = 0
    top_risks: List[Dict[str, Any]] = []
    highlights: List[Dict[str, str]] = []
    for key, item in diagnosed_items:
        snapshot = build_vs_snapshot(key, item)
        evidence = item.get("evidence_ledger") if isinstance(item.get("evidence_ledger"), dict) else {}
        aspect_terms = item.get("aspect_terms") if isinstance(item.get("aspect_terms"), list) else []
        trust_score = safe_float(evidence.get("evidence_trust_score"), 0.0)
        confidence = safe_float(evidence.get("confidence"), 0.0)
        if confidence <= 0 and aspect_terms and isinstance(aspect_terms[0], dict):
            confidence = safe_float(aspect_terms[0].get("confidence"), 0.0)
        if trust_score > 0:
            evidence_confidences.append(trust_score / 100)
        elif confidence > 0:
            evidence_confidences.append(confidence)
        evidence_count = safe_int(evidence.get("evidence_count"), 0)
        evidence_items += evidence_count
        market_gap = item.get("market_gap") if isinstance(item.get("market_gap"), dict) else {}
        support_pack = item.get("algorithm_support") if isinstance(item.get("algorithm_support"), dict) else {}
        risk_reason = str(item.get("radar_alert_reason") or item.get("direction") or "等待进一步归因")
        top_risks.append({
            "product_id": snapshot["product_id"],
            "product_name": snapshot["product_name"],
            "score": snapshot["score"],
            "negative_ratio": snapshot["negative_ratio"],
            "top_complaints": snapshot["top_complaints"][:3],
            "top_pain": get_product_top_pain(item),
            "direction": snapshot["direction"],
            "action": snapshot["action"],
            "radar_status": item.get("radar_status", "normal"),
            "risk_reason": risk_reason,
            "market_gap_score": market_gap.get("gap_score", 0),
            "evidence_trust_score": evidence.get("evidence_trust_score", 0),
            "evidence_count": evidence_count,
            "evidence_verdict": support_pack.get("verdict", ""),
        })
        highlights.append({
            "product_name": snapshot["product_name"],
            "signal": f"健康分 {snapshot['score']}，负面率 {snapshot['negative_ratio']}%",
            "evidence": f"核心痛点：{get_product_top_pain(item)}；证据 {evidence_count} 条",
            "action": snapshot["action"],
        })

    top_risks.sort(key=lambda item: (item["radar_status"] != "critical", item["score"], -item["negative_ratio"]))
    highlights = highlights[:5]
    evidence_trust = round((sum(evidence_confidences) / len(evidence_confidences)) * 100) if evidence_confidences else 0
    if evidence_trust == 0 and evidence_items > 0:
        evidence_trust = min(88, 48 + evidence_items * 4)

    suggested_action = "保持巡检，优先放大健康商品。"
    if critical_items:
        names = "、".join(item.get("product_name") or key for key, item in critical_items[:3])
        suggested_action = f"先处理 {names} 的红线客诉，再恢复投放。"
    elif diagnosed_count < total_products:
        suggested_action = "先补齐未诊断商品，再做横向选品判断。"
    elif average_score < 75:
        suggested_action = "收缩预算，集中修复低分商品的供应链问题。"

    lineage_entries: Dict[str, Dict[str, Any]] = {}
    support_methods: Dict[str, Dict[str, Any]] = {}
    support_verdicts: List[str] = []
    for _, item in diagnosed_items:
        for metric_id, entry in (item.get("metric_lineage") or {}).items():
            if isinstance(entry, dict) and metric_id not in lineage_entries:
                lineage_entries[metric_id] = entry
        support_pack = item.get("algorithm_support") if isinstance(item.get("algorithm_support"), dict) else {}
        if support_pack.get("verdict"):
            support_verdicts.append(str(support_pack["verdict"]))
        for method in support_pack.get("primary_methods", []):
            if isinstance(method, dict) and method.get("method_id"):
                support_methods[str(method["method_id"])] = method

    scorecard = build_executive_scorecard(total_products, diagnosed_count, average_score, len(critical_items), evidence_trust, evidence_items)
    snapshot: Dict[str, Any] = {
        "schema": "tk_executive_report_snapshot_v2",
        "total_products": total_products,
        "diagnosed_count": diagnosed_count,
        "average_score": average_score,
        "critical_count": len(critical_items),
        "evidence_trust": evidence_trust,
        "evidence_items": evidence_items,
        "suggested_action": suggested_action,
        "top_risks": top_risks[:5],
        "highlights": highlights,
        "scorecard": scorecard,
        "metric_lineage": lineage_entries,
        "algorithm_support": {
            "schema": "tk_algorithm_support_summary_v2",
            "methods": list(support_methods.values()),
            "verdicts": support_verdicts[:5],
            "references": [
                reference_source("review_mining"),
                reference_source("absa"),
                reference_source("wilson"),
                reference_source("nab"),
                reference_source("evidently"),
                reference_source("ragas"),
                reference_source("recbole"),
            ],
        },
    }
    snapshot["method_lineage"] = build_executive_method_lineage(snapshot)
    return snapshot


def build_rule_based_executive_sections(snapshot: Dict[str, Any]) -> Dict[str, str]:
    """模型不可用时生成稳定的三段式中文经营复盘。"""
    if snapshot["diagnosed_count"] == 0:
        return {
            "summary": "当前还没有完成诊断的商品，首页经营结论暂不具备足够样本支撑。",
            "risk": "风险原因尚未形成有效归因，需要先完成核心 SKU 的评论抓取、情感诊断与证据沉淀。",
            "action": "先选择 1-3 个核心商品启动 AI 诊断，再回到首页生成完整经营复盘。",
        }

    score = snapshot["average_score"]
    critical = snapshot["critical_count"]
    trust = snapshot["evidence_trust"]
    risk_names = "、".join(item["product_name"] for item in snapshot["top_risks"][:3]) or "暂无明确风险商品"
    top_pain = snapshot["top_risks"][0].get("top_pain") if snapshot.get("top_risks") else "核心痛点待确认"

    if score >= 85:
        summary = f"当前已诊断商品平均健康分为 {score}，整体处于可控放量区间；建议把预算集中给证据可信度更高的健康 SKU。"
    elif score >= 70:
        summary = f"当前已诊断商品平均健康分为 {score}，经营盘面处于修复区间，适合先稳住预算节奏再做小批量验证。"
    else:
        summary = f"当前已诊断商品平均健康分为 {score}，盘面风险偏高，应暂停高风险商品的继续放量。"

    if critical:
        risk = f"当前有 {critical} 个商品触发红线，优先关注 {risk_names}；主要风险集中在“{top_pain}”及相关供应链体验波动。"
    else:
        risk = f"当前未发现红线商品，重点风险来自样本覆盖不足与证据沉淀深度，需继续验证 {risk_names} 的真实用户反馈。"
    if trust >= 65:
        risk += f" 证据可信度约 {trust}%，已能支撑本轮经营判断。"
    else:
        trust_label = trust if trust > 0 else "不足"
        risk += f" 证据可信度约 {trust_label}，报告已按低样本口径降权，建议继续抓取评论样本。"

    action = snapshot["suggested_action"]
    return {
        "summary": summary,
        "risk": risk,
        "action": action,
    }


def flatten_executive_sections(sections: Dict[str, str]) -> str:
    """把三段式报告压平，兼容旧版 report 字段。"""
    ordered = [sections.get("summary", ""), sections.get("risk", ""), sections.get("action", "")]
    return "".join(part.strip() for part in ordered if part and part.strip())


def normalize_executive_sections(value: Any, fallback: Dict[str, str]) -> Dict[str, str]:
    """校验模型返回的结构化报告，缺字段时回落本地兜底。"""
    if not isinstance(value, dict):
        return fallback

    normalized: Dict[str, str] = {}
    for key in ("summary", "risk", "action"):
        text = str(value.get(key, "")).strip()
        normalized[key] = text or fallback[key]
    return normalized


def parse_executive_sections(raw_text: str, fallback: Dict[str, str]) -> Dict[str, str]:
    """解析模型 JSON 输出；失败时把纯文本放入摘要并保留兜底动作。"""
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        payload = json.loads(cleaned)
        return normalize_executive_sections(payload, fallback)
    except Exception:
        if cleaned:
            return {
                "summary": cleaned,
                "risk": fallback["risk"],
                "action": fallback["action"],
            }
        return fallback


def build_rule_based_executive_report(snapshot: Dict[str, Any]) -> str:
    """模型不可用时生成稳定的中文经营复盘，兼容旧调用。"""
    return flatten_executive_sections(build_rule_based_executive_sections(snapshot))


def build_executive_report_prompt(snapshot: Dict[str, Any]) -> str:
    """构造 GPT-5.5 综合经营报告 Prompt。"""
    compact_payload = json.dumps({
        "scorecard": snapshot.get("scorecard", []),
        "top_risks": snapshot.get("top_risks", []),
        "highlights": snapshot.get("highlights", []),
        "suggested_action": snapshot.get("suggested_action", ""),
        "method_lineage": snapshot.get("method_lineage", {}),
    }, ensure_ascii=False, indent=2)
    return f"""
你是 TK 跨境电商 AI 经营分析官。请只基于下方证据快照，输出首页可展示的三段式中文经营报告。

证据快照：
{compact_payload}

输出要求：
1. 只输出严格 JSON，不要 Markdown，不要代码块，不要额外解释。
2. JSON 必须包含三个字符串字段：summary、risk、action。
3. summary 写经营摘要，解释整体健康分和当前盘面状态，控制在 45-90 字。
4. risk 写风险原因，必须引用具体商品、痛点或证据可信度，控制在 60-120 字。
5. action 写下一步动作，只给清晰可执行的运营建议，控制在 35-80 字。
6. 不要堆砌 Lift、Confidence、ABSA 等后台术语；如果必须提及，请转译成中文经营含义。
7. 禁止编造证据快照之外的 GMV、销量、市场份额或平台规则。
""".strip()


def call_executive_report_model(snapshot: Dict[str, Any]) -> Dict[str, str]:
    """调用 OpenAI/sub2api 的 GPT-5.5 生成首页综合经营报告。"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，使用本地经营报告兜底。")

    from ai_diagnose import (
        DEFAULT_MODEL,
        DEFAULT_OPENAI_API_STYLE,
        DEFAULT_OPENAI_BASE_URL,
        DEFAULT_TIMEOUT_SECONDS,
        build_openai_client,
        extract_responses_text,
    )

    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model_name = os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL)
    api_style = os.getenv("OPENAI_API_STYLE", DEFAULT_OPENAI_API_STYLE)
    timeout_seconds = safe_int(os.getenv("OPENAI_TIMEOUT_SECONDS"), DEFAULT_TIMEOUT_SECONDS)
    client = build_openai_client(api_key, base_url, timeout_seconds)
    prompt = build_executive_report_prompt(snapshot)

    if api_style == "responses":
        response = client.responses.create(model=model_name, input=prompt, timeout=timeout_seconds)
        text = extract_responses_text(response).strip()
    else:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "你输出简洁、可信、可执行的三段式中文经营分析，并严格返回 JSON。"},
                {"role": "user", "content": prompt},
            ],
            timeout=timeout_seconds,
        )
        text = (response.choices[0].message.content or "").strip()

    if not text:
        raise RuntimeError("GPT-5.5 返回的综合经营报告为空。")
    fallback = build_rule_based_executive_sections(snapshot)
    return parse_executive_sections(text, fallback)


def generate_executive_report(products: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """生成首页 AI 综合经营报告，优先模型、失败走本地兜底。"""
    snapshot = build_executive_report_snapshot(products)
    model_used = os.getenv("OPENAI_MODEL_NAME", "gpt-5.5")
    try:
        sections = call_executive_report_model(snapshot)
        source = "openai"
    except Exception as exc:
        sections = build_rule_based_executive_sections(snapshot)
        source = "local_fallback"
        append_log(f"AI 综合经营报告已启用本地兜底：{exc}")

    support = snapshot.get("algorithm_support", {}) if isinstance(snapshot.get("algorithm_support"), dict) else {}
    citations = support.get("references", []) if isinstance(support.get("references"), list) else []
    return {
        "status": "success",
        "schema": "tk_executive_report_v2",
        "report": flatten_executive_sections(sections),
        "sections": sections,
        "summary": snapshot,
        "scorecard": snapshot.get("scorecard", []),
        "highlights": snapshot.get("highlights", []),
        "method_lineage": snapshot.get("method_lineage", {}),
        "citations": citations,
        "source": source,
        "model_used": model_used,
        "generated_at": current_timestamp(),
    }

# =========================
# AI 决策 Brief 导出 2.0
# =========================

BRIEF_TYPE_CONFIG: Dict[str, Dict[str, str]] = {
    "supply_chain": {
        "label": "供应链改良 Brief",
        "title": "《供应链改良执行 Brief》",
        "role": "跨境电商供应链质量负责人",
        "focus": "把评论证据转译为材料、结构、包材、质检和打样验收动作。",
    },
    "appeal": {
        "label": "运营申诉 Brief",
        "title": "《平台申诉证据准备 Brief》",
        "role": "TikTok Shop 运营合规负责人",
        "focus": "把异常负面声浪转译为申诉证据清单、平台沟通口径和内部整改记录。",
    },
    "selection": {
        "label": "选品决策 Brief",
        "title": "《选品决策与差异化 Brief》",
        "role": "跨境选品策略负责人",
        "focus": "把客诉缺口、竞品弱点和证据可信度转译为下一轮上新或调款策略。",
    },
}


def normalize_brief_type(raw_type: str) -> str:
    """把前端 brief 类型归一到白名单，避免任意 prompt 注入式类型扩散。"""
    brief_type = (raw_type or "").strip().lower().replace("-", "_")
    aliases = {
        "supply": "supply_chain",
        "factory": "supply_chain",
        "sourcing": "supply_chain",
        "operation": "appeal",
        "ops": "appeal",
        "appeals": "appeal",
        "product": "selection",
        "market": "selection",
    }
    brief_type = aliases.get(brief_type, brief_type)
    if brief_type not in BRIEF_TYPE_CONFIG:
        raise PipelineRuntimeError(400, "不支持的 Brief 类型，请选择 supply_chain / appeal / selection。")
    return brief_type


def choose_brief_product_id(products: Dict[str, Dict[str, Any]], requested_id: str = "") -> str:
    """选择最适合生成 Brief 的商品：显式商品优先，其次红线/低分/有诊断数据商品。"""
    requested_id = (requested_id or "").strip()
    if requested_id and requested_id in products:
        return requested_id
    if not products:
        raise PipelineRuntimeError(404, "当前没有可用于生成 Brief 的商品数据。")

    def priority(item: tuple[str, Dict[str, Any]]) -> tuple[int, int, int, str]:
        key, product = item
        score = safe_int(product.get("score"), 100)
        has_data = 1 if product.get("sentiment") or product.get("keywordLabels") else 0
        radar_boost = 1 if product.get("radar_status") == "critical" else 0
        return (radar_boost, has_data, 100 - score, key)

    return max(products.items(), key=priority)[0]


def build_executive_brief_context(payload: ExecutiveBriefRequest) -> Dict[str, Any]:
    """合并商品、经营报告、算法依据，形成三类 Brief 共用证据包。"""
    brief_type = normalize_brief_type(payload.brief_type)
    products = payload.products if payload.products else load_products()
    if not products:
        raise PipelineRuntimeError(404, "当前没有可用于生成 Brief 的商品数据。")

    product_id = choose_brief_product_id(products, payload.product_id)
    product = products[product_id]
    snapshot = build_executive_report_snapshot(products)
    report = payload.executive_report if isinstance(payload.executive_report, dict) else {}
    report_sections = report.get("sections") if isinstance(report.get("sections"), dict) else {}
    top_complaints = [
        {"label": str(label), "count": safe_int(count, 0)}
        for label, count in zip(product.get("keywordLabels", [])[:5], product.get("keywords", [])[:5])
    ]
    evidence_sources = [
        reference_source("review_mining"),
        reference_source("absa"),
        reference_source("wilson"),
        reference_source("nab"),
        reference_source("evidently"),
        reference_source("ragas"),
    ]
    if brief_type == "selection":
        evidence_sources.append(reference_source("recbole"))
    if brief_type == "supply_chain":
        evidence_sources.append(reference_source("association_rules"))

    return {
        "brief_type": brief_type,
        "config": BRIEF_TYPE_CONFIG[brief_type],
        "product_id": product_id,
        "product": {
            "product_id": product.get("product_id", product_id),
            "product_name": product.get("product_name", product_id),
            "score": safe_int(product.get("score"), 100),
            "sentiment": product.get("sentiment", []),
            "top_complaints": top_complaints,
            "insight": product.get("insight", ""),
            "direction": product.get("direction", ""),
            "action": product.get("action", ""),
            "radar_status": product.get("radar_status", "normal"),
            "radar_alert_reason": product.get("radar_alert_reason", ""),
        },
        "executive_sections": {
            "summary": str(report_sections.get("summary") or ""),
            "risk": str(report_sections.get("risk") or ""),
            "action": str(report_sections.get("action") or ""),
        },
        "scorecard": snapshot.get("scorecard", []),
        "highlights": snapshot.get("highlights", []),
        "method_lineage": snapshot.get("method_lineage", {}),
        "evidence_sources": evidence_sources,
        "generated_at": current_timestamp(),
    }


def build_executive_brief_prompt(context: Dict[str, Any]) -> str:
    """构造三类经营 Brief 共用 Prompt，强制证据约束。"""
    config = context["config"]
    payload = json.dumps(context, ensure_ascii=False, indent=2)
    return f"""
你是{config['role']}。请基于证据包生成{config['title']}。

核心目标：{config['focus']}

证据包如下：
{payload}

输出要求：
1. 只输出中文 Markdown/Text，不要代码块。
2. 必须包含：标题、适用商品、关键判断、证据依据、执行动作、验收口径、风险备注。
3. 每个关键动作必须对应证据包中的评论痛点、健康分、风险状态、经营报告或算法依据之一。
4. 禁止编造 GMV、销量、市场份额、平台规则、真实供应商承诺或证据包之外的数据。
5. 不要堆砌英文算法名；如果引用方法，请翻译成经营语言，例如“样本少时保守降权”“异常波动观察”。
6. 风格要像能直接发给老板、运营或供应链同事执行的专业 Brief，简洁有力。
""".strip()


def build_rule_based_executive_brief(context: Dict[str, Any]) -> str:
    """模型不可用时生成结构完整、证据约束的本地 Brief。"""
    config = context["config"]
    product = context["product"]
    complaints = product.get("top_complaints", [])
    complaint_text = "、".join(
        f"{item.get('label')}({item.get('count', 0)})" for item in complaints if item.get("label")
    ) or "当前评论痛点样本不足"
    score = product.get("score", 100)
    sentiment = product.get("sentiment", [])
    negative_rate = sentiment[2] if len(sentiment) > 2 else "--"
    evidence_names = "、".join(source["name"] for source in context.get("evidence_sources", [])[:4])
    direction = product.get("direction") or "继续补充评论样本，并把高频客诉转为可验收动作。"
    action = product.get("action") or "先复核证据，再小步执行。"

    if context["brief_type"] == "supply_chain":
        action_block = f"""1. 将“{complaint_text}”转为工艺、包材、结构或说明书的改良项。
2. 打样阶段必须提供改良前后对照样，并把客诉点纳入复测。
3. 首批大货采用小批量试产，出货前按关键客诉项逐项验收。
4. 若涉及包装破损，补充跌落、抗压、边角缓冲和封箱记录；若涉及材质体验，补充耐用、色牢、起球或结构寿命测试。"""
        acceptance = "供应链验收以改良样、测试照片、QC 记录、包装复测和买家体验复盘为准。"
    elif context["brief_type"] == "appeal":
        action_block = f"""1. 汇总异常评论截图、订单号、物流轨迹、客服聊天和仓库 QC 记录。
2. 将负面原因拆分为卖家可控与第三方物流/异常评价两类。
3. 对红线或集中爆发商品申请人工复核，同时提交已执行的整改动作。
4. 申诉文案只陈述证据，不做无证据指控。"""
        acceptance = "申诉包至少包含评价 ID、订单证据、处理记录、物流页面和内部整改说明。"
    else:
        action_block = f"""1. 以“{complaint_text}”作为下一轮差异化卖点反推，而不是继续堆同质化低价款。
2. 新品候选必须避开当前高频差评点，并保留可被图片、短视频和详情页证明的改良证据。
3. 先用小样本评论验证，再进入投放放量；证据可信度不足时不做重仓判断。
4. 后台保留复杂算法指数，前台只输出“做/不做/怎么做”的结论。"""
        acceptance = "选品通过条件为：痛点明确、改良可验证、素材可表达、风险可复盘。"

    return f"""# {config['title']}

## 1. 适用商品
- 商品：{product.get('product_name')}（{product.get('product_id')}）
- 当前健康分：{score}/100
- 负面声量：{negative_rate}%
- 雷达状态：{product.get('radar_status', 'normal')}

## 2. 关键判断
当前主要证据集中在：{complaint_text}。
系统建议方向：{direction}
当前执行建议：{action}

## 3. 证据依据
- 评论挖掘：来自商品诊断中的高频客诉标签与情感比例。
- 可信度处理：样本不足时按保守口径降权，避免把偶发评论当成确定趋势。
- 异常观察：若短期负面集中上升，优先进入人工复核与证据补采。
- 参考方法：{evidence_names}

## 4. 执行动作
{action_block}

## 5. 验收口径
{acceptance}

## 6. 风险备注
本 Brief 只基于当前证据包生成，不包含未验证的 GMV、销量、市场份额或平台规则判断。若后续评论样本增加，需要重新生成并更新结论。"""


def call_executive_brief_model(context: Dict[str, Any]) -> str:
    """调用 GPT-5.5 生成经营 Brief，失败时由上层走本地模板。"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，已切换本地 Brief 模板。")

    from ai_diagnose import (
        DEFAULT_MODEL,
        DEFAULT_OPENAI_API_STYLE,
        DEFAULT_OPENAI_BASE_URL,
        DEFAULT_TIMEOUT_SECONDS,
        build_openai_client,
        extract_responses_text,
    )

    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model_name = os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL)
    api_style = os.getenv("OPENAI_API_STYLE", DEFAULT_OPENAI_API_STYLE)
    default_brief_timeout = min(DEFAULT_TIMEOUT_SECONDS, 25)
    timeout_seconds = safe_int(os.getenv("OPENAI_EXECUTIVE_BRIEF_TIMEOUT_SECONDS"), default_brief_timeout)
    timeout_seconds = max(8, min(timeout_seconds, 45))
    client = build_openai_client(api_key, base_url, timeout_seconds)
    prompt = build_executive_brief_prompt(context)

    if api_style == "responses":
        response = client.responses.create(model=model_name, input=prompt, timeout=timeout_seconds)
        text = extract_responses_text(response).strip()
    else:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "你生成证据约束、简洁可执行的中文经营 Brief。"},
                {"role": "user", "content": prompt},
            ],
            timeout=timeout_seconds,
        )
        text = (response.choices[0].message.content or "").strip()

    if not text:
        raise RuntimeError("GPT-5.5 返回的 Brief 为空。")
    return text


def generate_executive_brief(payload: ExecutiveBriefRequest) -> Dict[str, Any]:
    """生成供应链、运营申诉、选品三类可执行 Brief。"""
    context = build_executive_brief_context(payload)
    brief_type = context["brief_type"]
    model_enabled = os.getenv("EXECUTIVE_BRIEF_MODEL_ENABLED", "0").strip() == "1" or payload.force_model

    if model_enabled:
        try:
            brief_text = call_executive_brief_model(context)
            source = "openai"
            append_log(f"📄 {context['config']['label']} 已由 GPT-5.5 生成：{context['product']['product_name']}。")
        except Exception as exc:
            brief_text = build_rule_based_executive_brief(context)
            source = "local_evidence_template"
            append_log(f"📄 {context['config']['label']} 已启用即时证据模板：{exc}")
    else:
        brief_text = build_rule_based_executive_brief(context)
        source = "local_evidence_template"
        append_log(f"📄 {context['config']['label']} 已由即时证据模板生成：{context['product']['product_name']}。")

    return {
        "status": "success",
        "schema": "tk_action_brief_v2",
        "brief_type": brief_type,
        "brief_label": context["config"]["label"],
        "title": context["config"]["title"],
        "product_id": context["product_id"],
        "product_name": context["product"].get("product_name", context["product_id"]),
        "brief_text": brief_text,
        "evidence_sources": context.get("evidence_sources", []),
        "source": source,
        "model_used": os.getenv("OPENAI_MODEL_NAME", "gpt-5.5") if source == "openai" else "evidence_template_v2",
        "generated_at": current_timestamp(),
    }


# =========================
# AI 一键英文申诉抗辩书生成逻辑
# =========================

def build_appeal_context(product_id: str, product: Dict[str, Any]) -> Dict[str, Any]:
    """把商品诊断信息压缩成申诉信生成所需的证据上下文。"""
    snapshot = build_vs_snapshot(product_id, product)
    complaint_labels = [item["label"] for item in snapshot["top_complaints"]]
    complaint_text = "、".join(complaint_labels) or "negative review spike"
    return {
        "product_id": product_id,
        "product_name": snapshot["product_name"],
        "score": snapshot["score"],
        "negative_ratio": snapshot["negative_ratio"],
        "positive_ratio": snapshot["positive_ratio"],
        "neutral_ratio": snapshot["neutral_ratio"],
        "complaints": complaint_labels,
        "complaint_text": complaint_text,
        "direction": snapshot["direction"],
        "action": snapshot["action"],
        "insight": snapshot["insight"],
        "radar_status": product.get("radar_status", "normal"),
        "radar_alert_reason": product.get("radar_alert_reason", ""),
    }


def build_appeal_prompt(context: Dict[str, Any]) -> str:
    """构造 GPT-5.5 专项英文申诉抗辩 Prompt。"""
    payload = json.dumps(context, ensure_ascii=False, indent=2)
    return f"""
You are a senior cross-border ecommerce compliance counsel specialized in TikTok Shop seller appeals.

Draft a formal English appeal letter for the seller based on the following product diagnosis:
{payload}

Requirements:
1. Output plain English text only. Do not use Markdown code fences.
2. The letter must include:
   - "To TikTok Shop Appeal Team,"
   - Clear appeal reason.
   - Evidence checklist with placeholders for screenshots, order IDs, tracking pages, buyer chats, and warehouse QC records.
   - If complaints include logistics delay, cite platform standard logistic carrier delay / third-party carrier delay as an exemption argument.
   - If complaints show sudden abnormal concentration, include a "Potential Competitor Malicious Sabotage" paragraph and ask for manual review.
   - Process improvement statement and request to remove or exclude unfair negative reviews from NRR/SPS calculation.
3. Keep the tone professional, factual, and non-accusatory.
4. End with "Sincerely," and a seller signature placeholder.
""".strip()


def build_rule_based_appeal(context: Dict[str, Any]) -> str:
    """模型不可用时生成一封结构完整的英文兜底申诉信。"""
    product_name = context["product_name"]
    product_id = context["product_id"]
    complaints = [item.lower() for item in context.get("complaints", [])]
    complaint_text = context.get("complaint_text") or "negative buyer feedback"
    logistics_related = any(
        keyword in " ".join(complaints)
        for keyword in ["logistics", "shipping", "delivery", "carrier", "物流", "延迟", "慢"]
    )
    sabotage_related = context.get("radar_status") == "critical" or context.get("negative_ratio", 0) >= 55

    paragraphs = [
        "To TikTok Shop Appeal Team,",
        "",
        f"We respectfully submit this appeal regarding recent negative reviews associated with product \"{product_name}\" (Product ID: {product_id}). Based on our internal monitoring and product diagnosis, the recent negative feedback is concentrated around: {complaint_text}. We request a manual review of whether these reviews should be excluded from the seller's NRR/SPS calculation where they are caused by factors outside the seller's direct control or by abnormal review behavior.",
        "",
        "Appeal reason:",
        "The current review cluster does not fully reflect the seller's product quality control or service standard. Our team has reviewed the diagnosis, buyer feedback categories, and operational records, and we believe part of the negative impact should be reviewed under TikTok Shop's seller appeal and review moderation process.",
        "",
        "Evidence checklist for seller upload:",
        "1. [Attach order IDs and affected review IDs here]",
        "2. [Attach buyer chat screenshots showing service response and resolution attempts]",
        "3. [Attach warehouse QC records, product inspection photos, and packing records]",
        "4. [Attach tracking pages or carrier status screenshots where applicable]",
        "5. [Attach refund, replacement, or after-sales handling records]",
    ]

    if logistics_related:
        paragraphs.extend([
            "",
            "Carrier delay / logistics exemption argument:",
            "Several complaints appear to relate to delivery speed, tracking updates, or package handling. These issues may be attributable to platform standard logistics, third-party carrier delay, or last-mile delivery conditions rather than the seller's product quality. We respectfully request that reviews primarily caused by carrier-side delay or logistics exceptions be reviewed for exclusion from NRR/SPS impact."
        ])

    if sabotage_related:
        paragraphs.extend([
            "",
            "Potential Competitor Malicious Sabotage:",
            "The feedback pattern shows an abnormal concentration of negative sentiment within a short monitoring window. We request TikTok Shop to review whether these reviews display signs of coordinated activity, duplicate wording, suspicious buyer behavior, or competitor malicious sabotage. The seller is prepared to provide backend chat screenshots, order timelines, and review IDs for further manual verification."
        ])

    paragraphs.extend([
        "",
        "Corrective and preventive actions:",
        f"Our team has already identified the key improvement direction as: {context.get('direction')}. We are implementing enhanced QC checks, clearer listing communication, faster after-sales response, and additional packaging/logistics verification to prevent similar buyer dissatisfaction.",
        "",
        "Request:",
        "We respectfully request TikTok Shop Appeal Team to manually review the attached evidence and remove, suppress, or exclude unfair or non-seller-responsible negative reviews from the NRR/SPS calculation. We remain committed to maintaining platform trust, buyer experience, and full compliance with TikTok Shop policies.",
        "",
        "Sincerely,",
        "[Seller Name / Store Name]",
        "[TikTok Shop Seller ID]",
    ])
    return "\n".join(paragraphs)


def call_appeal_model(context: Dict[str, Any]) -> str:
    """调用 GPT-5.5 生成英文申诉抗辩信。"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，已切换英文兜底申诉信。")

    from ai_diagnose import (  # 延迟导入，避免服务启动强依赖 OpenAI SDK。
        DEFAULT_MODEL,
        DEFAULT_OPENAI_BASE_URL,
        DEFAULT_TIMEOUT_SECONDS,
        build_openai_client,
        extract_responses_text,
    )

    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model_name = os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL)
    timeout_seconds = safe_int(os.getenv("OPENAI_TIMEOUT_SECONDS"), DEFAULT_TIMEOUT_SECONDS)
    client = build_openai_client(api_key, base_url, timeout_seconds)
    prompt = build_appeal_prompt(context)

    try:
        response = client.responses.create(
            model=model_name,
            input=prompt,
            timeout=timeout_seconds,
        )
        text = extract_responses_text(response).strip()
        if text:
            return text
    except Exception as responses_exc:
        append_log(f"申诉信 Responses API 调用失败，尝试 Chat Completions：{responses_exc}")

    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "You draft professional TikTok Shop seller appeal letters."},
            {"role": "user", "content": prompt},
        ],
        timeout=timeout_seconds,
    )
    text = response.choices[0].message.content or ""
    if not text.strip():
        raise RuntimeError("GPT-5.5 返回的申诉信为空。")
    return text.strip()


def generate_appeal_document(payload: AppealRequest) -> Dict[str, Any]:
    """读取商品诊断结果并生成英文申诉抗辩书。"""
    product_id = payload.product_id.strip()
    products = load_products()
    if product_id not in products:
        raise PipelineRuntimeError(404, f"商品不存在：{product_id}")

    product = products[product_id]
    context = build_appeal_context(product_id, product)
    try:
        appeal_text = call_appeal_model(context)
        source = "openai"
        append_log(f"🛡️ 申诉抗辩书已由 GPT-5.5 生成：{context['product_name']}。")
    except Exception as exc:
        appeal_text = build_rule_based_appeal(context)
        source = "local_fallback"
        append_log(f"🛡️ 申诉抗辩书已启用本地英文兜底模板：{exc}")

    return {
        "status": "success",
        "product_id": product_id,
        "product_name": context["product_name"],
        "appeal_text": appeal_text,
        "source": source,
        "generated_at": current_timestamp(),
    }


# =========================
# 1688 采购 Brief 生成逻辑
# =========================

def build_brief_context(product_id: str, product: Dict[str, Any], factory_name: str) -> Dict[str, Any]:
    """把商品差评痛点和目标工厂信息压缩成采购 Brief 上下文。"""
    factories = build_recommended_factories(product)
    target_factory = next(
        (factory for factory in factories if factory["factory_name"] == factory_name),
        {
            "factory_name": factory_name,
            "category": "待确认类目",
            "advantage": "请工厂基于客诉痛点提供工艺改良与测试方案。",
            "scores": [80, 80, 80, 75],
            "weighted_score": 80,
        },
    )
    return {
        "product_id": product_id,
        "product_name": product.get("product_name", product_id),
        "factory": target_factory,
        "complaints": product.get("keywordLabels", [])[:5],
        "complaint_counts": product.get("keywords", [])[:5],
        "sentiment": product.get("sentiment", []),
        "insight": product.get("insight", ""),
        "direction": product.get("direction", ""),
        "action": product.get("action", ""),
        "radar_alert_reason": product.get("radar_alert_reason", ""),
    }


def build_brief_prompt(context: Dict[str, Any]) -> str:
    """构造 GPT-5.5 采购技术 Brief 生成 Prompt。"""
    payload = json.dumps(context, ensure_ascii=False, indent=2)
    return f"""
你是“供应链高级工程质量审核官（ISO Auditor）”，擅长把跨境电商差评痛点转译为 1688 工厂能执行的中文工业采购改性 Brief。

请基于以下商品诊断和目标工厂信息起草采购技术规范书：
{payload}

输出要求：
1. 输出中文 Markdown/Text，不要代码块。
2. 必须包含：
   - 标题：《ISO 工艺/包材改良采购 Brief》
   - 目标 1688 工厂名称
   - 海外客诉痛点转译
   - 大货质量改性标准
   - 包材物理加固规范
   - 入库质检抽样基准：AQL 2.5/4.0
   - 打样验收清单
3. 如果出现面料起球/起毛/pilling，必须写入 ISO 12945-2 抗起球测试 4 级及以上。
4. 如果出现包装破损/压扁/物流损伤，必须写入 ISTA 1A 跌落试验和 150g 加厚高弹双瓦楞纸箱。
5. 如果出现插头、连接件、断裂、塑胶件问题，必须写入插拔寿命、跌落冲击、材料阻燃与结构加筋要求。
6. 语气必须像正式采购技术规范，便于直接发给 1688 工厂旺旺。
""".strip()


def build_rule_based_brief(context: Dict[str, Any]) -> str:
    """模型不可用时生成可直接发给 1688 工厂的中文采购 Brief。"""
    product_name = context["product_name"]
    factory = context["factory"]
    complaints = [str(item) for item in context.get("complaints", [])]
    complaint_text = "、".join(complaints) or "海外买家体验不稳定"
    lower_text = " ".join(complaints).lower()

    quality_lines = [
        "1. 请基于现有样品重新确认关键失效点，并提供改良前后对比样。",
        "2. 大货首批须提供关键材料、结构件及成品的 QC 检验记录。",
    ]
    packaging_lines = [
        "1. 外箱须统一升级为跨境运输抗压方案，封箱、护角、缓冲内托需能覆盖海外尾程挤压风险。",
        "2. 每箱外观、边角、封口和内托完整性纳入出货全检项目。",
    ]

    if any(token in lower_text for token in ["起球", "起毛", "pilling", "面料"]):
        quality_lines.append("3. 面料起毛起球性须达到 ISO 12945-2 国际标准 4 级及以上，优先采用免磨抗静电物理整理工艺。")
        quality_lines.append("4. 面料须追加色牢度、弹力回复率和洗后尺寸稳定性测试，避免二次差评。")

    if any(token in lower_text for token in ["包装", "压扁", "破损", "物流", "box"]):
        packaging_lines.append("3. 外箱包装须通过 ISTA 1A 跌落试验，升级采用 150g 加厚高弹双瓦楞纸箱。")
        packaging_lines.append("4. 易压损商品须增加蜂窝纸板或 EPE 缓冲内托，并提供跌落测试照片。")

    if any(token in lower_text for token in ["插头", "断裂", "塑胶", "连接", "蓝牙", "外壳"]):
        quality_lines.append("3. 插头/连接件须提供插拔寿命测试记录，建议不少于 3,000 次插拔循环。")
        quality_lines.append("4. 塑胶结构件须增加关键受力位加筋，材料需满足出口市场阻燃与跌落冲击要求。")

    return f"""# 《ISO 工艺/包材改良采购 Brief》

## 1. 目标 1688 工厂
- 工厂名称：{factory['factory_name']}
- 主营类目：{factory.get('category', '待确认')}
- 推荐理由：{factory.get('advantage', '具备问题款改良配合能力')}
- 系统加权评分：{factory.get('weighted_score', '--')}/100

## 2. 海外客诉痛点转译
当前商品「{product_name}」的核心海外客诉集中在：{complaint_text}。
请工厂不要仅按普通打样处理，而需将上述用户口语问题转译为材料、结构、包材和 QC 流程的可验证技术指标。

## 3. 大货质量改性标准
{chr(10).join(quality_lines)}

## 4. 包材物理加固规范
{chr(10).join(packaging_lines)}

## 5. 入库质检抽样基准
- 采用 AQL 2.5/4.0 抽样：主要缺陷按 AQL 2.5，次要外观缺陷按 AQL 4.0。
- 抽检项目必须覆盖：外观、尺寸、功能、包装完整性、关键客诉复测项。
- 任一关键客诉复测项不合格，整批暂停入库并返工复验。

## 6. 打样验收清单
- 请提供 3 套改良样：原方案对照样、工艺改良样、包材加固样。
- 请随样提供测试照片、材料说明、包装跌落记录和报价阶梯表。
- 采购确认样品后再进入小批量试产，首批建议不超过 300-500 件。"""


def call_brief_model(context: Dict[str, Any]) -> str:
    """调用 GPT-5.5 生成中文 ISO 采购 Brief。"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("未检测到 OPENAI_API_KEY，已切换本地采购 Brief 模板。")

    from ai_diagnose import (
        DEFAULT_MODEL,
        DEFAULT_OPENAI_BASE_URL,
        DEFAULT_TIMEOUT_SECONDS,
        build_openai_client,
        extract_responses_text,
    )

    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model_name = os.getenv("OPENAI_MODEL_NAME", DEFAULT_MODEL)
    timeout_seconds = safe_int(os.getenv("OPENAI_TIMEOUT_SECONDS"), DEFAULT_TIMEOUT_SECONDS)
    client = build_openai_client(api_key, base_url, timeout_seconds)
    prompt = build_brief_prompt(context)

    try:
        response = client.responses.create(
            model=model_name,
            input=prompt,
            timeout=timeout_seconds,
        )
        text = extract_responses_text(response).strip()
        if text:
            return text
    except Exception as responses_exc:
        append_log(f"采购 Brief Responses API 调用失败，尝试 Chat Completions：{responses_exc}")

    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": "你是供应链高级工程质量审核官（ISO Auditor）。"},
            {"role": "user", "content": prompt},
        ],
        timeout=timeout_seconds,
    )
    text = response.choices[0].message.content or ""
    if not text.strip():
        raise RuntimeError("GPT-5.5 返回的采购 Brief 为空。")
    return text.strip()


def generate_sourcing_brief(payload: BriefRequest) -> Dict[str, Any]:
    """读取商品诊断结果并生成 1688 工厂采购技术 Brief。"""
    product_id = payload.product_id.strip()
    factory_name = payload.factory_name.strip()
    products = load_products()
    if product_id not in products:
        raise PipelineRuntimeError(404, f"商品不存在：{product_id}")

    context = build_brief_context(product_id, products[product_id], factory_name)
    try:
        brief_text = call_brief_model(context)
        source = "openai"
        append_log(f"📄 采购 Brief 已由 GPT-5.5 生成：{context['product_name']} -> {factory_name}。")
    except Exception as exc:
        brief_text = build_rule_based_brief(context)
        source = "local_fallback"
        append_log(f"📄 采购 Brief 已启用本地 ISO 兜底模板：{exc}")

    return {
        "status": "success",
        "product_id": product_id,
        "product_name": context["product_name"],
        "factory_name": factory_name,
        "brief_text": brief_text,
        "source": source,
        "generated_at": current_timestamp(),
    }


# =========================
# 实时终端日志队列
# =========================

def append_log(message: str) -> None:
    """写入一行带时间戳的后端状态日志，并限制队列最大长度。"""
    line = f"[{time.strftime('%H:%M:%S')}] {message}"
    print(f"[PipelineLog] {line}", flush=True)

    with LOG_LOCK:
        LOG_QUEUE.append(line)
        if len(LOG_QUEUE) > MAX_LOG_LINES:
            del LOG_QUEUE[: len(LOG_QUEUE) - MAX_LOG_LINES]


def reset_logs() -> None:
    """每次新流水线开始前清空旧日志，避免前端看到上一轮残留。"""
    with LOG_LOCK:
        LOG_QUEUE.clear()


def get_log_slice(after: int = 0) -> Dict[str, Any]:
    """按前端游标返回增量日志。"""
    safe_after = max(0, int(after or 0))
    with LOG_LOCK:
        logs = LOG_QUEUE[safe_after:]
        next_index = len(LOG_QUEUE)

    return {
        "logs": logs,
        "next_index": next_index,
        "next_offset": next_index,
        "running": PIPELINE_LOCK.locked() or RADAR_LOCK.locked(),
    }


# =========================
# URL 识别与通道分流
# =========================

def detect_channel(url: str) -> ChannelConfig:
    """
    根据 URL 自动选择爬虫通道。

    规则：
    - youtube.com / youtu.be -> YouTube 通道
    - tiktok.com -> TikTok 通道
    - 无法识别 -> 默认降级到 YouTube 通道
    """
    normalized_url = url.strip()
    lower_url = normalized_url.lower()
    parsed = urlparse(normalized_url)
    hostname = (parsed.hostname or "").lower()

    is_youtube = "youtube.com" in lower_url or "youtu.be" in lower_url
    is_tiktok = "tiktok.com" in lower_url

    if is_youtube:
        return CHANNELS["youtube"]

    if is_tiktok:
        return CHANNELS["tiktok"]

    print(
        f"[Gateway][WARN] 无法识别链接来源，已安全降级至 YouTube 通道。url={normalized_url}, host={hostname}",
        flush=True,
    )
    return CHANNELS["youtube"]


def build_auto_product_id(source_type: str, url: str) -> str:
    """为未显式选择商品的诊断任务生成稳定动态 ID，避免落回固定类目。"""
    digest = hashlib.sha1(url.strip().encode("utf-8")).hexdigest()[:10]
    return f"{source_type}_auto_{digest}"


def resolve_runtime_channel(payload: PipelineRequest, base_channel: ChannelConfig) -> ChannelConfig:
    """把 URL 通道和前端选择的产品信息合并成一次运行时通道。

    如果前端没有传入已有商品 ID，就根据 URL 生成动态商品节点。
    这样抓鞋子、包、服饰或其他未知品类时，不会再被硬绑定到 apparel/electronics。
    """
    product_id = (payload.product_id or "").strip()
    if not product_id:
        product_id = build_auto_product_id(base_channel.source_type, payload.url)

    product_name = (payload.product_name or "").strip() or AUTO_PRODUCT_NAME

    return ChannelConfig(
        source_type=base_channel.source_type,
        script_path=base_channel.script_path,
        product_key=product_id,
        product_id=product_id,
        product_name=product_name,
        display_name=f"{base_channel.source_type} -> {product_id} / {product_name}",
    )


# =========================
# 子进程执行与错误收敛
# =========================

def shorten_text(text: str, limit: int = 2500) -> str:
    """避免把过长日志完整塞进 HTTP 响应。"""
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[-limit:]


def summarize_crawler_error(stdout: str, stderr: str) -> str:
    """把爬虫子进程日志压缩成适合前端展示的错误文案。"""
    stderr_lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    stdout_lines = [line.strip() for line in (stdout or "").splitlines() if line.strip()]
    all_lines = stderr_lines + stdout_lines

    for line in all_lines:
        if "YouTube 评论抓取失败" in line:
            return line
        if "抓取失败" in line or "未抓取到有效" in line:
            return line

    if stderr_lines:
        return stderr_lines[-1]

    if stdout_lines:
        return (
            "爬虫未抓取到有效评论，请检查视频是否可访问、评论是否开启、地区限制或网络代理。"
            f"最后日志：{stdout_lines[-1]}"
        )

    return "爬虫执行失败，请检查 URL、评论区权限、地区限制或网络代理。"


def format_command(command: List[str]) -> str:
    """把 subprocess 参数列表格式化成可读命令，便于前端错误提示定位。"""
    formatted: List[str] = []
    for part in command:
        if any(ch.isspace() for ch in part):
            formatted.append(f'"{part}"')
        else:
            formatted.append(part)
    return " ".join(formatted)


def format_shell_command(command: List[str]) -> str:
    """把参数列表转成 Windows shell 安全命令串，确保 URL 里的 & 不会被拆开。"""
    parts: List[str] = []
    for part in command:
        quoted = subprocess.list2cmdline([str(part)])
        if not (quoted.startswith('"') and quoted.endswith('"')):
            quoted = f'"{quoted}"'
        parts.append(quoted)
    return " ".join(parts)


def build_subprocess_env() -> Dict[str, str]:
    """继承当前环境变量，并强制 Python 子进程使用 UTF-8 输出。"""
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def run_subprocess(command: List[str], stage_name: str, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    """
    执行外部脚本并实时捕获输出。

    子进程的 stdout/stderr 会被合并读取，并逐行写入 LOG_QUEUE，让前端终端可以轮询展示。
    如果返回码非 0 或超时，抛出 PipelineRuntimeError，由 API 层转换为 HTTPException。
    """
    output_lines: List[str] = []
    append_log(f"{stage_name}命令启动：{format_command(command)}")

    try:
        process = subprocess.Popen(
            command,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=build_subprocess_env(),
            shell=False,
        )

        def read_output() -> None:
            if process.stdout is None:
                return
            for raw_line in process.stdout:
                line = raw_line.rstrip()
                if not line:
                    continue
                output_lines.append(line)
                append_log(f"{stage_name}: {line}")

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()

        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            process.wait()
            reader.join(timeout=2)
            stdout = shorten_text("\n".join(output_lines))
            detail = (
                f"{stage_name}执行超时，请检查网络、页面可访问性或模型接口响应时间。\n"
                f"命令：{format_command(command)}\n"
                f"超时：{timeout_seconds}s\n"
                f"stdout：{stdout or '无'}"
            )
            append_log(f"{stage_name}执行超时，任务已终止。")
            raise PipelineRuntimeError(500, detail) from exc

        reader.join(timeout=2)
        stdout = "\n".join(output_lines)
        completed = subprocess.CompletedProcess(
            args=command,
            returncode=return_code,
            stdout=stdout,
            stderr="",
        )
    except subprocess.TimeoutExpired as exc:
        detail = (
            f"{stage_name}执行超时，请检查网络、页面可访问性或模型接口响应时间。\n"
            f"命令：{format_command(command)}\n"
            f"超时：{timeout_seconds}s\n"
            f"stdout：{shorten_text(str(exc.stdout or '')) or '无'}"
        )
        append_log(f"{stage_name}执行超时，任务已终止。")
        raise PipelineRuntimeError(500, detail) from exc

    if completed.returncode != 0:
        stdout = shorten_text(completed.stdout)
        stderr = shorten_text(completed.stderr)
        if stage_name == "爬虫":
            detail = summarize_crawler_error(stdout, stderr)
            append_log(f"{stage_name}执行失败：{detail}")
            raise PipelineRuntimeError(400, detail)

        detail = (
            f"{stage_name}执行失败，子进程返回码：{completed.returncode}\n"
            f"命令：{format_command(command)}\n"
            f"stdout：{stdout or '无'}\n"
            f"stderr：{stderr or '无'}"
        )
        append_log(f"{stage_name}执行失败，子进程返回码：{completed.returncode}")
        raise PipelineRuntimeError(500, detail)

    append_log(f"{stage_name}执行完成。")
    return completed


async def run_subprocess_async(
    command: List[str],
    stage_name: str,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    """
    使用 asyncio.create_subprocess_shell 执行外部脚本，并把 stdout/stderr 实时穿透到网页终端。
    """
    output_lines: List[str] = []
    shell_command = format_shell_command(command)
    append_log(f"{stage_name}异步命令启动：{shell_command}")

    process = await asyncio.create_subprocess_shell(
        shell_command,
        cwd=str(BASE_DIR),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=build_subprocess_env(),
    )

    async def read_output() -> None:
        if process.stdout is None:
            return
        while True:
            raw_line = await process.stdout.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            output_lines.append(line)
            append_log(f"{stage_name}: {line}")

    reader_task = asyncio.create_task(read_output())

    try:
        return_code = await asyncio.wait_for(process.wait(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        process.kill()
        await process.wait()
        try:
            await asyncio.wait_for(reader_task, timeout=2)
        except asyncio.TimeoutError:
            reader_task.cancel()
        stdout = shorten_text("\n".join(output_lines))
        detail = (
            f"{stage_name}执行超时，请检查网络、页面可访问性或模型接口响应时间。\n"
            f"命令：{shell_command}\n"
            f"超时：{timeout_seconds}s\n"
            f"stdout：{stdout or '无'}"
        )
        append_log(f"{stage_name}执行超时，任务已终止。")
        raise PipelineRuntimeError(500, detail) from exc

    await reader_task
    stdout = "\n".join(output_lines)
    completed = subprocess.CompletedProcess(
        args=command,
        returncode=return_code,
        stdout=stdout,
        stderr="",
    )

    if completed.returncode != 0:
        stdout = shorten_text(completed.stdout)
        stderr = shorten_text(completed.stderr)
        if stage_name == "爬虫":
            detail = summarize_crawler_error(stdout, stderr)
            append_log(f"{stage_name}执行失败：{detail}")
            raise PipelineRuntimeError(400, detail)

        detail = (
            f"{stage_name}执行失败，子进程返回码：{completed.returncode}\n"
            f"命令：{shell_command}\n"
            f"stdout：{stdout or '无'}\n"
            f"stderr：{stderr or '无'}"
        )
        append_log(f"{stage_name}执行失败，子进程返回码：{completed.returncode}")
        raise PipelineRuntimeError(500, detail)

    append_log(f"{stage_name}执行完成。")
    return completed


def require_script(path: Path, stage_name: str) -> None:
    """在执行前检查脚本是否存在，缺失时返回明确错误。"""
    if not path.exists():
        raise PipelineRuntimeError(
            500,
            f"{stage_name}脚本不存在：{path}。请确认工作区文件完整。",
        )


def read_comments_after_crawl(stdout: str, stderr: str) -> List[Dict[str, Any]]:
    """读取爬虫产出的 raw_comments.json，并验证其中存在有效评论。"""
    if not RAW_COMMENTS_PATH.exists():
        raise PipelineRuntimeError(
            500,
            (
                "爬虫执行结束，但 raw_comments.json 未生成。\n"
                f"stdout：{shorten_text(stdout) or '无'}\n"
                f"stderr：{shorten_text(stderr) or '无'}"
            ),
        )

    try:
        raw_data = json.loads(RAW_COMMENTS_PATH.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        raw_data = json.loads(RAW_COMMENTS_PATH.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise PipelineRuntimeError(
            500,
            f"raw_comments.json 不是合法 JSON：{exc}",
        ) from exc

    if isinstance(raw_data, list):
        comments = raw_data
    elif isinstance(raw_data, dict) and isinstance(raw_data.get("comments"), list):
        comments = raw_data["comments"]
    else:
        raise PipelineRuntimeError(
            500,
            "raw_comments.json 结构不符合预期，应为评论数组或包含 comments 数组的对象。",
        )

    valid_comments = [
        item for item in comments
        if isinstance(item, dict) and str(item.get("comment_text", "")).strip()
    ]

    if not valid_comments:
        raise PipelineRuntimeError(
            400,
            (
                "爬虫未抓取到有效评论，请检查 URL、登录状态、评论区权限、地区限制或视频是否关闭评论。\n"
                f"stdout：{shorten_text(stdout) or '无'}\n"
                f"stderr：{shorten_text(stderr) or '无'}"
            ),
        )

    return valid_comments


def run_crawler(channel: ChannelConfig, url: str, limit: int) -> List[Dict[str, Any]]:
    """按通道调用对应爬虫脚本，并返回有效评论列表。"""
    require_script(channel.script_path, "爬虫")

    if RAW_COMMENTS_PATH.exists():
        RAW_COMMENTS_PATH.unlink()
        append_log("已清理上一轮 raw_comments.json，准备写入新评论。")

    command = [
        sys.executable,
        str(channel.script_path),
        url,
        "--limit",
        str(limit),
        "--output",
        str(RAW_COMMENTS_PATH),
    ]

    print(f"[Gateway] 开始执行爬虫通道：{channel.display_name}", flush=True)
    print(f"[Gateway] Crawler command: {format_command(command)}", flush=True)
    append_log(f"路由进入爬虫通道：{channel.display_name}")

    completed = run_subprocess(command, "爬虫", CRAWLER_TIMEOUT_SECONDS)
    comments = read_comments_after_crawl(completed.stdout, completed.stderr)
    append_log(f"爬虫产出有效评论 {len(comments)} 条。")
    return comments


def run_ai_diagnose(channel: ChannelConfig) -> Dict[str, Any]:
    """调用 ai_diagnose.py，把 raw_comments.json 诊断为单产品报告。"""
    require_script(AI_DIAGNOSE_SCRIPT, "AI 诊断")

    if TEMP_DIAGNOSED_PRODUCT_PATH.exists():
        TEMP_DIAGNOSED_PRODUCT_PATH.unlink()

    command = [
        sys.executable,
        str(AI_DIAGNOSE_SCRIPT),
        "--input",
        str(RAW_COMMENTS_PATH),
        "--output",
        str(TEMP_DIAGNOSED_PRODUCT_PATH),
        "--product-id",
        channel.product_id,
        "--product-name",
        channel.product_name,
    ]

    print(f"[Gateway] 开始执行 AI 诊断：{channel.product_id} / {channel.product_name}", flush=True)
    print(f"[Gateway] Diagnose command: {format_command(command)}", flush=True)
    append_log(f"开始调用 OpenAI/sub2api 诊断：{channel.product_name}。")

    run_subprocess(command, "AI 诊断", DIAGNOSE_TIMEOUT_SECONDS)

    if not TEMP_DIAGNOSED_PRODUCT_PATH.exists():
        raise PipelineRuntimeError(
            500,
            f"AI 诊断执行结束，但临时诊断结果未生成：{TEMP_DIAGNOSED_PRODUCT_PATH}",
        )

    try:
        report = json.loads(TEMP_DIAGNOSED_PRODUCT_PATH.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        report = json.loads(TEMP_DIAGNOSED_PRODUCT_PATH.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise PipelineRuntimeError(
            500,
            f"AI 诊断结果不是合法 JSON：{exc}",
        ) from exc
    finally:
        try:
            TEMP_DIAGNOSED_PRODUCT_PATH.unlink()
        except FileNotFoundError:
            pass

    if not isinstance(report, dict):
        raise PipelineRuntimeError(500, "AI 诊断结果顶层不是 JSON 对象。")

    report["product_id"] = channel.product_id
    if not str(report.get("product_name", "")).strip():
        report["product_name"] = channel.product_name or AUTO_PRODUCT_NAME
    append_log("AI 诊断 JSON 已生成并通过结构检查。")
    return report


def merge_report_into_products(
    channel: ChannelConfig,
    report: Dict[str, Any],
    source_url: str = "",
    radar_previous_product: Dict[str, Any] | None = None,
    emit_radar_alert: bool = False,
    radar_sample_size: int = 0,
    radar_history_source: str = "pipeline",
) -> Dict[str, Any]:
    """把单产品诊断报告合并回前端产品字典，并保留 AI 识别出的真实商品名。"""
    products = load_products()
    previous_product = products.get(channel.product_key, {})
    product_name = str(report.get("product_name", "")).strip() or channel.product_name or AUTO_PRODUCT_NAME
    enriched = enrich_product_for_dashboard(report)
    enriched["product_id"] = channel.product_id
    enriched["product_name"] = product_name
    enriched["pending"] = False
    remembered_url = (
        source_url
        or str(previous_product.get("source_url") or previous_product.get("url") or "")
    )
    if remembered_url:
        enriched["source_url"] = remembered_url
        enriched["url"] = remembered_url
    enriched = apply_radar_evaluation(
        channel.product_key,
        enriched,
        radar_previous_product or previous_product,
        emit_alert=emit_radar_alert,
        sample_size=radar_sample_size,
        history_source=radar_history_source,
    )
    products[channel.product_key] = enriched
    save_products(products)
    append_log(f"已写回 diagnosed_products.json：{channel.product_key} / {product_name}。")
    return enriched


def run_pipeline_sync(payload: PipelineRequest) -> Dict[str, Any]:
    """同步执行完整流水线：识别 URL -> 爬虫 -> AI 诊断 -> 合并前端数据。"""
    url = payload.url.strip()
    if not url:
        raise PipelineRuntimeError(400, "URL 不能为空。")

    base_channel = detect_channel(url)
    channel = resolve_runtime_channel(payload, base_channel)
    limit = payload.limit or DEFAULT_LIMIT
    append_log(f"收到诊断请求：{channel.product_name}，评论链接：{url}")

    comments = run_crawler(channel, url, limit)
    report = run_ai_diagnose(channel)
    diagnosed_product = merge_report_into_products(channel, report, url, radar_sample_size=len(comments), radar_history_source="pipeline")
    product_name = str(diagnosed_product.get("product_name", "")).strip() or channel.product_name
    message = (
        f"Pipeline 执行成功：{channel.source_type} -> {channel.product_key} / {product_name}，"
        f"已抓取 {len(comments)} 条有效评论并完成 AI 诊断。"
    )

    return {
        "source_type": channel.source_type,
        "product_key": channel.product_key,
        "product_id": channel.product_id,
        "product_name": product_name,
        "raw_comment_count": len(comments),
        "diagnosed_product": diagnosed_product,
        "message": message,
    }


async def run_crawler_async(channel: ChannelConfig, url: str, limit: int) -> List[Dict[str, Any]]:
    """异步调用对应爬虫脚本，并返回有效评论列表。"""
    require_script(channel.script_path, "爬虫")

    if RAW_COMMENTS_PATH.exists():
        RAW_COMMENTS_PATH.unlink()
        append_log("已清理上一轮 raw_comments.json，准备写入新评论。")

    command = [
        sys.executable,
        str(channel.script_path),
        url,
        "--limit",
        str(limit),
        "--output",
        str(RAW_COMMENTS_PATH),
    ]

    print(f"[Gateway] 开始执行爬虫通道：{channel.display_name}", flush=True)
    print(f"[Gateway] Crawler command: {format_command(command)}", flush=True)
    append_log(f"路由进入爬虫通道：{channel.display_name}")

    completed = await run_subprocess_async(command, "爬虫", CRAWLER_TIMEOUT_SECONDS)
    comments = read_comments_after_crawl(completed.stdout, completed.stderr)
    append_log(f"爬虫产出有效评论 {len(comments)} 条。")
    return comments


async def run_ai_diagnose_async(channel: ChannelConfig) -> Dict[str, Any]:
    """异步调用 ai_diagnose.py，把 raw_comments.json 诊断为单产品报告。"""
    require_script(AI_DIAGNOSE_SCRIPT, "AI 诊断")

    if TEMP_DIAGNOSED_PRODUCT_PATH.exists():
        TEMP_DIAGNOSED_PRODUCT_PATH.unlink()

    command = [
        sys.executable,
        str(AI_DIAGNOSE_SCRIPT),
        "--input",
        str(RAW_COMMENTS_PATH),
        "--output",
        str(TEMP_DIAGNOSED_PRODUCT_PATH),
        "--product-id",
        channel.product_id,
        "--product-name",
        channel.product_name,
    ]

    print(f"[Gateway] 开始执行 AI 诊断：{channel.product_id} / {channel.product_name}", flush=True)
    print(f"[Gateway] Diagnose command: {format_command(command)}", flush=True)
    append_log(f"开始调用 OpenAI/sub2api 诊断：{channel.product_name}。")

    await run_subprocess_async(command, "AI 诊断", DIAGNOSE_TIMEOUT_SECONDS)

    if not TEMP_DIAGNOSED_PRODUCT_PATH.exists():
        raise PipelineRuntimeError(
            500,
            f"AI 诊断执行结束，但临时诊断结果未生成：{TEMP_DIAGNOSED_PRODUCT_PATH}",
        )

    try:
        report = json.loads(TEMP_DIAGNOSED_PRODUCT_PATH.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        report = json.loads(TEMP_DIAGNOSED_PRODUCT_PATH.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise PipelineRuntimeError(
            500,
            f"AI 诊断结果不是合法 JSON：{exc}",
        ) from exc
    finally:
        try:
            TEMP_DIAGNOSED_PRODUCT_PATH.unlink()
        except FileNotFoundError:
            pass

    if not isinstance(report, dict):
        raise PipelineRuntimeError(500, "AI 诊断结果顶层不是 JSON 对象。")

    report["product_id"] = channel.product_id
    if not str(report.get("product_name", "")).strip():
        report["product_name"] = channel.product_name or AUTO_PRODUCT_NAME
    append_log("AI 诊断 JSON 已生成并通过结构检查。")
    return report


async def run_pipeline_async(payload: PipelineRequest) -> Dict[str, Any]:
    """异步执行完整流水线：识别 URL -> 爬虫 -> AI 诊断 -> 合并前端数据。"""
    url = payload.url.strip()
    if not url:
        raise PipelineRuntimeError(400, "URL 不能为空。")

    base_channel = detect_channel(url)
    channel = resolve_runtime_channel(payload, base_channel)
    limit = payload.limit or DEFAULT_LIMIT
    append_log(f"收到诊断请求：{channel.product_name}，评论链接：{url}")

    comments = await run_crawler_async(channel, url, limit)
    report = await run_ai_diagnose_async(channel)
    diagnosed_product = merge_report_into_products(channel, report, url, radar_sample_size=len(comments), radar_history_source="pipeline")
    product_name = str(diagnosed_product.get("product_name", "")).strip() or channel.product_name
    message = (
        f"Pipeline 执行成功：{channel.source_type} -> {channel.product_key} / {product_name}，"
        f"已抓取 {len(comments)} 条有效评论并完成 AI 诊断。"
    )

    return {
        "source_type": channel.source_type,
        "product_key": channel.product_key,
        "product_id": channel.product_id,
        "product_name": product_name,
        "raw_comment_count": len(comments),
        "diagnosed_product": diagnosed_product,
        "message": message,
    }


# =========================
# FastAPI 应用生命周期
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """服务启动时确保默认数据文件存在。"""
    global RADAR_TASK
    ensure_products_file()
    RADAR_TASK = asyncio.create_task(auto_radar_patrol_loop())
    try:
        yield
    finally:
        if RADAR_TASK:
            RADAR_TASK.cancel()
            try:
                await RADAR_TASK
            except asyncio.CancelledError:
                pass
            RADAR_TASK = None


app = FastAPI(
    title="TK Cross-border Ecommerce AI Diagnosis Gateway",
    version="2.0.0",
    lifespan=lifespan,
)


def get_client_identity(request: Request) -> str:
    """优先读取反向代理传入的真实客户端 IP，用于限流分桶。"""
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip", "")
    if real_ip:
        return real_ip.strip()
    if request.client:
        return request.client.host
    return "unknown"


def consume_rate_limit(bucket_key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
    """滑动窗口限流；返回是否允许以及建议客户端等待的秒数。"""
    now = time.monotonic()
    cutoff = now - window_seconds

    with RATE_LIMIT_LOCK:
        bucket = RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= limit:
            retry_after = max(1, int(window_seconds - (now - bucket[0])))
            return False, retry_after

        bucket.append(now)
        return True, 0


def extract_operator_token(request: Request) -> str:
    """只接受 Authorization: Bearer <token> 原生会话令牌。"""
    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def requires_operator_auth(request: Request) -> bool:
    """保护所有 /api 路由，仅放行登录、健康检查和预检请求。"""
    if request.method == "OPTIONS":
        return False
    if not request.url.path.startswith("/api/"):
        return False
    return request.url.path not in {"/api/health", "/api/login"}


def constant_time_text_equal(left: str, right: str) -> bool:
    """Compare UTF-8 text in constant time, including non-ASCII secrets."""
    return hmac.compare_digest(
        (left or "").encode("utf-8"),
        (right or "").encode("utf-8"),
    )


def is_operator_authorized(request: Request) -> bool:
    """Validate operator bearer token with constant-time comparison."""
    supplied_token = extract_operator_token(request)
    return bool(supplied_token) and constant_time_text_equal(supplied_token, OPERATOR_TOKEN)


def is_login_password_valid(password: str) -> bool:
    """Prefer OPERATOR_PASSWORD and fall back to OPERATOR_TOKEN."""
    expected_password = OPERATOR_PASSWORD or OPERATOR_TOKEN
    return bool(expected_password) and constant_time_text_equal(password, expected_password)


@app.middleware("http")
async def security_headers_and_rate_limit(request: Request, call_next: Any):
    """生产级基础防护：安全响应头 + IP 滑动窗口限流。"""
    if requires_operator_auth(request) and not is_operator_authorized(request):
        return JSONResponse(
            status_code=401,
            content={"detail": "Unauthorized"},
        )

    if SECURITY_RATE_LIMIT_ENABLED and request.method != "OPTIONS":
        client_id = get_client_identity(request)
        allowed, retry_after = consume_rate_limit(
            f"all:{client_id}",
            RATE_LIMIT_MAX_REQUESTS,
            RATE_LIMIT_WINDOW_SECONDS,
        )
        if allowed and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            allowed, retry_after = consume_rate_limit(
                f"mutation:{client_id}",
                RATE_LIMIT_MUTATION_MAX_REQUESTS,
                RATE_LIMIT_WINDOW_SECONDS,
            )

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "请求过于频繁，请稍后再试。",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


# 生产默认只信任 void52.site 域名族；本地调试可显式设置 ALLOW_LOCAL_CORS=1。
cors_allow_origins = [
    "https://void52.site",
    "https://dashboard.void52.site",
    "https://api.void52.site",
    "https://tk-api.void52.site",
    *CORS_EXTRA_ORIGINS,
]
if ALLOW_LOCAL_CORS:
    cors_allow_origins.extend([
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:8010",
        "http://127.0.0.1:8010",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ])

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allow_origins,
    allow_origin_regex=r"^https://([a-zA-Z0-9-]+\.)*void52\.site$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# 落地上线体检
# =========================

def sha256_file(path: Path) -> str:
    """计算文件 hash，用于确认前端多份静态入口是否同步。"""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_static_artifact_readiness() -> Dict[str, Any]:
    """检查 index、Dashboard 备份页和 Cloudflare Pages zip 是否一致。"""
    index_path = BASE_DIR / "index.html"
    dashboard_path = BASE_DIR / "TK_AI_ECommerce_Dashboard.html"
    zip_path = BASE_DIR / "cloudflare-pages-platinum-dashboard.zip"
    index_hash = sha256_file(index_path)
    dashboard_hash = sha256_file(dashboard_path)
    zip_hash = ""
    zip_has_index = False
    if zip_path.exists():
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zip_has_index = "index.html" in zf.namelist()
                if zip_has_index:
                    zip_hash = hashlib.sha256(zf.read("index.html")).hexdigest()
        except Exception:
            zip_hash = ""

    synced = bool(index_hash and dashboard_hash and zip_hash and index_hash == dashboard_hash == zip_hash)
    return {
        "index_exists": index_path.exists(),
        "dashboard_exists": dashboard_path.exists(),
        "zip_exists": zip_path.exists(),
        "zip_has_index": zip_has_index,
        "synced": synced,
        "index_hash": index_hash[:12],
        "dashboard_hash": dashboard_hash[:12],
        "zip_index_hash": zip_hash[:12],
    }


def build_landing_readiness() -> Dict[str, Any]:
    """把当前项目是否能落地演示/试运营压缩成可执行体检结果。"""
    products = load_products()
    products, migrated = ensure_radar_fields_for_products(products)
    if migrated:
        save_products(products)

    total_products = len(products)
    diagnosed_count = 0
    pending_count = 0
    source_url_count = 0
    evidence_items = 0
    evidence_product_count = 0
    critical_count = 0
    scores: List[int] = []
    product_gaps: List[Dict[str, str]] = []

    for product_key, raw_product in products.items():
        if not isinstance(raw_product, dict):
            continue
        product = enrich_product_for_dashboard(raw_product)
        product_name = product_display_name(product_key, product)
        has_url = bool(get_product_source_url(product))
        if has_url:
            source_url_count += 1
        if product.get("pending") is True:
            pending_count += 1
        diagnosed = is_product_diagnosed(product)
        if diagnosed:
            diagnosed_count += 1
            score = safe_int(product.get("score"), 0)
            scores.append(score)
            if product.get("radar_status") == "critical":
                critical_count += 1
            evidence = product.get("evidence_ledger") if isinstance(product.get("evidence_ledger"), dict) else {}
            count = safe_int(evidence.get("evidence_count"), 0)
            if count > 0:
                evidence_items += count
                evidence_product_count += 1
        missing: List[str] = []
        if not has_url:
            missing.append("缺监控链接")
        if not diagnosed:
            missing.append("未完成诊断")
        if diagnosed and not product.get("evidence_ledger"):
            missing.append("证据账本不足")
        if missing:
            product_gaps.append({
                "product_id": str(product.get("product_id") or product_key),
                "product_name": product_name,
                "gap": "、".join(missing),
            })

    average_score = round(sum(scores) / len(scores)) if scores else 0
    diagnosis_coverage = round((diagnosed_count / total_products) * 100) if total_products else 0
    source_url_coverage = round((source_url_count / total_products) * 100) if total_products else 0
    evidence_coverage = round((evidence_product_count / diagnosed_count) * 100) if diagnosed_count else 0
    static_artifacts = build_static_artifact_readiness()
    scripts = {
        "tiktok": SCRAPE_TIKTOK_SCRIPT.exists(),
        "youtube": SCRAPE_YOUTUBE_SCRIPT.exists(),
        "diagnose": AI_DIAGNOSE_SCRIPT.exists(),
    }
    brief_templates_ready = all(key in BRIEF_TYPE_CONFIG for key in ("supply_chain", "appeal", "selection"))

    checks: List[Dict[str, str]] = []

    def add_check(name: str, status: str, message: str, next_action: str = "") -> None:
        checks.append({
            "name": name,
            "status": status,
            "message": message,
            "next_action": next_action,
        })

    add_check(
        "账号密码与 Bearer 鉴权",
        "pass" if bool(OPERATOR_TOKEN and OPERATOR_USERNAME and OPERATOR_PASSWORD) else "fail",
        "内置账号密码登录与 API Bearer 鉴权已启用。" if bool(OPERATOR_TOKEN and OPERATOR_USERNAME and OPERATOR_PASSWORD) else "登录凭据或 OPERATOR_TOKEN 未配置完整。",
        "在 .env 配置 OPERATOR_USERNAME、OPERATOR_PASSWORD、OPERATOR_TOKEN，并重启服务。",
    )
    add_check(
        "云端数据存储",
        "pass" if get_storage_backend_name() != "local_json" else "warn",
        f"当前存储后端：{get_storage_backend_name()}。",
        "生产环境建议使用 Redis/PostgreSQL，避免容器重建导致本地 JSON 状态漂移。",
    )
    add_check(
        "真实商品数据",
        "pass" if total_products > 0 else "fail",
        f"当前监控商品 {total_products} 个，已诊断 {diagnosed_count} 个。",
        "先注册 3-5 个真实商品链接，覆盖主推款、风险款和竞品参考款。",
    )
    add_check(
        "诊断覆盖率",
        "pass" if total_products > 0 and diagnosis_coverage >= 80 else ("warn" if diagnosed_count > 0 else "fail"),
        f"诊断覆盖率 {diagnosis_coverage}%。",
        "对未诊断商品运行抓取与 AI 诊断，避免首页报告只基于单个样本。",
    )
    add_check(
        "监控链接覆盖",
        "pass" if total_products > 0 and source_url_coverage >= 80 else ("warn" if source_url_count > 0 else "fail"),
        f"有可复跑监控链接的商品占比 {source_url_coverage}%。",
        "给每个商品保留 source_url/url，确保雷达巡检和复盘可重复。",
    )
    add_check(
        "证据账本",
        "pass" if diagnosed_count > 0 and evidence_coverage >= 60 else ("warn" if diagnosed_count > 0 else "fail"),
        f"证据覆盖率 {evidence_coverage}%，证据条目 {evidence_items} 条。",
        "补充真实评论样本，优先让核心商品形成 evidence_ledger。",
    )
    add_check(
        "抓取与诊断脚本",
        "pass" if all(scripts.values()) else "fail",
        f"脚本可用性：TikTok={scripts['tiktok']}，YouTube={scripts['youtube']}，诊断={scripts['diagnose']}。",
        "缺失脚本会阻断数据闭环，需从仓库恢复并重新部署。",
    )
    add_check(
        "OpenAI / sub2api 诊断通道",
        "pass" if bool(os.getenv("OPENAI_API_KEY", "").strip()) else "fail",
        "模型 API Key 已配置。" if bool(os.getenv("OPENAI_API_KEY", "").strip()) else "模型 API Key 未配置，无法稳定生成真实 AI 诊断。",
        "在 .env 配置 OPENAI_API_KEY、OPENAI_BASE_URL、OPENAI_MODEL_NAME 后重启。",
    )
    add_check(
        "24H 雷达巡检",
        "pass" if RADAR_TASK is not None and not RADAR_TASK.done() else "warn",
        f"雷达任务 enabled={RADAR_TASK is not None and not RADAR_TASK.done()}，running={RADAR_LOCK.locked()}，last_run_at={RADAR_LAST_RUN_AT or '尚未完成'}。",
        "保持商品 source_url 完整，等待巡检积累历史点；必要时手动触发 /api/run-radar-patrol。",
    )
    add_check(
        "三类 Brief 导出",
        "pass" if brief_templates_ready else "fail",
        "供应链、申诉、选品三类即时证据 Brief 已就绪。" if brief_templates_ready else "Brief 类型配置缺失。",
        "保持默认证据模板快速导出；只有需要深度润色时再打开模型 Brief。",
    )
    add_check(
        "静态前端包同步",
        "pass" if static_artifacts["synced"] else "fail",
        "index.html、Dashboard 备份页和 Cloudflare zip 已同步。" if static_artifacts["synced"] else "前端入口或 zip 包不同步。",
        "运行 scripts/deploy_cloud.sh 或重新打包 cloudflare-pages-platinum-dashboard.zip。",
    )

    statuses = [item["status"] for item in checks]
    overall = "fail" if "fail" in statuses else ("warn" if "warn" in statuses else "pass")
    next_actions: List[str] = []
    for item in checks:
        if item["status"] != "pass" and item.get("next_action"):
            next_actions.append(item["next_action"])
    if total_products < 3:
        next_actions.append("落地前建议至少准备 3 个真实商品：主推款、风险款、竞品/替代款，用于验证诊断和 Brief 是否稳定。")
    if product_gaps:
        next_actions.append("优先补齐 product_gaps 中的监控链接、诊断结果和证据账本。")

    return {
        "status": overall,
        "generated_at": current_timestamp(),
        "ready_for_demo": overall != "fail" and total_products > 0 and diagnosed_count > 0,
        "ready_for_pilot": overall == "pass" and total_products >= 3 and diagnosed_count >= 3,
        "summary": {
            "products": total_products,
            "diagnosed_products": diagnosed_count,
            "pending_products": pending_count,
            "diagnosis_coverage": diagnosis_coverage,
            "source_url_coverage": source_url_coverage,
            "evidence_coverage": evidence_coverage,
            "evidence_items": evidence_items,
            "average_score": average_score if diagnosed_count else None,
            "critical_products": critical_count,
            "vs_reports": len(load_vs_reports()),
            "audit_logs": len(load_admin_audit_logs()),
        },
        "checks": checks,
        "product_gaps": product_gaps[:20],
        "next_actions": list(dict.fromkeys(next_actions))[:10],
        "static_artifacts": static_artifacts,
        "method_boundaries": {
            "usable_now": [
                "真实抓取评论后的情感分、客诉标签、证据账本和经营 Brief。",
                "24H 雷达在有历史点后用于发现异常上升，不直接等同销量预测。",
                "证据可信度用于决定报告语气强弱，样本少时必须保守。",
            ],
            "not_claimed": [
                "不编造 GMV、销量、市场份额或平台内部规则。",
                "公开论文和 GitHub 项目只作为方法参考，不当作当前商品的真实市场数据。",
            ],
        },
    }

# =========================
# API 路由
# =========================

@app.get("/api/health")
async def health() -> Dict[str, Any]:
    """健康检查接口，供前端判断本地后端是否在线。"""
    return {
        "status": "ok",
        "service": "tk-ai-diagnosis-gateway",
        "version": "2.0.0",
        "storage": {
            "backend": get_storage_backend_name(),
            "cloud_enabled": get_storage_backend_name() != "local_json",
            "products_store_key": PRODUCTS_STORE_KEY,
            "vs_reports_store_key": VS_REPORTS_STORE_KEY,
        },
        "local_products_file_enabled": get_storage_backend_name() == "local_json",
        "diagnosed_products_exists": (
            DIAGNOSED_PRODUCTS_PATH.exists()
            if get_storage_backend_name() == "local_json"
            else False
        ),
        "raw_comments_exists": RAW_COMMENTS_PATH.exists(),
        "operator_auth_enabled": bool(OPERATOR_TOKEN),
        "radar": {
            "enabled": RADAR_TASK is not None and not RADAR_TASK.done(),
            "running": RADAR_LOCK.locked(),
            "last_run_at": RADAR_LAST_RUN_AT,
            "interval_seconds": RADAR_PATROL_INTERVAL_SECONDS,
            "startup_delay_seconds": RADAR_PATROL_STARTUP_DELAY_SECONDS,
        },
        "scripts": {
            "tiktok": SCRAPE_TIKTOK_SCRIPT.exists(),
            "youtube": SCRAPE_YOUTUBE_SCRIPT.exists(),
            "diagnose": AI_DIAGNOSE_SCRIPT.exists(),
        },
    }


@app.get("/api/readiness")
async def readiness() -> Dict[str, Any]:
    """受保护的上线体检接口，供云端脚本和后台检查当前落地状态。"""
    return build_landing_readiness()


@app.post("/api/login")
async def login(payload: LoginRequest) -> Dict[str, Any]:
    """账号密码登录，返回前端 localStorage 持久化使用的 Bearer 令牌。"""
    username = payload.username.strip()
    if not constant_time_text_equal(username, OPERATOR_USERNAME) or not is_login_password_valid(payload.password):
        raise HTTPException(status_code=401, detail="账号或密码不正确")

    append_admin_audit("login", f"运营账号登录：{username}。")
    return {
        "status": "success",
        "token": OPERATOR_TOKEN,
        "access_token": OPERATOR_TOKEN,
        "username": username,
    }


@app.get("/api/products")
async def get_products() -> Dict[str, Dict[str, Any]]:
    """读取前端产品诊断数据。"""
    products = load_products()
    products, migrated = ensure_radar_fields_for_products(products)
    if migrated:
        save_products(products)
    return products


@app.get("/api/pipeline-logs")
async def get_pipeline_logs(
    after: int = 0,
    offset: int | None = Query(default=None),
) -> Dict[str, Any]:
    """前端终端日志轮询接口，兼容 after/next_index 与 offset/next_offset 两套游标。"""
    cursor = after if offset is None else offset
    return get_log_slice(cursor)


@app.get("/api/admin/export-data")
async def admin_export_data() -> Dict[str, Any]:
    """导出当前云端商品大盘、竞品 PK 历史与最近审计日志。"""
    payload = build_admin_export_payload()
    append_admin_audit(
        "export_data",
        f"导出云端备份：商品 {payload['products_count']} 个，VS 报告 {payload['vs_reports_count']} 条。",
    )
    return payload


@app.get("/api/admin/audit-logs")
async def admin_audit_logs(limit: int = Query(default=80, ge=1, le=300)) -> Dict[str, Any]:
    """读取最近后台操作审计日志。"""
    logs = load_admin_audit_logs()
    return {
        "status": "success",
        "count": min(len(logs), limit),
        "logs": logs[-limit:],
    }


@app.get("/api/admin/alert-status")
async def admin_alert_status() -> Dict[str, Any]:
    """查看外部告警 Webhook 配置状态，不返回密钥或 URL 明文。"""
    return build_alert_status()


@app.post("/api/admin/test-alert")
async def admin_test_alert() -> Dict[str, Any]:
    """发送一条模拟告警，用于验证 Webhook 通道是否可达。"""
    result = send_test_alert()
    append_admin_audit(
        "test_alert",
        f"测试告警通道：{'成功' if result.get('status') == 'success' else '失败'}。",
        {"alert_result": result.get("alert_result", {})},
    )
    return result


@app.post("/api/admin/restore-data")
async def admin_restore_data(payload: AdminRestoreRequest) -> Dict[str, Any]:
    """用上传的备份 JSON 恢复云端商品与 VS 报告数据。"""
    return restore_admin_backup(payload)


@app.post("/api/add-product")
async def add_product(payload: AddProductRequest) -> Dict[str, Any]:
    """新增一个待诊断商品，并持久化到 diagnosed_products.json。"""
    product_id = payload.product_id.strip()
    product_name = payload.product_name.strip()
    url = payload.url.strip()

    if not product_id:
        raise HTTPException(status_code=400, detail="商品 ID 不能为空。")
    if not product_name:
        raise HTTPException(status_code=400, detail="商品名称不能为空。")

    products = load_products()
    products[product_id] = build_pending_product(product_id, product_name, url)
    save_products(products)
    append_log(f"已新增监控商品：{product_id} / {product_name}。")
    append_admin_audit("add_product", f"新增监控商品：{product_id} / {product_name}。")

    return {
        "status": "success",
        "message": f"已添加新监控商品：{product_name}",
        "product_key": product_id,
        "product": products[product_id],
    }




@app.post("/api/executive-report")
async def post_executive_report(payload: ExecutiveReportRequest) -> Dict[str, Any]:
    """生成首页 AI 综合经营报告。"""
    products = payload.products if payload.products else load_products()
    try:
        result = generate_executive_report(products)
        append_admin_audit("executive_report", "生成首页 AI 综合经营报告。")
        return result
    except Exception as exc:
        append_log(f"AI 综合经营报告生成失败：{exc}")
        raise HTTPException(status_code=500, detail=f"AI 综合经营报告生成失败：{exc}") from exc
@app.post("/api/run-vs-pipeline")
async def post_run_vs_pipeline(payload: VsPipelineRequest) -> Dict[str, Any]:
    """
    执行竞品横向 PK 诊断。

    前端请求体示例：
        {"product_ids": ["apparel", "electronics", "home"]}
    """
    try:
        result = run_vs_pipeline(payload)
        append_admin_audit(
            "run_vs_pipeline",
            f"竞品 PK 完成：{', '.join(result.get('product_ids', []))}。",
            {"report_id": result.get("report_id", "")},
        )
        return result
    except PipelineRuntimeError as exc:
        append_log(f"竞品横向 PK 执行失败：{exc.detail}")
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        append_log(f"竞品横向 PK 发生未处理异常：{exc}")
        raise HTTPException(
            status_code=500,
            detail=f"竞品横向 PK 执行失败：{exc}",
        ) from exc


@app.post("/api/run-radar-patrol")
async def post_run_radar_patrol() -> Dict[str, Any]:
    """手动触发一轮雷达巡检，便于本地调试和演示异常报警链路。"""
    try:
        result = await run_radar_patrol_once(trigger="manual")
        append_admin_audit(
            "run_radar_patrol",
            f"手动雷达巡检完成：目标 {result.get('checked_count', 0)} 个，红线 {result.get('critical_count', 0)} 个。",
        )
        return result
    except PipelineRuntimeError as exc:
        append_log(f"雷达巡检手动触发失败：{exc.detail}")
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        append_log(f"雷达巡检手动触发发生未处理异常：{exc}")
        raise HTTPException(
            status_code=500,
            detail=f"雷达巡检执行失败：{exc}",
        ) from exc


@app.post("/api/generate-appeal")
async def post_generate_appeal(payload: AppealRequest) -> Dict[str, Any]:
    """根据当前商品舆情诊断结果生成英文官方申诉抗辩信。"""
    try:
        result = generate_appeal_document(payload)
        append_admin_audit("generate_appeal", f"生成申诉抗辩书：{payload.product_id}。")
        return result
    except PipelineRuntimeError as exc:
        append_log(f"申诉抗辩书生成失败：{exc.detail}")
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        append_log(f"申诉抗辩书生成发生未处理异常：{exc}")
        raise HTTPException(
            status_code=500,
            detail=f"申诉抗辩书生成失败：{exc}",
        ) from exc


@app.post("/api/generate-executive-brief")
async def post_generate_executive_brief(payload: ExecutiveBriefRequest) -> Dict[str, Any]:
    """生成供应链、申诉、选品三类经营 Brief。"""
    try:
        result = generate_executive_brief(payload)
        append_admin_audit(
            "generate_executive_brief",
            f"生成{result.get('brief_label', '经营 Brief')}：{result.get('product_name', '')}。",
            {"brief_type": result.get("brief_type", ""), "product_id": result.get("product_id", "")},
        )
        return result
    except PipelineRuntimeError as exc:
        append_log(f"经营 Brief 生成失败：{exc.detail}")
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        append_log(f"经营 Brief 生成发生未处理异常：{exc}")
        raise HTTPException(status_code=500, detail=f"经营 Brief 生成失败：{exc}") from exc


@app.post("/api/generate-brief")
async def post_generate_brief(payload: BriefRequest) -> Dict[str, Any]:
    """根据商品客诉与目标 1688 工厂生成中文 ISO 工艺/包材采购 Brief。"""
    try:
        result = generate_sourcing_brief(payload)
        append_admin_audit(
            "generate_brief",
            f"生成采购 Brief：{payload.product_id} -> {payload.factory_name}。",
        )
        return result
    except PipelineRuntimeError as exc:
        append_log(f"采购 Brief 生成失败：{exc.detail}")
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        append_log(f"采购 Brief 生成发生未处理异常：{exc}")
        raise HTTPException(
            status_code=500,
            detail=f"采购 Brief 生成失败：{exc}",
        ) from exc


@app.post("/api/run-pipeline")
async def post_run_pipeline(payload: PipelineRequest) -> Dict[str, Any]:
    """
    执行智能双通道分流流水线。

    前端最小请求体：
        {"url": "https://www.youtube.com/watch?v=xxxxx"}
    """
    if PIPELINE_LOCK.locked():
        raise HTTPException(
            status_code=409,
            detail="已有一条诊断流水线正在执行，请稍后再试。",
        )

    async with PIPELINE_LOCK:
        reset_logs()
        append_log("诊断流水线已启动。")
        try:
            result = await run_pipeline_async(payload)
            append_log("诊断流水线执行成功，前端看板可以刷新数据。")
            append_admin_audit(
                "run_pipeline",
                f"单品诊断完成：{result['product_key']} / {result['product_name']}，评论 {result['raw_comment_count']} 条。",
                {"source_type": result["source_type"]},
            )
            return {
                "status": "success",
                "message": result["message"],
                "raw_comment_count": result["raw_comment_count"],
                "product_key": result["product_key"],
                "product_id": result["product_id"],
                "product_name": result["product_name"],
                "source_type": result["source_type"],
            }
        except PipelineRuntimeError as exc:
            append_log(f"诊断流水线执行失败：{exc.detail}")
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
        except Exception as exc:
            append_log(f"诊断流水线发生未处理异常：{exc}")
            raise HTTPException(
                status_code=500,
                detail=f"Pipeline 执行失败：{exc}",
            ) from exc


# =========================
# 本地一键启动
# =========================

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=False,
        log_level="info",
    )
