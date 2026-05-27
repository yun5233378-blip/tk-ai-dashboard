# TK 跨境电商 AI 看板云端部署指南

本指南用于把本地全栈舆情 SaaS 项目拆分为 Cloudflare Pages 静态前端和云服务器容器化 FastAPI 后端，并最终合流到 `void52.site` 域名族。

## 1. 架构目标

- 前端：`TK_AI_ECommerce_Dashboard.html` 作为静态看板托管到 Cloudflare Pages。
- 后端：`server.py` 以 Docker 容器运行，暴露 `https://tk-api.void52.site`。
- 数据层：线上必须配置 `REDIS_URL` 或 `DATABASE_URL`，产品大盘与 VS 报告会写入云端共享存储，不再依赖本地 `diagnosed_products.json`。
- 安全边界：FastAPI CORS 默认只信任 `https://void52.site`、`https://dashboard.void52.site`、`https://tk-api.void52.site` 以及所有 `https://*.void52.site` 子域。

## 2. 后端云服务器部署

### 2.1 准备环境变量

在云服务器或 PaaS 控制台配置以下变量：

```bash
PORT=8000
HOST=0.0.0.0
REDIS_URL=redis://default:your_password@your-redis-host:6379/0
# 或者使用 Postgres：
# DATABASE_URL=postgresql://user:password@host:5432/dbname

OPENAI_API_KEY=your_openai_or_sub2api_key
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL_NAME=gpt-5.5

RADAR_PATROL_INTERVAL_SECONDS=43200
RADAR_PATROL_STARTUP_DELAY_SECONDS=60
ALLOW_LOCAL_CORS=0
```

推荐优先使用托管 Redis，因为当前项目的产品大盘天然适合 Key-Value JSON 存储。如果团队更偏好审计与 SQL 运维，可以使用托管 Postgres，服务会自动创建 `tk_ai_kv_store` JSONB 表。

### 2.2 构建并启动容器

```bash
docker build -t tk-ai-api .
docker run -d \
    --name tk-ai-api \
    --restart unless-stopped \
    -p 8000:8000 \
    --env-file .env \
    tk-ai-api
```

`Dockerfile` 已使用 Playwright 官方 Python 镜像 `mcr.microsoft.com/playwright/python:v1.40.0-jammy`。该镜像内置 Chromium 与 Linux 图形依赖，能避免云端无头浏览器启动失败。

### 2.3 后端健康检查

```bash
curl http://127.0.0.1:8000/api/health
```

重点确认：

```json
{
    "status": "ok",
    "storage": {
        "backend": "redis",
        "cloud_enabled": true
    }
}
```

如果看到 `backend` 为 `local_json`，说明线上还没有配置 `REDIS_URL` 或 `DATABASE_URL`，此时不建议对外开放。

## 3. Cloudflare DNS 后端分流

### 3.1 新增 API 子域解析

在 Cloudflare 控制台进入 `void52.site` 的 DNS 面板，新增记录：

- Type：`A`
- Name：`api`
- IPv4 address：你的云服务器公网 IP
- Proxy status：开启橙色小云朵

如果后端部署在 Render、Railway、Fly.io 等 PaaS，并提供平台域名，则新增：

- Type：`CNAME`
- Name：`api`
- Target：平台给出的服务域名
- Proxy status：开启橙色小云朵

### 3.2 HTTPS 模式

在 Cloudflare `SSL/TLS` 中设置：

- 推荐：`Full (strict)`。本项目的 `docker-compose.yml` 已内置 Caddy 反代容器，会在 `tk-api.void52.site` 解析到服务器且 80/443 端口放通后自动签发源站 HTTPS 证书。
- 临时调试：`Full`，适合源站证书刚签发或 DNS 刚切换时短暂验证。

完成后访问：

```bash
curl https://tk-api.void52.site/api/health
```

如果直接访问服务器 IP 的 `8000` 端口，只适合临时调试。正式接入 Cloudflare 时建议走 `https://tk-api.void52.site`，由 Caddy 负责把 80/443 流量反代到 Docker 内部的 FastAPI。

## 4. Cloudflare Pages 前端托管

### 4.1 准备入口文件

Cloudflare Pages 需要默认入口为 `index.html`。部署前执行以下其中一种方式：

```bash
copy "开发思路文档\TK_AI_ECommerce_Dashboard.html" index.html
```

或者在仓库中创建 `public/index.html`，内容来自 `开发思路文档/TK_AI_ECommerce_Dashboard.html`。

前端源码已内置云端 API 自动识别逻辑：

```javascript
const API_BASE_URL = window.TK_API_BASE_URL || (
    window.location.hostname === 'void52.site' || window.location.hostname.endsWith('.void52.site')
        ? 'https://tk-api.void52.site'
        : 'http://localhost:8000'
);
```

本地打开时会继续访问 `http://localhost:8000`，部署到 `void52.site` 或任意子域时会自动访问 `https://tk-api.void52.site`。

### 4.2 上传到 Cloudflare Pages

Cloudflare Pages 控制台操作：

1. 进入 `Workers & Pages`。
2. 点击 `Create application`。
3. 选择 `Pages`。
4. 可以选择 Git 仓库部署，也可以选择 Direct Upload。
5. 如果是纯静态上传，上传包含 `index.html` 的目录即可。
6. Build command 留空，Output directory 选择包含 `index.html` 的目录。

### 4.3 绑定域名

在 Pages 项目的 `Custom domains` 中添加：

- 主域方案：`void52.site`
- 子域方案：`dashboard.void52.site`

Cloudflare 会自动补齐 CNAME 记录并签发 HTTPS 证书。证书签发完成后，浏览器访问对应域名即可打开看板。

## 5. 生产安全建议

- 线上必须配置 `REDIS_URL` 或 `DATABASE_URL`，避免多用户写入本地 JSON。
- 保持 `ALLOW_LOCAL_CORS=0`，只允许 `void52.site` 域名族跨域访问。
- OpenAI 或 sub2api 密钥只放在后端环境变量中，绝不要写进 HTML。
- Cloudflare 橙色小云朵建议保持开启，用于基础 DDoS 缓解、WAF 与源站隐藏。
- 雷达巡检生产间隔建议使用 `RADAR_PATROL_INTERVAL_SECONDS=43200`，也就是每 12 小时巡检一次。

## 6. 上线验收清单

- `https://tk-api.void52.site/api/health` 返回 `status=ok`。
- `/api/health` 中 `storage.cloud_enabled=true`。
- `https://void52.site` 或 `https://dashboard.void52.site` 可打开看板。
- 看板右上角显示实时 API 在线。
- 添加新商品后，另一台设备刷新页面能看到相同商品。
- 点击 AI 抓取、VS PK、申诉抗辩、采购 Brief 等功能，均通过 `tk-api.void52.site` 调用后端。

