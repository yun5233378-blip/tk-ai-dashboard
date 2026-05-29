# TK AI 看板落地准备说明

本文件用于把项目从“继续加功能”收敛到“可演示、可试运营、可复盘”的状态。所有判断以当前云端真实数据和可复跑链路为准，不把论文、GitHub 项目或 GPT 文案当作真实市场数据。

## 当前可落地的核心闭环

1. 注册商品监控链接：商品必须保留 `source_url` 或 `url`，否则无法复跑抓取和雷达巡检。
2. 抓取真实评论：TikTok / YouTube 爬虫写入 `raw_comments.json` 或云端中间状态。
3. AI 诊断：`ai_diagnose.py` 生成情感比例、健康分、高频客诉和证据账本。
4. 经营报告：首页只展示健康分、风险商品、证据可信度、建议动作四类核心结论。
5. 可执行 Brief：供应链、申诉、选品三类 Brief 默认使用即时证据模板，保证秒级导出；如需深度润色，再打开模型 Brief。

## 数据与方法边界

可用于当前决策的数据：

- 已抓取的真实评论样本。
- 商品健康分、情感比例、高频客诉标签。
- `evidence_ledger` 中的证据数量、置信度和来源覆盖。
- 24H 雷达历史点，前提是商品链接可复跑并且已经积累多次巡检。
- 后台审计日志、备份包、VS 报告历史。

只能作为方法参考的数据/项目：

- 公开论文、GitHub 项目、Amazon Reviews 等公开语料只能支持“算法设计思路”，不能当作当前商品的销量、市场份额或平台规则。
- GPT 经营报告只允许基于当前证据包总结，不允许编造 GMV、销量、市场规模。
- 样本少时，系统必须保守提示“继续补样本”，不能给确定性经营结论。

## 一键落地体检

在云端执行：

```bash
cd /opt/tk-ai
scripts/landing_readiness.sh
```

脚本会检查：

- Python / HTML / zip 静态一致性。
- Docker Compose 服务状态。
- 公开 `/api/health`。
- 受保护 `/api/readiness`。
- 三类 Brief 的真实线上导出链路。
- Cloudflare Pages 前端关键入口。
- Git 工作区状态。

如果 `/api/readiness` 返回 `fail`，先处理 `next_actions`，不要继续加新模块。

## 上线前最小数据要求

演示可用：

- 至少 1 个真实商品。
- 至少 1 个商品完成真实评论诊断。
- 登录、鉴权、Brief 导出、备份导出可用。

试运营建议：

- 至少 3 个真实商品：主推款、风险款、竞品或替代款。
- 诊断覆盖率大于等于 80%。
- 每个商品都有可复跑链接。
- 证据覆盖率大于等于 60%。
- 24H 雷达至少完成 2-3 轮巡检。

## 当前基础设施命令

验证代码与静态包：

```bash
cd /opt/tk-ai
scripts/codex_cloud_check.sh
```

部署 API 和 Cloudflare Pages：

```bash
cd /opt/tk-ai
scripts/deploy_cloud.sh
```

导出云端备份：

```bash
curl -fsS -H "Authorization: Bearer $OPERATOR_TOKEN"   https://tk-api.void52.site/api/admin/export-data > backup.json
```

查看上线体检 JSON：

```bash
curl -fsS -H "Authorization: Bearer $OPERATOR_TOKEN"   https://tk-api.void52.site/api/readiness | python3 -m json.tool
```

## 下一步优先级

1. 补齐 3-5 个真实商品链接，不再新增概念型指数。
2. 对每个商品跑一次真实抓取和诊断，形成证据账本。
3. 用 `scripts/landing_readiness.sh` 作为是否进入演示/试运营的唯一门槛。
4. 把 Brief 作为实际交付物：发给供应链、运营或选品人员验证是否能执行。
5. 收集实际反馈后再决定是否需要新增算法或页面。


## 多源采集边界

本项目吸收 BettaFish 的“多源采集 + 任务化流水账 + 报告沉淀”思路，但不复制其 GPL 代码，也不做绕登录、绕验证码、逆向私有接口的采集。

当前支持源：

- YouTube：保留原真实评论抓取链路。
- TikTok：保留原真实评论抓取链路。
- 抖音：通过多源适配器识别链接；公开页面只能抽取可访问文本信号，评论受限时使用 `comments://` 或文本文件导入。
- 小红书：通过多源适配器识别链接；公开页面只能抽取可访问文本信号，评论受限时使用 `comments://` 或文本文件导入。
- 手动导入：运营可在前端复制多行评论，或用 `comments://评论1%0A评论2` 进入同一 AI 诊断链路。

前端已提供“导入抖音 / 小红书评论文本”入口，运营不需要手写 `comments://`：

1. 打开 AI 诊断。
2. 选择要写回的商品。
3. 展开“导入抖音 / 小红书评论文本”。
4. 选择来源平台，粘贴每行一条评论。
5. 点击“导入分析”，系统会复用现有 AI 诊断、证据账本、经营报告和 Brief 链路。

线上接口：`POST /api/import-comments-pipeline`，请求体包含 `source_platform`、`product_id`、`product_name` 和 `comments` 数组。该接口已在生产环境完成冒烟验证。

这样做的目的不是“假装全网爬虫”，而是先让国内市场讨论样本能合规进入现有诊断、报告和 Brief 闭环。后续如果有官方 API、授权 Cookie 或企业数据源，再在 `scrape_multi_source_comments.py` 中增加专用适配器。
