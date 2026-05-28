# TK AI 舆情看板算法依据与升级蓝图

> 目标：让前台保持极简，只展示健康分、风险商品、证据可信度、建议动作和 AI 综合经营报告；复杂指数进入后台明细，并且每个指数都有论文、公开数据集或开源项目作为依据。

## 当前开发阶段

项目已经进入 **云端可用产品的第二阶段：安全登录 + 白金极简主看板 + 后台证据/机会雷达**。

已经完成：
- 云端服务器 `/opt/tk-ai` 作为唯一开发、部署和 Git 提交工作区。
- Docker 化 FastAPI 后端、Redis 数据持久化、Cloudflare Pages 前端发布。
- 内置账号密码登录与 Bearer Token 鉴权，替代 Cloudflare Access 邮箱门禁。
- 首页指标减负：只保留健康分、风险商品、证据可信度、建议动作。
- `AI 综合经营报告` 已升级为三段式：经营摘要、风险原因、下一步动作。
- Market Gap Radar、Evidence Audit、Battlecard 已收进后台明细，避免首页堆指数。

下一阶段：
- 把后台复杂指数升级成“有文献和开源实现支撑”的算法模块。
- 后端输出结构化证据，让 GPT 只做经营解释，不凭空生成指标。
- 前端默认只读结论，算法细节折叠展示。

## 指数体系原则

1. 前台只展示业务决策，不展示算法噪音。
2. 后台可以展示复杂指数，但每个指数必须有来源说明。
3. GPT 只负责总结和解释，不负责发明原始分数。
4. 所有分数要保留可审计字段：样本量、时间窗、置信度、命中证据。
5. 没有真实样本时必须降权或标记“样本不足”。

## 1. 情感健康分

用途：衡量单个商品当前评论舆情健康程度。

建议公式：

```text
health_score = 100
    - negative_ratio * 55
    - critical_aspect_ratio * 25
    - recent_negative_spike * 15
    + evidence_confidence_bonus * 5
```

前台展示：健康分。
后台展示：负面率、核心负面方面、近期负面波动、置信加权。

理论依据：
- Hu & Liu, 2004, Mining and Summarizing Customer Reviews：把评论拆成产品特征和用户观点，是商品评论摘要和方面级分析的经典基础。
- SemEval 2014 Task 4：Aspect Based Sentiment Analysis 的标准任务定义，包括 aspect term、aspect category、polarity。
- PyABSA：开源 ABSA 框架，可借鉴其 aspect extraction、polarity classification 和批量推理接口设计。

可借鉴开源：
- PyABSA: https://github.com/yangheng95/PyABSA
- Hugging Face Transformers 情感分类管线: https://github.com/huggingface/transformers

落地建议：
- 短期继续使用现有 GPT/规则提取 `keywordLabels` 和 `aspect_terms`。
- 中期新增 `aspect_score` 字段，把“质量、尺码、物流、包装、客服”等中文经营维度统一归一。
- 长期可接入 PyABSA 或微调 BERT/DeBERTa 模型，减少纯 GPT 抽取的不稳定性。

## 2. 24H 雷达风险预警

用途：识别短时间负面评论异常飙升，避免商品继续投流造成 NRR/SPS 损失。

建议公式：

```text
recent_rate = negative_count_24h / max(total_count_24h, 1)
baseline_rate = negative_count_7d / max(total_count_7d, 1)
z_like_delta = (recent_rate - baseline_rate) / sqrt(max(baseline_rate * (1 - baseline_rate), 0.01) / sample_size)
radar_status = critical if z_like_delta >= 2.0 and negative_count_24h >= min_samples
```

前台展示：风险商品数。
后台展示：24H 负面率、7D 基线、样本量、触发原因。

理论依据：
- EWMA/CUSUM 在质量控制和在线异常检测中常用于检测小幅持续漂移。
- Numenta Anomaly Benchmark 提供了时序异常检测的开源评估框架，可借鉴其异常窗口、延迟惩罚和流式检测思想。

可借鉴开源：
- Numenta Anomaly Benchmark: https://github.com/numenta/NAB
- river 在线机器学习/漂移检测: https://github.com/online-ml/river

落地建议：
- 目前先实现 EWMA + 最小样本量阈值，不上复杂模型。
- 对缺时间戳的数据降级为“历史风险”，不要假装 24H。
- 前端只显示红线数量，详细触发依据放后台明细。

## 3. 证据可信度

用途：告诉运营“这份报告有多少样本支撑”，避免少量评论造成误判。

建议公式：

```text
evidence_trust = 100 * wilson_lower_bound(positive_evidence_hits, total_evidence_items)
coverage_bonus = min(log1p(total_reviews) / log1p(target_reviews), 1)
final_trust = 0.75 * evidence_trust + 0.25 * coverage_bonus * 100
```

前台展示：证据可信度。
后台展示：评论样本量、证据条数、来源链接、命中关键词、Wilson 下界。

理论依据：
- Wilson score interval 常用于小样本比例估计，能避免“样本很少但比例很高”的虚假高分。
- Review helpfulness prediction 文献通常使用评论长度、投票、情感强度、可读性等信号评估评论证据价值。

可借鉴开源：
- Wilson score interval 示例与讨论: https://www.evanmiller.org/how-not-to-sort-by-average-rating.html
- Amazon Reviews 数据集可用于验证评论样本量与评分稳定性。

落地建议：
- 后端新增 `evidence_score_breakdown`：sample_count、source_count、wilson_lower、coverage。
- GPT 报告只解释“证据是否足够支撑判断”，不直接显示复杂公式。

## 4. Market Gap Radar / 机会分

用途：识别竞品被骂但需求仍在的“可改良机会”。

建议公式：

```text
gap_score = demand_signal * 0.35
    + complaint_intensity * 0.30
    + fixability * 0.20
    + evidence_confidence * 0.15
```

其中：
- `demand_signal`：评论量、互动量、商品热度或同类商品数量。
- `complaint_intensity`：负面方面聚集度。
- `fixability`：投诉是否能通过供应链或描述修正解决。
- `evidence_confidence`：证据可信度。

后台展示：机会分、机会类型、证据样例、可修复点。
前台展示：只在 AI 综合经营报告里总结“优先关注哪个机会”。

理论依据：
- Association rules 的 support、confidence、lift 可用于衡量“投诉方面与商品类型/结果”的共现强度。
- 市场篮分析常用 lift 识别非随机关联，可迁移到“评论痛点 -> 商品机会”的关联发现。

可借鉴开源：
- mlxtend frequent_patterns / association_rules: https://github.com/rasbt/mlxtend
- Microsoft Recommenders 可借鉴评估与推荐工程结构: https://github.com/recommenders-team/recommenders

落地建议：
- 不在首页显示 Lift/Confidence。
- 后台折叠展示：支持度、置信度、提升度、样本数。
- 机会分必须带 `sample_count`，低样本自动降权。

## 5. Battlecard / 竞品战术卡

用途：把竞品短板转成采购、页面、投流和客服动作。

建议结构：

```json
{
    "opponent_weakness": "竞品主要短板",
    "our_countermove": "我方动作",
    "sourcing_requirement": "供应链要求",
    "listing_message": "页面卖点",
    "risk_guardrail": "不要踩的坑",
    "evidence": ["评论证据1", "评论证据2"]
}
```

理论依据：
- Aspect-based opinion summarization 支持把用户痛点聚合成可行动摘要。
- SWOT/竞争分析本身不是算法，但应由 ABSA、证据可信度和机会分驱动，避免纯主观写作。

可借鉴开源：
- PyABSA 的 aspect 输出结构。
- Amazon Reviews 公开数据集里的标题、评分、评论文本、时间戳结构。

落地建议：
- GPT 只负责把后端算出的痛点和证据转成战术卡。
- 每张卡必须附 2-5 条原始评论证据或摘要证据。

## 6. SPS 半衰预测

用途：模拟差评撤回、新增好评对店铺绩效的短期修复影响。

当前公式：

```text
W(t) = 0.7 ^ floor(t / 15)
```

建议升级：

```text
weighted_negative = sum(review_negative * decay(days_since_review))
decay(t) = exp(-lambda * t)
lambda = ln(2) / half_life_days
```

理论依据：
- 推荐系统和评分系统常用时间衰减处理用户兴趣和评分时效性。
- TimeSVD++ 等模型证明时间动态会显著影响评分预测。

可借鉴开源：
- Microsoft Recommenders: https://github.com/recommenders-team/recommenders
- Surprise 推荐系统库: https://github.com/NicolasHug/Surprise

落地建议：
- 前台保留滑块和预测曲线。
- 后台显示半衰参数、样本窗口和模拟假设。
- 报告中提示“模拟结果，不是平台官方分数”。

## 7. 可用公开数据集

优先级最高：
- Amazon Reviews 2023 / McAuley Lab：大规模商品评论，包含评分、文本、时间、商品元数据，可用于验证情感健康分、证据可信度和机会分。
- Amazon Reviews 2018：老版本但生态资料更多。

可补充：
- SemEval ABSA datasets：训练/验证方面级情感分析。
- Kaggle e-commerce review datasets：适合快速原型，但引用严谨性不如学术数据集。

## 下一步代码升级顺序

1. `server.py` 增加统一 `metric_lineage` 字段，标记每个指数的公式、样本量、来源和降权原因。
2. 后台明细新增“算法依据”折叠区，展示每个指数为什么可信。
3. Market Gap Radar 改成样本量降权 + lift/support/confidence 后台展示。
4. Evidence Trust 改成 Wilson 下界 + coverage 组合分。
5. 24H Radar 改成 EWMA/基线漂移，并对无时间戳样本降级。
6. 最后再让 GPT 综合经营报告读取这些结构化结果，输出中文三段式报告。

## 参考链接

- PyABSA: https://github.com/yangheng95/PyABSA
- mlxtend association rules: https://github.com/rasbt/mlxtend
- Numenta Anomaly Benchmark: https://github.com/numenta/NAB
- river online ML: https://github.com/online-ml/river
- Hugging Face Transformers: https://github.com/huggingface/transformers
- Microsoft Recommenders: https://github.com/recommenders-team/recommenders
- Amazon Reviews 2023: https://amazon-reviews-2023.github.io/
- UCSD McAuley Lab datasets: https://cseweb.ucsd.edu/~jmcauley/datasets.html
- Wilson score interval article: https://www.evanmiller.org/how-not-to-sort-by-average-rating.html
