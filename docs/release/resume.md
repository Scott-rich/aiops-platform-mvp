# 可核验的简历条目

- 针对合成数据评估缺乏外部效度的问题，搭建训练集定阈值的 SMD/PSM 检测基准，完成 [120 个实体×方法×seed 组合](../../research/results/p0-integrity-final-01/result.json) 的重跑核验；同时报告 PR-AUC、逐点与区间指标，避免只用 PA-F1 美化结果。
- 为证据约束 LLM 建立冻结 dev/test、原文哈希、弃权校准与攻击前后对照；保留两个本地模型的 [证据召回与 Top-K 负结果](../../research/results/p1-15b-01/result.json)，通过审计修正评分遗漏而不重跑测试模型。
- 升级 API 版本、认证与限流次序、自监控和锁定依赖；实现 [509 项通过、10 项跳过、覆盖率 87.93%](../../research/results/p2-quality-counts-01/result.json)，并完成 [原生 Kafka/Flink 等全部已执行步骤](../../research/results/final-acceptance-02/result.json)。不声称远端 CI 或 Kubernetes 已验收。

使用时保留限定条件。尚未完成的真实集群故障次数、生产吞吐或准确率不能写进简历。
