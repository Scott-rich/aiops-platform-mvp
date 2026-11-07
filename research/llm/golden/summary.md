# 实验记录

命令：`/tmp/aiops-research-venv/bin/python -m scripts.research.build_golden --output /tmp/aiops-golden-03`

**证明了什么**：冻结可重复的合成评测集，尚未证明真实服务根因能力。

**数据与样本**：六类场景，分层固定 dev/test。

**对比对象**：模型未参与标注。

**不确定性**：同家族变体不独立；没有人工复核。

**失效边界**：不能替代 OpenRCA 或生产故障标注。

**下一步**：冻结提示词后运行 dev 校准与一次 test；人工复核仍是未完成门禁。

数值和完整配置：[result.json](result.json)。
