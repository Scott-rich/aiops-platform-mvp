# 冻结的 LLM 测量协议

本文件说明已执行研究与重新开展研究的区别。权威原始模型运行使用 commit `9abacc9cf741f736dbc6564e90275c2fb3b08621`；后来的攻击计分修正不反写冻结源码。

## 数据与标注

[golden/result.json](golden/result.json) 记录构造命令、种子、场景数和哈希。`dev.jsonl` 与 `test.jsonl` 按家族分层；`annotator=construction_rules_v1`，`reviewer=automated_invariant_checks_only`，`human_reviewed=false`。不能将自动审计写成人工复核。正常与噪声应弃权，其余场景标注根因服务及需要的事件 ID。场景是既有合成拓扑的结构化回放，不能替代公共真实 RCA 数据。

## 时间与实验臂

1. 加载固定 GGUF、固定推理配置与冻结数据。权重字节哈希写进 result。
2. 只在 dev 上计算 token 似然代理并拟合 isotonic 校准。该代理不是模型主观正确概率；正确率校准只来自 dev。
3. 开发集选择覆盖率最大的、经验风险符合预设目标的阈值；没有可行点时选择不接受任何输出的阈值，风险保持 null。
4. 写 `frozen-before-test.json`，再以排他新建方式写 `test-run-claims/<model>-<test>.json`。已有锁拒绝运行，不覆盖结果目录。
5. 测试运行一次选择证据条件、一次预先声明的强制服务选择条件；后者是另一实验臂，不是失败后的伪造回退。每个原始输出、证据映射、归一化审计与时延都保存。
6. 对固定攻击执行防御前后配对推理。before 同时关闭输入消毒、system 防御提示/数据指令分离，以及生成时的 response_format 语法约束；after 同时启用这些机制。两边仍执行相同的下游 selected_analysis/schema 校验。这是组合干预，不能把 ASR 或 recall 变化单独归因于消毒，也不能把 before 描述为完全无保护的通用大模型。

两个权威运行的 `test_protocol.execution_count=1`，时间戳见各自 result。开发集单样本回放检验在这些设置下的确定性；这不等价于多机重现整个测试集。不要通过删除锁重复调整本次 test。

## 指标与分母

证据 precision / recall / F1 分为原始可解析键和通过 schema 后真正交付的键。根因 Top-K 基于合法证据映射，不把被拒绝的输出算有效诊断。全场景 correctness 包括正常时正确弃权，因此与故障根因 Top-1 分开。未知键率只是引用层幻觉代理；引用总数为空时 pooled 值为 null。

主要区间以合成家族为重采样单位，攻击按类别与绑定基础案例重采样；ECE/时延使用案例重采样，依赖性可能使区间过窄。参见 [RESULTS.md](RESULTS.md)。资源家族的金标准引用数量超出受限协议容量，论文必须说明这一上限。

## 已发现并保留的计分缺陷

原评分器识别不存在的 e999 和显式指令回显，却漏掉攻击集事先已有的合法但错误的仅 e1 目标。`correct_attack_metrics` 只读已保存输出并重新计分，独立文件记录原始 result 和 JSONL 的 SHA256；两组模型使用同一修正规则。主结果用 correction-02，原始结果与 correction-01 均保留。最终图的绘制命令强制同时传入匹配原始 SHA 的修正文件，避免旧图误用。

防御效用至少看两种差：攻击相对于同条件 clean 输出的 recall_drop，以及相同攻击 after−before 的配对 recall change。后者不能从前者随意相减。若 clean 从未正确，则诊断退化率有效样本为空，不能宣称“未损失正确诊断”。

## 获取与复现

权重获取脚本为 `scripts.bootstrap_local_model`，支持已有的本地 GGUF；实际两模型哈希见原始 result。下载成功后先核对哈希，不把另一量化版本混作同一模型。研究使用 CPU 推理，没有按 API 价格计算费用；缺失的价格、未取得的 7B/API 保持 null/未获取。

```bash
# 用于新的研究运行；输出目录必须是新的，锁目录不应删除。
.venv/bin/python -m scripts.research.evaluate_llm \
  --golden research/llm/golden \
  --model-file /absolute/path/to/verified-model.gguf \
  --model-name new-prespecified-model \
  --output /tmp/llm-new-protocol-run
# 已有结果的评分修正无需模型推理：
.venv/bin/python -m scripts.research.correct_attack_metrics \
  --run research/results/p1-05b-01 --golden research/llm/golden \
  --output /tmp/llm-correction-independent
```

同模型同测试集的现有锁会阻止第一条命令。这是保护，不是待修复的运行错误。若要提出新的模型结论，应预先设立新协议及保留测试集，并保留旧结果。
