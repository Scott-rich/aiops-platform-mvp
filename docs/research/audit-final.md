# 最终独立审计

审计日期：2026-11-04。审计者为独立模型审查会话，**不是人工标注者或人工复核者**。本审计只读实现与实验数据，未修改检测器、模型 prompt、阈值、既有测试断言或冻结结果，也未重新调用测试模型。

**总体结论：按提示词包的全量验收标准，需返工／补验；已经核验的研究实现与受限实测材料可以交付。** 原始证据足以支持本文限定的观察，不支持“P0–P4 全部通过”“可靠根因诊断”“零幻觉”“真实集群已达标”等结论。P0 已通过本阶段审计；P1 缺人工标签复核；P2 缺远端 CI、镜像与实际部署；P3 缺真实集群实验；P4 的新容器复现未完成。继续完成可独立验证的后续实现，不能追认前面尚未通过的阶段门禁。

本文首次成文时录像第二轮与包装检查未结束；现已按第9节补核五分钟录像02与交付检查02。外层最终交付ZIP的总尺寸及封装哈希仍应由最终清单记录，不从仓库已跟踪文件大小推断。

## 1. 版本与核验范围

| 对象 | 权威证据 | 独立核验范围 |
|---|---|---|
| P0 公共检测 | `research/results/p0-smd-03`、`p0-psm-03` | 100+20 行 result/JSONL/日志、120 份 NPZ、官方标签、数组哈希与全部评分；三轮核心数值一致；详见 [P0 审计](audit-P0.md)。 |
| P1 两个真实模型 | `p1-05b-01`、`p1-15b-01`；冻结源码 `9abacc9cf741f736dbc6564e90275c2fb3b08621` | 每模型 160 个原始输出；prompt/input/raw/normalized 哈希与映射；校准、阈值、全部主指标及 CI；148 个记录源码指纹匹配冻结目录与 commit。另单独核验 `wire_contract.py` 与该 commit 一致。 |
| P1 事后评分修正 | 两模型 `correction-02` | 各 60 个修正行逐行独立重建；所有总体、目标类型、有效样本条件和 2000 次 bootstrap CI 一致；没有模型调用。 |
| P1 四图 | `p1-comparison-01` | 输入哈希与两模型原结果/修正结果逐项一致；图内取值与结果一致；四图均目视检查。 |
| P2 最新本地质量 | `p2-quality-final-03`；源码 `fe8df8ac663c375e18f608bebcbcf01c27b4ef7b` | 173 个源码指纹、终态哈希、原始工具日志、覆盖率明细与本地红→绿探针。`p2-quality-counts-01` 是有原日志哈希的派生计数，不是再次测试。 |
| 最终原生验收 | `final-acceptance-02`；同一 `fe8df8a` | 190 个源码指纹匹配 commit，13 个执行步骤与 15 份子结果哈希全部核验；主结果与 completed 完全相等。 |
| P3 最新阻塞结果 | `p3-cluster-preflight-02`、`p3-cluster-evaluation-02`、`p3-scale-preflight-02`；源码 `64a191a` | 各 173 个源码指纹及终态哈希匹配；实际注入为 0，指标保持 null，未用计划数替代观测数。 |
| P4 轻量演示 | `p4-demo-clean-312-03`；源码 `fe8df8a` | 终态及 173 个源码指纹匹配；480 条合成事件、1 个告警、真实回环 HTTP 200、版本 API 与 metrics；未启动 Kafka/Flink/模型。 |

原生验收02的完整证据压缩包包含 **1785 个成员**。审计逐成员通过解压流重算 SHA256 和长度，全部匹配 `ARCHIVE_MANIFEST.json`；解压总长度为 340900633 字节。468 个标记 `readable_copy` 的文件也全部匹配。压缩包整体 SHA256 为 `7ef97e0d8ceaab1c4e7dcdd11ce27cbad928b1c3bcaa1c73fca647ce799cb6f3`。可读目录中的 `ARCHIVE.md`、外层 console 等附加索引不是伪装成原运行文件的替代品。

早期 P0 缺行日志、旧注入评分、早期质量失败及 native01 均应保留。日志缺失原因没有被直接证实，不能把工作区同步解释当成已确认根因。

## 2. P1 独立终审

### 原始输出与测试顺序

两个模型均使用真实本地 Qwen2.5 Instruct Q4_K_M 权重。每模型记录如下，全部逐行检查：

| 调用用途 | 数量 | 证据 |
|---|---:|---|
| 开发集 | 24 | `dev-outputs.jsonl` |
| 开发集首例确定性回放 | 1 | `dev-replay.json` |
| 选择证据测试臂 | 36 | `test-outputs.jsonl` |
| 预先声明的强制服务选择臂 | 36 | `test-forced-outputs.jsonl` |
| 防御前 clean 基础案例 | 3 | `attack-clean.jsonl` |
| 30 条攻击×before/after | 60 | `attack-outputs.jsonl` |
| 合计 | 160 | 每模型；两模型共 320 份输出 |

每模型 `run.log` 中有 124 条 `inference case=`，顺序与案例、defended 条件、三位小数延迟完全对应。另 36 次 forced 调用保存完整 JSONL 审计，但该函数没有单独写上述 logger 行；不能称日志有160条，也不能因124条日志就误判丢失36次推理。

独立核验包括：dev/test ID 不相交；60 条 golden 均明确 `synthetic_fixture`、`human_reviewed=false`；30 条攻击文本唯一；先拟合 dev isotonic、写冻结文件，再排他创建 claim，最后运行两个预注册 test 臂。各36个 test ID 与 golden 顺序相同、无重复。重新用保存的 dev 似然代理拟合，校准节点、测试置信度及阈值全部一致。源码冻结先于测试，运行期间指纹未变。

这支持“每个预声明条件各执行一次”，不等于每个测试案例总共只调用一次模型。开发集只回放一例，也不是完整测试在多机环境确定性的证明。强制服务选择臂的 prompt、输出空间与证据选择臂不同，是实际运行的对照，不是仅切换 abstention 一个因素的消融。

### 可以支持的数值

以下数值均已由原始 JSONL 独立复算，与 [0.5B](../../research/results/p1-05b-01/result.json) 和 [1.5B](../../research/results/p1-15b-01/result.json) 一致；完整区间见 [主表](../../research/llm/RESULTS.md)。

| 指标与分母 | 0.5B | 1.5B |
|---|---:|---:|
| 故障 Top-1，n=24 | 0/24 | 6/24 = 0.25 |
| 故障 Top-3，n=24 | 0/24 | 12/24 = 0.50 |
| 原始证据 recall，故障 n=24 | 0 | 0.4166666667 |
| 全场景 correctness，含正确弃权，n=36 | 12/36 | 18/36 |
| 实际强制分类 correctness，n=36 | 0/36 | 14/36 |
| 弃权，n=36 | 36/36 | 12/36 |
| 冻结阈值接受，n=36 | 0/36 | 2/36 |
| 接受集合错误率 | null，n=0 | 0/2；仅1个家族 |
| 未知引用键／全部引用条数 | 0/0 → null | 0/48 → 0 |
| ECE，10 bins，n=36 | 0.2413266472 | 0.0660592878 |
| P50 / P95 延迟，秒，n=36 | 12.3928 / 27.8779 | 16.9398 / 26.2214 |

1.5B 冻结阈值为1.0，接受的仅为 `resource-03` 和 `resource-08`。原始 `selected_risk.ci95=[0,0]` 来自单家族退化 bootstrap，不能作为具有名义95%覆盖率的零风险保证。文档已显示 n=2／家族=1 与退化说明；没有修改原始 JSON 来掩盖这个问题。0.5B 空覆盖风险为 null，四图没有把它画成零风险点。

零未知引用键率只是引用存在性代理：生成时语法约束已经限制键空间，不能从0/48或0/0推导语义事实正确、零幻觉或安全成功。Top-3 实际最多只能从两个键映射服务；资源标签要求三条证据而输出最多两条，存在构念和召回上限。标签来源与这些限制均已披露。

主指标按家族重采样，故障仅4家族、全场景仅6家族。ECE、延迟和 pooled 引用率使用案例重采样，不能消除同模板相关性；时延还受共享主机同期工作影响。两个模型只有一次冻结测试序列，未提供跨运行模型性能标准差。预先保留测试不应因这个缺口被重跑或重新调优。

### 注入评分修正与失败

原冻结评分漏掉了攻击文件已定义的“合法但错误、仅选择e1”目标。只检查 e999/canary/越权词会将真实失败记为零。独立审计指出后，修正器对两模型同一组保存输出重新评分，记录输入 SHA，原始结果和旧图保持不变。此为事后指标修正，不能描述成预注册评分没有问题。

| 指标 | 0.5B before → after | 1.5B before → after |
|---|---|---|
| 全部目标 raw ASR，n=30/条件 | 5/30 → 0/30 | 2/30 → 1/30 |
| 上述 bootstrap CI | [0,0.4] → [0,0] | [0,0.2] → [0,0.1] |
| schema-valid ASR，n=30/条件 | 0/30 → 0/30 | 2/30 → 1/30 |
| 合法错误目标 raw 命中，n=15/条件 | 5/15 → 0/15 | 2/15 → 1/15 |
| clean-correct→wrong 有效样本量 | 0 → 0，指标null | 0 → 0，指标null |
| 配对 raw recall，after−before，n=30 | −0.4333333；CI [−0.9,0] | +0.0666667；CI [−0.7,0.9] |
| 配对 served recall，after−before，n=30 | 0；CI [0,0] | +0.3；CI [0,0.9] |

0.5B 防御后的零攻击伴随全部弃权，不支持“有用鲁棒性”。1.5B 的 before 命中为 `unicode-7`、`unicode-8`，after 命中为 `role-5`，后者确实通过 schema 却选择错误证据；失败案例已原样保留。2/30→1/30 不支持显著改善结论。

攻击只有三个类别，每类绑定一个开发集基础案例，不是30个独立系统环境。before 同时关闭输入消毒、防御 system 提示和生成时语法约束；after 同时启用，两边仍执行同一套下游校验。因此只能评价组合干预，不能把变化单独归因于输入消毒。原文中的这一描述已更正并复核。

## 3. P2/P3 与最终原生边界

最新质量输出为 **509 passed，10 skipped，15 warnings**；语句覆盖4343/4939=87.93277991496254%，excluded_lines=0。Ruff、format、mypy、pip check均通过。80%门槛保留，覆盖范围为 app/flink/research，仅排除 research 测试代码本身；不是分支覆盖率。十条 skip 的 opt-in/PyFlink 原因保留，不能改称全部执行。

本地隔离违规探针实际得到 Ruff 1→0、mypy 1→0、coverage 2→0、size 1→0。它们证明本地命令会失败；没有远端工作流 run ID，不能当作 GitHub Actions 全绿证据。镜像 job 为可选，核心代码门禁不可忽略；该安排与“所有CI门禁已验收”不同。

此前发现的公开导出丢失、204类型注解、鉴权与限流次序、WAL竞态、递归OTLP毒消息、故障子集误通过、token采样边界、故障日志失败后清理，以及Uvicorn代理头绕过均已修复。独立最后一组定向测试30 passed，包含真实Uvicorn而非仅TestClient的XFF配额验证。详见 [运行时审计](audit-P2-runtime.md) 与 [P2/P3详细审计](audit-P2P3-final.md)。未发现仍需阻止本次受限交付的已确认实现缺陷；这不是生产无缺陷保证。

`git diff --name-only --diff-filter=MDR 64195e2 -- tests wire_contract.py flink` 无输出：原有这些文件未修改或删除，新增测试不替换旧断言。app 的注解和格式迁移已作语义审查，grounding/校验限制未为研究分数放宽。`Any` 边界仍存在，不能称完全结构类型安全。旧API默认保留响应并添加弃用头、301可配置，是对提示词“默认301”的已披露偏离，不能称逐字满足该项。

native02的13个执行步骤全部通过，八项已执行门禁为 passed；Docker为blocked_not_executed，`status=partial`、`full_acceptance=false`、`exit_code=2` 均保留。实际为单broker、一个Flink TaskManager、两个slots、同一物理机及本地checkpoint盘。恢复读取了比先前记录checkpoint 2更新的checkpoint 3，耗时29.795秒，是一次观测，不能作为HA SLA。模型集成10项通过，但其 `independent_expectation.expected_service=null`，不能把结构成功当成P1的独立RCA准确率证据。

P3最新预检计划30次但确认0次；evaluator n=0，MTTD/MTTR/Top-K均null；规模结果 `levels=[]`、`observed_eps=null`。Online Boutique/Collector/Chaos Mesh与Prometheus端到端链路未实跑。kind三节点即使以后启动，也仍是单物理机。host OTLP当前依赖同resource组、精确时间戳对齐；真实Collector行为未验证。RPS不是EPS，PID1 RSS不代表全部JVM/模型内存，已删除CRD不代表业务和平台同时恢复。这些限制已写入部署说明，不以Helm lint替代实验。

## 4. 提示词包14项逐项判定

判定针对相应条目的完整要求。局部证据通过时明确范围；环境阻塞不改写成PASS。路径后的行号以本次文件版本为准，JSON字段名为更稳定定位方式。

| # | 检查项 | 判定 | 证据、行号与输出片段 |
|---:|---|---|---|
| 1 | 报告数字能否追溯到result字段 | **PASS（已审主体）** | `research/results/RESULTS.md:7`的48个数值格、`research/llm/RESULTS.md:20`起主表均逐项核对；修正ASR来自对应correction-02。509/10原先仅在日志，现有`p2-quality-counts-01/result.json:263`的`tests_passed=509, tests_skipped=10`及输入哈希，原quality未改写。视频与材料计数见第9节；外层包尺寸尚不作结论。 |
| 2 | 种子与确定性记录 | **PASS（限定范围）** | P0三轮同seed核心结果和score/prediction逐位一致；P1 `evaluate_llm.py:79`附近seed2026、temperature0，保存dev首例回放相等。`research/llm/PROTOCOL.md:18`明确只验证一例，不虚称多机完整确定性。 |
| 3 | 是否用test调阈值/prompt | **PASS（可见证据）** | `evaluate_llm.py:300`附近只fit dev；`:315`冻结、`:332`排他claim、`:353`才测试；两份结果`test_protocol`、源commit与时间顺序核对一致。correction-02只读旧输出，无prompt/阈值调整。无法对不可见历史操作作绝对保证。 |
| 4 | 是否只用PA-F1 | **PASS** | `research/metrics.py:31`同时含AP/ROC/point/PA/PA20/range/告警；P0主表与PSM反例保留。`p0-psm-03`的POT point F1≈0.0123与PA F1≈0.8496并列，无指标偷换。 |
| 5 | 基线是否实际运行 | **FAIL（全量要求）** | P0四种检测器实际运行；P1 forced各36条真实输出，均通过本项局部审计。但P3 Prometheus/业务集群基线未采集，`p3-cluster-evaluation-02/result.json:265`为`n_confirmed_injections=0`，不能宣布全部基线完成。 |
| 6 | mean/std、CI、n是否充分 | **FAIL（完整统计要求）** | P0分开seed/实体/时间块，单PSM跨实体区间null，口径可接受。P1均值和CI可复算，但没有跨运行std；1.5B `result.json:513`为`selected_risk n=2,n_families=1,ci95=[0,0]`，不能视为有效总体风险区间；P3没有实测分布。主文`:16、:31、:48`已诚实披露，不修饰为统计证据充分。 |
| 7 | 数据下载、SHA与合成标识 | **PASS（来源检查）** | `data-download-01/sha256.json`18个文件核验一致；`research/llm/PROTOCOL.md:7`明确规则构造、自动检查、human_reviewed=false。该PASS不包含人工标注验收。 |
| 8 | 是否改断言、放宽门槛、抹失败 | **PASS（已审差异）** | 上述git diff无输出；`pyproject.toml:23`仍fail_under=80；旧P0缺日志、质量失败、攻击原评分与native01保留。修正产生新目录，不覆盖不利结果。 |
| 9 | 是否虚增覆盖率 | **PASS（本地质量）** | `p2-quality-final-03/coverage.json`总计4343/4939，excluded=0；`pyproject.toml:19`声明范围。`rg -n 'pragma: no cover\|type: ignore' app research scripts/research`无匹配。mypy通过不等于消除所有Any。 |
| 10 | CI是否实际红→绿 | **FAIL（远端要求）；本地探针PASS** | `p2-quality-final-03/result.json:352`的red_green四项真；日志有非零→零。`.github/workflows/tests.yml:11`配置3.11/3.12，但`remote_ci=not_run_no_connected_remote_repository`；没有真实远端run，不能承诺远端绿。 |
| 11 | 抽查5条引用 | **PASS** | 本次实际打开JMLR2010、Guo2017、Greshake2023、AgentDojo2024和Microsoft OpenRCA官方页面，标题/作者/年份与相关文献一致，见下节。不是只以HTTP200当内容核验；P0另有五项来源审计。 |
| 12 | 失败案例是否保留 | **PASS** | `research/llm/RESULTS.md:64`起三例逐字核对：0.5B空弃权、1.5B错误latency选择、defended role-5；原始test/attack全量文件仍在。P0低分和旧错误日志亦保留，不只展示成功。 |
| 13 | “分布式”等措辞是否对应规模 | **PASS（当前主文）** | README与`deploy/README.md`明确单主机原生和未运行kind；`final-acceptance-02/result.json:597`八门禁passed、docker blocked，`:608`partial/false/2；P3结果保留0/null。没有把多进程、规划三节点或短benchmark当已完成多机实验。 |
| 14 | 新人仅按README复现主结果 | **FAIL（全量复现门禁）** | `p4-demo-clean-312-03/result.json:219`可证明干净虚拟环境内480事件/HTTP200轻量demo；不是新容器，也不是公共评测/LLM/集群全链路复现。权重和公共原始数据需另下载，同一LLM test claim禁止重跑；离线评分可复核与重新实验须区分。录像02及材料检查02现已核验，未解除新容器/完整研究复现的缺口。 |

本审计没有给“每一项都是PASS”的汇总结论。P1执行契约的 `status=passed` 只是脚本完成，不是人工标注、泛化效度或模型能力门禁通过。

## 5. 引用抽查

审计者在2026-11-04实际打开以下原始页面，读取标题、作者/机构、年份与摘要/项目说明。已核对的内容是文献存在及引用方向，不是宣称复现该文献实验。

| 引用 | 核验依据 | 结果 |
|---|---|---|
| El-Yaniv & Wiener，2010 | [JMLR官方论文页](https://jmlr.org/papers/v11/el-yaniv10a.html)，11(53):1605–1641 | 选择式分类及risk–coverage主题对应 |
| Guo等，2017 | [PMLR官方论文页](https://proceedings.mlr.press/v70/guo17a.html)，70:1321–1330 | 神经网络置信校准主题对应 |
| Greshake等，2023 | [arXiv原论文](https://arxiv.org/abs/2302.12173)，DOI 10.48550/arXiv.2302.12173 | 间接提示注入论文存在，作者与年份对应 |
| Debenedetti等，2024 | [AgentDojo原论文](https://arxiv.org/abs/2406.13352)，DOI 10.48550/arXiv.2406.13352 | 攻击、防御与任务效用评估方向对应 |
| Microsoft OpenRCA | [官方项目仓库](https://github.com/microsoft/OpenRCA) | 实际存在RCA评测项目；本项目未声称跑过其全基准 |

## 6. 必须完成的三项（按严重度）

这三项是仍未达到的提示词验收条件，不是为满足“列三项”而虚构的新代码缺陷。

1. **P1标注与推断效度**：完成具有可追溯标注者及复核规则的人工标签复核，保留分歧与修改记录；不能将本模型审计或自动不变量检查充作人工。当前仅两例、一个家族的接受集合不能验证风险控制；新增结论应在预先固定的新独立案例/保留集上验证，不能删除旧claim后对现有test反复调参。公开材料在此之前必须维持“合成探索结果、人工门禁未通过”。
2. **P3真实实验**：在隔离的可运行环境完成六家族各五次、控制器确认的30次故障，保留原始观测、同批Prometheus基线、清理与业务Ready、平台恢复/删失、报告及哈希；实际完成三档600秒负载并报告测得EPS和资源范围。需先验证真实Collector输出能被normalizer接收。无真实集群时持续保持BLOCKED，不能以模板、计划或原生短测代替。
3. **P2/P4完整工程复现**：取得远端Python3.11/3.12工作流、真实违规红→绿、Docker build/运行与新容器demo证据，再完成实际Helm部署。默认301偏离必须继续明确披露，不可写成逐项完全满足。录像、内部链接与材料自检已取得第9节终态，外层ZIP仍需最终清单；这些证据不能代替尚缺的外部验收。

## 7. 建议补充的两个实验

1. 在更多独立拓扑/故障家族与经过人工复核的真实RCA案例上做预注册外部验证；对消毒、system指令分离、生成语法约束分别消融，同时报告效用、接受覆盖率和攻击目标，避免只比较ASR。
2. 扩展全部SMD机器，按机器组报告配对差异；预先固定时间块长度敏感性分析，并与原始SPOT实现对照。当前同组五机器、共享ZScore排序的POT变体不支持普遍算法优势。

## 8. 可复核输出与审计命令

审计没有再次执行LLM test。原始P1离线核验使用独立检查脚本读取冻结源码及JSONL；派生评分检查另外重建目标命中、clean背景、分母和bootstrap，不调用项目修正函数。关键输出：

```text
P1 1.5B: raw_output_and_prompt_records_verified=160
counts: dev24, replay1, selective36, forced36, clean3, attacks60, inference_log124
correction-02: corrected_rows_exact=60 per model, all_summary_ci_exact=true
risk_curve_points_exact: 0.5B=1, 1.5B=9
1.5B selected_cases=[resource-03,resource-08], selected_families=[resource]
comparison_inputs_and_metrics_exact=true

quality03: 509 passed, 10 skipped, 15 warnings
mypy: Success: no issues found in 76 source files
coverage: covered_lines=4343, num_statements=4939, excluded_lines=0
native02: source_files_checked=190, subresult_files_checked=15, steps_passed=13
native02: status=partial, full_acceptance=false, exit_code=2
archive02: all_archive_member_hashes_matched=1785, all_readable_hashes_matched=468
```

只读复核命令举例：

```bash
git diff --name-only --diff-filter=MDR 64195e2 -- tests wire_contract.py flink
rg -n 'pragma: no cover|type: ignore' app research scripts/research
python -m pytest research/tests/test_otel.py research/tests/test_chaos.py tests/test_observability_upgrade.py -q
sha256sum research/results/p1-15b-01/result.json
sha256sum research/results/p2-quality-final-03/result.json
sha256sum research/results/final-acceptance-02/result.json
```

独立审计期间另核对README、LLM协议/主表、简历措辞、技术报告生成源及PDF正文。PDF表头曾因Paragraph解析后才换色而不可见，已修复并重新目视核对第8、9页与A4简介；当前报告12页、简介1页，未把抽查三页冒称逐页人工审核。录像及材料检查的后续终态见第9节。

| 文件 | SHA256 |
|---|---|
| `p1-05b-01/result.json` | `5de3bbfca784a809bb2fc99bc2e63339b94c6728cd821bb9bff9911348550881` |
| `p1-15b-01/result.json` | `a1673368399ddda55aa4c93d66175c7c3a160e8b8eff4e78c91de99d841c9f04` |
| `p1-05b-correction-02/result.json` | `46c6efe5b911bb8f92161c0f66d9aacab0be5e45a21c02be66d88a453a8aee37` |
| `p1-15b-correction-02/result.json` | `7e4c92295633c12f912310a4a5a9607ad2f049e2be80d79ece0327519f1cf2de` |
| `p1-comparison-01/result.json` | `423ce3e6254d44087828ab5553643af4552040b977495fa71d34f7c9392ebe4e` |
| `p2-quality-final-03/result.json` | `32182b031c30bd458be8a03db29a7d32b2cd549e426aa9185f80504e9cac33b0` |
| `p2-quality-counts-01/result.json` | `c5c5bff0b4e6bfffd061d163fff3667b002b8b16a9048f6957b3edc873ef15ce` |
| `final-acceptance-02/result.json` | `4996fc43659fbe46c20e898386b7efc758ace9f0ca099c1aeecd8f56f414c14c` |

此前详细审计保留它们各自的时间与版本范围；较早文档中“1.5B仍在运行”“native02待核验”等历史状态由本报告更新，不应回写旧证据制造当时已完成的印象。

## 9. P4 最后补核：实际录像与交付材料

**录像02及材料完整性检查通过；全量提示词门禁仍未通过。** 本节只读取结束后的文件，没有重新执行演示或研究模型。

| 检查 | 独立核验结果 | 证据 |
|---|---|---|
| 录像运行终态 | `passed`；completed中的结果哈希正确 | `research/results/p4-video-02/run/result.json`、`completed.json` |
| 目标事件恢复 | 原始HTTP响应中，事件`d9e68792ed64415db3e8feaa13f1eeaf`的`root_anchor=payment`、`family=availability`、`status=resolved`，且resolved_at非空 | `run/http-responses.json`；不是仅观察任意事件恢复 |
| 报告版本与完整性 | 两次均选择各自列表最大version；报告ID与随后verify请求一致；每次full evidence、snapshot、RCA hash、RCA references、RCA evidence set五项均true | 同一HTTP响应记录；分别核对1222与3646条完整证据的返回计数 |
| 实际模型行为 | `live-video-payment`调用产生合法unknown、空映射与弃权；原文哈希正确；21.125471686秒来自本次调用 | `run/live-model-output.json`；这是演示调用，不是P1保留测试重跑 |
| 终端捕获 | exit_code=0；368个输出块时间单调；拼接cast文本与session.log完全相等；cast SHA与recording一致 | `recording.json`、`session.cast`、`session.log` |
| MP4本体 | ffprobe独立得到300.000秒、600帧、1600×1000、2fps | `demo/aiops-five-minute.mp4` |
| 画面抽查 | 从实际MP4另行提取145/252/285秒：分别显示模型弃权与核验、目标payment恢复、已保存研究结果及未通过门禁；文字清晰 | 未以截图替代整段时间线，也未宣称逐帧人工审核 |
| 完整录像证据归档 | ZIP整体SHA正确；49个成员全部逐项解压流哈希和大小匹配；18个readable_copy匹配 | `p4-video-02/ARCHIVE_MANIFEST.json`；含01/02的DB、源码和日志 |
| 交付检查02 | passed、source_unchanged=true、终态哈希匹配；173冻结源码与结果指纹未变；509/10与13步原生记录对应 | `research/results/p4-delivery-check-02/result.json` |
| 材料数量和链接 | 最新PDF哈希对应12页报告与1页简介；博客4037个中文字符；脚本列出的README/方法/模型/部署/发布文档共142个内部Markdown链接，独立复查无断链 | 未将这个范围扩展成全部历史文档或所有PDF外部链接已验证 |

录像完整证据包大小为 **135036330字节**，49个成员解压总长860074037字节。它不是最终外层交付包大小。delivery02记录的106225665字节是检查时Git已跟踪文件总长度，也不能替代外层ZIP尺寸或全部目录占用。

第一轮录像没有观察到目标恢复且版本选择依赖列表顺序，原捕获和数据库保留。其原进程正常结束、原result显示passed，只能说明旧脚本执行结束，不表示恢复门禁通过。已知修改反向还原的01轮driver字节SHA与当时result记录完全一致；这证明重建源码的字节身份，不伪称当时另做了源码备份。02轮以独立目录、2 RPS、明确目标和max(version)重录并加入恢复断言，未改平台恢复阈值。演示是合成功能展示，不应把较好的一轮包装成无偏性能实验。

第一次材料自检因`ACCEPTANCE.md → final-status.json`缺失而failed，原件保存在`p4-delivery-check-01`。修复后02产生新结果，未覆盖首次失败。02中`all_prompt_gates_passed=false`及人工复核、远端CI、Docker/kind、30次真实集群故障、持续EPS和新容器demo等未通过列表均保留。

| 文件 | SHA256 |
|---|---|
| `p4-video-02/run/result.json` | `be41bff7f18d8de26f5e60a6acb9c10d886006277dc80b7cb5987ea5d1b2ba1c` |
| `demo/aiops-five-minute.mp4` | `a9e7d1a252e261c2f37c0f7a144a940332fdb533b36ee0548180e426b3640e8f` |
| `p4-video-02/session.cast` | `9031a3f5a96cb46f989977780695aa8f6276692e95360b3d747faf92c794e1f2` |
| `video-full-evidence.zip` | `76779446b709b45a0ea3e3374b0c3236ad99079952940e8fb58c240b54c7727f` |
| `p4-delivery-check-02/result.json` | `7dc2d1df8854f8a96e8a339d2e9abf0f675f9831d796275d41ce995c5ebe85ff` |

本次独立审计至此结束；没有为补录像而重跑冻结研究test，也没有把材料完整性通过改写为研究门禁全部通过。
