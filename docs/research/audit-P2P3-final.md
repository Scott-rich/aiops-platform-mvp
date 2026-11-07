# P2/P3 与原生验收独立终审

**结论：可交付已完成的本地质量、原生验收和受限研究结果；不能宣告 P2/P3 全部通过。** 本轮发现的实现问题已独立复核关闭，没有剩余已确认的实现阻断。P2 的远端 CI、镜像与实际 Kubernetes 部署未完成；P3 的真实集群注入、基线和规模实验仍为 BLOCKED。原生 Kafka/Flink/HTTP/SQLite/LLM 集成通过与这些阻塞并不矛盾。以下是独立模型的代码与证据审查，不是人工标注复核。

## 证据版本与边界

| 证据 | 核验结果 | 不代表什么 |
|---|---|---|
| `research/results/p2-quality-01` | 原始日志 504 passed、10 skipped；覆盖 4343/4939 行，87.93277991496254%，excluded_lines=0；Ruff/mypy/format/pip check 通过 | 不是远端 GitHub Actions，也不是完整 Python 3.12 矩阵运行 |
| 上述质量源快照 | 173 个源码指纹对应 `a3ead82`；运行期间不变 | 不应误写成之后的 `50247d7`；后者另改了 native bootstrap 的监控依赖 |
| `research/results/final-acceptance-01` | 13 个执行步骤通过；15 个子结果文件 SHA256 全部匹配；190 个源码指纹匹配 `50247d7`；终态与主结果一致 | 不含 Docker、kind 或真实 Boutique/OTEL/Chaos Mesh 部署 |
| 原生验收汇总 | local/native/model/reliability/faults/http_benchmark/kafka_benchmark/flink_benchmark 八门禁 passed；docker=blocked_not_executed；status=partial、full_acceptance=false、exit_code=2 | 退出码 2 不是错误地放宽为通过，也不能删掉 Docker 状态后称“完整验收通过” |
| `p3-cluster-preflight-01` | 计划 30 次；确认 0 次；docker/kind/kubectl 均不可用；status=blocked | 计划数量不能当真实注入样本量 |
| `p3-cluster-evaluation-01` | n=0；MTTD/MTTR/Top-K/引用有效性均 null；status=blocked | null 不是 0 秒、零失败或满分 |
| `p3-scale-preflight-01` | levels=[]、observed_eps=null、execute=false；status=blocked | 没有达到 200/1000/3000 EPS，也没有三个 600 秒实测档位 |
| `p1-05b-correction-02` | 60 个派生行和全部汇总/CI 独立复算一致；只读既有输出 | 不是重新推理，也不是新的独立 test；人工复核门禁仍未完成 |

`p2-quality-01/result.json` SHA256 为 `fbb668a3de1e71e2feb9a4e852ee8e86e7ef3f163e1ac7c5aee00075ac72cd7a`。原生验收主结果 SHA256 为 `766796da2fb708d0784c19022807b33216c7ca7ce20aacb1fcc79b3fc18d45c4`。相关 `completed.json`、源指纹与结果哈希均由审计者重新读取核验。较晚的 Helm EXTERNAL listener 静态附件在 `p2-quality-01/EXTRA_FILES.md` 单独注明，没有改写原结果。

原生完整证据压缩包含 1787 个成员，ZIP SHA256 与 `ARCHIVE_MANIFEST.json` 一致；468 个标为 readable_copy 的文件全部逐项哈希匹配。审计没有逐个解压重散列全部 1787 个成员；关键子结果、可读日志以及整体压缩包已检查。原始数据库与 Kafka 文件仍保留在交付归档中，不能因只查看可读目录就误判为被删除。

## 本轮发现及修复

| 问题 | 复现/影响 | 复核状态 |
|---|---|---|
| 深嵌套 OTLP 毒消息 | 4001 字节合法嵌套 JSON 原先抛出 `RecursionError`，DLQ publish=0，重放可持续终止 bridge | 已修：`scripts/research/otel_bridge.py` 纳入 RecursionError；DLQ 必须 broker ACK，DLQ 失败仍抛出，不提前提交 offset。独立定向测试通过。 |
| 小样本日志可使集群 evaluator 通过 | 原先只需至少一个 injected 和少量 report 指标即可留下 passed，未落实 30 次/六类各五次 | 已修：`scripts/research/evaluate_cluster.py:completeness` 检查唯一故障数量、家族、controller 确认、cleanup/ready、观测/报告覆盖、延迟分布、计划/采集终态哈希及 acquisition_complete；不完整保留 partial。原 BLOCKED 记录不改写。 |
| token 模式下监控采样 401 | Prometheus 与 scale 脚本原先不带 read token | 已修：Prometheus 只挂载 Secret 的 API_READ_TOKEN 文件；scale 只对平台 API 带 token，Flink 不带。独立模板解析确认只暴露 read token；定向测试验证不把 token 发送给 Flink。 |
| 已创建故障在日志写盘失败时未清理 | create 后 submitted append/fsync 原先在 try/finally 外 | 已修：`managed_fault` 在独占 create 成功后即包住 journal 与执行；磁盘失败仍执行 delete，create 失败不删除别人的同名对象。需将最新定向测试与其源版本一并保留。 |
| Helm 反扩容探针未纳入整体失败条件 | 旧 quality runner 记录“拒绝双副本”布尔值，却没有要求它为 true；本次原值恰为 true | 已修：最新 passed 条件纳入 `helm_rejects_unsafe_sqlite_scaling is True`。旧结果仍可保留，新源码需要新的门禁运行证据。 |
| Uvicorn 默认代理头与“直接 IP”承诺不一致 | 独立真实 Uvicorn 探针：同一 loopback 连接、burst=1，更换 X-Forwarded-For 原先得到 200、200、429；实际桶 IP 来自 header | 已修：Docker、Compose、本地/原生/API验收及demo入口显式加入 `--no-proxy-headers`。独立执行从真实 Docker CMD 提取的 Uvicorn 启动测试，同连接更换XFF得到200/429，不能重置配额。未来自行另写服务器入口仍需保留此配置。 |

初次独立定向组合执行 OTEL、chaos、observability 测试：**28 passed**；cleanup 与真实服务器代理回归加入后再次独立执行，**30 passed**（`/tmp/audit-p2p3-final-targeted.log`）。新版测试验证日志写入失败仍清理、create冲突不误删，以及真正的Uvicorn而非仅TestClient的IP边界。Helm lint 与 token 模式模板渲染由审计者另行执行通过；它们是静态检查，不是 Collector/Kubernetes 实际运行证据。

本报告记录的504项全质量运行和13步完整原生运行属于修复前各自冻结版本，不能追溯覆盖较晚的改动。父任务将补最终质量03与原生验收02；截至本次复核，后续运行终态尚未纳入此报告，需保留独立版本及失败记录再更新。

之前 P2 运行时发现的 204 返回注解、公开导出丢失、外层 301/429 绕过鉴权及 WAL 消失竞态已独立关闭，详见 `audit-P2-runtime.md`。原有 `tests/` 中既有文件、`wire_contract.py` 和 Flink 实现相对 `64195e2` 未被修改或删除；新增测试不替换旧断言。`Any` 迁移保持公开限制，不称为完整结构类型安全。

## 可以支持的运行结论

原生运行有真实 Kafka broker、Flink 1.20.3、checkpoint 完成后恢复、HTTP 微服务与 worker 报告证据；native 25 项检查通过，恢复同一个 checkpoint 3，记录的恢复耗时为一次运行的 18.205 秒。它是单 broker、一个 TaskManager、同主机进程与本地磁盘，不能推广为高可用恢复 SLA。

原生 model 步骤实际加载本地 1.5B 权重、通过 HTTP 调用、保存原始/归一化审计并验证预算/重开不重复调用；10 项检查通过。其 payment 候选不是独立测得的模型准确率：`independent_expectation.expected_service=null`，结果也明确写结构成功不等于 RCA 能力。该集成调用不是 P1 的 held-out test 重跑。

质量测试中的 10 个 skip 均保留原因：两个 opt-in 集成入口与八个未装 PyFlink 的算子/桥接测试。单独 native/model 验收提供了真实集成证据，但不应把那十条 skip 改称全部执行。CI 配置采用 Python 3.11/3.12，当前可用证据仍是本地质量检查与另行的 3.12 demo；未出现远端 CI run ID。

## P3 的剩余互操作与统计限制

- Helm 的 API/worker 共 Pod、单副本 SQLite、Recreate 与共享 PVC 限制合理；拒绝 app/kafka 副本数大于 1。kind 三节点共用宿主 checkpoint 路径仍是一台物理机，不支持跨机恢复的结论。
- Collector 0.123.0、固定 Boutique tracing patch、spanmetrics Prometheus 指标名/标签、Kafka EXTERNAL listener 与 host 采集均尚未端到端部署验证。不能把 `helm lint` 写成这些链路已打通。
- host normalizer 当前只接受同一 resourceMetrics 对象内、纳秒时间戳完全一致的 CPU idle 与单个 memory used 样本。真实 Collector 的分组/时间对齐未验证；不匹配时跳过并保留原 OTLP，不补零。这个限制可能使真实 host 事件缺失，必须先检查真实输入再评价资源压力检测。
- 原始 OTLP 有 broker ACK 后才提交位点，stable event ID 支持下游去重；这是至少一次投递，不是 Kafka 端到端 exactly-once。源数据保留受 Kafka retention 约束。
- 故障仅限固定研究 context/namespace、显式标签与允许服务；只把 controller AllInjected=True 计入确认数。提交 CRD、删除 CRD、workload Ready、平台 resolved 是不同事件。
- MTTD/MTTR 的分位数只对观测到的值计算；删失比例和观测失败数必须一起报告。已经 firing 的 Prometheus 告警可能混淆归因，当前 evaluator 已披露此限制，实际实验必须查看注入前基线。
- Top-K 只在匹配到的首份报告上计算；缺报告单列。竞争排名保留并列，不按列表顺序虚构唯一第一名。诊断时延是注入到报告的壁钟时间，不是隔离 LLM 推理时延。
- 规模脚本控制的是 HTTP RPS，以 Kafka offsets 推导实际 telemetry EPS；128 并发槽与 reservoir 都会影响达到的负载，drop 数保留。CPU 是所选容器 cgroup，RSS 是 PID 1；未覆盖 Kafka/Flink JVM、子进程及独立 LLM。不能把目标 RPS 标签换成已达标的 EPS。

## 0.5B 注入修正的独立结论

`p1-05b-correction-02/result.json` SHA256 为 `46c6efe5b911bb8f92161c0f66d9aacab0be5e45a21c02be66d88a453a8aee37`。审计逐行重建目标命中/clean 背景/有效输出标记，60 行完全一致，所有汇总与 2000 次类别 bootstrap CI 一致：

| 派生指标 | before | after |
|---|---:|---:|
| 全部目标 raw 命中 | 5/30；CI [0, 0.4] | 0/30；CI [0, 0] |
| 合法错误仅 e1 目标 raw 命中 | 5/15；CI [0, 0.8] | 0/15；CI [0, 0] |
| schema-valid 攻击成功 | 0/30 | 0/30 |
| clean-correct→wrong eligible n | 0 | 0 |

配对 after−before raw recall 差为 −0.4333333333，CI [−0.9,0]；served recall 差为 0，CI [0,0]。事后评分修正已明确披露，原始 result/输出/旧图保留。两边 clean 有效故障诊断皆为零，且 defended 全弃权，不能据此称具有有用鲁棒性或显著安全提升。此修正关闭初审的目标漏计缺陷；P1 全阶段及 1.5B 仍需各自终审。

## 14 项检查

| # | 项目 | 判定 | 证据/限制 |
|---|---|---|---|
| 1 | 报告数字能追溯 | PASS（已核验部分） | 质量日志/coverage JSON、13步验收、P3 null、0.5B派生结果均有原文件；不扩大到尚未完成的最终图文。 |
| 2 | 种子与确定性 | PASS（计划/派生计算） | seed=2026；故障计划可重建；真实耗时明确非确定性。 |
| 3 | 未用 test 调参 | 本次不重判 P1 | P1 冻结9ab独立运行；本轮未干预模型。0.5B修正只重新评分。 |
| 4 | 未只用 PA-F1 | 不适用 P2/P3 | P0另有原始/调整指标审计；P3没有捏造检测分数。 |
| 5 | 基线真的运行 | PASS（原生步骤）；P3 FAIL/未完成 | 原生真实 broker/Flink/CPU模型有记录；Prometheus/Boutique基线未实际采集。 |
| 6 | n、CI与分母 | PASS（已产出结果）；P3未完成 | P3 n=0保持null；未把删失填0。0.5B修正CI独立一致。 |
| 7 | 数据来源/合成标识 | PASS（来源）；人工复核仍未完成 | 原生数据包含明确合成输入；P3计划不冒充真实注入。 |
| 8 | 未改断言/放宽门槛/抹失败 | PASS（已审差异） | 原有测试/共享校验未改，80%门槛保留，失败和阻塞产物保留。 |
| 9 | 未虚增覆盖率 | PASS（本地质量快照） | 4343/4939，excluded_lines=0；research/tests只排除测试代码自身；未发现pragma/type-ignore规避。 |
| 10 | CI真正红→绿 | 本地工具PASS；远端无法判断 | Ruff 1→0、mypy 1→0、coverage 2→0、size 1→0均有日志；这是临时项目本地探针，不是远端CI演示。 |
| 11 | 引用真实性 | 本轮未重审全部文献 | P0引用核验另有报告；模板版本/外部文献存在不等于实际部署验证。 |
| 12 | 失败记录保留 | PASS | 旧质量失败、P3 BLOCKED、旧攻击评分与完整原生归档均留存。 |
| 13 | “分布式”等措辞与规模一致 | PASS（已审部署说明）；完整分布式实测未完成 | 明确单物理机、kind未运行、无30次故障/三档EPS结果。 |
| 14 | 新人只按 README 复现 | 尚无法判定全部 | 5分钟本地demo与完整原生/集群复现是不同入口；最终README与干净容器还需单独验证。 |

验收前必须保留三项边界：给审计修复后的当前源码补最新质量与原生验收证据而不覆盖旧日志；保持远端CI、Docker/kind和P3实测为未完成；明确OTEL互操作、单进程限流及单物理机存储的适用范围。本轮已确认的实现问题全部关闭，不额外虚构三项缺陷。建议后续实验为：隔离容器主机上的完整30次故障与实际Collector字段核对；三档真实EPS稳态控制并覆盖全部关键进程资源。未执行前不得用模板、计划或原生短时 smoke 替代这些结论。
