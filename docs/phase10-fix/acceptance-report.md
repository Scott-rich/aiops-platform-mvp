# 第十阶段最终修复交付与验收报告

## 交付结论

交付 `aiops-platform-mvp-phase10-fixed.zip`，在原第十阶段项目上增量修复，保留原接口、事件协议、历史产物和备份。**核心支持 Python 3.11/3.12；最终完整验收使用 Python 3.11、原生 Kafka/Flink 与真实本地模型，活动数据库放在不参与文件同步的本机目录。** 最终冻结源码的全部可执行门禁通过；Docker门禁仍未执行，`full_acceptance=false`，严格验收退出码为2。不能将此交付描述为所有环境均通过、无任何缺陷或生产高可用。

早期在共享同步目录中运行时，Python 3.11和3.12都出现了真实SQLite损坏。inotify随后直接捕获到同步程序把临时目录中的WAL/SHM文件替换到活动数据库路径；这些外部文件替换不遵守应用/SQLite锁，构成明确的数据一致性风险。证据在`artifacts/phase10-fix/storage-path-watch-01/`，独立复核在`native-db-diagnosis/`。相同代码在非同步的`/tmp`目录下，3.11/WAL、3.11/DELETE实验、3.12/WAL均19/19通过；本版**保留原WAL策略，恢复3.11/3.12支持**。先前版本限制判断已更正，旧日志未删除。不能声称捕获了每一次历史坏库发生的瞬间，也不应把问题笼统归咎于Python或WAL。

最终全套测试在`/tmp/aiops-3fa2499b-final-01`运行，全部进程结束后才复制归档并逐文件核对SHA256。生产应使用本机持久卷；`/tmp`只用于隔离验收，不应作为生产持久化承诺。原始失败DB/WAL/SHM保留在`storage-module/python312-failure-forensics.zip`（名称是当时记录，非版本归因）；它是失败取证，禁止用于恢复工作数据库。没有自动重建坏库、删除WAL或静默迁移用户现有数据库。

最终运行时间：2026-11-04T08:04:37.913300+00:00 — 2026-11-04T08:13:36.680058+00:00。运行时：`3.11.17 (main, Oct 21 2026, 17:46:53) [Clang 23.1.3 ]`。最终源码清单与结果哈希都未发生变化；包内源码逐项与终测清单核对，校验详情另见随包校验文件。

## 对照用户最终交付标准

| 标准 | 本轮状态 | 验收边界 |
|---|---|---|
| 干净环境按文档部署 | 部分验证 | Python3.11新venv从依赖缓存安装通过；Docker镜像/Compose未实测 |
| 核心服务启动通信 | 已真实验证 | 回环HTTP微服务、原生Kafka/Flink、SQLite、模型HTTP服务 |
| Kafka/Flink完整链路 | 已真实验证 | 真实输入、窗口、输出、重复/乱序/迟到、checkpoint与TaskManager重启 |
| 至少一次真实模型诊断 | 已真实验证 | 本地Qwen CPU推理，独立故障Top-1和健康unknown；未调用外部商业提供商 |
| 故障检测、关联、分析 | 已验证标记场景 | 多场景与超上限关联反例通过；检测仍有冷启动/低流量漏检 |
| 根因引用真实证据 | 已验证 | 候选服务对应真实事件；不宣称已证明内部机制因果 |
| 故障恢复确认 | 已验证本地场景 | 持续健康保持、错误span否决、重启历史；不是多机HA认证 |
| 性能和效果可量化 | 已实现且实测 | JSON/CSV、固定动态对照、RCA标签、三档短突发；非长期容量报告 |
| 失败/超时/异常输入处理 | 已验证 | 有限重试、DLQ、幂等、规则回退、预算、数据库保护、运行时拒绝 |
| 文档与实现一致 | 已核对当前入口 | CLI/静态配置/源哈希；未实测范围在文档中保留 |

状态区分：**已实现并验证**为上表明确通过的本地/原生能力；**部分实现或限定验证**为候选级RCA、短时性能、保守保留策略及单机恢复；**仅有配置尚未部署验证**为Docker路径与外部商业模型配置；**尚未交付认证**为多机高可用、生产安全和长期容量。历史冻结证据和报告不会自动删除，需监测磁盘容量；本轮未实现自动淘汰冻结审计集合的策略。

## 实际执行结果

| 模块 | 结果 | 通过/失败口径 | 实际耗时秒 |
|---|---|---|---:|
| unit | passed | 454 通过，0 失败，0 错误，10 跳过 | 68.663 |
| runtime-http | passed | 14/14 检查通过 | 1.120 |
| launcher | passed | 10/10 检查通过 | 2.134 |
| evidence | passed | 19/19 检查通过 | 24.664 |
| detection | passed | 同一合成数据集 2716 个标记指标决策；固定/动态对照完成 | 0.973 |
| rca | passed | 12 个标记场景；根因Top-1 8/8 | 0.328 |
| benchmark-http | passed | 15/15 检查通过 | 17.822 |
| native | passed | 25/25 检查通过 | 120.298 |
| reliability | passed | 23/23 检查通过 | 83.633 |
| faults | passed | 46/46 检查通过 | 28.517 |
| benchmark-kafka | passed | 6/6 检查通过 | 19.569 |
| flink-benchmark | passed | 14/14 检查通过 | 84.432 |
| model | passed | 10/10 检查通过 | 86.422 |

另有以下独立检查，不能与主单测简单相加成“唯一测试数量”：

- 原始20项审计探针：最终支持环境下20/20通过，原断言不变；模拟LLM响应只验证保护逻辑。
- 可选PyFlink运行时单测：8/8通过，失败0、错误0；补足核心环境中缺少PyFlink导致的8项跳过。
- 健康真实HTTP证据的实际模型调用：11/11检查通过，输出`unknown`，实际诊断124.144秒。72条事件和6条trace未改写，只有诊断触发器是明确标注的测试构造。
- 故障模型原始输出的独立Top-1复核：从模型选择的证据键映射服务，而非借用规则结果；结果见`model-controls/fault-top1-final-review.json`。
- Python 3.11全新虚拟环境使用锁定依赖从本地缓存安装、依赖兼容与核心导入通过；这不是重新联网下载或Docker镜像构建测试。
- 安全/故障边界28项、3.11运行时相关74项、3.12相关15项通过；3.11/3.12两个真实preflight通过。3.10/3.13拒绝行为为注入版本检测的单测，未冒称安装并运行了这些解释器。Docker配置仅完成静态检查，未计作部署成功。

### 主单测跳过明细

- `test_real_stream_restore_http_and_diagnosis (integration.test_native_streaming.NativeStreamingIntegration.test_real_stream_restore_http_and_diagnosis)`：real Kafka/Flink test: set RUN_NATIVE_STREAMING=1 after bootstrap_native
- `test_configured_model_on_archived_actual_evidence (integration.test_real_llm.RealModelTests.test_configured_model_on_archived_actual_evidence)`：opt-in real model integration; use scripts.verify_llm for retained artifacts
- `test_invalid_utf8_keeps_original_bytes_and_legitimate_replacement_character (test_source_bridge.SourceBridgeTests.test_invalid_utf8_keeps_original_bytes_and_legitimate_replacement_character)`：optional PyFlink 1.20 runtime not installed
- `test_java_timestamp_guard_matches_python_metric_contract (test_source_bridge.SourceBridgeTests.test_java_timestamp_guard_matches_python_metric_contract)`：optional PyFlink 1.20 runtime not installed
- `test_source_schema_and_partition_watermark_construct_with_real_api (test_source_bridge.SourceBridgeTests.test_source_schema_and_partition_watermark_construct_with_real_api)`：optional PyFlink 1.20 runtime not installed
- `test_keyed_window_global_state_reused (test_stream_operators.AdaptiveOperatorTests.test_keyed_window_global_state_reused)`：optional PyFlink 1.20 runtime not installed
- `test_cleanup_reclaims_capacity_and_clears_timer (test_stream_operators.StreamOperatorTests.test_cleanup_reclaims_capacity_and_clears_timer)`：optional PyFlink 1.20 runtime not installed
- `test_configuration_rejects_non_numeric_and_out_of_range (test_stream_operators.StreamOperatorTests.test_configuration_rejects_non_numeric_and_out_of_range)`：optional PyFlink 1.20 runtime not installed
- `test_duplicate_conflict_and_bounded_capacity (test_stream_operators.StreamOperatorTests.test_duplicate_conflict_and_bounded_capacity)`：optional PyFlink 1.20 runtime not installed
- `test_parser_poison_values_all_reach_dlq_with_coordinates (test_stream_operators.StreamOperatorTests.test_parser_poison_values_all_reach_dlq_with_coordinates)`：optional PyFlink 1.20 runtime not installed

真实Kafka/Flink与真实模型分别由本轮独立集成步骤执行，未把这些跳过当作成功。

## 本轮修复的文件与行为

完整增量文件、修改前后SHA256见 `docs/phase10-fix/changed-files.json`；原代码备份见同目录`source-before-fixes.zip`。分组说明如下。

| 文件/模块 | 修改原因与最终行为 | 验证入口 |
|---|---|---|
| `app/incidents.py`、事件关联/恢复测试 | 先按告警服务找种子，再分页补齐相关trace；行数/字节/trace有上限并明确截断；新鲜失败span阻止恢复并重置健康保持 | 单测、原streaming探针、native、reliability |
| `app/model_grounding.py`、`analysis_schema.py`、`diagnosis.py`、`llm_client.py`、`llm_config.py`、`rca.py`、`reporting.py` | 模型只能引用实际证据；健康证据不能支持故障候选；保留原输出和归一化哈希；内部原因未知时不伪造因果结论；保留analysis-v2兼容协议，增加紧凑证据选择协议 | 模型单测、原7项反例、真实故障/健康调用 |
| `scripts/verify_llm.py`及模型测试 | 实际模型与模拟测试分开；严格标签不进入提示词；候选/unknown和证据列表联合约束；调用预算持久化、模型进程有限启动和清理 | 最终model步骤、健康控制、原始选择复核 |
| `app/storage.py`、`app/evidence.py`及初始化测试 | 已有库DDL/WAL变更前备份；附属表统一受锁事务初始化；备份失败禁止迁移/清理；保留旧报告和历史数据 | 初始化/并发/升级测试，23005条证据HTTP验收 |
| `app/runtime_support.py`、`scripts/preflight.py`、`scripts/local_platform.py`、CI | 支持已验证的3.11/3.12；未支持版本在触盘前失败；明确版本限制不能替代正确的存储卷选择 | 9项运行时测试、3.11/3.12真实preflight |
| `benchmarks/http.py`、`simulator/service.py`及质量测试 | 每个场景核对预期事件/trace完整性；健康对照确有异常则标退化/阻塞，不篡改标签；区分服务处理与遥测发布耗时 | 真实HTTP benchmark和单测 |
| `benchmarks/flink.py`、`scripts/benchmark_flink.py`及测试 | 真正测Kafka→Flink→Kafka→SQLite；核对去重窗口计数/ID/持久化/lag；协调器错误有界重试，未知lag不当0；与真实聚合器共用规则版本 | 12项专项单测、最终真实Flink性能14项 |
| `scripts/acceptance.py`、`acceptance_contracts.py`及runner测试 | 空、未知、矛盾结果失败；核对终态哈希；源清单覆盖配置/用例/嵌套Dockerfile；保留venv Python路径；各可选门禁独立记录 | 原工程10项探针、最终完整runner |
| `.env.example`、`docker-compose.yml`、两Dockerfile | 暴露模型协议配置；容器安装锁定依赖并执行依赖检查；保持本机端口边界 | 单测和静态配置检查；容器未实测 |
| `README.md`、`README-history.md`、`docs/phase10-fix/*` | 主入口按当前实现重写；保存历史说明；给出部署、架构、迁移备份、复测命令与限制 | CLI帮助核对、链接核对、最终包源哈希检查 |

## 真实流处理性能与效果评估

本轮Kafka 3.9.1、Flink 1.20.3、connector 3.3.0-1.20；3个分区、并行度2、30秒窗口/10秒滑动、5秒乱序容忍。输入是明确标记的测试故障事件，经过真实服务链路；指标是实测，不是预估容量。

| 目标事件/秒 | 确认唯一/重复事件数 | 实际发送事件/秒 | 含窗口落库等待完成事件/秒 | 自然窗口末输入→落库p95毫秒 | 控制尾窗末输入→落库p95毫秒 |
|---:|---:|---:|---:|---:|---:|
| 20 | 60 / 3 | 19.931 | 4.644 | None | 10166.0 |
| 60 | 180 / 9 | 59.679 | 17.977 | None | 7030.0 |
| 120 | 360 / 18 | 119.909 | 52.379 | None | 3881.0 |

`None`/JSON `null`表示没有该类样本，绝不是零延迟。短突发结束后通过明确记录的控制水位关闭尾窗；尾窗延迟不能当作连续生产流的自然检测SLO。发送吞吐与完成吞吐分开；最终checkpoint提交积压为0。直接拥有PID的CPU/RSS实测保存在JSON/CSV，不包括全部PyFlink后代进程，RSS可能重复计算共享页。未执行超过300秒的长期容量或TTL过期重放认证。

同一标记合成数据集上的检测结果：

| 方案 | 准确率 | 精确率 | 召回率 | 误报率 | 漏报率 | 决策覆盖率 |
|---|---:|---:|---:|---:|---:|---:|
| 固定 | 0.9330 | 0.1200 | 0.1132 | 0.0337 | 0.8868 | 0.9264 |
| 动态 | 0.9919 | 1.0000 | 0.7925 | 0.0000 | 0.2075 | 0.6966 |

动态方案检测到14/18个故障episode；低流量和冷启动仍可能漏检/弃权。以上是已公开生成器上的对照，不是生产准确率。RCA标记集共12例，其中带根因标签8例Top-1与Top-3均8/8；另有未知/健康案例。小样本结果不能推广成通用100%根因准确率。

实际故障模型候选为`payment`，诊断耗时77.152秒，模型名、权重SHA、提示词版本、输入证据集、原始输出、token使用和预算记录均在`model/result.json`及模型报告。无价格配置时费用为null，未编造货币成本。平台呈现可核验异常观测和候选服务，不把相关性当因果；内部机制未知，置信等级上限low。

## 失败记录与剩余边界

首次总回归 `acceptance-01` 真实失败且完整保留：旧提示词版本测试、证据库完整性、Flink协调器启动错误及模型状态矛盾。Flink第二次定位到测量器规则版本过滤错误，修复后专项及最终总回归通过。第一次命名为`acceptance-final`的尝试仍在同步目录中，native真实损坏导致失败，后续模型因缺少该次故障报告未完成；它不是本交付最终通过证据。权威终态是`acceptance-final-local/completed.json`。不得删除失败记录或把旧失败改写成成功。

Docker/Podman/容器socket均不可用，rootless namespace探针被运行环境拒绝，无法验证镜像构建、Compose启动和容器重启。不是已完成Docker部署。外部商业模型API未提供密钥，没有借用或泄露任何凭证；真实诊断使用本地Qwen GGUF模型服务。远程访问仍要求令牌、TLS和网络隔离；内部Kafka/Flink未作公网安全认证。多机HA、宿主机网络损伤、长期负载及生产质量不在本轮已验证范围。

消息语义为Flink内部checkpoint一致性、Kafka至少一次输出与数据库业务幂等；不宣称端到端exactly-once。迟到消息隔离，不自动改写已结束窗口。完整证据表示捕获时已入库且符合查询的集合，不代表跨主题所有遥测已到齐。数据清理默认dry-run；主动应用前必须有通过完整性校验的备份和归档。未知/损坏数据库不会自动重建。

## 实际执行命令

以下为最终runner逐步执行的原始命令；其中绝对路径是本次测试环境路径。干净环境复现使用README和deployment中的对应安装路径，不应复制临时路径到其他主机。

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.test_suite --output /tmp/aiops-3fa2499b-final-01/unit
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.verify_runtime_http --output /tmp/aiops-3fa2499b-final-01/runtime-http
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.verify_launcher --output /tmp/aiops-3fa2499b-final-01/launcher
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.verify_evidence --legacy-source /workspace/scratch/3fa2499b629d/repair/aiops-platform/artifacts/phase7/reliability-05/data/aiops.sqlite3 --output /tmp/aiops-3fa2499b-final-01/evidence
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.evaluate_detection --output /tmp/aiops-3fa2499b-final-01/detection
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.evaluate_rca --output /tmp/aiops-3fa2499b-final-01/rca
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.benchmark_platform --component http --business-rates 2,5 --ingest-rates 20,60 --duration 2 --repeats 1 --output /tmp/aiops-3fa2499b-final-01/benchmark-http
```

```bash
/tmp/aiops-phase10-native/venv/bin/python -m scripts.native_streaming --runtime-dir /tmp/aiops-phase10-native --output /tmp/aiops-3fa2499b-final-01/native
```

```bash
/tmp/aiops-phase10-native/venv/bin/python -m scripts.verify_reliability --kafka-home /tmp/aiops-phase10-native/kafka_2.13-3.9.1 --output /tmp/aiops-3fa2499b-final-01/reliability
```

```bash
/tmp/aiops-phase10-native/venv/bin/python -m scripts.verify_faults --scenario all --output /tmp/aiops-3fa2499b-final-01/faults --kafka-home /tmp/aiops-phase10-native/kafka_2.13-3.9.1 --allow-resource-pressure
```

```bash
/tmp/aiops-phase10-native/venv/bin/python -m scripts.benchmark_platform --component kafka --kafka-home /tmp/aiops-phase10-native/kafka_2.13-3.9.1 --kafka-rates 50,150,300 --duration 3 --repeats 1 --output /tmp/aiops-3fa2499b-final-01/benchmark-kafka
```

```bash
/tmp/aiops-phase10-native/venv/bin/python -m scripts.benchmark_flink --runtime-dir /tmp/aiops-phase10-native --rates 20,60,120 --duration 3 --repeats 1 --output /tmp/aiops-3fa2499b-final-01/flink-benchmark
```

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.verify_llm --output /tmp/aiops-3fa2499b-final-01/model --evidence-report /tmp/aiops-3fa2499b-final-01/native/actual-http-report.json --local-model-file /tmp/aiops-phase10-model/qwen2.5-1.5b-instruct-q4_k_m.gguf --model-python /tmp/aiops-phase10-model/venv/bin/python
```

健康模型的实际独立命令为：

```bash
/workspace/scratch/3fa2499b629d/repair/clean-install/venv/bin/python -m scripts.verify_llm --evidence-report /workspace/scratch/3fa2499b629d/repair/results/model-controls/healthy-input/healthy-evidence-report.json --output /workspace/scratch/3fa2499b629d/repair/results/model-controls/healthy-model-final --local-model-file /tmp/aiops-phase10-model/qwen2.5-1.5b-instruct-q4_k_m.gguf --model-python /tmp/aiops-phase10-model/venv/bin/python --expect-unknown
```

## 交付内容与后续验收

项目包包含完整修复源码、原历史产物、部署与架构文档、本轮JSON/CSV/日志/报告快照、原始失败取证和修改前代码备份。运行时二进制、模型权重、虚拟环境、新测试Kafka segment/checkpoint和可再生成数据库不打包（原历史包中的文件保持不动）；按bootstrap命令下载/构建。

本轮可在现有环境完成的修复和回归已结束。还需具备Docker的隔离环境才能补齐容器部署门禁；活动SQLite必须避开文件同步目录；任何生产暴露或容量承诺须另有相应实测。未完成项目没有被算作成功。
