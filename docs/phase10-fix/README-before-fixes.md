# 分布式系统智能运维平台 · 第十阶段联调与验收

第十阶段新增可选 API 令牌权限、请求大小/读取超时边界、脱敏结构化日志、检测循环就绪检查及可重复的验收入口。旧事件协议和数据表保持不变。

- [第十阶段验收报告](docs/phase10/第十阶段验收报告.md)
- [部署、接口权限与验收命令](docs/phase10/部署与架构.md)
- [基线审计、计划与风险](docs/phase10/现状审计与实施计划.md)

默认保持本地隔离研究用途；要使用令牌模式，先阅读部署说明。**原生 Kafka/Flink、真实本地模型、Docker、外部商业模型是不同验收项目**，以本轮报告逐项结果为准。

以下保留第九阶段的历史交付说明：

第九阶段在第八阶段基线上增加启动前配置检查、模拟器子进程凭证隔离、Python 3.12 依赖锁定快照及 CI 配置。详细测试和未完成项见 [第九阶段验收报告](docs/phase9/第九阶段验收报告.md) 与 [运行指南](docs/phase9/部署与工程化说明.md)。本项目仍限可信本地隔离环境；**没有用户认证或授权，不能公开或供不可信网络使用**。Docker 和真实外部模型的本轮结果不得用前期记录代替。


第八阶段完善证据管理与报告完整性：去除原先 20,000 条扫描限制，完整证据按批冻结并支持分页/筛选，保留最多 1,800 条的分析快照。报告明确记录完整集 ID、条数、实际引用和分析截断；新增历史分页、版本比较、流式哈希校验、归档恢复与先备份后清理。

- [第八阶段验收报告](docs/phase8/第八阶段验收报告.md)
- [证据 API、部署、架构、归档和保留策略](docs/phase8/证据管理部署与架构.md)
- [现状审计、修改计划与兼容性风险](docs/phase8/现状审计与实施计划.md)

```bash
# 安装 requirements.txt 后，不需要 Kafka、GPU 或模型密钥即可执行。
python -m unittest discover -s tests -v
python -m scripts.verify_evidence --output artifacts/evidence-run-01 --count 23005
# 查询已有运行平台的固定证据集；下一页使用返回的 next_cursor。
curl 'http://127.0.0.1:8000/evidence-sets/FULL_SET_ID/records?page_size=200'
```

完整集表示“冻结事务时已入库且匹配查询的记录”，不表示所有 Kafka 主题数据已经到齐，也不表示模型分析了全部证据。旧报告不改写，完整性未知；原有模型上下文 ID 和 RCA 快照 ID 保持原义。默认不自动删除数据，原始事件清理只能通过本机 CLI 显式执行，先在线备份和校验归档。

本阶段实际执行的测试、失败修复记录和原始结果位于 `artifacts/phase8/`。Docker/Compose、真实外部模型、浏览器点击端到端和多副本高可用未验证；不要将配置、合成测试或历史结果当作本轮实测。完整证据与历史版本会增加磁盘需求，当前没有自动淘汰冻结集的策略。

---

# 第七阶段历史交付

第七阶段补齐故障恢复与系统可靠性：连续错误状态持久化、事件到告警关联的事务边界、Kafka坏消息重放幂等、有界存储重试与offset提交超时，以及更保守的恢复确认。保留既有业务接口、事件字段和报告结构。

- [第七阶段验收报告：实测结果、失败修复与限制](docs/phase7/第七阶段验收报告.md)
- [可靠性部署、故障恢复、配置和数据丢失边界](docs/phase7/可靠性部署与恢复指南.md)
- [第七阶段现状审计与实施计划](docs/phase7/现状审计与实施计划.md)

```bash
# 安装requirements.txt后，先执行不依赖外部服务的回归。
python -m unittest discover -s tests -v
# 用专属Kafka/worker/API验证中断与重启；Kafka安装见第七阶段指南。
python -m scripts.verify_reliability \
  --kafka-home /tmp/aiops-phase7-kafka/kafka_2.13-3.9.1 \
  --output artifacts/reliability-run-01
```

本轮重新运行真实Kafka/Flink及TaskManager checkpoint恢复，并验证旧库副本升级。Docker/Compose仍未运行验收；真实外部模型未配置，LLM故障路径使用明确标注的回环超时端点。单broker、单SQLite不构成高可用，历史SQLite损坏根因仍未确定。各次原始结果位于`artifacts/phase7/`，不能以通过的复测抹去失败记录。

---

# 第六阶段历史交付

第六阶段新增真实 HTTP/Kafka 分档负载、延迟分位数、CPU/RSS、Kafka积压与排空时间，以及统一的检测/RCA/模型统计入口。保留第五阶段业务代码、接口和数据库结构。所有实测数值、失败记录与限制见验收报告。

- [第六阶段验收报告](docs/phase6/第六阶段验收报告.md)
- [性能评估部署、指标定义和复现命令](docs/phase6/性能评估部署与架构.md)
- [第六阶段现状审计与实施计划](docs/phase6/现状审计与实施计划.md)

```bash
# 安装既有 requirements.txt 后；此命令不需要 Kafka/模型，启动本次专属本机服务。
python -m scripts.benchmark_platform --component http --output artifacts/my-http-benchmark
# 含Kafka的完整命令、慢消费对照、真实模型独立开关见第六阶段指南。
```

本阶段Kafka测量使用生产Python持久化/快速检测路径，未运行Flink引擎。真实模型无配置，耗时/费用保持未测；合成标签准确率与真实注入样例分别报告。输出目录不覆盖，测试结束自动清理本次服务。

---

## 第五阶段历史交付


第五阶段新增十类可重复故障场景、独立故障生命周期审计、受限资源worker、真实TCP延迟代理，以及真实Kafka暂停消费/积压/追赶测试。保留第四阶段的检测、RCA、接口与数据结构。

- [第五阶段最终验收报告](docs/phase5/第五阶段验收报告.md)
- [十类场景运行命令、部署和安全边界](docs/phase5/故障注入部署与架构.md)
- [本轮现状审计与实施计划](docs/phase5/现状审计与实施计划.md)

```bash
# 项目根目录；先按下方步骤安装 requirements.txt。
python -m scripts.verify_faults --scenario cascade --output artifacts/my-cascade
# 资源默认关闭；显式启用单独验收：
python -m scripts.verify_faults --scenario memory --allow-resource-pressure --output artifacts/my-memory
# 完整十类需Java17和Kafka；获取与全量命令见第五阶段指南。
```

本阶段真实HTTP与专属Kafka/worker验证结果见最终报告。没有重跑Flink引擎，没有调用真实LLM；历史Kafka/Flink记录仍保留，不能当作本阶段新实测。所有压力只针对本次受限子进程。每次输出必须使用新目录。

---

用 Python、Linux、Kafka 和 Flink 构建“服务运行 → 实时采集 → 异常发现 → 事件关联 → 证据诊断 → 恢复确认”的运维闭环。现在已提供可持续运行的本地控制台、四个真实 HTTP 服务、故障注入、渐进式报告和证据校验。

第四阶段新增默认可关闭的动态异常检测：按服务/指标/UTC时段维护中位数与MAD基线，处理暖机、低流量、缺失、过期、重复窗口和状态恢复，并提供同数据固定/动态算法对照。默认仍为`DETECTION_MODE=fixed`，现有接口及历史数据保留。

- [第四阶段验收报告](docs/phase4/第四阶段验收报告.md)
- [动态检测配置、架构与复现](docs/phase4/动态检测部署与评估.md)
- [本轮现状和修改计划](docs/phase4/现状与实施计划.md)

以下第三阶段和更早数字为历史记录，第四阶段实测以本轮验收报告为准。

在v0.2工程上逐阶段迭代。第一阶段已有真实Kafka/Flink链路；第二阶段结构化模型接入已实现，但真实模型诊断仍未验收。按用户要求进入第三阶段，新增同trace依赖图、多候选根因、服务角色、证据核验和可重复RCA评估。SQLite损坏在本轮再次出现，存储可靠性仍有阻塞；Docker/Compose尚未运行验收。
- [改动前现状审计](docs/engineering-upgrade/现状审计报告.md)
- [分阶段改造计划](docs/engineering-upgrade/分阶段改造计划.md)
- [第一阶段交付验收报告](docs/engineering-upgrade/阶段交付验收报告.md)
- [架构与可靠性边界](docs/engineering-upgrade/第一阶段架构.md)
- [第二阶段复核与实施范围](docs/phase2/阶段复核与实施记录.md)
- [模型接入、架构和独立验证指南](docs/phase2/模型接入与验证指南.md)
- [第二阶段验收报告](docs/phase2/第二阶段验收报告.md)
- [数据库保护与本轮失败记录](docs/phase2/数据库保护与本轮失败记录.md)
- [算力与推理加速](docs/phase2/算力与推理加速.md)

**第三阶段历史交付：RCA功能已实现并验证，整体平台仍未完全验收。** 最终回归210通过、0失败、2跳过；真实HTTP 12项通过；真Kafka/Flink第二次25项通过。首次native 16项通过后因SQLite索引损坏失败，原库和日志已保全，复测通过不代表损坏原因已经解决。12个固定合成算例的Top-1/Top-3为8/8，仅代表该验收集。第二阶段真实模型仍因历史调用输出截断而未通过，本阶段未再次调用模型。

- [第三阶段验收报告与失败记录](docs/phase3/第三阶段验收报告.md)
- [第三阶段现状与计划](docs/phase3/现状与实施计划.md)
- [根因分析架构与验证指南](docs/phase3/根因分析架构与验证指南.md)
- [第三阶段部署与复现](docs/phase3/部署与复现.md)
- [第二阶段继续修复记录](docs/phase2/继续修复记录.md)

## 0. 复现真实 Kafka/Flink 验收

Linux已有Java17和uv时，在项目根目录执行：

```bash
python3 -m scripts.bootstrap_native --runtime-dir /tmp/aiops-native-runtime
/tmp/aiops-native-runtime/venv/bin/python -m scripts.native_streaming \
  --runtime-dir /tmp/aiops-native-runtime --output artifacts/my-native-run
```

第二条命令会启动并最终清理本次隔离服务，输出目录不能已存在。首次需联网下载依赖；该测试不会调用外部模型。原始成功记录位于 `artifacts/native-attempt-04/result.json`，前三次失败和原始日志也保留。完整说明见 [原生验收指南](infra/NATIVE_TESTING.md)。没有Docker不再被当作跳过真实流处理的理由；原生验收与容器验收仍分开。

**体验持续控制台可使用下面的本地模式。** 它用Python检测规则和HTTP遥测，不运行Kafka/Flink。默认规则诊断；只有配置模型并通过返回结构/引用校验才标记模型辅助。

## 1. 一条命令启动控制台

在项目目录使用Python3.11或3.12。以下命令适用于Linux/WSL：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m scripts.local_platform
```

浏览器打开 <http://127.0.0.1:8000>。平台会启动API、gateway/order/inventory/payment四个独立Uvicorn进程和约3RPS的持续负载。控制台可查看服务状态、故障事件、报告版本、证据详情和哈希校验，并对模拟服务注入或清除故障。API文档位于 <http://127.0.0.1:8000/docs>。

默认服务端口为18081—18084；日志和SQLite数据位于`data-local/`。按Ctrl+C停止全部子进程，数据保留。端口冲突或需独立数据目录时：

```bash
python -m scripts.local_platform --port 8100 --base-port 18181 --data-dir data-second --rps 3
```

此模式通过HTTP传输遥测，复用Python窗口规则和完整事件/报告流水线；**不运行Kafka或Flink引擎**。Linux环境会额外只读采集`/proc`可见的CPU、内存和负载；其他系统仍可运行服务模拟。

## 2. 体验一次故障闭环

先保持健康流量约35秒，再在控制台选择payment，注入HTTP错误；也可使用API：

```bash
curl -X POST http://127.0.0.1:8000/faults/payment \
  -H 'Content-Type: application/json' \
  -d '{"kind":"error","duration_seconds":120,"error_rate":1.0}'
```

payment返回503，order和gateway传播502。连续5个5xx触发快速预警；30秒窗口按10秒滑动进一步确认错误率。平台根据共同trace和异常叶子服务关联告警，把同一次传播故障汇聚为事件。缺少共同证据时保守地保留服务级事件，不能仅凭时间相近就合并。

事件先生成不调用模型的`preliminary`初报；至少等待3秒，在证据条件满足后生成`enriched`补证报告，等待上限15秒。缺失信息、关联服务或候选服务变化可产生后续版本。所有版本保留独立快照；慢模型调用不阻塞其他事件的初报。

清除故障：

```bash
curl -X POST http://127.0.0.1:8000/faults/payment \
  -H 'Content-Type: application/json' -d '{"kind":"none"}'
```

恢复确认要求受影响服务在最近10秒内各至少10个健康请求样本，最新数据不超过5秒，采样最大间隔不超过5秒，并继续健康保持10秒。无流量、遥测中断或只有过期样本都不能自动判为恢复。确认后生成不可变的`resolved`报告；旧窗口告警仍可补入历史告警计数，但不会改写已冻结的恢复报告。

`/notifications`保存每个事件幂等的打开/恢复记录，是本地持久化通知箱，**没有向邮件、Slack等外部渠道发送消息**。平台不自动执行重启、扩容或其他修复。

## 3. 核心能力与接口

| 能力 | 当前实现 |
| --- | --- |
| 服务模拟 | gateway → order → inventory / payment；真实HTTP、trace和父子span传播 |
| 故障控制 | 有时限的延迟、5xx、超时、业务不可用；CLI补充真实进程不可用、TCP延迟、默认关闭的有界资源压力、消息故障 |
| 检测双通道 | 连续5个5xx快速预警；窗口可选固定/动态基线，暖机与样本下限；默认30s/10s、5s乱序容忍 |
| 事件关联 | 共同trace、异常叶子服务及实例、规则族关联；保守处理歧义 |
| 报告版本 | 初报、补证、恢复；每事件一个模型任务，HTTP尝试受持久化预算限制 |
| 证据追溯 | 原始事件、来源坐标、trace、案例版本、提示词、模型信息、SHA-256 |
| 恢复管理 | 新鲜样本、样本量、时间跨度及健康保持联合确认 |
| 控制台 | 服务状态、事件、故障控制、版本报告、依赖图、多候选及证据核验 |
| 根因分析 | 同trace父子图、分异常类型传播、多候选/双角色、unknown、冻结RCA和评分依据 |
| Kafka/Flink | v3分区watermark、来源坐标、冲突隔离、有界DLQ；第三阶段原生复测25项通过；首次本轮SQLite损坏仍未解决，容器未验收 |

| API | 用途 |
| --- | --- |
| `GET /health`、`GET /services` | API/数据库状态和模拟服务进程状态 |
| `GET /ready` | Kafka/Flink全部算子/worker依赖就绪，未就绪返回503；本地模式明确跳过分布式探针 |
| `GET /alerts`、`GET /incidents` | 原始告警与关联事件列表 |
| `GET /incidents/{incident_id}` | 事件详情、恢复条件及实时历史告警计数 |
| `GET /incidents/{incident_id}/reports` | 不可变报告版本列表 |
| `GET /report-versions/{report_id}` | 完整报告JSON |
| `GET /report-versions/{report_id}/rca`、`GET /reports/{report_id}/rca` | 对应报告冻结RCA及校验 |
| `GET /incidents/{incident_id}/topology` | 最新报告依赖图、证据集和校验 |
| `GET /report-versions/{report_id}/markdown` | Markdown导出 |
| `GET /report-versions/{report_id}/verify` | 快照一致性校验 |
| `GET /notifications` | 本地打开/恢复通知记录 |
| `POST /faults/{service}` | 模拟服务故障控制 |

`/health`成功不表示Kafka/Flink已就绪；`/services`为进程存活检查，不代表业务请求健康。旧`/reports`接口供基线示例保留，v0.2主入口使用事件和`/report-versions`。

## 4. 配置模型与历史案例

可复制`.env.example`为`.env`并配置：

```dotenv
LLM_BASE_URL=https://YOUR_COMPATIBLE_SERVER/v1
LLM_MODEL=YOUR_MODEL_NAME
LLM_API_KEY=YOUR_KEY
```

重启本地控制台后生效。适配器请求`LLM_BASE_URL/chat/completions`，不绑定厂商SDK；环境变量优先于`.env`。无配置或模型调用失败时，报告保持`audit.analysis_mode=heuristic`；只有严格v2结构、观测服务和全部证据引用通过校验后才标记`llm-assisted`。允许“无法确定”，置信等级附依据且不冒充校准概率。每事件一个持久化任务，默认最多2次HTTP调用（包含有限重试）；超时不自动重试。全局分钟限流、输入/输出大小和绝对时限见`.env.example`及[模型指南](docs/phase2/模型接入与验证指南.md)。

真实模型独立验收：配置环境变量后运行`python -m scripts.verify_llm --output artifacts/my-real-model-run`；无配置退出2并记录blocked。无密钥时可用指南中的可选Qwen CPU运行时，真实推理与离线HTTP替身测试分开。首次建模型审计表前自动在线备份SQLite；已开始但结果未知的崩溃调用不盲目重试。

`cases/`三份JSON都是显式标记的示例案例，不是真实历史故障。可按现有结构填入自己的复盘案例，包含`case_id、version、services、rule_ids、keywords、symptoms、root_cause、verification、remediation`。本地运行重启后读取；容器模式需重建api/worker。检索采用服务、规则和关键词打分，报告冻结匹配内容及版本；未实现向量检索。

模型只读取观测证据，故障注入标签不进入诊断输入。候选故障服务不等于已证明内部根因；引用存在和快照哈希正确也不能证明推理正确。日志发送到外部模型之前，应按自己的业务补充字段脱敏和访问控制。

## 5. Kafka/Flink标准部署

Linux安装Docker Engine及Docker Compose v2后：

```bash
cp .env.example .env
docker compose config
docker compose up -d --build
docker compose ps -a
docker compose logs -f flink-submit taskmanager worker
```

首次构建需联网下载镜像、依赖与Connector JAR。建议开发机预留4核、16GB内存和10GB以上空闲磁盘；这是规划估计，未做容量压测。版本固定为Kafka3.9.1、Flink/PyFlink1.20.3、Java17、Kafka Connector3.3.0-1.20，属于兼容基线。PyFlink独立运行在匹配镜像中，不要直接装入API使用的Python3.12环境。

| 入口 | 地址 |
| --- | --- |
| 控制台/API | <http://127.0.0.1:8000> |
| Flink UI | <http://127.0.0.1:8081> |
| 模拟网关 | <http://127.0.0.1:8001/health> |
| 宿主机Kafka | `localhost:19092` |

公开端口仅绑定本机。确认Flink作业`aiops-event-time-detector-v3`为RUNNING，再进行故障注入；初始化及作业提交容器完成后退出属于正常现象。详细命令见`infra/README.md`。

Flink v3拓扑继续使用v2增量聚合算法：25ms延迟桶、60秒普通范围，正常范围P95上界误差小于25ms；800ms恰好是桶边界，因此该阈值判断保持一致。溢出桶会显式标注精度限制。去重仍需保留事件ID与紧凑值，总状态为O(n)，不能称为常量内存。v3 source/parser/dedup/window UID与旧v2状态不兼容，应备份并停止旧作业后全新提交、从Kafka重放，不能直接恢复v2 checkpoint。本地Python对照规则仍使用精确最近秩P95。

宿主机只读采集器可单独运行：

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 AGENT_SERVICE=linux-node python -m app.linux_agent
```

`docker compose --profile collector up -d linux-agent`只反映容器可见的`/proc`，没有cgroup配额换算或节点到业务服务的资产映射。停止容器使用`docker compose down`保留命名卷；加`-v`会删除证据和状态。

## 6. 复现验证与已知边界

第一阶段历史实测（2026-10-20）；第二阶段当前结果见[本阶段验收报告](docs/phase2/第二阶段验收报告.md)：

| 验证 | 通过 / 失败 / 跳过 | 原始结果 |
| --- | --- | --- |
| 最终完整单元/API/Java构造回归 | 111 / 0 / 1 | `artifacts/final-regression/unittest-summary.json`及`unittest.log` |
| 实际Kafka/Flink完整原生验收 | 22 / 0 / 0 | `artifacts/native-attempt-04/result.json`及`logs/` |
| 110秒实际本地HTTP故障闭环 | 9 / 0 / 0 | `artifacts/local-attempt-02/optimized-run.json` |

重型unittest入口默认跳过，不计通过；对应真实脚本已独立完整执行。可在匹配的Python3.11/PyFlink1.20.3环境配置桥接/connector路径后执行所有111个测试；普通API环境会额外跳过可选PyFlink/Java测试。真实流测试使用显式合成事件作精确对照，并另跑实际HTTP服务场景，不能把合成事件称为生产观测。

保留的失败：native-attempt-01测试脚本追时错误，02实际PyFlink retention API不兼容，03测试脚本原因文本匹配过窄；修复后04通过。local-attempt-01曾有一次遥测超时（8项通过/1项失败），原因未证实；最终独立复测02通过，未抹去此前失败。

历史注记：以下为早期阶段的待办记录；故障场景、性能基准及证据分页/归档已在后续阶段实现并记录，具体限制参见各阶段报告；生产安全与高可用仍未完成。多候选RCA在第三阶段实现；动态基线和同数据量化对照在第四阶段实现，验证范围见对应报告。真实模型验证的状态与能力限制单列于第二阶段验收报告。第一阶段报告中的“真实LLM调用0”是当时的历史事实。


```bash
python -m unittest discover -s tests -v
python -m scripts.verify_optimized --output examples
```

第二条命令自动启动独立本地进程，执行35秒健康、30秒支付错误、45秒恢复的110秒验证，完成后停止进程并输出：

- `examples/optimized-run.json`：过程采样、里程碑、检查结果和最终状态。
- `examples/optimized-report-versions.json`：完整的版本报告和证据快照。
- `examples/optimized-report.md`：最后的恢复报告。

以下为保留的v0.2历史验证，**不是本轮新结果**：54项通过，耗时22.487秒；记录为3946条事件、18条告警（3条快速、15条窗口）关联成1个故障事件、4个报告版本和2条本地通知；8项闭环检查全部通过。首个事件和初报在注入约1.62秒后出现，清除约20.45秒后确认恢复。这是单次本机运行结果，不是性能SLA。最后恢复报告的快照达到1800条上限，`truncated=true`，属于明确标识的部分证据；其冻结告警数为15，之后3条历史告警使实时事件计数达到18，报告保持不变。完整范围见`docs/验证记录.md`。

旧`scripts.demo`为离线样例重放；`scripts.live_demo`为早期110秒HTTP基线。它们保留用于对照，默认体验请使用`local_platform`和`verify_optimized`。

Kafka消费采用先持久化再提交offset，事件/告警按稳定ID去重，通知箱也幂等。Flink内部有checkpoint，Kafka输出至少一次；本工程没有端到端exactly-once或生产高可用承诺。跨主题没有共同完整性水位线，报告会记录观测覆盖和缺失项。SQLite、单节点Kafka和本地端口适合开发验证；原始数据和报告暂不自动清理，需监控磁盘。SHA-256用于数据一致性校验，不是外部数字签名。

## 7. 文件导航

```text
app/          API、控制台、事件关联、证据、检索、模型及报告版本
simulator/    四个HTTP服务、负载与故障控制
flink/        v3流拓扑、Java来源桥接、v2聚合算法、精确对照规则
infra/        Kafka初始化与Flink运行说明
scripts/      持续运行入口、优化闭环验收及基线示例
cases/        显式标记的示例故障案例
tests/        核心、API、服务与优化逻辑测试
examples/     实际运行记录和报告快照
docs/         系统设计、路线图及验证记录
```
