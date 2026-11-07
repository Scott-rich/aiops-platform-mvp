# Kafka / Flink 流处理现状审计（只读阶段）

审计日期：2026-10-20。对象：`aiops-platform` v0.2。本文只报告源码审查、实际执行的纯 Python 测试和运行环境探测；没有启动 Kafka / Flink，也没有完成 Docker 构建。

## 已检查范围

`docker-compose.yml`、`flink/{detector.py,aggregate.py,rules.py,Dockerfile}`、`infra/{submit_job.py,init-topics.sh,README.md}`、`app/{contracts.py,worker.py,local_detection.py}`、`simulator/telemetry.py`、`tests/test_flink_accumulator.py`、`requirements.txt`。并查看缓存中的 PyFlink 1.20.3 wheel API 源码，核对窗口、side output、watermark、KafkaSource / KafkaSink 接口；这不是运行时导入或作业提交验证。

## 能力分类

| 能力 | 当前状态 | 证据与限制 |
| --- | --- | --- |
| HTTP 模拟服务向 Kafka 发送真实遥测 | 已实现，Kafka 实际运行未验证 | `KafkaProducer`，acks=all、有限重试和超时；异步失败会计数/记录，但无持久缓冲，broker 不可用可能丢遥测 |
| Kafka KRaft、topic 初始化、依赖启动 | 仅配置，未实际部署验证 | 单 broker、3 分区、7 天保留，Kafka 健康检查和 init 完成条件存在；副本数 1，不是高可用 |
| Kafka → PyFlink → 窗口告警 → Kafka | 代码已实现，完整链路未验证 | KafkaSource / AggregateFunction / KafkaSink 为真实 API；没有本次作业 RUNNING、消息消费、输出、checkpoint 证据 |
| 时间窗口、乱序与过迟数据 | 部分实现 | 30 秒窗口 / 10 秒滑动 / 5 秒乱序 / 10 秒 idle / allowed_lateness=0；需分区乱序实测；全输入静默尾窗口不会自动结束 |
| 非法事件 DLQ | 部分实现 | JSON / 多数字段校验，Kafka DLQ；大整数 OverflowError 未捕获；Flink DLQ 未保留 Kafka topic/partition/offset |
| 重复消息、聚合状态 | 部分实现，纯 Python 部分已测试 | 每窗口 event_id 精确去重、稳定 alert_id；冲突重复值抛异常，可毒化作业；应用与 Flink 的冲突处理语义不一致 |
| checkpoint / 重启 | 仅配置，未恢复验证 | 每 10 秒 exactly-once 内部 checkpoint、共享卷、固定延迟重启；输出 at-least-once；JobManager 无 HA 自动恢复，重新提交默认 earliest |
| 本地与 Flink 对照 | 部分实现 | 有精确 v1 Python 规则和 v2 直方图聚合对照测试；没有真实 Flink 输出对照；本地 detector 不等于 Flink 运行验证 |
| 流处理集成 / 恢复测试 | 尚未实现 | 缺少可重复 broker/作业生命周期工具、分区乱序、DLQ 溯源、重启恢复断言与机器可读报告 |

## 已确认问题

1. **非法大整数会绕过 DLQ，触发作业异常。** `flink.rules.validate_metric` 调用 `math.isfinite(10**1000)` 会抛 `OverflowError: int too large to convert to float`；`ParseMetric` 只捕获 ValueError / TypeError / KeyError。实际纯 Python 重现成功。需要数值边界和 OverflowError 转换，并增加非法值矩阵测试。
2. **两个事件校验器有不兼容规则。** 实际调用证实：小数 event_time 被 `app.contracts` 接受、被 Flink 拒绝；未来 120 秒时间被 app 接受、被 Flink 拒绝；300 字符 service 被 app 拒绝、被 Flink 接受。相同事件可能进入证据库却未参加检测。二者均接受 request 缺少 trace_id；新规范需要明确哪些事件必须有调用关联 ID，兼容现有数据。
3. **冲突重复消息是确定性作业失败。** `WindowAccumulator.add` 对相同 event_id、不同检测值抛 ValueError；没有捕获、旁路或持久隔离策略。恢复重读同一消息仍会失败。文档有披露，但不满足可恢复异常输入验收，应制定统一冲突策略并记录原始证据。
4. **Flink DLQ 缺少 Kafka 原消息坐标。** 使用 value-only deserializer 丢弃 topic / partition / offset；非法和过迟 DLQ 仅含 raw/event、reason、observed_at。worker 对原遥测的 SQLite 记录保留 origin，但不能可靠将非法 raw 字符串与 Kafka 坐标关联。需要带来源的 Kafka envelope 或 Java deserializer 小桥接，避免虚构 Python 不存在的 API。
5. **watermark 在 source 后按并行子任务生成，缺少 Kafka 分区级事件时间隔离。** source 使用 no_watermarks，JSON 解析后 assign_timestamps_and_watermarks。3 个分区 / 2 个 source 并行度下，同一 subtask 的较新分区可推动 watermark，较旧分区回放事件可能被视为过迟。官方建议 source 处利用 partition-aware watermark；应先建立偏斜分区测试，再决定 source envelope / timestamp 方案。当前 with_idleness 再 with_timestamp_assigner 顺序正确：PyFlink 1.20 的 with_idleness 会返回新对象。
6. **恢复能力不能从 checkpoint 配置推断。** 无本次完成 checkpoint、TaskManager 故障恢复、offset/state 恢复实测；Session JobManager 丢失不能自动恢复作业。`infra/submit_job.py` 只跳过正在运行的同名作业，没有显式 restore 参数或最新有效 checkpoint 选择。重新提交 earliest 依靠告警业务幂等，但不是状态恢复。
7. **消息丢失与停机边界需要测量。** producer 有界重试但没有 spool；worker 初始化 KafkaConsumer 无限循环（每 5 秒）且无显式上限/取消控制；DLQ topic 不由证据 worker 消费。broker outage / DB failure 行为尚无集成断言。
8. **v2 聚合不是常量状态。** 固定 2402 桶外仍维护每窗口 unique event_id 映射，状态随流量增长；代码与文档目前对此说明准确。25ms 桶在 800ms 阈值边界保持决策一致；超过 60s 使用观测最大值，精度标记为 null。应保留这部分有效实现。

## 本次实际执行结果

```bash
PYTHONPATH=/tmp/aiops-python-deps:. python -m unittest discover -s tests -p test_flink_accumulator.py -v
```

结果：8 passed / 0 failed，0.020 秒。覆盖 accumulator 阈值一致性、桶误差、溢出、重复 / 合并、冲突原子性、主机均值、pickle、稳定告警 ID。这些测试没有导入 pyflink，不能当成真实 Flink 验证。

另执行两个短 Python 探针：事件校验器差异矩阵，以及大整数 OverflowError 重现；均为问题复现，不计入通过测试数。未运行任何 Kafka / Flink 进程。

## 原生运行可行性（本次实际探测）

- Linux x86_64；Java 17.0.20；Python 3.12.14；有 uv / curl / wget；无 docker、podman、Python 3.10/3.11；无 `/opt/kafka`、`/opt/flink`。
- 缓存存在一个 PyFlink 1.20.3 cp310 wheel，只能用于源码核对，不能在 Python 3.12 直接运行。官方 Flink 1.20 文档要求 Python 3.8–3.11。
- 3 个最长 10 秒 HEAD 请求均获得 HTTP 200：Kafka 3.9.1 tgz（约122MB）、Flink 1.20.3 tgz（约484MB）、Kafka 3.3.0-1.20 connector JAR（约5.6MB）。只读阶段没有下载这些大文件。
- 可行路径：使用独立 Python 3.11 环境、Kafka 原生 KRaft 单节点、PyFlink 1.20.3 匹配 libs / 原生 Flink、固定 connector，在 loopback 和项目外临时目录启动隔离进程；需要实际下载、启动、生成 checkpoint 后才可升级验证状态。Docker Compose 仍必须独立标记未验证。

## 第一阶段建议改动与验收

| 文件 | 最小变更 | 主要风险 / 验收 |
| --- | --- | --- |
| `app/contracts.py`, `flink/rules.py`（可引入共享轻量契约模块） | 统一事件校验、数值上限、关联字段、兼容策略 | 保留 schema v1 兼容；相同输入两个检测路径判定一致 |
| `flink/detector.py`, `flink/aggregate.py` | 非法输入可靠 DLQ、冲突重复策略、配置化 watermark / checkpoint / broker、稳定 UID | 避免反复恢复 poison record；真实乱序/重复/偏斜分区测试 |
| `flink/Dockerfile`, 少量 Java 桥接（仅在必要时） | Kafka envelope 来源元数据和 source 时间语义 | PyFlink Python builder 不提供通用 deserializer；必须运行验证，不能凭 API 猜测 |
| `infra/submit_job.py` | 可配置 REST / 路径、就绪轮询、状态检查、显式 checkpoint restore | 新提交与恢复不可混淆；恢复 job ID、checkpoint ID、耗时留证 |
| `docker-compose.yml`, `infra/init-topics.sh` | 保留有效依赖，补输出健康 / 启动参数与退出行为 | Compose 未运行前不得称部署完成；不删除命名卷 |
| 新 `scripts/native_*`, `tests/integration/test_streaming*.py` | 可选原生隔离启动、真实 Kafka/Flink 探针、清理和 JSON 结果 | 不依赖 Docker 时可运行；失败返回非零、输出实测与阻塞 |
| `infra/README.md`, 阶段报告 | 真实命令、至少一次链路、restart、差异结果 | 配置、模拟、真实运行分开记录 |

第一阶段最低验收：Kafka 和 Flink 真进程启动；固定时间事件到真实 topics；至少一个真实窗口告警并与 Python 对照；非法 / 重复 / 乱序 / 过迟输入测试；完成 checkpoint 后 TaskManager 或作业故障恢复；原始 origin 和告警 ID 可追溯；所有计时、计数和失败均落 JSON。资源不足可缩并行度但须记录。不能凭源码正确或 Compose 解析通过满足该验收。

## 官方依据

- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/installation/ （Python 3.8–3.11；PyFlink 1.20.3 安装）
- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/ （KafkaSource / KafkaSink、at-least-once 与 checkpoint 语义）
- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/ （输入最小 watermark、idle、partition-aware source watermark）
