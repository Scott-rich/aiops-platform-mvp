# 第一阶段独立代码复核

日期：2026-10-20。只读复核当前工程相对备份 `aiops-platform-v0.2-before-engineering-upgrade.zip` 的改动。未修改运行源码，未启动 Kafka/Flink 服务。检查范围：共享数据契约、Java source envelope/partition watermark、有限去重及窗口冲突、worker 持久化和 offset、readiness/submitter/Compose、原生与本地验收脚本。

## 发现和处理

| 优先级 | 发现与实际证据 | 复核时处理状态 |
| --- | --- | --- |
| P1 | `ops.alerts` evidence 含 NaN、`ops.deadletter` raw 含 NaN，decode_record 接受，随后 persist_batch 在 canonical 序列化抛 ValueError；不提交 offset，反复重放 poison。使用临时 SQLite 实际复现两例。 | 主任务已修：decode 阶段 canonical UTF-8 检查；再次运行相同输入，均进入 deadletters（2条）而非抛异常。 |
| P1 | Java `new String(bytes, UTF_8)` 会替换非法 UTF-8，而 Python JSON bytes 解码拒绝，导致检测与证据不一致。 | 流处理模块已改严格 decoder、明确 encoding_error、保留原字节 base64；增加 Java/契约测试。最终真实作业需使用新 jar。 |
| P1 | 原生脚本仅在 jar 不存在时编译；源码更新后会复用旧 jar，实测版本可能与交付源码不一致。 | 原生工具已改每次编译，记录源和 jar SHA；最终验收以最新完整运行结果为准。 |
| P1/P2 | launcher 实际设置 `local-http-python-rules`，readiness 只识别 `local-http`，本地模式会错误探测 Kafka/Flink。 | 主任务已兼容两种值。 |
| P1 | 大非法输入的 DLQ 会膨胀超限：600001 字节原始非法 JSON 经 deadletter 转义后为 1200216 字节，超过默认 Kafka producer max.request.size 1048576 和 worker 1000000 字节限制；base64 有同类问题。纯函数实际复现。 | 已修：bounded_deadletter_json 最终 UTF-8 输出上限131072字节，保留哈希/长度/显式截断/prefix/origin；流处理负责人已报告9项纯函数测试通过。真实大输入输出验证由最终原生验收记录。 |
| 验收工具 | attempt-01 的 wall-clock 等待使用所有输入最大时间戳，包含故意注入的未来非法事件；即使 guard 正确也会超时。 | 已改仅记录明确推进的有效 watermark。attempt-01 失败必须保留，不能改写成成功；后续完整运行独立记录。 |

## 实际复核测试

```bash
PYTHONPATH=/tmp/aiops-python-deps:. python -m unittest tests.test_worker tests.test_submit_job tests.test_readiness -v
```

结果：20 passed，0 failed/errors/skipped，0.293 秒。原始输出 `stage1-independent-review-tests.log`。测试运行时源码仍持续改进，此数仅是该次专项测试数，不应代替最终全量回归。

另有临时 SQLite 的 NaN poison 重现/修复后复查、600001 字节 DLQ 膨胀探针。均为实际小规模测试，不是实际 Kafka 服务集成验收。

## 需要保留的验收边界

- 内部 checkpoint exactly-once 与 Kafka sink at-least-once 分开说明；worker 事件主键幂等有效，但 deadletters 重放可能重复，快速检测内存状态重启不恢复。
- v3 source/dedup/window UID 与旧 v2 checkpoint 不兼容。升级只能显式保留旧备份、fresh replay；不能宣称旧 checkpoint 原位可恢复。
- 不从 Job 顶层 RUNNING 推断所有 operator 已 RUNNING；当前 submitter/readiness 已加入任务计数条件。
- 原生真实运行不等同 Docker 镜像/Compose 部署验证；无有效模型配置不等同真实 LLM 诊断通过。
- per-window accumulator 仍按 unique event ID 增长，不能称常量空间。全输入静默时尾窗口不会按墙钟自动结束。
- 当前 review 不覆盖后续动态检测、全故障矩阵、性能、完整证据归档等计划能力，不能把它们标成完成。

结论：本次复核找到了并已推动修复多个真实 poison-input / 验收一致性问题。大 DLQ 边界已增加有界封装；以最终源码的全量回归、最新 jar 的真实 Kafka/Flink 完整运行与保留的失败记录共同决定阶段验收。

## submitter CLI 独立复查

主任务提出 `FLINK_REST_URL=http://jobmanager:8081` 直接传给 `flink run -m` 是否兼容。使用实际安装的 Flink1.20.3 Java `DefaultCLI.parseCommandLineOptions` → `toConfiguration` 验证，没有启动或联系任何集群。`jobmanager:8081`、`http://jobmanager:8081`、`http://127.0.0.1:28081`、`http://[::1]:8081` 四个值均成功解析；前两个生成相同 REST host/port 和 SSL=false。因此这不是已确认缺陷，不建议为猜测修改代码。

原始结果：`stage1-submit-cli-parser-probe.log`。这是实际 CLI 参数解析探针，不能算成真实作业提交验收。
