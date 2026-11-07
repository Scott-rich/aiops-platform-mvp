# 阶段 1 配套工程修复：验收工具可信度与模型配置隔离

本次修改遵循既有本地运行入口和输出文件约定，没有改动数据库结构、迁移或既有运行数据。没有增加高级故障类型，没有修改遥测传输策略。

## 修改文件与功能

- `scripts/verify_optimized.py`：删除所有验收 `assert`，改用运行时显式检查；同时验证支持证据和反对证据；独立计算 API 哈希/本地哈希/引用结果，保存逐报告详情；遥测检查必须具有四服务及真实整数失败计数，缺字段也不能通过；要求四服务齐备且健康；Markdown 响应必须成功；启动或中途异常仍保存 failed JSON 和已执行检查，未执行项目为 null；finally 清除尚未结束的故障并停止 launcher，普通清除异常不跳过停止；保存清理结果、错误类型、结束时间和实际总耗时。异常结果不写任意异常文本以避免意外记录密钥，原始终端堆栈仍可在启动日志定位。
- `scripts/local_platform.py`：提取现有安全 `.env` 读取逻辑，保留继承环境优先，包括显式空字符串；模拟服务和 loadgen 移除全部 LLM_* 配置及常见模型供应商密钥，API 仍获取模型配置；保留既有代理绕过行为。
- `tests/test_verification.py`：12 项测试，覆盖两种引用、快照篡改、缺字段、未执行检查、四服务就绪、遥测失败统计、启动/页面失败 JSON、强制终止、清除故障普通异常、环境变量优先与隔离，以及真实 `python -O` 子进程仍以非零码拒绝虚构反对证据。

## 实际执行结果

### 最终源码的模块测试

```bash
PYTHONPATH=/tmp/aiops-python-deps:. python -m unittest discover -s tests -p test_verification.py -v
```

**12 通过，0 失败，0 错误，0 跳过，1.439 秒，退出码 0**。日志：`stage1-verification-tests.log`；JSON：`stage1-verification-tests.json`。测试中的模型/HTTP异常是明确的替代传输故障，`-O` 检查是实际启动 Python 子进程；未调用外部模型。

### 实际本机 TCP/HTTP 演练：失败已保留

```bash
PYTHONPATH=/tmp/aiops-python-deps:. python -m scripts.verify_optimized \
  --output /workspace/scratch/599403941817/upgrade-audit/stage1-local-verification \
  --port 19000 --base-port 19181
```

实际运行 35 秒健康、30 秒 payment 503、45 秒恢复；**退出码 1，9 项检查中 8 通过、1 失败**。`telemetry_failures_zero=false`：inventory 的 HTTP 遥测发生 1 次 TimeoutError。结果记录 `status=failed、completed=false`，没有将失败标为通过。

实际收集 3876 条事件、25 条告警、1 个 availability 事件与 1 个 latency 事件，均已恢复，8 版报告、4 条本地通知；所有报告的双向引用和快照哈希通过。这里只报告真实本机 HTTP + Python 处理结果，**没有 Kafka/Flink 或真实 LLM 执行**。

保留的原始文件：

- `stage1-local-verification.log`
- `stage1-local-verification/optimized-run.json`
- `stage1-local-verification/optimized-report-versions.json`
- `stage1-local-inventory-error.log`
- `stage1-local-failure-analysis.json`

原始临时目录 `/tmp/aiops-v2-verification-kf7n_p39` 的日志和数据库保持原样，未删除、未压缩整份运行数据。实测五个监听端口 19000、19181–19184 均已释放。

故障定位边界：inventory 错误日志最后写入约 07:17:33.979 UTC，演练 +38.417 秒；原日志没有嵌入时间戳，因此此时间依据文件 mtime。原始 SQLite 记录显示三条 inventory 证据 ingestion 延迟 3173–3247ms，且同批 trace 的 order/gateway 出现 504。首个告警接收 +39.267 秒、首报创建 +40.003 秒，均晚于该超时，可排除启动竞态和首报先生成造成阻塞。现有证据**无法区分 SQLite/ingest-lock 等待、线程调度或共享资源争用；同期 JVM 负载只是未经证实的假设，不能认定为根因**。没有通过盲重试、扩大超时或忽略错误来让验收变绿。

该 110 秒进程启动于最后的就绪/清理/Markdown 小修订之前；最终修订由上述 12 项测试验证，最终源码的独立完整运行由主任务随后执行，并使用新目录，不覆盖本次失败证据。

## 限制、依赖和下一步

模块测试需要现有 Python 依赖，不需要外部 API、Docker、特殊硬件或有效密钥。本机演练需要可用 loopback 端口和 Linux `/proc`；不等同于 Kafka/Flink 验收。未宣称高可用，尚未证实遥测错误的具体内部原因。

下一步由主任务先完成实际 Kafka/Flink 集成；待其启动活动结束后，以最终源码和新目录独立重跑本地演练，比较共享负载边界。如再次出现超时，应先增加 receipt/ingest/lock 时间测量再定位修复。现有 HTTP 失败可能丢弃事件的边界保持明确，没有在本轮扩展传输架构。
