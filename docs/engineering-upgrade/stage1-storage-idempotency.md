# 第一阶段：事件持久化幂等边界

## 修改

- `app/storage.py`：`ingest_many()` 先验证整个 batch，再在一个事务中处理事件。相同 `event_id` 和相同 canonical payload 忽略，忽略新传输来源差异；同 ID 异内容采用 first-persisted-wins，原事件和来源不变。冲突完整 JSON 及本次来源写入现有 deadletters，并记录 warning；不修改 schema。返回实际新增的 `(event, source)` entries。
- `app/engine.py`：FastErrorDetector 只观察上述新增 entries，数据库重放和冲突事件不会重新计入连续错误或生成告警。
- `tests/test_storage_idempotency.py`：新增 6 个测试，覆盖 canonical 相等/来源变化、同批冲突、跨批及重开数据库冲突、超过 100 KB 冲突原文保留、非法 batch 原子性、Pipeline 重启重放和冲突不得驱动检测器。

旧的 ingest()/已有 API 和数据库表结构保留。写入 events 和对应 deadletters 位于同一事务；验证失败时整批不写。未操作项目历史数据库、删除数据或迁移。

## 实际验证

2026-10-20，在项目根目录执行：

```bash
python -m unittest tests.test_storage_idempotency tests.test_engine tests.test_core -v > /workspace/scratch/599403941817/upgrade-audit/stage1-storage-idempotency-tests.log 2>&1
```

结果：**20 通过、0 失败，13.628 秒**。其中新增测试 6 个，既有测试 14 个。原始输出保存在 `stage1-storage-idempotency-tests.log`。全部使用临时 SQLite 和本地检测器，无外部 API、特殊硬件或 Kafka/Flink 依赖。该结果仅验证本模块，不代表真实 Kafka/Flink 链路已验收。

辅助检查：`rg -n 'ingest_many\\(' . --glob '*.py'` 检查调用者；除 Pipeline 外均不依赖原返回值。项目目录不是 Git 工作树，`git diff` 无法用于本次检查；没有将此命令误计为产品测试失败。

## 边界与下一步

- 幂等边界是持久化事件 ID，不是端到端 exactly-once。FastErrorDetector 状态仍在内存；持久化提交后、检测前进程崩溃可能漏掉快速警告，重启不重建连续错误状态。独立 Flink 检测和重启/回放验收应单独验证。
- 重复冲突的每次输入都会保留一条 deadletter，便于追踪每次来源；保留清理策略属于后续证据工程阶段。
- 尚未验证真实 Kafka offset 提交/重平衡边界、Flink 故障恢复或并行消费者。下一步由主任务打通并验证这些链路，不能用本地单元测试替代。
