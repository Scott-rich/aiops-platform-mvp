# P2 运行时部分独立审计

审计范围：`app/observability.py`、API/本地摄取边界，以及相对基线 `64195e2` 的全部 `app/` 类型迁移。**本次只审运行时部分，尚不能判定整个 P2 可交付**。CI、覆盖率红→绿、镜像、Helm、kind 部署、仓库产物外置和最终 README 仍由后续证据单独验收。本审计由独立模型执行，不代表人类复核。

当前结论：**本次运行时范围可交付，未发现剩余阻断问题**。初审发现的中间件组合回归和 WAL/SHM 指标采样竞态均已修复，并由审计者独立复测。这个结论不表示整个 P2、CI 或部署已经通过。

## 发现与修复要求

| 严重度 | 状态 | 证据与影响 | 修复要求 |
|---|---|---|---|
| 高 | 已关闭，独立复核 | 初审时外层 429/301 跳过鉴权、请求标识和安全响应头。现 `app/observability.py:109` 单独实现 `RequestPolicy`，`:234` 将它放到既有边界内侧；外层 `Instrumentation` 仍记录 401。 | 联合探针依次收到 401→301→200→429，四者均有唯一 request ID 与 nosniff；未授权请求不消耗 token。新增 `CombinedBoundaryTests` 也通过。 |
| 中 | 已关闭，独立复核 | 初审的 `exists()`→`stat()` 存在 WAL/SHM 消失竞态。现 `app/observability.py:198` 逐个 stat，并只捕获 `FileNotFoundError`。 | 独立故障注入让 WAL 的 stat 抛出 `FileNotFoundError`，指标仍返回 200，DB+SHM 大小正确为 1280 字节。没有把其他磁盘错误静默吞掉。 |
| 已关闭 | 已复核 | 自动补充返回 `Any` 曾使 204 摄取路由被 FastAPI 当作具有响应体的路由，启动时断言失败；`app/local_api.py:110` 现为 `Response`。 | 保留失败日志；本次针对性测试通过，不删除既有断言。 |
| 已关闭 | 已复核 | Ruff 清理曾移除被其他模块使用的隐式导出。`app/contracts.py:7` 的 `__all__` 与 `app/diagnosis.py:17` 的 `prompt_records` 导出已恢复。 | 今后的清理继续检查跨模块导入契约。 |

首项探针原始结果保存在 `/tmp/p2-boundary-probe.json`；摘要如下，不包含凭证：

```text
anonymous /v1/items/a -> 401, request_id/nosniff present
same IP, valid credentials -> 429, request_id/nosniff absent
legacy redirect enabled, anonymous /items/a -> 301, request_id/nosniff absent
```

修复后独立探针保存在 `/tmp/p2-boundary-probe-fixed.json`，旧失败记录未覆盖。实际中间件顺序为 `Instrumentation → RequestBoundary → RequestPolicy`。同一 IP 的匿名访问先获 401，授权旧 GET 获 301，授权版本化 GET 获 200，再次授权访问因配额耗尽获 429；所有响应均保留边界头。

## 已完成的独立验证

1. 对 31 个已改动 `app/*.py` 文件执行 AST 结构对比，去除注解、格式、import 排序和等价的 `zip(strict=False)` 后逐项审查剩余变化。检测器阈值、事件/告警校验、报告哈希、证据引用约束、LLM grounding、保留/归档与模型预算没有被放宽。新增的 `completion is None` 和动态检测器 state 缺失检查明确失败；原有有效路径保持不变。
2. `git diff 64195e2 -- tests wire_contract.py flink` 为空：原有测试断言、共享 wire contract、Flink 实现没有修改。新增测试不替代原断言。
3. 独立执行 `/tmp/aiops-research-venv/bin/python -m pytest tests/test_phase10_runtime.py tests/test_observability_upgrade.py tests/test_api.py tests/test_runtime_support.py -q`：初审 **41 passed**；加入联合边界测试并修复后重新执行，**42 passed**，仅第三方弃用警告。另以独立探针覆盖 200 路径和 WAL 消失分支。
4. 检查 `/tmp/p2-targeted-03.log` 的 **32 passed** 与 `/tmp/p2-targeted-04.log` 的 **33 passed**。查看 `/tmp/p2-regression-02.log`：历史全回归为 **2 failed, 485 passed, 10 skipped**，两项失败均为已修复的 204 路由导入问题；该历史记录不能当作全回归通过。
5. 指标 route 标签来自已注册模板，未知路径归为 `unmatched`，未知 method 归为 `OTHER`；未把 IP、事件 ID 或原始 URL 作为 Prometheus 标签。令牌桶有锁、条目上限和规范化后的路径键；旧路径与 `/v1` 别名共享限额。
6. `/v1/demo/ingest` 使用去前缀后的权限路径匹配，保留只允许 ingest token 写入指定入口的限制；既有请求体大小、分块总量、超时、跨源写入和 FastAPI 参数校验仍存在。
7. `metrics` 使用持久化记录数量 Gauge，而非冒称历史累计吞吐量；计数器与限额均为单进程内状态，不能直接声称跨副本共享配额。
8. 迁移主要采用显式 `Any`。`docs/research/type-migration.json` 与 `CONTRIBUTING.md` 已说明这只是动态 JSON 边界的注解覆盖，**不构成完整的结构类型安全**。未发现新 `pragma: no cover`、`type: ignore` 或 `noqa`。

## 对提示词 14 项的本次判定

| # | 检查 | 判定 | 本次证据或边界 |
|---|---|---|---|
| 1 | 报告数值可追溯到 JSON | 无法判断（完整 P2） | 运行时探针有独立 JSON；最终 P2 `result.json` 和报告尚未提交。 |
| 2 | 种子/确定性 | 不适用 | 本次 API 契约审查不使用随机模型；令牌桶测试使用固定时钟值。 |
| 3 | test 集未调阈值/prompt | 不适用 | P1 在 `/tmp/aiops-p1-source-v2` 冻结快照运行；本次不审其结果。 |
| 4 | 检测指标口径 | 不适用 | 本次未修改检测评测结果；沿用 P0 独立报告。 |
| 5 | 基线实际运行 | 不适用 | 本次为运行时契约；不是检测器/模型对比。 |
| 6 | 样本量与统计 CI | 不适用 | 42 项确定性契约测试不是统计性能估计；没有把它们写成部署性能结论。 |
| 7 | 数据来源与合成标识 | 不适用 | 本次不引入训练或评测数据。 |
| 8 | 未改断言/放宽门禁/删失败 | PASS（运行时范围） | 既有 `tests/`、wire contract、Flink 相对基线无变化，失败日志仍保留。 |
| 9 | 未虚增覆盖率 | PASS（代码检查）；覆盖率未判断 | 未发现忽略标记；`Any` 限制公开说明，尚未核验整体覆盖率门禁。 |
| 10 | CI 真正红→绿 | 无法判断 | CI 正在构建；本地通过不是远端 CI 通过证明。 |
| 11 | 引用真实性 | 不适用 | 本次代码审查未新增论文事实；P0 引用另有核验。 |
| 12 | 失败案例保留 | PASS（已观察记录） | 保留 204 路由失败日志与联合边界探针；不只展示成功测试。 |
| 13 | 部署措辞符合规模 | 无法判断（完整 P2） | 这里只证明进程内 API 行为；Docker/kind/Kafka/Flink 部署不在本次结论范围。 |
| 14 | 新人按 README 复现 | 无法判断 | 最终 README 和部署说明尚在构建。 |

本次必须修复项已全部关闭；没有为凑满三项而添加未经证实的缺陷。后续建议补两个运行实验：真实 HTTP 进程的鉴权/限流/指标压力回放；并发 SQLite checkpoint 与持续指标抓取的稳定性检查。当前已执行的是契约测试和受控文件消失注入，不是上述负载实验。

兼容性说明：旧 GET 的 301 当前由 `API_LEGACY_GET_REDIRECT=1` 显式开启，默认继续返回原路径内容并带弃用头；写请求保留原方法与请求体。这个选择需要在最终 README 写清，不能声称默认所有旧 API 已自动 301。最终 P2 仍需单独补齐 CI、镜像与部署证据。
