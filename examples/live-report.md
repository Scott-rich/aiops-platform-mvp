# 诊断报告 59985833-2dd0-4ae0-9bd7-dccc087ecf4c

- 告警：04733120e7b751baf7bc1e87
- 服务：gateway / 规则：high_error_rate
- 分析方式：heuristic
- 窗口：1792466640000–1792466670000（UTC epoch ms）
- 证据 SHA-256：`f89591c8d76d79f223e31439ecf479f2b42abcb717d819c2be3249ea9714b655`

## 分析

gateway 触发 high_error_rate。建议先验证 payment；此结论来自规则分析。

### 候选原因：payment

payment 出现异常观测，可能影响调用方；现有证据不能确定内部根因

判断等级：low

支持证据：a8e1d96920c24eef8d5a5632e2716d04, a6fa7e0130a8413ab85ec56613c6dfda, 1f205cded1dd43f3b7910323cbb42cee, 331334e0cb4f47378165141cfb000b4d, b0189e013eb44f978031369093d9dc53

验证步骤：

- 按 trace_id 检查 payment 与调用方的起止时间和错误码
- 检查依赖可用性及近期变更，复测故障恢复后的延迟和错误率

## 覆盖范围与限制

```json
{
  "all_topics_complete": "unknown; independent Kafka topics have no shared watermark",
  "latest_service_metric_time": 1792466674683,
  "metric_time_reached_window_end": true,
  "range_candidate_count": 1442,
  "related": 1428,
  "scanned": 1442,
  "selected": 1428,
  "truncated": false
}
```

- 候选原因需要人工验证；相关性不等于因果
- confidence 是判断等级，不是校准后的概率
- 快照只包含接收时已入库且在查询上限内的数据

完整原始证据、Kafka 坐标、历史案例和模型输入输出见同名 JSON 报告。
