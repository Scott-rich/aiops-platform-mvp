# 诊断报告 0d9e9917-39e3-42f0-bc7c-7ad38c0e46e1

- 告警：c2e4809ee2c3ac34d3f5b49d
- 服务：gateway / 规则：high_error_rate
- 分析方式：heuristic
- 窗口：1792465329333–1792465359333（UTC epoch ms）
- 证据 SHA-256：`aafdfb5e83ece43cbf9ba2f0c56dbd38b7da7e4717bb35d9d65d63b8e0acaa4c`

## 分析

gateway 触发 high_error_rate。建议先验证 payment；此结论来自规则分析。

### 候选原因：payment

payment 出现异常观测，可能影响调用方；现有证据不能确定内部根因

判断等级：low

支持证据：fixture-15-payment-span, fixture-16-payment-span, fixture-17-payment-span, fixture-18-payment-span, fixture-19-payment-span

验证步骤：

- 按 trace_id 检查 payment 与调用方的起止时间和错误码
- 检查依赖可用性及近期变更，复测故障恢复后的延迟和错误率

## 覆盖范围与限制

```json
{
  "all_topics_complete": "unknown; independent Kafka topics have no shared watermark",
  "latest_service_metric_time": 1792465348833,
  "metric_time_reached_window_end": false,
  "range_candidate_count": 480,
  "related": 480,
  "scanned": 480,
  "selected": 480,
  "truncated": false
}
```

- 候选原因需要人工验证；相关性不等于因果
- confidence 是判断等级，不是校准后的概率
- 快照只包含接收时已入库且在查询上限内的数据

完整原始证据、Kafka 坐标、历史案例和模型输入输出见同名 JSON 报告。
