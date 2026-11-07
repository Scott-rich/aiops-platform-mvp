# 接口兼容与监控口径

原有路径、响应结构、方法和查询参数保持兼容；新增 `/v1` 别名。默认旧路径继续服务并返回 Deprecation/Sunset（2027-02-04）。若迁移客户端已准备好，可设置 `API_LEGACY_GET_REDIRECT=1`，使旧 GET/HEAD 返回 301 并保留查询串；写请求仍保留方法和 body。**这是对提示词“默认301”的明确偏差，用于满足更高优先级的原契约及断言保留要求。**

中间件顺序为：外层观察 → 既有鉴权/请求体边界 → 限流/可选重定向 → 路由。401/403 不耗用已认证请求限流预算；401、301、429 均有 x-request-id 和 nosniff。IP 使用直接连接地址；未信任 X-Forwarded-For。路由标签使用模板，未知路径归 unmatched，防止任意 URL 导致指标基数增长。默认每秒 2000 token、burst 10000、最多 10000 桶；不能把该上限当实测吞吐。

| 指标 | 口径 |
|---|---|
| aiops_http_requests_total | 按 method/route/status 累计的 HTTP 请求 |
| aiops_http_request_seconds | API 延迟直方图，含拒绝响应 |
| aiops_events_stored / aiops_alerts_stored | 数据库当前存量 gauge，非终身累计入库 counter |
| aiops_database_bytes | SQLite 文件、WAL、SHM 当前字节和；并发消失文件忽略 |
| aiops_rate_limited_total | 已经过请求边界后被令牌桶拒绝的请求 |

兼容保留已有 JSON detail；新增 x-aiops-error-code=http_状态码。429 额外含 code=RATE_LIMITED 和 Retry-After。主要状态：400遥测/请求错误，401未认证，403无权限，404不存在，408请求体超时，413超限，422参数校验，429限流，503配置/存储不可用。未把所有历史错误响应强制改为新 body 结构。

类型迁移对动态 JSON 边界显式使用 Any；mypy disallow_untyped_defs 通过不意味着已有 schema 全部变成结构类型。详细范围见 type-migration.json。既有 wire validator、grounding 校验、阈值和测试断言没有放宽。
