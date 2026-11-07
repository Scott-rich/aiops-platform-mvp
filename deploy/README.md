# 部署与验证

这是单机研究型部署。kind 配置包含三个 Kubernetes 节点容器，仍然只有一台物理机。当前交付的集群前置探针明确为 BLOCKED：执行环境没有 Docker、kind、kubectl，且 user namespace 创建被系统拒绝。**没有 kubectl get pods 成功证据，没有 30 次真实 Chaos Mesh 注入结果。** Helm 模板的静态校验不等于部署成功。

本次已在新建 Python 3.12 虚拟环境执行 `make demo`。这验证真实本地 HTTP API 与合成 fixture；不是干净容器或外部微服务基准。原生 Kafka/Flink 验收的证据另列，不代替三节点实验。

## 隔离主机上的复现

准备 Docker Engine、kind 0.27.0、kubectl 1.32.2、Helm 3.17.3。建议给全栈预留 16 GiB 内存；资源上限在 values.yaml 与固定的 Boutique 清单中，可按设备调整，但必须在实验报告保留实际配置。以下命令只用于专用研究主机。

```bash
mkdir -p /tmp/aiops-kind-storage/flink/checkpoints /tmp/aiops-kind-storage/flink/savepoints
# Flink 官方镜像的运行账户；仅修改本实验专用目录。
sudo chown -R 9999:9999 /tmp/aiops-kind-storage/flink
kind create cluster --config deploy/kind-three-nodes.yaml
kubectl --context kind-aiops-research create namespace aiops
kubectl --context kind-aiops-research create namespace boutique
kubectl --context kind-aiops-research label namespace boutique aiops-chaos-target=true

docker build -t aiops-research:0.3.0 .
docker build -f flink/Dockerfile -t aiops-flink:0.3.0 .
kind load docker-image aiops-research:0.3.0 aiops-flink:0.3.0 --name aiops-research
helm lint deploy/helm/aiops
helm upgrade --install aiops deploy/helm/aiops --namespace aiops --wait --timeout 10m
kubectl --context kind-aiops-research apply -k deploy/boutique
kubectl --context kind-aiops-research -n boutique rollout status deployment/frontend --timeout=5m
kubectl --context kind-aiops-research get pods -A -o wide
```

运行端必须保存命令日志、Pod/节点资源 JSON 与完整版本。`helm upgrade --wait` 不能代替检查 Flink job：再查询 `/jobs/overview` 确认所有 task RUNNING，并检查 topics/submit-flink Job。

API 和 worker 同 Pod、共享 PVC，部署策略 Recreate，副本数限定为 1。增加 API 副本会让 Helm 主动失败。Kafka 是单 broker KRaft。Flink 可以有多个 TaskManager；默认 checkpoint 目录由三个 kind 节点挂载同一宿主目录，**不适用于真正跨物理机恢复**。跨机环境必须提供共享持久存储 `flink.existingClaim` 并另行验证。

默认 API local 模式用于隔离集群。需要 token 模式时创建包含 API_AUTH_MODE=token 及三个互不相同、32–512 字符 token 的 Kubernetes Secret，再设置 `app.authSecret`。不把凭据写入 values 或结果文件。

## 观测链路与边界

Collector 0.123.0 → 原始 `otel.traces/logs/metrics` → OTLP bridge → `telemetry.*` → Kafka/Flink/worker。桥接器在发布得到 broker ACK 后才提交原始消息位点；重放用稳定 event ID 去重。原始 OTLP 按 Kafka retention 保留，DLQ 记录原消息位置和摘要。不要把原始 OTLP JSON 直接送给 schema-v1 worker。

- trace：SERVER span 同时投影为 request；CLIENT span 不重复计算请求。保留 trace/parent/span ID。
- gRPC 非零状态投影为旧协议 500；这是兼容字段，不是真实 HTTP 响应码。payment 依赖故障通过真实网络 partition 形成 gRPC 不可用，不冒称返回 HTTP 503。
- log：保留字符串 message 和可用的关联 ID；不伪造缺失 trace ID。
- host：仅接收同资源、同采样时间成对的 CPU idle / memory used utilization；不把缺失数据填 0。不支持/无法配对的指标计数并保留上游原始数据。
- 固定 Boutique v0.10.4 的六个服务使用上游已有 tracing 开关；adservice 等未覆盖服务不得声称有完整链路。供应的 tracing patch 尚未在集群验证。
- Prometheus baseline 从同批 spanmetrics 计算 30 秒错误率和 p95。必须在实际部署核对 exporter 的指标名/标签，不能以模板存在认定对照已完成。

## 真实故障与规模实验

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update chaos-mesh
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh --version 2.6.7 \
  --namespace chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock

# 各 port-forward 放在独立终端；地址必须保持 loopback。
kubectl --context kind-aiops-research -n aiops port-forward svc/aiops-api 8000:8000
kubectl --context kind-aiops-research -n aiops port-forward svc/aiops-prometheus 9090:9090
kubectl --context kind-aiops-research -n aiops port-forward svc/aiops-jobmanager 8081:8081
kubectl --context kind-aiops-research -n aiops port-forward svc/aiops-kafka 19092:19092
kubectl --context kind-aiops-research -n boutique port-forward svc/frontend 8080:80

python -m scripts.research.cluster_experiment --seed 2026 --execute --output /tmp/chaos-NEW
python -m scripts.research.evaluate_cluster --input /tmp/chaos-NEW --output /tmp/chaos-evaluation-NEW
python -m scripts.research.scale_experiment --execute --duration 600 --rates 200,1000,3000 --output /tmp/scale-NEW
```

Kafka 分开配置 INTERNAL（集群 DNS）与 EXTERNAL（localhost:19092）listener。host 采样必须按上面命令转发 EXTERNAL 端口，才能让 broker metadata 回到本机；该部署配置仍待实际集群验证。

`cluster_experiment` 只计控制器 AllInjected=True 的故障；提交 CRD 不计数。删除故障资源、workload Ready、平台判定 resolved 分别记录。任何失败保存记录并在 finally 删除当前故障资源。采样缺失/漏检/未恢复是删失；不能当 0 秒。Top-K 按原始竞争排名处理并列。

规模脚本发出的是 HTTP RPS 请求负载，记录实际 Kafka offset 导出的 EPS；**不会把计划的 200/1000/3000 RPS 改名为已达成的 EPS**。当前严格三档 EPS 稳态目标尚未验证。CPU 来自选定容器 cgroup v2，RSS 来自 PID 1；Kafka/Flink JVM 和独立 LLM 进程 RSS 未纳入这一采样器。

安全启动入口显式 `--no-proxy-headers`，直接连接 IP 作为限流身份。反向代理会共享分桶；若自行开启代理头支持，必须另行限定可信代理并重新验证，不能沿用当前保证。Token 部署中 Prometheus 只挂载 Secret 的 API_READ_TOKEN 文件；从主机运行规模/故障采样器时通过环境变量 API_READ_TOKEN 提供同一只读令牌，不写入命令参数或结果。OTEL host 指标目前要求同 resourceMetrics 和完全一致的纳秒时间戳；真实 Collector 分组/对齐尚未验证，不声明 host 端到端互操作通过。
