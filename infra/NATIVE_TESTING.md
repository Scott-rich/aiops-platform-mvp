# 原生 Kafka / Flink 验收（与 Docker 验收分开）

用于没有 Docker、但能运行 Java 17 的 Linux 开发环境。它启动真实 Kafka KRaft 单节点、Flink JobManager / TaskManager、Python worker / API，以及四个实际 HTTP 服务进程。端口均绑定 `127.0.0.1`；只停止本次启动的进程组，不修改系统服务。

## 准备和执行

要求：Java 17、`uv`、Python 3.11 或 3.12 用来执行引导脚本；网络可访问 Apache archive、Maven Central 和 Python 包源。引导脚本会建立独立 Python 3.11 环境安装 PyFlink 1.20.3，不向 API 的 Python 3.12 环境安装 PyFlink。

```bash
python -m scripts.bootstrap_native --runtime-dir /tmp/aiops-native-runtime
/tmp/aiops-native-runtime/venv/bin/python -m scripts.native_streaming \
  --runtime-dir /tmp/aiops-native-runtime \
  --output artifacts/native-acceptance
```

输出目录必须不存在，以防覆盖已有运行证据。重复测试使用新目录。首次下载 Kafka 3.9.1、PyFlink 及依赖、Kafka Connector 3.3.0-1.20、ECJ 编译器，磁盘和网络耗时因环境不同；运行时使用 Kafka 最大 384MiB Java 堆、Flink JobManager 768MiB / TaskManager 1536MiB 的进程内存配置，外加 Python worker 和 Beam 子进程。这不是总内存上限或性能承诺。

Kafka 发行版使用 Apache 发布的 SHA-512 校验。旧 Maven connector / ECJ 只发布 SHA-1，脚本如实记录 SHA-1 校验及额外本地 SHA-256，不将其称为签名验证。`runtime.json` 记录依赖路径和摘要，`dependency-freeze.txt` 记录实际解析的 Python 依赖版本。仅有 JRE、没有 `javac` 时，使用下载的 ECJ 编译小型 Java source bridge；不提交构建后的 JAR。

## 实际检查内容

1. 真 broker 建立五个 topic，每个3分区；Flink所有任务 RUNNING 后才发输入。
2. 固定标注的测试事件实际写入 Kafka，原始发送 topic / partition / offset 保存在结果中；30s / 10s窗口与本地 Python 精确规则比较告警规则、ID、样本数和 P95 误差。
3. 乱序、分区时间偏斜、相同消息重投、同 ID 冲突，健康输入不告警。
4. 非法 JSON、超大数值、过远未来时间、非法 UTF-8、冲突、过迟事件逐条与 DLQ 原始坐标匹配，并检查原因；非法 UTF-8 保留原始字节的 base64。
5. 先发送10条异常事件，等待已完成 checkpoint 和源 offset 提交，停止并重启 TaskManager；验证 REST `latest.restored` 的实际 checkpoint ID / path，再补10条，窗口精确得到20条。此处是状态恢复，不是重新从 earliest 提交作业。
6. 启动 inventory / payment / order / gateway，真实 HTTP 请求和 payment 5xx 注入；验证 Kafka 遥测被 Flink 告警，worker生成规则诊断报告，支持/反对引用均存在、快照哈希有效、遥测发送失败计数为零。
7. `/ready` 实际探测 Kafka、Flink作业和worker心跳。退出时逐项记录清理结果；清理失败使运行整体失败。

测试事件明确是测试数据；实际 HTTP 场景由真实服务进程产生。二者都经过真 Kafka/Flink，不会替换成内存队列。测试进程移除模型密钥/配置，调用真实 LLM 次数为0；真实模型验收必须独立进行。

## 结果和失败证据

`result.json` 记录环境、配置、源码/bridge摘要、每项检查、Kafka输入/输出坐标、checkpoint及恢复REST原文、里程碑与错误。`logs/` 保存 broker、JM、TM、提交器、worker、API和模拟服务原始日志。`actual-http-report.json` 保存完整报告。失败返回非零并保留 `status=failed` 和错误；不会把未执行项写成成功。

输出中的 `kafka-data/` 是隔离 broker 数据，`checkpoints/` 是本次真实状态目录，不是项目依赖。归档报告时可排除 broker 大型日志段，但应保留 JSON 和原始运行日志；执行测试时不要复用或删除其他实例的目录。默认不执行 CPU/内存耗尽、网络 `tc`、宿主机故障或数据删除。

也可通过显式集成测试入口触发整套验收：

```bash
RUN_NATIVE_STREAMING=1 AIOPS_NATIVE_RUNTIME=/tmp/aiops-native-runtime \
  /tmp/aiops-native-runtime/venv/bin/python -m unittest \
  tests.integration.test_native_streaming -v
```

未设置 `RUN_NATIVE_STREAMING=1` 时该项为 **skipped**，不计作 Kafka/Flink 通过。`--skip-http` / `--skip-restart` 仅用于定位问题，结果会记录请求的场景；这种开发 smoke run 不能作为完整阶段验收。

## 边界

这是原生单节点开发集成测试。它不验证 Docker 镜像构建/Compose启动，不验证多 broker 副本、JobManager HA、远程集群共享 checkpoint 或长期压测。Kafka sink为 at-least-once，业务幂等依赖稳定 alert_id / event_id。所有输入停顿后，事件时间水位线不自动推进，尾窗口需继续数据才能关闭；测试使用明确的正常遥测标记推动水位线，生产不能拿未来伪造事件代替真实数据。

## 第三阶段增量验证

报告等待条件包含enriched版本，新增payment候选与gateway/order受影响角色、RCA哈希/真实证据、拓扑API三项检查，共25项。第三阶段首次运行16项通过后因SQLite索引损坏失败；第二次在全新目录25项通过，两次记录均保留。复测成功不证明存储根因已修复，详见docs/phase3/第三阶段验收报告.md。默认依然不调用LLM。
