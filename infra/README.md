# Kafka / Flink 部署与恢复

本目录用于单机隔离研究环境。真实原生执行结果见 `artifacts/native-attempt-*/result.json`；Docker 构建/Compose部署需要单独验证，原生成功不能替代容器验收。

## 固定版本与前提

Kafka 3.9.1（单 broker KRaft）、Flink/PyFlink 1.20.3、Java17、Kafka SQL connector 3.3.0-1.20、setuptools 80.9.0。PyFlink单独使用Python3.11；API/模拟器可用3.11/3.12。Java source桥接仅负责Kafka元数据和分区watermark，主要业务逻辑仍为Python。Docker多阶段构建需要下载JDK；没有JDK的原生环境可使用固定ECJ编译器。原生bootstrap需已有Java17、uv及网络，不安装宿主系统服务。

## 无 Docker 的真实集成测试

在项目根目录执行：

```bash
python3 -m scripts.bootstrap_native --runtime-dir /tmp/aiops-native-runtime
/tmp/aiops-native-runtime/venv/bin/python -m scripts.native_streaming \
  --runtime-dir /tmp/aiops-native-runtime --output artifacts/my-native-run
```

输出目录必须不存在，防止覆盖旧实验。首次bootstrap需下载Kafka/PyFlink/connector/ECJ，校验公开checksum并记录本地SHA256；不会打包这些二进制依赖。每次测试从当前Java源码重新构建bridge，记录源码与JAR哈希，并启动独立broker、JobManager、TaskManager、worker、API及四个HTTP服务；端口动态选择且只绑定127.0.0.1。进程结束时清理本次启动的PID，结果、日志和隔离数据目录保留。测试中包含显式合成事件用于精确对照，也包含实际HTTP故障调用；两者分别记录。外部模型配置会被移除，避免流测试发生付费调用。

`--skip-http`、`--skip-restart`只供定位问题，结果明确记录跳过；这样的运行不能算完整阶段验收。运行超时/断言/关闭错误均使整体失败。`check_counts`只计实际完成的检查，另有运行错误；阅读时必须同时看整体`status`、`error`和`cleanup_errors`。

脚本实际测试：3分区、2并行，30s窗口/10s滑动/5s乱序；真实Kafka输出与精确Python规则对照；重复/冲突、分区偏斜、JSON/数值/未来时间/Unicode/UTF8/大报文与迟到DLQ；已完成checkpoint后的TaskManager恢复及窗口样本/消费offset；真实HTTP级联故障、Kafka来源报告、引用/哈希、API就绪。它不是吞吐压测，也不验证JobManager HA或真实LLM。

## Docker Compose 路径（本环境未执行）

前提：Docker Engine + Compose v2，能访问镜像仓库、APT/PyPI/Maven。资源预算建议4核、16GiB内存、10GiB磁盘，属于规划值。命令：

```bash
cp .env.example .env
docker compose config
docker compose up -d --build
docker compose ps -a
docker compose logs -f flink-submit taskmanager worker
curl --fail http://127.0.0.1:8000/ready
```

Kafka健康→topic初始化→JM健康→TM注册健康→作业提交并等待全部算子RUNNING→loadgen开始。`flink-submit`成功退出正常；API `/health`仅存活，`/ready`检查broker、唯一v3作业及全部运行算子、worker新鲜心跳，不表示所有窗口完成。worker心跳不单独证明broker可用。模拟服务`/health`是存活与发送计数，不代表请求成功。

默认作业名 `aiops-event-time-detector-v3`。端口仅绑定本机：API8000、Flink8081、网关8001、Kafka19092。公开远程访问之前必须实现认证授权、网络隔离与故障控制权限。

## 输入、窗口与一致性

`wire_contract.py`定义统一schema v1：整数UTC Unix毫秒、最多未来60秒、ID长度≤256、有限JSON数值、合法Unicode、保留字段不可伪造。旧schema v1无trace_id仍可读，新生产器必须尽可能发送关联字段。示例及字段详见 `docs/engineering-upgrade/事件格式.md`。

分区watermark先经Java校验时间guard再交Python完整校验；非法数据不推动水位线。允许已观测最大事件时间后的5秒乱序，10秒空闲分区不阻塞水位线。窗口一旦关闭不接受更正；同事件仍可能进入尚未关闭的重叠窗口。全部输入静默不推进尾窗口，不应当作故障恢复。

窗口至少20个请求才判定P95>800ms或5xx比例>20%；主机窗口至少3个样本才判均值>90%。25ms直方图在普通60秒范围内P95高估<25ms，800ms是桶边界；溢出精度单独标注。Python对照保留精确最近秩规则。

Flink固定bucket+容量+处理时间TTL去重，冲突第一观测值保留、原冲突入DLQ；容量耗尽明确拒绝，不静默丢弃。水位线长时间停滞后，即使TTL过期，窗口最后防线也不让冲突毒化作业。窗口状态仍随唯一ID数增长，不能称常量内存。SQLite第一持久化值保留，相同canonical重复忽略，冲突原文及来源入库；跨分区冲突的第一到达顺序并不保证一致，生产者必须保证event_id对应内容唯一。

Kafka sink是at-least-once，内部checkpoint配置不构成端到端exactly-once。告警ID仍由服务/规则/窗口边界生成，SQLite按ID业务幂等。worker先持久化后提交offset；DB/commit失败退出，重启重放；启动与poll异常重试均有预算，Compose可重启进程。快速检测内存状态不持久化，持久化后检测前崩溃可能漏快速预警，不能把它当作主Flink状态。

DLQ最终UTF8消息≤128KiB。超限明确`truncated=true`，包含原序列化长度、SHA256、raw长度/哈希、有限前缀和原Kafka坐标；必须在源topic保留期内回取原件，哈希不是完整归档。worker现有deadletters列最多保留100KB raw文本，超大DLQ JSON在SQLite仍可能截断，完整DLQ须查Kafka；后续证据管理阶段负责解除这一限制。

## checkpoint 与升级

每10秒checkpoint，externalized状态取消后保留。v3已改变source/parser/dedup/window UID，**不能将v2 checkpoint直接恢复到v3**。先备份旧工程、数据和旧作业状态，停止旧作业；v3从保留的Kafka输入重放，告警ID仍保持业务幂等。本轮代码无SQLite schema迁移，改动前工程备份随交付保存于 `backups/`。

TaskManager故障由Flink策略恢复。JobManager丢失没有HA元数据，不能自动重建作业。新提交与显式恢复分开操作；如果已经有同名活动作业，提交器不重复提交，带restore参数时直接拒绝。

```bash
# 作业不存在时全新提交，默认从earliest读取。
docker compose run --rm flink-submit

# 仅恢复v3的有效checkpoint，替换URI；保持窗口、slide、dedup bucket配置兼容。
docker compose run --rm \
  -e FLINK_RESTORE_PATH=file:///opt/flink/checkpoints/REPLACE_WITH_VALID_V3_CHECKPOINT \
  flink-submit
```

提交器参数：`FLINK_REST_URL`（默认http://jobmanager:8081）、`FLINK_CLI`、`FLINK_JOB_FILE`、`FLINK_PY_FILES`、`FLINK_JOB_NAME`、`FLINK_STARTUP_TIMEOUT_SECONDS`（5–600，默认120）、`FLINK_RESTORE_PATH`。流参数完整列表见 `flink/README.md`；修改恢复配置需评估状态兼容。

`docker compose down`保留命名卷；不要在需保留证据时运行`down -v`。原生测试输出保留broker数据/checkpoints；只清理已停止且无需恢复的实验目录。现未做自动保留/归档/清理，需监控磁盘。单broker、SQLite、本地checkpoint不构成高可用。

## 官方资料

- [Flink1.20 Python安装](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/installation/)
- [Kafka source/sink与投递保证](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/)
- [事件时间与watermark](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/)
- [Flink Docker部署](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/)
