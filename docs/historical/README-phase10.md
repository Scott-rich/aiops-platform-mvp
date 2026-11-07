# AIOps Platform MVP · 第十阶段修复版

可运行、可复测的本地分布式智能运维研究项目。保留现有事件协议、API、历史报告和数据结构；本轮修复事件关联遗漏、错误恢复、模型证据约束、初始化备份及验收脚本，并增加真实 Kafka→Flink 性能验证。

- [本轮最终验收与未验证范围](docs/phase10-fix/acceptance-report.md)
- [部署、配置与系统架构](docs/phase10-fix/deployment.md)
- [修复计划与兼容性风险](docs/phase10-fix/implementation-plan.md)
- [原阶段文档与命令](README-history.md)

本地 Python、原生 Kafka/Flink、真实模型、Docker 是分别记录的验证路径。以本轮验收结果为准，不把历史成功当作当前成功。Docker 在当前验证环境没有运行条件，仍未完成部署实测。

## 1. 本地闭环

核心 Python 服务支持 **Python 3.11 / 3.12**，CI、Docker和独立PyFlink/本地模型运行时以3.11为基准。该路径启动真实 HTTP 微服务、SQLite 和 Python 检测，不需要 Kafka、GPU 或模型密钥。**活跃 SQLite DB/WAL/SHM 禁止放在同步或共享目录**，需使用不受同步工具管理的本机持久磁盘目录，例如 `$HOME/.local/state/aiops-platform`。外部文件同步可绕过数据库锁并破坏一致性；本轮实际捕获的替换事件与排查记录见最终验收报告。

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-lock.txt
python -m pip check
# 仅不存在时创建，不覆盖已有配置。
test -f .env || cp .env.example .env
python -m scripts.preflight
python -m scripts.local_platform --data-dir "$HOME/.local/state/aiops-platform" --rps 3
```

浏览器打开 http://127.0.0.1:8000 。启动器绑定回环地址；Ctrl+C停止子进程并保留数据。`/live`表示进程存活，`/ready`检查依赖与近期检测循环。该命令不启动Kafka/Flink。

`.env`由本地启动器和预检读取为配置，不执行shell。直接运行其他模块时用环境变量配置；不要将含密钥的`.env`提交到Git。默认`API_AUTH_MODE=local`只用于可信本地环境。令牌模式使用三个不同的32～512字符令牌：`API_READ_TOKEN`、`API_CONTROL_TOKEN`、`TELEMETRY_INGEST_TOKEN`。现有浏览器页面没有登录/令牌录入功能；令牌模式应使用带Bearer的API客户端或受控网关。

## 2. 真实 Kafka/Flink

原生验证要求Linux、Java17、uv及下载依赖的网络；所有服务由测试启动为独立本机进程，使用新数据目录，结束后清理进程。

```bash
python -m scripts.bootstrap_native --runtime-dir /tmp/aiops-native-runtime
/tmp/aiops-native-runtime/venv/bin/python -m scripts.native_streaming \
  --runtime-dir /tmp/aiops-native-runtime --output /tmp/aiops-native-run-01
```

覆盖实际消息发送、事件时间/乱序/迟到、重复/冲突、Python/Flink对照、checkpoint和TaskManager恢复、HTTP故障证据与报告。输出`result.json`和日志；这不是Docker部署或多机高可用测试。

```bash
/tmp/aiops-native-runtime/venv/bin/python -m scripts.benchmark_flink \
  --runtime-dir /tmp/aiops-native-runtime --output /tmp/aiops-flink-performance-01 \
  --rates 20,60,120 --duration 3 --repeats 1
```

这个性能入口确实经过Flink。发送速率和完整窗口持久化完成速率分别记录；自然窗口与控制水位关闭的尾窗分开统计。`--duration 310`可请求较长实验，但不能据短测宣称已验证长期容量。CPU/RSS口径和未包含进程在结果中明确说明。

## 3. 真实模型诊断

兼容OpenAI风格HTTP接口：`LLM_BASE_URL`、`LLM_MODEL`、`LLM_API_KEY`均从环境配置。无模型配置继续使用规则诊断；超时、非法结构、错误引用、预算耗尽会记录原因并回退。

`LLM_OUTPUT_PROTOCOL=analysis-v2`保留供应商输出协议；`evidence-selection-v1`让模型选择短证据键、候选排序和受控验证动作，适用于本地小模型。两种协议最终都生成现有v2报告。平台只呈现可核对的异常观测，保留模型原文与转换审计；缺少内部机制证据时根因未知、置信等级上限low。此等级不是统计概率，也不宣称任意自然语言推断已得到证明。

本地真实模型需要Linux x86_64 AVX2/FMA、uv、gcc/g++、足够内存和下载网络，不需商业API密钥：

```bash
python -m scripts.bootstrap_local_model --runtime-dir /tmp/aiops-model-runtime --model-size 1.5b
python -m scripts.verify_llm --output /tmp/aiops-model-run-01 \
  --evidence-report /tmp/aiops-native-run-01/actual-http-report.json \
  --local-model-file /tmp/aiops-model-runtime/qwen2.5-1.5b-instruct-q4_k_m.gguf \
  --model-python /tmp/aiops-model-runtime/venv/bin/python --expected-service payment
```

`--expected-service`是独立验收标签，不发送给模型；要求模型Top-1正确。保持虚拟环境Python路径，不要手动解引用符号链接。单事件调用预算持久化，重启不重复调用。未配置价格时成本为null，不是免费或真实账单金额。

## 4. 完整复测入口

输出目录必须是新的本机非同步目录；以下临时验收统一放在 `/tmp/aiops-...`，不能把 `/tmp` 当永久数据存储。此命令会实际启动本机隔离服务并进行有限故障注入，CPU/内存压力仅施加到有上限的专属子进程。

```bash
python -m scripts.acceptance --output /tmp/aiops-acceptance-run-01 --require-full \
  --native-runtime /tmp/aiops-native-runtime \
  --benchmark --reliability --faults --allow-resource-pressure \
  --kafka-benchmark --flink-benchmark --real-model \
  --local-model-file /tmp/aiops-model-runtime/qwen2.5-1.5b-instruct-q4_k_m.gguf \
  --model-python /tmp/aiops-model-runtime/venv/bin/python
```

模型自动使用本次native步骤的新故障报告。默认运行只做本地门禁；可选项没有执行时不会标通过。退出码1表示失败；`--require-full`下未完成所有门禁返回2；当前Docker门禁仍单列未执行，因此即使可运行门禁均通过，也不能把返回2改写为“全量部署验收通过”。完整终态与每一步结果哈希保存在`completed.json`。模型真实调用与单元测试的受控响应分开统计。

```bash
python -m unittest discover -s tests -v
python -m scripts.verify_evidence --output /tmp/aiops-evidence-run-01 --count 23005
python -m scripts.evaluate_detection --output /tmp/aiops-detection-run-01
python -m scripts.evaluate_rca --output /tmp/aiops-rca-run-01
```

完成验收并确认所有写入进程已退出、SQLite完整性通过后，才把关闭后的成果复制回项目；不要同步活跃数据库。保留失败轮次，不覆盖已有结果：

```bash
mkdir -p artifacts
test ! -e artifacts/acceptance-run-01 && cp -a /tmp/aiops-acceptance-run-01 artifacts/acceptance-run-01
diff -qr /tmp/aiops-acceptance-run-01 artifacts/acceptance-run-01
```

已有数据库不得擅自移动；先停止所有写入者、保留DB/WAL/SHM现场并做一致备份，再在独立本机目录验证恢复。跨进程锁、WAL或DELETE日志模式都不能防止外部同步进程替换数据库文件。详见部署指南的备份章节。

## 5. 证据、恢复与兼容性

- 完整证据集按页读取，支持服务、时间、类型过滤和版本比较；分析快照仍有上限，并明确采样。完整表示入库查询范围完整，不代表所有独立Kafka主题已到齐。
- 事件关联先查告警服务再查相关trace，行数/字节/trace数超限会明确标记不完整并保守处理。
- 恢复需要持续、足够且新鲜的健康请求；恢复窗口内的新鲜错误span会否决恢复并重置保持时间。服务级恢复与每一个历史实例恢复不是同一承诺。
- 已有库初始化变更前先做一致备份，失败不迁移；附属表也走同一入口。历史报告不原地重写，新报告保留版本和审计。
- Flink内部checkpoint与至少一次Kafka输出、确定性告警ID和数据库幂等共同工作；不宣称端到端exactly-once。迟到事件进入DLQ，不自动修正已结束窗口；去重状态有TTL和容量边界。

## 6. 容器路径与安全范围

```bash
# 在具备Docker Engine及Compose的隔离机器执行；本轮没有容器实测结果。
docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose logs --tail=100 worker flink-submit api
```

宿主映射默认只绑定127.0.0.1。不要在已有数据上运行`down -v`；升级前备份数据卷和配置。容器构建使用核心锁文件并执行pip check，但镜像拉取、构建、健康检查和容器恢复仍需实际Docker环境验证。公开访问需另行配置TLS、身份、网络隔离等控制；本交付不宣称生产高可用。
