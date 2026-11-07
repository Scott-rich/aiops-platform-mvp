# 修复版部署与复测指南

本指南对应当前修复源码，保留现有 API、事件协议和历史报告读取能力。实际通过、失败、阻塞及环境记录以[最终验收报告](acceptance-report.md)为准；命令存在、静态配置检查通过不等于服务部署成功。数据路径、投递语义和模型证据边界见[架构说明](architecture.md)。

## 1. 选择运行路径

| 路径 | 实际运行内容 | 前提与边界 |
|---|---|---|
| 本地闭环 | FastAPI、4个独立 HTTP 微服务、Python检测、SQLite、报告 | Python3.11 / 3.12 及锁定依赖；不运行 Kafka/Flink |
| 原生流处理验收 | 真实 Kafka、Flink、worker、HTTP服务与独立数据目录 | Linux、Java17、uv和下载网络；独立 Python3.11/PyFlink 运行时 |
| 真实本地模型 | 实际 GGUF CPU 推理与证据校验 | Linux x86_64、AVX2/FMA、uv、gcc/g++、模型下载与足够内存；无需GPU或商业API密钥 |
| 兼容模型服务 | 配置地址上的真实模型调用 | 服务地址、模型名及该服务要求的凭据；只在显式模型验收时调用 |
| Docker Compose | 容器化 Kafka/Flink/API/worker/模拟服务 | **本轮无 Docker CLI/daemon，未实际构建或部署**；需另有 Docker Engine/Compose 环境 |

核心 Python 服务支持 Python3.11 / 3.12，CI、Docker、独立PyFlink和本地模型环境以3.11为基准。当前3.11原生链路也出现真实SQLite损坏，改用DELETE日志仍未避开；相同证据脚本在 `/tmp` 的3.11 WAL/DELETE实验分别通过19项检查。工作区发现主动双向同步，文件事件记录捕获rsync将临时文件重命名覆盖运行中的SQLite WAL/SHM。因此先前用3.11限定来隔离问题的判断已撤回，不归因于3.12。`preflight`和存储入口仍拒绝其它未支持版本，但版本检查不能保证存储文件安全。

不要混用 API 环境与 PyFlink 环境。不要把新测试指向已有业务数据库、生产 Kafka 或宿主机压力目标。重型测试应顺序运行，按结果中的实测 CPU/RSS、磁盘和下载规模规划资源，不把短测资源值当最低硬件保证。

## 2. 本地闭环：首次启动

在项目根目录执行；虚拟环境和数据目录分开。**活跃 SQLite DB/WAL/SHM 禁止放在同步工具管理、共享或远程文件系统目录**；必须选择本机持久磁盘且排除同步的目录，例如 `$HOME/.local/state/aiops-platform`。不要仅凭目录名称假定它是本地；若用户主目录也由同步软件管理，请另选本机磁盘。

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-lock.txt
python -m pip check
test -f .env || cp .env.example .env
chmod 600 .env
python -m scripts.preflight --env-file .env
python -m scripts.local_platform --data-dir "$HOME/.local/state/aiops-platform" --rps 3
```

已有 `.env` 不覆盖，先保留私有备份再合并新字段。`local_platform` 与 `preflight` 使用相同的受限键值解析，不执行 shell；环境变量优先。直接运行其他模块通常只读取进程环境，不能假设其自动加载 `.env`。预检不访问数据库、模型或 Docker，`ok=true` 可以同时包含未配置模型/本地无认证的 warning。

默认访问 `http://127.0.0.1:8000`，API说明为 `/docs`。模拟器默认端口为18081～18084，启动前检查占用；可用 `--port`、`--base-port` 改端口，`--no-load` 关闭持续负载。Ctrl+C会停止启动器所属进程，数据留在指定目录；不要通过删除数据库来处理启动错误。

| 探针 | 含义 |
|---|---|
| `/live` | 进程存活；不查询数据库，启动初始化失败时不会接收流量 |
| `/health` | 兼容原接口并实际读取存储；数据库错误返回503 |
| `/ready` | 本地模式检查近期成功检测循环；Kafka模式检查Kafka/Flink/worker及存储；不是吞吐或所有窗口完成证明 |

## 3. 权限、密钥与配置

默认 `API_AUTH_MODE=local` 仅用于可信本机隔离环境。**一旦通过远程网络、代理或多人环境暴露，必须启用身份认证和授权；不得把 local 模式直接公开。** 当前可选共享令牌是基础控制，还需要 TLS、受控网关/网络访问规则、令牌轮换和操作审计；高风险运维操作不可由匿名接口执行。API令牌不保护 Kafka、Flink REST 或模拟器自己的控制端点。

在运行持久服务的私有终端设置下列环境变量，生成值不会写入源码：

```bash
export API_AUTH_MODE=token
export API_READ_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export API_CONTROL_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export TELEMETRY_INGEST_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SIMULATOR_CONTROL_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
python -m scripts.preflight --env-file .env
python -m scripts.local_platform --data-dir "$HOME/.local/state/aiops-platform" --rps 3
```

持久化令牌时使用私有配置或密钥管理，勿打印、截图或提交；备份配置同样按秘密保护。前三个API令牌必须不同、32～512个URL安全字符。READ允许GET/HEAD，CONTROL允许读写，INGEST仅允许POST `/demo/ingest`；token模式匿名只可访问存活路径。模拟器另用 `SIMULATOR_CONTROL_TOKEN`。已有网页控制台没有登录或Bearer录入界面，token模式请用带认证头的客户端或受控网关，不能为了页面可用而公开关闭认证。

主要参数完整列表见根目录 `.env.example`，以下是关键语义：

| 参数 | 默认及用途 |
|---|---|
| `DETECTION_MODE` | `fixed`；`dynamic` 使用分服务、指标及UTC时间桶的动态基线，样本不足会弃权 |
| `DB_TIMEOUT_SECONDS` | 3秒有限等待；不可用时返回错误而非无限缓存/重试 |
| `EVIDENCE_OPERATION_TIMEOUT_SECONDS` | 30秒证据操作时限，范围1～120秒 |
| `EVIDENCE_RETENTION_DAYS` | 30天，范围7～3650；不会自动执行删除 |
| `LOG_LEVEL` / `LOG_FORMAT` | INFO / json；平台日志可切text，Java/模拟器日志不是全系统统一格式 |
| `API_BODY_TIMEOUT_SECONDS` | 5秒读取整段请求体；应用写请求上限1,000,000字节 |
| `LOCAL_READY_MAX_AGE_SECONDS` | 10秒；就绪要求检测循环近期成功，不只检查进程在运行 |
| `LLM_BASE_URL` / `LLM_MODEL` / `LLM_API_KEY` | 模型兼容服务配置；地址如服务的`/v1`前缀，实际调用追加`/chat/completions`；无配置走规则 |
| `LLM_OUTPUT_PROTOCOL` | `analysis-v2`兼容既有输出；`evidence-selection-v1`用于证据短键选择，两者均产生现有v2报告 |
| `LLM_MAX_CALLS_PER_INCIDENT` | 默认2次，包括有限重试；持久预算防止重启重复消耗 |
| `LLM_TIMEOUT_SECONDS` / `LLM_TOTAL_TIMEOUT_SECONDS` | 20 / 30秒；超时不盲目重试；本地模型专用验收会显式使用更长有界时限 |
| 模型单价参数 | 未配置则成本未知/null，不解释为免费或实际账单 |

## 4. 真实 Kafka/Flink 验收

以下只用于独立本机验证。使用新的 `/tmp/aiops-...` 非同步输出目录；`env`前缀只作用于测试进程，避免继承持久服务的认证模式/模拟器令牌而让无Bearer的隔离探针失败，不能把该前缀用于公开服务。

```bash
python -m scripts.bootstrap_native --runtime-dir /tmp/aiops-native-runtime
env API_AUTH_MODE=local SIMULATOR_CONTROL_TOKEN= \
  /tmp/aiops-native-runtime/venv/bin/python -m scripts.native_streaming \
  --runtime-dir /tmp/aiops-native-runtime --output /tmp/aiops-native-run-01
```

bootstrap需要网络获取固定版本运行时及依赖，原生工具使用新目录的Kafka/SQLite/checkpoint，完成或失败后清理所属进程。下载被阻塞、校验失败、服务未就绪均不能标成功。原生验证覆盖实际发送和窗口计算、乱序/迟到/重复/冲突、Python结果对照、TaskManager恢复与HTTP故障证据；不等同于Docker、多机容灾或JobManager高可用验证。

```bash
env API_AUTH_MODE=local SIMULATOR_CONTROL_TOKEN= \
  /tmp/aiops-native-runtime/venv/bin/python -m scripts.benchmark_flink \
  --runtime-dir /tmp/aiops-native-runtime --output /tmp/aiops-flink-performance-01 \
  --rates 20,60,120 --duration 3 --repeats 1
```

该性能工具实际经过Flink，记录计划/实际发送、未发出负载、唯一事件持久化、窗口结果、lag、CPU/RSS及完整排空时间。尾部控制水位与自然窗口时延分开。较长实验可用 `--rates 120 --duration 310 --repeats 1`，**只有实际跑完才可宣称长跑**；超过TTL的时长本身也不等于验证了TTL后重放。Kafka→Python benchmark另有用途，不能当成Flink吞吐。

## 5. 真实模型验证

真实模型调用与HTTP替身单元测试分开。外部兼容服务配置环境变量后，可执行：

```bash
python -m scripts.verify_llm --output /tmp/aiops-provider-model-run-01 \
  --evidence-report /tmp/aiops-native-run-01/actual-http-report.json \
  --expected-service payment
```

它会把选定证据发送到配置的服务，使用前确认数据允许离开本地；token/API key不写入Git。`--expected-service payment`只适用于这份原生payment故障证据，标签用于验收且不发送给模型；其他场景须用对应独立标签，不能套用。无配置明确blocked。

本地Qwen不需要外部商业API密钥：

```bash
python -m scripts.bootstrap_local_model --runtime-dir /tmp/aiops-model-runtime --model-size 1.5b
python -m scripts.verify_llm --output /tmp/aiops-local-model-run-01 \
  --evidence-report /tmp/aiops-native-run-01/actual-http-report.json \
  --local-model-file /tmp/aiops-model-runtime/qwen2.5-1.5b-instruct-q4_k_m.gguf \
  --model-python /tmp/aiops-model-runtime/venv/bin/python \
  --expected-service payment
```

必须保留venv解释器入口，不手工解析其符号链接。专用本地验证默认采用紧凑证据选择协议、12条/16KB上下文和512输出token，记录实际协议与限制；这不表示模型读取了全部冻结证据。可用 `--expect-unknown` 验证有来源、哈希完整的健康/不足证据控制，不能与 `--expected-service` 同用。

HTTP200、模型权重加载成功或规则回退成功不等于模型诊断验收通过。需要查看结构/引用/观测约束和独立标签检查。内部机制没有证据时必须未知，模型置信等级上限low；原文与归一化审计保留，不声称实现通用自然语言因果验证。

## 6. 完整测试与结果读取

停止源码修改后，在专用验证终端执行。验收的数据库、Kafka状态、checkpoint与结果先写入新的 `/tmp/aiops-...` 本机非同步目录，不使用项目目录中的活跃数据库。`/tmp`可被系统清理，只用于隔离测试，不用于持久服务：

```bash
env API_AUTH_MODE=local SIMULATOR_CONTROL_TOKEN= \
  python -m scripts.acceptance --output /tmp/aiops-acceptance-run-01 --require-full \
  --native-runtime /tmp/aiops-native-runtime \
  --benchmark --reliability --faults --allow-resource-pressure \
  --kafka-benchmark --flink-benchmark --real-model \
  --local-model-file /tmp/aiops-model-runtime/qwen2.5-1.5b-instruct-q4_k_m.gguf \
  --model-python /tmp/aiops-model-runtime/venv/bin/python
```

CPU/内存压力只作用于有时长、内存/CPU上限和清理机制的专属子进程。没有这些显式flag不会自动启用资源压力。模型自动使用本次native的新故障报告；runner目前没有透传 `--expected-service`，需要独立标签的严格模型检查仍按上一节单独执行。

默认不加重型flag时，runner执行本地单元、HTTP权限/生命周期、启动器、证据、固定/动态与RCA评估。终态为 `completed.json`；每一步完整结果/摘要哈希、命令、退出码、源码冻结范围均保留。benchmark摘要还会校验对应详细结果。退出1表示失败；`--require-full`存在未完成门禁退出2；默认退出0也不代表所有可选门禁已通过。Docker当前始终单列未执行，不能把返回2“修成”全面验收成功。

HTTP benchmark无注入但确实观测到慢请求时，健康对照会标degraded/blocked，保留实测值和观测开销；缺失遥测、错误定位或健康输入错误归因仍失败。重复实验有不同结果时必须全部保留，不只选择通过的一轮。

验收终态后，先查看每个步骤的cleanup结果，确认所有写入进程已停止并完成SQLite完整性检查；再复制关闭后的完整成果到项目，随后比较文件。原始失败结果必须保留：

```bash
mkdir -p artifacts
test ! -e artifacts/acceptance-run-01 && cp -a /tmp/aiops-acceptance-run-01 artifacts/acceptance-run-01
diff -qr /tmp/aiops-acceptance-run-01 artifacts/acceptance-run-01
```

复制不是数据恢复；源库仍损坏或进程仍持有写连接时不得把复制结果标作健康归档。外部同步可能替换inode、数据库或侧文件，跨进程锁以及WAL/DELETE模式都不是对这种外部改写的防护。

## 7. 备份、归档、保留与回退

不要擅自移动已有数据库。升级或迁出同步目录前停止全部对应写入者，保留原代码、配置、DB/WAL/SHM现场、Kafka状态和checkpoint，再在本机非同步目录生成和校验一致备份；已有损坏须保留现场诊断，不自动修复。现有数据库需要结构或日志模式变更时，Store先执行一致在线备份，备份失败不迁移；附属表复用统一入口。不要用启动新版本代替部署人员的独立升级备份。对损坏库保留DB/WAL/SHM现场，不删除WAL、空建同名库或覆盖原文件。

以下示例只生成新备份，源库只读，不覆盖已有文件：

```bash
python - <<'PY'
from pathlib import Path
import sqlite3, uuid
source = Path('~/.local/state/aiops-platform/aiops.sqlite3').expanduser().resolve()
if not source.is_file():
    raise SystemExit('source database is missing')
folder = Path('~/.local/state/aiops-platform-backups').expanduser()
folder.mkdir(parents=True, exist_ok=True)
target = folder / ('manual-' + uuid.uuid4().hex + '.sqlite3')
with target.open('xb'):
    pass
with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as src:
    with sqlite3.connect(target) as dst:
        src.backup(dst)
        if dst.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('backup verification failed; preserve files for inspection')
print(target)
PY
```

SQLite在线备份包含已提交WAL记录的一致逻辑快照，不要求物理字节相同。回退时先保留当前状态，再将已校验备份恢复到新的独立数据目录测试；不要让新旧进程同时写同一SQLite。模型预算、事件状态、报告版本与冻结证据都在该数据目录内，不能只迁移部分表。

证据归档/恢复/清理使用现有CLI，ID和时间占位符须替换为实际值：

```bash
python -m scripts.evidence_admin --data-dir "$HOME/.local/state/aiops-platform" archive \
  --set-id FULL_SET_ID --output archives/evidence-01.jsonl.gz
python -m scripts.evidence_admin verify-archive --input archives/evidence-01.jsonl.gz
python -m scripts.evidence_admin --data-dir /tmp/aiops-restored-data-01 restore \
  --input archives/evidence-01.jsonl.gz
python -m scripts.evidence_admin --data-dir "$HOME/.local/state/aiops-platform" prune \
  --before BEFORE_EPOCH_MS --batch-size 500 --output-dir archives
```

最后一条默认dry-run，不删除；审核范围后才显式加 `--apply`，按“在线备份→归档→回读校验→删除一个有限批次”执行。只清理满足保留条件的原始事件，活动故障/未完成报告等受保护；冻结集、报告、告警、故障和备份不自动淘汰。归档只能恢复其保存的原始证据，不替代整库备份。磁盘需要持续监控；详细分页与保留规则见[证据管理说明](../phase8/证据管理部署与架构.md)。

## 8. 容器路径：仍待真实环境验证

以下是在具备Docker的隔离机器上待执行的部署命令，**不是本轮成功记录**：

```bash
docker compose config --quiet
docker compose up --build -d
docker compose ps -a
docker compose logs --tail=100 api worker flink-submit taskmanager
```

API/模拟器镜像安装 `requirements-lock.txt` 并 `pip check`；Flink使用独立运行时依赖。镜像digest、系统包、所有传递依赖与容器启动尚未完全验证。`.env`在Compose中由Compose解析，不假定与本地解析器完全相同。

数据库卷也必须位于本机持久磁盘，不能将其绑定到同步/共享目录。发布端口默认只绑定127.0.0.1。停止使用 `docker compose down` 保留命名卷；**不要对已有数据执行 `down -v`**。实际容器验收还需测试就绪、发送事件、作业checkpoint恢复、worker/数据库恢复及卷持久化，不能只看容器running或`/live`返回200。

## 9. 本文档的核验范围

已实际执行10个相关模块及4个证据管理子命令的 `--help`，均退出0；使用PyYAML完成7项静态检查，包含依赖引用/无环、回环端口、锁文件引用和模型协议变量映射。备份示例仅校验Python语法，没有实际备份/删除数据。日志位于交付验收资料中的 `documentation/`。这些检查没有启动服务，没有加载模型，也没有执行Docker Compose配置命令；重型测试结果仍以最终验收报告为准。
