# Astra 改造提示词包（可直接复制）

配套文件：
- `改造路线图-顶尖本科版.md`（差距诊断）
- `参考资料-数据集与论文.md`（链接清单）
- `独立复测与报告核验.md`（当前基线实测）

用法：
1. 第一次把「0. 总控提示词」作为项目级说明（或首条消息）给 Astra，让它先跑基线、再动手。
2. 之后按 P0 → P1 → P2 → P3 → P4 顺序，一次只发一个阶段提示词；每次附上上一阶段的 `result.json` 路径。
3. 每个阶段结束，用「6. 审计提示词」让另一个模型（或新会话）交叉审查，不过审不许进入下一阶段。

---

## 0. 总控提示词（项目级，先发这一条）

```text
你是资深 AIOps 研究工程师 + 开源项目维护者。当前仓库是一个"微服务智能运维平台"的第十阶段修复版，
已有：Kafka→Flink→SQLite→LLM 诊断链路、454 个通过的单元测试、13 个验收门禁、哈希可核的归档产物。
工程完成度高，但科学有效性不足：所有指标都建立在自造的合成数据上，没有外部基线、没有统计检验。

【最高优先级目标】
把这个仓库从"工程演示"改造为"可证伪、可对比、可复现的研究型项目"，
使它能支撑：毕业设计评优、研究生复试、智能运维岗求职。

【铁律，违反任何一条即视为任务失败】
1. 不可编造任何数字、图表、引用、实验结论。所有数值必须有对应的
   result.json + 命令 + 随机种子 + 日志文件路径，三者缺一不可。
2. 不可为了"通过"而修改既有测试断言、放宽阈值、删除失败记录或覆盖 artifacts。
   失败必须保留原样并记录原因。
3. 每项实验必须固定随机种子并写入结果；同一命令连续跑两次，关键指标必须一致
   （允许浮点误差 ≤1e-9；若因并发/时间导致不可复现，必须在结果里标注并说明原因）。
4. 不可用合成数据冒充公共数据；不可只报告单一指标（例如只报 point-adjust F1）。
5. 不可只写文档不跑实验；也不可只跑实验不落盘。文档里的每个数字都要能点到 result.json。
6. 不可引入未固定版本的依赖；新增依赖必须进 requirements-lock.txt 并跑 pip check。
7. 改动前先跑一次基线（python -m scripts.acceptance --output /tmp/baseline-<date>）并保存输出；
   结束前再跑一次，证明没有回归。
8. 每个阶段独立成一个 git commit（或补丁包 + 说明），commit message 说明"做了什么 / 为什么 / 怎么验证"。

【工作方法】
- 先读 docs/phase10-fix/acceptance-report.md、architecture.md、deployment.md，理解既有契约：
  结果目录必须新建、result.json 契约、completed.json 终态、哈希清单。
- 新代码放 research/（研究）与 scripts/research/（入口），不要污染 app/ 的运行时逻辑；
  若必须改 app/，必须补测试并说明对既有 13 个门禁的影响。
- 每个脚本支持 --output，输出目录必须不存在时创建（与既有脚本一致），并写 result.json。
- 复用既有风格：类型标注、docstring、错误用显式异常、日志用 logging、不用 print 调试残留。

【产出契约（每个实验步骤都要满足）】
output_dir/
  result.json        # 机器可读：config + metrics + environment + source_sha256 + seeds + timings
  <step>.log         # 完整 stdout/stderr
  summary.md         # 人可读摘要（含命令、结论、局限）
并在 research/results/RESULTS.md 维护一张总表：每个数字后标注来源 result.json 的相对路径。

【每阶段结束必须回答的 6 个问题（写进 summary.md）】
1. 这次证明了什么？2. 用什么数据/多少样本？3. 对比对象是谁？4. 不确定性有多大？
5. 什么情况下结论会失效？6. 下一步最该做什么？

现在先做三件事，做完停下来等我确认，不要继续往下做：
(a) 跑一遍既有基线并贴出结果摘要；
(b) 列出你要新增/修改的文件清单与理由；
(c) 给出 P0 阶段的详细执行计划（含预计耗时与风险）。
```

---

## 1. P0 提示词：把"评测"做成可信的（3 周，最高优先级）

```text
【阶段目标】
把"检测"从 4 个硬阈值（p95>800ms / err>20% / cpu>90 / mem>90）升级为"可对比的方法学"，
在公共数据集上产出第一张能写进论文的主表。

【必读参考（先读再动手，读不通就说明原因）】
- SMD 数据集与 OmniAnomaly 官方实现：https://github.com/NetManAIOps/OmniAnomaly
- 时序异常评测的坑（point-adjust 会虚高）：Kim et al., "Towards a Rigorous Evaluation of
  Time-series Anomaly Detection", AAAI 2022 —— 必须读，并解释你如何避免
- SPOT（流式极值理论）：Siffer et al., KDD 2017
- Isolation Forest：Liu et al., ICDM 2008
- pyod（基线工具箱）：https://github.com/yzhao062/pyod

【任务清单】
1. research/datasets/
   - base.py：统一接口 `load() -> (X_train, y_train, X_test, y_test, meta)`，
     其中 meta 含 entity 名称、维度名、采样间隔、标签语义（点/区间）。
   - smd.py：读 SMD（ServerMachineDataset/{train,test,test_label}/machine-*.txt），
     支持 --entities 选择若干台机；标签为 0/1 序列。
   - smap_msl.py、psm.py 至少再接一个（有多余时间再接 SWaT，需申请，写明未获取）。
   - 下载器：数据不进 git，放 data/（.gitignore），提供 download.sh + 文件 SHA256 清单。
2. research/detectors/
   - 统一接口：`fit(X_train)` / `score(X) -> float[]` / `decide(scores) -> 0/1[]`；
     阈值只能在 train 上定（或按 train 分位数/极值理论），禁止用 test 标签调阈值。
   - 实现：zscore（逐维 3σ，兼容既有阈值语义）、mad（med±k·MAD）、iforest（sklearn/pyod）、
     spot（流式 POT，自实现，不引入 spams）。
   - 每个 detector 附 3 个单测：常数序列不报警、单调上升报警、训练/推理 shape 一致。
3. scripts/research/evaluate_detectors.py
   - 参数：--dataset smd --entities machine-1-1,machine-2-1,... --detectors zscore,mad,iforest,spot
           --seeds 0,1,2,3,4 --output <new dir>
   - 指标（每个实体 + 汇总，全部给 mean±std 与 bootstrap 95% CI）：
     PR-AUC、ROC-AUC、point-adjust F1、PA%K(k=0.2)、range-based recall、
     以及"告警条数 / 真实事件数"（工程侧真正关心的量）。
   - 同一实体不同 seed 只影响随机算法（iforest），其余必须完全确定；
     在 result.json 里记录每个 detector 的 seeds 与是否确定性。
4. research/results/RESULTS.md
   - 主表：行=数据集×检测器，列=PR-AUC / PA-F1 / PA%K / 每事件告警数，均值±std。
   - 附一张 PR 曲线图（matplotlib，存 png，同时存生成脚本）。
   - 明确写"局限"：阈值选择方式、标签粒度、SMD 只有单变量维度集等。

【验收门禁（不达标不算完成）】
- SMD 至少 5 台机 × 4 个检测器 × 5 seeds 全部跑完，无崩溃；
- 表里每一格都能点到 result.json；
- 同一命令重跑，非随机检测器的指标逐位一致；
- 至少给出一个"我们的方法/既有阈值 vs 基线"的结论，并说明其统计不确定性；
- 代码通过 ruff（新增文件 0 error）与 mypy（新增文件全标注）。

【明确禁止】
- 只跑 1 台机、只报 point-adjust F1、用 test 标签调阈值；
- 把 SMD 的某一维当"服务指标"硬套进现有 app/ 管线（P0 只做离线对照，不接线上）；
- 为了好看挑数据集/挑机器（要么全跑，要么说明抽样规则）。

【交付格式】
1) 文件清单与 diff 摘要；2) 主表（markdown）；3) 三条结论 + 三条局限；
4) 你踩到的 3 个坑（写进 summary.md，作为后续经验）。
```

---

## 2. P1 提示词：把 LLM 变成"可测量的对象"（3 周，决定项目上限）

```text
【阶段目标】
现有 LLM 部分已有 grounding（只能引用真实证据）、abstention（可弃权）、审计（保留原文与归一化哈希）、
置信上限 low。这些设计很好，但一个数都没测。本阶段把它们变成 4 张图 + 1 张表。

【必读参考】
- 选择式预测 / 风险-覆盖曲线：El-Yaniv & Wiener, JMLR 2010；ECE：Guo et al., ICML 2017
- 提示注入：Greshake et al. 2023 "Not what you've signed up for"；AgentDojo（2024）；
  OWASP Top 10 for LLM Applications（LLM01 Prompt Injection）
- 真实 AIOps 上的 LLM 评测：OpenRCA（https://microsoft.github.io/OpenRCA/ ）、
  AIOpsLab（https://microsoft.github.io/AIOpsLab/ ）、ITBench（https://github.com/itbench-hub/ITBench）
- 本项目已有协议：app/model_grounding.py、app/analysis_schema.py、scripts/verify_llm.py（先读完再改）

【任务清单】
1. 造金标准集（golden set）
   - 用既有注入器生成 ≥60 个场景（正常/延迟/错误/依赖故障/资源压力/噪声），
     每个场景人工（或规则+人工复核）标注：正确服务、应引用的证据 ID、是否应弃权。
   - 划分 dev(40%) / test(60%)：prompt 与阈值只准在 dev 上调，test 只跑一次并记录时间戳。
   - 落盘 research/llm/golden/{dev,test}.jsonl，含标注者与复核者字段（可以是"规则+人工抽检 20%"）。
2. 指标一：证据选择质量
   - 证据选择 precision / recall / F1（对比模型选的证据键 vs 金标准证据 ID）；
   - 候选服务 Top-1 / Top-3；
   - 幻觉率：模型输出中出现但不在证据集里的"事实"条数 / 总条数（用引用校验器统计）。
3. 指标二：弃权校准
   - 扫描 abstention 阈值，画 risk–coverage 曲线（覆盖率 vs 错误率）；
   - 计算 ECE（分箱 10）；对比"强制不弃权"基线。
4. 指标三：提示注入鲁棒性
   - 攻击面：证据文本（日志 message、span 属性）里注入 "ignore previous instructions…"、
     伪 JSON 指令、超长重复 token、Unicode 混淆；共 3 类 × 10 payload = 30 例。
   - 指标：攻击成功率（输出被改变 / 泄露系统提示 / 越权建议）；
   - 防御前后对照：证据消毒（白名单字段 + 长度截断 + 分隔符转义）、指令/数据分离、输出 schema 强校验。
5. 指标四：模型规模-成本-延迟权衡
   - 0.5B / 1.5B（已有）/ 7B 或 API（有 key 才做，没有就写"未获取"）：准确率、注入成功率、P50/P95 延迟、token 成本（无价格则 null）。
6. 产出 research/llm/RESULTS.md + 4 张图 + 1 张汇总表。

【验收门禁】
- dev/test 严格分离（结果里必须写"test 只跑了一次，时间戳 XXX"）；
- 每个指标都有 n 与 95% CI（bootstrap）；
- 注入实验有 before/after 对比，且防御不是"把证据删光"（要报 recall 代价）；
- 复用既有 verify_llm 的审计字段，不新造一套结果格式。

【明确禁止】
- 用 test 集反复调 prompt 后仍宣称是 test 结果；
- 用"模型很谨慎所以弃权率高"替代校准曲线；
- 注入实验只做 3 条就下结论；
- 修改 app/model_grounding.py 的校验逻辑来让指标变好（如需改，必须单列 diff + 回归前后对照）。

【交付格式】
1) 四张图（png + 生成脚本）；2) 汇总表；3) "加防御后 recall 掉多少、注入成功率降多少"的一句话结论；
4) 失败案例 3 个（原样贴模型输出，脱敏）。
```

---

## 3. P2 提示词：工程化到"能给别人看"（1 周，纯体力但有明确门禁）

```text
【阶段目标】
让面试官 clone 下来 5 分钟能判断"这人是工程师"，CI 徽章全绿，仓库不再像工作目录。

【任务清单】
1. 配置三件套
   - pyproject.toml：ruff（line-length 100，select E,F,W,I,B,UP,SIM,PL，忽略项目既有风格冲突并写明理由）、
     mypy（对 app/、research/ 开启 disallow_untyped_defs；历史脚本先 exclude 并列入 TODO）、
     coverage（source=app,flink,research；fail_under=80）。
   - .pre-commit-config.yaml：ruff format + ruff check --fix + mypy + 一个"禁止提交 >5MB 文件"的钩子。
   - .editorconfig。
2. CI（.github/workflows/）
   - matrix：python 3.11 / 3.12；
   - 步骤：install(lock) → pip check → ruff → mypy → coverage run（fail_under）→
     acceptance（本地门禁）→ 上传 artifacts（结果 JSON + 覆盖率 xml）；
   - 额外 job：docker build（有 docker 才跑，无则 continue-on-error 并在 README 标注未验证）。
   - 加 badge 到 README 顶部。
3. 平台自监控
   - app/api.py 增加 /metrics（Prometheus 文本格式：请求数、延迟直方图、事件入库量、告警数、DB 大小）；
   - 增加 API 版本前缀 /v1（保留旧路径 3 个月并 301，写弃用说明）；
   - 加简单令牌桶限流（每 IP 每路径，可配置；默认宽松，写进 .env.example）。
4. 部署形态
   - Dockerfile 改多阶段（builder 装依赖 → runtime 只带 venv），加 HEALTHCHECK；
   - 新增 deploy/helm/（api/worker/flink/kafka 四个 chart 或一个 umbrella），
     values.yaml 暴露资源限制与副本数；
   - 写 deploy/README.md：kind 三节点起集群 + 部署 + 验证命令（必须实际执行并贴证据）。
5. 仓库卫生
   - artifacts/ 2.1GB 外置：只保留 research/results 与 20 个代表性 JSON；
     其余打包上传为 Release 附件或对象存储，并在 README 写清"完整产物去向"；
   - 补 LICENSE（建议 Apache-2.0，与依赖兼容）、CHANGELOG.md（Keep a Changelog 格式）、
     CONTRIBUTING.md（如何跑测试、如何加 detector）；
   - 若原仓库无 git 历史：重建 30+ 个语义提交（按 phase 或按模块），不要一次性 initial commit；
   - .gitignore 增加 data/、*.gguf、*.sqlite3、artifacts/big/。
6. README 首屏重写（P2 只写骨架，内容在 P4 补）
   - 一行问题陈述、一张架构图（mermaid）、三行主结果（带 result.json 链接）、
     一条 5 分钟复现命令、状态徽章、局限声明。

【验收门禁】
- CI 全绿；ruff / mypy / coverage 三个门禁在 CI 中真实生效（故意制造一个违规，确认 CI 会红，再撤回）；
- docker build 在当前环境能成功（不能则如实标注"未验证"，并在 CI 里 continue-on-error）；
- helm lint 通过；kind 部署成功并贴 kubectl get pods 输出；
- 仓库体积 < 300MB（git clone 时间可接受）。

【明确禁止】
- 为了提高覆盖率给测试加 `# pragma: no cover`；
- 用 `# type: ignore` 批量压 mypy；
- 把 CI 门禁设成 allow_failure 然后声称"全绿"。
```

---

## 4. P3 提示词：把"分布式"做实（4 周，决定能否冲奖）

```text
【阶段目标】
用真实微服务系统 + 真实故障注入替换"4 个假服务 + 合成数据"，让结论具备外部效度。

【必读参考】
- TrainTicket（41 个微服务、多语言）：https://github.com/FudanSELab/train-ticket
- Online Boutique（11 个服务，轻量，适合单机 k8s）：https://github.com/GoogleCloudPlatform/microservices-demo
- Chaos Mesh（故障注入）：https://github.com/chaos-mesh/chaos-mesh
- OpenRCA / AIOpsLab / ITBench（对比它们的评测协议，尤其是 ground truth 与指标定义）

【任务清单】
1. 环境
   - kind 或 minikube 起 3 节点集群；部署 Online Boutique（资源小）或 TrainTicket（更真实，但重）；
   - 用 OpenTelemetry Collector 采集 trace/metrics/logs，统一进 Kafka（复用现有 pipeline）；
   - 明确记录：服务数、实例数、节点数、资源配额、采集频率、时钟同步方式。
2. 故障注入（Chaos Mesh）
   - 6 类：Pod Kill、网络延迟（100/300ms）、网络丢包（10%/30%）、依赖不可用（payment 返回 503）、
     CPU 压力（80%）、内存压力；
   - 每类 ≥5 次，随机时刻、随机目标；所有注入事件写入 ground-truth.jsonl（含起止时间、目标、类型）；
   - 注入脚本必须可复现：--seed 固定 ⇒ 注入序列可重放。
3. 端到端指标
   - MTTD（注入→首个告警）、MTTR（注入→平台判定恢复）、告警数/事件数、根因 Top-1/Top-3、
     报告引用有效率、诊断耗时 P50/P95；
   - 对照组：无平台（只有 Prometheus 阈值告警），用同一批注入跑一遍。
4. 平台自身可观测性
   - 采集平台进程 CPU/RSS、Kafka lag、Flink checkpoint 时长、DB 大小增长；
     画一张"压力下平台自身表现"的图（这是评委最爱问的）。
5. 规模实验
   - 至少 3 个负载档位（如 200/1000/3000 eps），每档 10 分钟；说明测试机规格与瓶颈位置。

【验收门禁】
- ≥30 次真实注入实验，全部有 ground truth 与结果；
- MTTD/MTTR 给分布（P50/P95）而非单值；与对照组有对比；
- 故障注入可重放（同 seed 两次注入序列一致）；
- 资源数据有来源（cgroup/psutil），说明采集口径与遗漏进程。

【明确禁止】
- 用 sleep + 手动改返回值冒充故障注入；
- 只挑"能检测到"的故障类型；
- 把 kind 单机多容器说成"多机分布式"（要写清节点数与物理机数）。
```

---

## 5. P4 提示词：把成果讲成"作品"（1 周）

```text
【阶段目标】
把研究结果变成三样东西：能扫的 README、能读的技术报告、能看的 5 分钟演示。

【任务清单】
1. README（首屏 1 屏内说完）
   - 一句话：面向微服务的证据约束 LLM 根因诊断平台；
   - 架构图（mermaid）+ 一张主结果表（检测跨数据集 / RCA Top-K / 注入成功率）；
   - 5 分钟复现：`git clone && make demo`（必须真的能跑，并在 CI 验证 smoke）；
   - 局限声明（单机/小规模/合成标签/未做多租户）；
   - 徽章：CI、coverage、license、python 版本。
2. 技术报告（10–12 页，毕设体例）
   - 1 问题与动机（为什么 AIOps 需要"证据约束"）2 相关工作（检测/ RCA / LLM for AIOps 三条线）
   - 3 系统设计（含失败模式与取舍）4 方法（grounding + abstention 的形式化）
   - 5 实验（P0/P1/P3 的表与图）6 威胁与效度（内部/外部/构念）
   - 7 结论与未来工作；参考文献必须可核验（DOI/arXiv/仓库链接）。
3. 演示视频（5 分钟）
   - 0:00–0:30 问题；0:30–2:00 注入故障 → 告警 → 关联；2:00–3:30 LLM 诊断与证据核对；
     3:30–4:30 恢复确认与报告版本；4:30–5:00 主结果表与局限。
   - 附 script.md（逐字稿）与录制命令（asciinema 或 OBS）。
4. 对外材料
   - 一页 A4 项目简介（PDF，给复试老师）；
   - 3 条简历 bullet（问题—手段—数字，每条带一个可核验指标）；
   - 一篇 4000 字中文技术博客大纲（含 3 张图）。

【验收门禁】
- README 里每个数字都能点到 result.json；
- 技术报告里每条引用都能打开；
- 视频里的命令与实际仓库一致（演示脚本单独目录，不进主流程）；
- `make demo` 在干净环境（新容器）能跑通。

【明确禁止】
- 用"业界领先""大幅提升"等无依据措辞；
- 数字与 result.json 不一致；
- 演示视频里用预录输出冒充实时运行。
```

---

## 6. 审计提示词（每阶段结束跑一次，交叉验证用）

```text
你是严格的论文审稿人 + 面试官。下面是另一个 agent 在本仓库完成的改造，请按 14 条清单逐条核查，
每条给 PASS / FAIL / 无法判断，并给出证据（文件路径 + 行号 + 命令输出片段）。

1. 每个报告的数字是否都能在 result.json 中找到对应字段？
2. 是否有随机种子记录？非随机算法是否证明确定性？
3. 是否用 test 集调过阈值/prompt（看时间戳与 commit 顺序）？
4. 是否只用 point-adjust F1？有没有 PR-AUC / PA%K / 告警条数？
5. 基线是否真的跑了（有日志）而不是"我们优于传统方法"这种断言？
6. 统计：是否给 mean±std 与 CI？样本量 n 是否写清？
7. 数据：公共数据集是否给出下载方式与 SHA256？合成数据是否被标注为合成？
8. 是否有为了通过而修改既有测试断言 / 放宽阈值 / 删除失败记录？（对比 git diff）
9. 覆盖率是否被 pragma 或 type: ignore 等手段虚增？
10. CI 是否真的会在违规时变红？（让它演示一次红→绿）
11. 引用是否真实存在？（抽查 5 条，打开链接/查 DOI）
12. 失败案例是否保留？是否只展示了成功样本？
13. "分布式"等措辞与实际部署规模是否一致？
14. 一个新人不看代码，能否只按 README 复现主结果？

最后输出：
- 总体结论（可交付 / 需返工 / 不可信）
- 必须修复的 3 项（按严重度）
- 建议补充的 2 个实验
```

---

## 7. 使用节奏（建议）

| 时间 | 动作 | 交付 |
|---|---|---|
| 第 1 天 | 发总控提示词，跑基线，确认 Astra 理解契约 | 基线结果 + 文件清单 |
| 第 1–3 周 | P0 | 跨数据集主表 + PR 曲线 |
| 第 4–6 周 | P1 | 4 张图 + 注入 before/after |
| 第 6 周 | 审计 | 交叉审查报告 |
| 第 7 周 | P2 | CI 全绿 + 仓库瘦身 |
| 第 8–11 周 | P3 | 真实注入 + MTTD/MTTR |
| 第 12 周 | P4 + 审计 | README/报告/视频/简历 |

先把 P0 做完再碰其他阶段。**一张跨数据集的主表，比五个新功能更能决定这个项目的档次。**
