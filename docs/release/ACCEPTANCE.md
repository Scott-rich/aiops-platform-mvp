# 最终验收对照

**总判定：交付可运行的研究实现与实测材料；提示词包全部门禁未通过。** 不将 `partial` / `blocked` 改为成功。以下区分代码实现、实际运行与研究有效性，所有缺失的实验结果保留空值。

| 阶段 | 要求与实际结果 | 判定 | 证据 |
|---|---|---|---|
| 基线 | 改动前运行既有验收，本地步骤通过，外部条件未执行 | 已执行 | [原始基线](../../research/results/baseline-20261104/result.json) |
| P0 数据 | SMD 预先选择字典序前五台机器；PSM 额外接入；官方固定提交与逐文件哈希 | PASS | [下载](../../research/results/data-download-01/result.json) |
| P0 对照 | 四种检测器与五个 seeds，SMD/PSM 共120行；训练定阈值 | PASS | [SMD](../../research/results/p0-smd-03/result.json)、[PSM](../../research/results/p0-psm-03/result.json) |
| P0 指标 | AP、ROC、point/PA/PA20、区间召回和告警负担；实体、seed与时间块区间分开 | PASS | [主表](../../research/results/RESULTS.md) |
| P0 复现 | 独立重算与三轮得分/预测一致；前两轮活动日志缺行，原件保留，第三轮权威 | PASS，有限口径 | [核验](../../research/results/p0-integrity-final-01/result.json) |
| P1 标注 | 六类规则合成场景；自动不变量检查，不是人工复核 | BLOCKED：人工复核缺失 | [金标准](../../research/llm/golden/result.json) |
| P1 推理 | 两个本地真实 GGUF；开发/测试分离、冻结文件、排他测试锁；预声明选择式和强制臂 | PASS：执行契约 | [0.5B](../../research/results/p1-05b-01/result.json)、[1.5B](../../research/results/p1-15b-01/result.json) |
| P1 统计 | 指标与样本量、区间已保存；1.5B冻结接受集合只有同家族两例，风险区间退化 | 部分：不能外推 | [模型主表](../../research/llm/RESULTS.md) |
| P1 注入 | 每模型30个固定攻击，before/after；事后修正合法错误e1漏计，模型不重跑 | PASS：计分可核 | [修正0.5B](../../research/results/p1-05b-correction-02/result.json)、[修正1.5B](../../research/results/p1-15b-correction-02/result.json) |
| P1 规模 | 0.5B与1.5B实际运行；7B/API未取得，价格缺失 | 可选项未取得 | [对比图与配置](../../research/results/p1-comparison-01/result.json) |
| P2 本地质量 | 509 passed、10 skipped；语句覆盖87.9328%；Ruff/mypy/pip check通过；无新增pragma/type ignore逃避 | PASS：本地 | [计数与覆盖率](../../research/results/p2-quality-counts-01/result.json)、[原质量](../../research/results/p2-quality-final-03/result.json)、[测试日志](../../research/results/p2-quality-final-03/tests.log) |
| P2 红绿门禁 | 隔离违规项目使lint/type/coverage/size失败，修正后通过；没有修改既有测试断言 | PASS：本地探针 | [red_green字段](../../research/results/p2-quality-final-03/result.json) |
| P2 CI | Python矩阵和不可忽略的核心门禁已配置；未连接远端仓库运行 | BLOCKED：无远端执行证据 | [工作流](../../.github/workflows/tests.yml) |
| P2 Docker | 多阶段镜像、非root、healthcheck已实现；没有daemon，未build | 未验证 | [最终集成门禁](../../research/results/final-acceptance-02/result.json) |
| P2 Helm/kind | Helm lint与拒绝SQLite多副本已测；kind与kubectl不可用 | 部分：未部署 | [本地质量](../../research/results/p2-quality-final-03/result.json)、[集群预检](../../research/results/p3-cluster-preflight-02/result.json) |
| P2 API迁移 | 保留旧API三个月语义；默认200+弃用头，GET/HEAD的301可配置启用 | 明确偏离默认301要求 | [兼容策略](API_COMPATIBILITY.md) |
| P2 仓库 | 大产物外置并保留索引；真实模块化提交；Apache-2.0、贡献说明与变更记录 | 已实现，包内尺寸实测见总清单 | [来源](PROVENANCE.md) |
| P3 公开应用 | 目标：用 Online Boutique / Sock Shop / Bookinfo 替换合成模拟器 | **BLOCKED（环境不兼容，5 次尝试）**：pkg.dev 不可达；mysql 容器 2–4 Gi 均 OOMKilled（mysqld anon-rss 2.09 GiB）；Bookinfo 3 个镜像两轮 240–480 s 超时 | [阻断记录](../../research/results/p3-public-app-attempt-01/README.md) |
| P3 真实环境 | kind 三节点集群已实际启动（k8s v1.37.0 + containerd 2.3.4），Chaos Mesh 2.8.7 落地；被测应用为项目自带 4 服务模拟器，不是 Boutique | 部分：单机 3 节点，非多机 HA | [矩阵结果](../../research/results/p3-cluster-matrix-01/result.json) |
| P3 注入 | 六类故障各 3 次真实注入，共 18 次（另有 6 次资源限制补充、6 次遥测耦合消融），Chaos 对象与 Pod 影响可核验 | 部分：每类 3 次，未达“每类 5 次” | [矩阵](../../research/results/p3-cluster-matrix-01/result.json)、[补充](../../research/results/p3-cluster-limits-01/result.json)、[消融](../../research/results/p3-cluster-ablation-01/result.json) |
| P3 指标 | 逐次 MTTD/MTTR/告警数/RCA 命中与无平台基线对照已取得；规则 RCA 0/24，改用调用方侧边级信号（每次下游调用时延+失败）后 8/8 命中 | 部分：边级为原型，需接入生产 RCA 路径 | [矩阵指标](../../research/results/p3-cluster-matrix-01/result.json)、[边级原型](../../research/results/p3-edge-rca-01/result.json) |
| P0 外部数据 | SMD/PSM 之外补 NAB 与 SMAP/MSL：NAB 上 4 检测器 PR-AUC≈0.20（负迁移），SMAP/MSL 0.24–0.39 | 部分：数据集为公开数据，结论仅限所测通道 | [NAB](../../research/results/p0-nab-01/result.json)、[SMAP/MSL](../../research/results/p0-smap-msl-01/result.json) |
| P3 规模 | 有界负载与采样已实现；本轮为 2 RPS 单档，未做三档持续运行 | 未完成（规模档位） | [规模预检](../../research/results/p3-scale-preflight-02/result.json) |
| P4 演示命令 | 干净Python3.12虚拟环境运行make demo；真实回环API+固定合成样本 | PASS：虚拟环境；新容器未验证 | [最新demo](../../research/results/p4-demo-clean-312-03/result.json) |
| P4 材料 | README、技术报告、A4简介、真实执行录像、逐字稿、简历与中文博客大纲 | 产物实测见包装检查 | [材料清单](final-status.json) |

## 关键解释

最终原生验收使用 `--require-full`，Docker 未执行将使退出码保持2、`full_acceptance=false`。这不是把已执行步骤误判为失败，也不是全部门禁通过。原生 Kafka/Flink、恢复、故障以及短时 benchmark 不能替代 P3 的真实业务集群、确认注入与持续负载。

P1 的 `status=passed` 指脚本完成且文件契约成立，不指人工标注、模型能力或全部研究门禁通过。0.5B 的空引用总体未知键率为 null。1.5B 的实际故障 Top-1 为 [0.25](../../research/results/p1-15b-01/result.json)，不能拿全场景 correctness [0.50](../../research/results/p1-15b-01/result.json)替换；冻结接受集合 [2例/1家族](../../research/results/p1-15b-01/result.json)不能支持零风险保证。防御后仍存在合法但错误的引用攻击，原文保留。

所有科学主张限于对应数据、协议与源码版本。缺失的人工、远端服务与集群条件在当前环境无法自行生成真实证据，因此没有伪造确认记录，也没有把需要真实验证的门禁降级。

## 审计驱动的修复

独立审计发现并推动修复：隐式Python导出、认证与限流次序、递归OTLP毒消息、集群少样本误判、故障创建后日志失败清理、只读token采样边界，以及服务器默认信任代理头导致的限流绕过。原始失败与后续定向测试均留存。最终源 `fe8df8a` 的质量与原生验收单独重跑，未借用早期版本的绿色结果。

严格缺口的最短后续路径已写在 deploy/README.md 和 research/llm/PROTOCOL.md：需要人工复核，以及可运行容器/隔离集群与远端CI的环境。没有这些证据之前，总验收字段必须保持 false。
