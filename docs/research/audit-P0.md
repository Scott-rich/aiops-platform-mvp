# P0 独立审计

审计结论：**P0 离线检测评测可交付**。该判断仅覆盖公共数据、四种检测策略、统计口径、重放和本阶段产物，不表示 P1–P4、生产部署或完整 CI 已通过。审计者只读取实验源码与结果，未修改检测器、既有断言或实验 JSON。

权威证据为 [SMD 第三轮](../../research/results/p0-smd-03/result.json) 与 [PSM 第三轮](../../research/results/p0-psm-03/result.json)。原始执行目录分别为 `/tmp/aiops-p0-smd-03`、`/tmp/aiops-p0-psm-03`；完成后才归档。归档的全部文件已与原运行目录逐字节比较。前两轮保留作重放与失败取证，不替代第三轮完整日志。

## 实际核验范围

|核验对象|SMD|PSM|证据|
|---|---:|---:|---|
|主结果行数|100|20|两份 `result.json` 的 `rows`|
|逐行记录数|100|20|对应 `rows.jsonl`；每行完整对象与主结果相等|
|模型运行日志条数|100|20|对应 `run.log` 中 entity/detector/seed 记录|
|NPZ 文件数|100|20|评分、预测与标签逐项核验|
|归档文件数|107|27|归档与 `/tmp` 原件逐字节一致|
|与第一、第二轮逐位一致|是|是|`rows`、`metrics`、`per_entity`、`temporal_bootstrap`、`comparisons`|

审计从每份 NPZ 重新执行 `research.metrics.detection()`：全部指标与主结果**完全相等**，无需用浮点容差掩盖差异。每份 NPZ 的标签也逐点等于官方标签文件。评分及预测数组字节 SHA256 与结果记录相同，`completed.json` 的终态哈希正确。重放比较排除了命令中的输出目录、时间戳和耗时；未将这些天然变化字段说成确定性结果。

主结果 SHA256：

```text
SMD  5946697b2ed23a6e815617744271105ab7747f5d17ce866c216310078b1ee751
PSM  a80614d8af0d2f7f3e0db3694958e1885f16463548de48178c9ff6748482587c
```

主表 [RESULTS.md](../../research/results/RESULTS.md:5) 的 8 行、48 个数值单元格均按其声明的统计单位，与 JSON 的四位小数表示逐项比对一致。SMD 使用实体均值及实体标准差；PSM 使用 `per_entity.<detector>.pooled` 的 seed 均值及标准差。PSM 跨实体标准差与 CI 为 `null`，没有被偷换成零不确定性。

## 提示词包的 14 项检查

|序号|检查|判定|证据与边界|
|---:|---|---|---|
|1|报告数字能否定位到结果字段|PASS|`research/results/RESULTS.md:7` 起主表逐格链接结果；48 格核对无差异。SMD 比较结论对应 `comparisons.mad_minus_zscore_pr_auc`。派生 `p0-integrity/result.json` 只汇总已有核验，不作为另一次模型运行。|
|2|随机种子及确定性证明|PASS|两份结果 `seeds=[0,1,2,3,4]`；`scripts/research/evaluate_detectors.py:61` 逐 seed 拟合。三次运行的完整行结果、评分/预测哈希与统计输出逐位一致，包括 IForest 在相同 seed 下的重放。|
|3|是否用测试标签调阈值|PASS（可见代码范围）|`research/datasets/base.py:20` 只从训练列计算填充值；`research/detectors/statistical.py:12`、`:23`、`:35` 的 `fit` 不接收标签；`evaluate_detectors.py:62` 传入 train。POT 无标签在线更新在 `research/METHODS.md:17` 明示。冻结源码未发现标签调参路径；不对不可见历史操作作保证。|
|4|是否只报 PA-F1|PASS|`research/metrics.py:31` 同时计算 AP、ROC、point-F1、PA-F1、PA20、区间召回及告警数；主表并列 point-F1 与 PA-F1。|
|5|基线是否实际运行|PASS|权威目录保留 100/20 条模型日志、100/20 份 NPZ；所有指标已由审计者从 NPZ 独立重算。对应代码 `evaluate_detectors.py:62`。|
|6|mean/std、CI 与样本量是否明确|PASS（带局限）|`research/metrics.py:52`、`:90`；JSON 分开保存 seed、实体、时间块统计；SMD 以 5 个实体均值为比较单位，PSM 仅一个归池实体。时间块 bootstrap 使用 256 点块、200 次重采样，条件于首个 seed 的既定预测。确定性 seed CI 不代表总体误差为零。|
|7|公共数据来源与 SHA256|PASS|`scripts/research/download.py:11` 固定官方 revision；`research/results/data-download-01/sha256.json` 的 18 个文件独立核验全匹配。SMD 预先取同组前五台机器，非随机代表性抽样；PSM 剔除 timestamp 后为 25 特征。|
|8|是否修改既有断言、放宽阈值、删除失败|PASS（本阶段 diff）|`git diff 64195e2 -- app tests flink` 无输出；旧实验目录和源归档保留。P0 新代码独立位于 research 与 scripts/research。未把旧日志缺失记录改成完整成功。|
|9|是否用 pragma/type-ignore 虚增覆盖率|PASS（代码检查）；覆盖率门禁未判断|`rg -n 'pragma: no cover\|type: ignore' research scripts/research` 无匹配；新增代码通过 mypy。P2 的全仓覆盖率及门禁不在本次通过声明中。|
|10|CI 违规是否真的变红|不适用／尚未证明|该项属于 P2。此处只独立验证本地 Ruff、mypy、依赖和研究单测，未将其称为远端 CI 全绿或红→绿验证。|
|11|引用是否存在|PASS（抽查五项）|SMD 官方仓库、PSM 官方仓库、Kim 论文 arXiv、Siffer 的 KDD 官方页面、Isolation Forest 作者主页 PDF 均实际打开，见下表。未审核参考清单全部文献。|
|12|失败与低分结果是否保留|PASS|`p0-smd-01` 的缺日志原件仍存在；第一轮 SMD 主结果及 NPZ 虽一致，但不完整日志没有被修写。PSM 的 POT 低 point-F1 与高 PA-F1 同时保留，不挑选好看的评分。|
|13|“分布式”等措辞是否符合实际|PASS（P0 范围）|`research/METHODS.md:3` 明确离线匿名 KPI 对照，未将 SMD 机器维度映射成微服务或称多机部署。P3 集群与真实注入尚未在此审计证明。|
|14|新人能否只按最终 README 复现|无法判断（待 P4）|已核验脚本、下载器和结果中的实际命令；最终 README、干净容器 `make demo` 属于后续阶段，本次不提前标 PASS。|

## 独立执行的质量检查

从项目根目录实际执行：

```bash
/tmp/aiops-research-venv/bin/ruff check research scripts/research
/tmp/aiops-research-venv/bin/mypy research scripts/research
/tmp/aiops-research-venv/bin/python -m unittest discover -s research/tests -v
uv pip check --python /tmp/aiops-research-venv/bin/python
git diff 64195e2 -- app tests flink
```

输出片段：

```text
All checks passed!
Success: no issues found in 17 source files
Ran 17 tests
OK
Checked 45 packages
All installed packages are compatible
```

17 项研究测试包含 7 项检测/指标测试和 10 项数据泄漏/实验失败契约测试；既有单元测试没有包含在这个计数内。独立契约测试最初在 `/tmp/test_research_contracts.py` 执行，加入 `research/tests/test_contracts.py` 并格式化后再次运行通过。新增测试是在权威实验终态之后加入，不冒称它已包含在当时实验的源码哈希清单内。

权威实验所记录的实际命令为：

```bash
/tmp/aiops-research-venv/bin/python -m scripts.research.evaluate_detectors --dataset smd --data-root data/public-01/smd --output /tmp/aiops-p0-smd-03
/tmp/aiops-research-venv/bin/python -m scripts.research.evaluate_detectors --dataset psm --data-root data/public-01/psm --output /tmp/aiops-p0-psm-03
```

这些命令记录了本次解释器和路径。复现者须先按锁文件安装依赖、下载官方数据，并换用不存在的新输出目录，不能覆盖本报告引用的目录。

## 已关闭的三项必须修复问题

1. **日志完整性**：旧工作目录中，第一轮 SMD 的 `rows.jsonl` 缺一行，审计时 `run.log` 仅有 55 条模型记录。原件保留；第三轮在独立 `/tmp` 目录运行，100 条记录全部核验后归档。缺失根因未被直接证实，不能断言一定由同步程序造成。
2. **不确定性误读**：单实体跨实体 CI 改为 `null`；时间块 CI 独立保留；文档明确 seed、实体、时间块的统计单位。SMD MAD 的 AP 差 bootstrap 区间虽为正，双侧 Wilcoxon `p=0.125`，报告未宣称显著优越性。
3. **结果契约与可复现性**：修正 `python -m` 命令记录、源码变化导致失败退出、失败摘要、stdout/stderr 捕获、参数唯一性与逐行保留。临时探针和持久化测试都验证了失败分支。

P0 当前无阻止进入下一阶段的未修复问题。最终交付仍必须完成后续阶段各自门禁，并保留本审计中的“未判断”状态，不能合并写成全项目通过。

## 两项建议补充实验

1. 扩展全部 SMD 机器，并按分组分析；当前同组前五台机器不足以代表整个数据集，更不能代表生产总体。
2. 对时间块长度做预先声明的敏感性实验；POT 还可与原始 SPOT 实现对照。当前 POT 是 GPD MLE 周期重拟合工程变体，与 ZScore 共享排序，因此 PR/ROC 一致是实现定义，不能描述成独立模型排名优势。

## 引用核验记录

|材料|实际打开的来源|结果|
|---|---|---|
|SMD|[OmniAnomaly 官方仓库](https://github.com/NetManAIOps/OmniAnomaly)|可打开|
|PSM|[RANSynCoders 官方仓库](https://github.com/eBay/RANSynCoders)|可打开|
|评测陷阱|[Kim 等论文 arXiv:2109.05257](https://arxiv.org/abs/2109.05257)|标题与作者对应；出版社页面本次受反机器人限制，另已打开官方 PDF|
|SPOT|[KDD 2017 官方论文页面](https://www.kdd.org/kdd2017/papers/view/anomaly-detection-in-streams-with-extreme-value-theory)|标题与作者对应；DOI 跳转本次工具不可达，未把不可达隐去|
|Isolation Forest|[作者主页论文 PDF](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf)|标题与作者对应|

该引用核验验证可获取性及基本书目信息，不代表复现了每篇论文的全部算法或实验。
