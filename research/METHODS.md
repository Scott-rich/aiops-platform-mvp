# 实验口径与可证伪边界

研究代码不把公共匿名 KPI 强塞进在线服务协议。SMD 按机器分别拟合；PSM 删除 timestamp 列。
数据来自固定官方 Git revision；原始数据下载清单在 results/data-download-01/sha256.json。
缺失值只使用训练列中位数填补，全缺失训练列回退为零并在 metadata 中列出。
该回退可能对训练完全缺失但测试出现数值的列产生假阳性，因此必须检查清单。
采样间隔未在文件中可靠确认，填 null，不用猜测时间单位。训练标签为正常假设，不代表人工逐点复核。

## 检测器与阈值

ZScore 为逐列训练均值/标准差标准化后的最大绝对偏差，固定阈值 3。
MAD 为训练中位数与 1.4826×MAD，零 MAD 的非恒定列用训练标准差兜底。
常数列保留很小的正尺度，使常数自身不报警，而离开常数值可检出。
IForest 使用 sklearn 的固定树数、样本数、单线程实现，训练得分的 99% 分位数定阈值。
所有阈值都没有访问测试标签；上述数值是预设配置，不是从测试成绩选择的参数。

POT 策略在同一个 ZScore 排序上工作。训练 98% 分位数初始化尾部，GPD 极值外推风险参数为 0.001，
每接收 64 个未报警尾部点重拟合。报警点不更新分布。每次 decide 重置到训练状态。
这是周期 GPD 最大似然拟合的工程变体，不是原论文 Grimshaw 实现的逐位复现。
由于评分函数相同，ZScore 和 POT 的 PR/ROC 应完全一致；比较价值来自告警决策和告警负担。
阈值可以随无标签测试流更新，但不能根据测试标签更新。

## 指标

PR-AUC 具体采用 average precision；不混用梯形面积。ROC-AUC 在单类标签上为 null。
point-F1 使用未调整的逐点预测。PA-F1 将存在任一命中的真异常区间整段填正；
PA20 仅在区间内已有至少 20% 点被检出时填正。两者都保留区间外假阳性。
range_recall 是各真异常区间内覆盖比例的宏平均（平坦位置权重、无碎片惩罚），
不是完整 Tatbul 参数族的实现。event_recall 是至少命中一点的区间比例。
告警条数定义为连续预测阳性区间数，每事件告警数为预测区间数/真异常区间数。

## 三种不确定性不可互换

* per_entity：同一实体不同 seed 的均值、样本标准差、seed bootstrap CI。确定性方法的 seed CI 退化，
  只能说明这些重复执行一致，不说明真实误差为零。
* metrics：先平均每个实体的 seeds，再以实体为 bootstrap 单位。一个实体时 std/CI 留空。
  SMD 只取预先指定的前五台机器，同组机器可能相关，不能宣称总体随机抽样。
* temporal_bootstrap：以首个 seed 的固定模型/预测为条件，在原序列上抽连续块。
  拼接边界可能改变事件个数，因此区间指标 CI 是近似量；不能证明平稳性或跨分布泛化。

配对 Wilcoxon 使用实体均值差，重复 seeds 不冒充新机器。小样本双侧 p 值不显著时不宣称优越性；
探索性多个比较未校正，不能把单个 p 值当确认性发现。PSM 只有一个归池实体，不执行伪造的跨实体检验。

## 可复现与失败保留

活动输出目录必须是新目录。当前运行环境出现过工作目录日志缺失，因此正式实验写入 `/tmp` 的
独立目录，完成、关闭全部句柄并校验后才复制到 results。每次保留 result.json、completed.json、
run.log、console.log、summary.md、逐行结果及 NPZ。失败目录永不改写成成功。
timings 随机器负载变化，明确排除在数值确定性断言外；评分/预测哈希、配置和核心指标进行重跑比对。

## 核验过的相关工作

* [SMD / OmniAnomaly 官方仓库](https://github.com/NetManAIOps/OmniAnomaly)
* [PSM 官方仓库](https://github.com/eBay/RANSynCoders)，数据许可 CC BY 4.0；源码 BSD-3。
* [Kim 等：Towards a Rigorous Evaluation of Time-series Anomaly Detection](https://arxiv.org/abs/2109.05257)
* [Siffer 等：Anomaly Detection in Streams with Extreme Value Theory](https://www.kdd.org/kdd2017/papers/view/anomaly-detection-in-streams-with-extreme-value-theory)
* [Liu、Ting、Zhou：Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf)
* [El-Yaniv、Wiener：选择式分类](https://jmlr.org/papers/v11/el-yaniv10a.html)
* [Guo 等：置信度校准](https://proceedings.mlr.press/v70/guo17a.html)

参考文件中的 MicroRCA/Sage/CIRCA 作者年份尚未逐项核实，本项目不照抄为已确认引文。
OpenRCA、AIOpsLab、ITBench 用于理解评测协议，不代表本项目已在其完整 benchmark 上运行。
