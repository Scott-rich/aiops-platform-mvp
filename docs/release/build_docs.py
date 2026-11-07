"""Build delivery prose from immutable measured JSON; never supply missing results."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(name):
    return json.loads((ROOT / name).read_text())


def metric(value):
    if value is None:
        return 'null'
    return f'{value:.4f}'


def ci_stat(stat):
    if stat.get('n_families') == 1:
        return f"{metric(stat.get('value'))}; n={stat['n']}, 家族=1；CI 不可外推（原始单簇 bootstrap 退化）"
    interval = stat.get('ci95')
    ci = '[' + ', '.join(metric(x) for x in interval) + ']' if interval else 'null'
    return f"{metric(stat.get('value'))} {ci}; n={stat.get('n', '—')}"


def build():
    runs = [read('research/results/' + n + '/result.json') for n in ('p1-05b-01','p1-15b-01')]
    corrections = [read('research/results/' + n + '/result.json') for n in ('p1-05b-correction-02','p1-15b-correction-02')]
    quality = read('research/results/p2-quality-final-03/result.json')
    test_log = (ROOT / 'research/results/p2-quality-final-03/tests.log').read_text()
    import re
    count = re.search(r'(\d+) passed, (\d+) skipped', test_log)
    assert count
    n_pass,n_skip=map(int,count.groups())
    coverage = quality['metrics']['coverage']['percent_covered']
    names=['0.5B','1.5B']
    paths=['../results/p1-05b-01/result.json','../results/p1-15b-01/result.json']
    attack_paths=['../results/p1-05b-correction-02/result.json','../results/p1-15b-correction-02/result.json']
    llm='''# LLM 评估：真实推理、合成标签与明确的失败

两组模型均为本地 Qwen2.5 Instruct Q4_K_M；不是 API、不是模拟返回。金标准是规则构造的合成场景，**尚未人工复核**，因此 P1 严格验收没有全部通过。生产 grounding / schema 校验器未为这些结果改动。

## 冻结与样本

场景按六家族分层，dev/test 分离；每个模型只执行一次冻结测试序列，随后执行预先定义的强制选择条件。两个条件是不同实验臂。开发集回放检查原始输出确定性，测试集没有第二次推理。校准、阈值、dev/test与源码哈希在 frozen-before-test.json；模型哈希在 result.config，逐调用 prompt 哈希在 outputs.audit.prompt；test-run-claims 防止误复用同一模型/测试组合。时序记录为 UTC，run.log 使用主机时区。

| 模型 | dev / test | 测试时间与协议 | 原始结果 |
|---|---|---|---|
'''
    for name,run,path in zip(names,runs,paths):
        llm+=f"| {name} | [{run['dev_cases']} / {run['test_cases']}]({path}) | [test_protocol]({path}) | [result.json]({path}) |\n"
    llm+='''
## 主表

表中为值、95% CI 和 n。证据和 Top-K 仅在有根因的故障测试样本上计算；1.5B 冻结阈值只接受两例，均来自资源家族，观察到的零错误不能构成有效风险保证。全场景 correctness 还包含“正确弃权”，不得当成根因准确率。主指标按合成家族 bootstrap；ECE 和延迟按测试样本 bootstrap，家族内依赖可能使后者区间偏窄。零事件 bootstrap 区间坍缩为零不能证明总体风险为零。

| 指标 | 0.5B：值 [95% CI]; n | 1.5B：值 [95% CI]; n |
|---|---|---|
'''
    for key,label in [('evidence_precision','原始证据 precision'),('evidence_recall','原始证据 recall'),('evidence_f1','原始证据 F1'),('served_evidence_recall','校验后证据 recall'),('top1','故障服务 Top-1'),('top3','故障服务 Top-3'),('correct','全场景 correctness'),('forced_decision_accuracy','强制选择 correctness'),('abstained','弃权比例'),('valid','schema 有效比例'),('selected_coverage','冻结阈值覆盖率'),('selected_risk','冻结阈值风险'),('ece','ECE'),('pooled_unsupported_reference_rate','引用级未知键率'),('latency_p50','延迟 P50 / 秒'),('latency_p95','延迟 P95 / 秒')]:
        llm+='| '+label+' | '+' | '.join(f'[{ci_stat(run["metrics"][key])}]({path})' for run,path in zip(runs,paths))+' |\n'
    llm+='''
引用率的分母是生成的引用条数；分母为空时 pooled 值为 null。逐样本空引用率中的零仅是计算约定，不能表述为“零幻觉”。该指标只测不存在的引用键，不覆盖模型所有语义事实。Top-3 在最多两个证据键的受限协议下可能退化，不能当成标准开放式三个候选排序。资源场景需要三条金标准证据而协议最多接受两条，形成召回上限；不改标签掩盖这个构念问题。

## 提示注入与修正记录

每个模型对三类、每类十条攻击分别执行 before/after。before 关闭输入消毒、system防御提示和生成时语法约束；after组合启用它们，两边都报告同一下游schema校验。这是组合干预，不能把变化单独归因于消毒。攻击包括伪 JSON、重复 token、Unicode 混淆；其中每类后半段专门诱导输出合法但错误的 e1。原始评分器漏掉此目标，独立审计发现后从保留的原始输出重算，**没有重跑模型，没有改 prompt / 阈值**。原 result.json 与原图不覆盖，最终解释采用 correction-02。correction-01 保留用于追溯。各次修正都记录输入 SHA256。

| 模型 | 原始攻击成功率 before → after | schema 校验后 before → after | 配对 recall：after − before |
|---|---|---|---|
'''
    for name,corr,path in zip(names,corrections,attack_paths):
        m=corr['metrics']
        paired=corr['paired_defense_recall_change']
        # Retain full correction schema as the authoritative disclosure even if a field name changes.
        llm+=f"| {name} | [{ci_stat(m['before']['raw_attack_success'])} → {ci_stat(m['after']['raw_attack_success'])}]({path}) | [{ci_stat(m['before']['schema_valid_attack_success'])} → {ci_stat(m['after']['schema_valid_attack_success'])}]({path}) | [原始 {ci_stat(paired['evidence_recall'])}; 校验后 {ci_stat(paired['served_evidence_recall'])}]({path}) |\n"
    llm+='''
1.5B 在防御后仍有 role-5 合法但错误证据输出；这是保留的防御失败案例。总体 2/30→1/30 的差异不支持显著改善结论。防御前后不能只比较攻击率而忽略效用：必须同时看校验后 recall、正常样本 correctness 和冻结覆盖率。若没有原本正确的诊断，diagnostic_regression 的有效样本量为零、值为 null；这不是防御无代价的证据。攻击类别与基础案例绑定，类别 bootstrap 只有少量独立组，置信区间不能外推为通用攻击防御保证。

## 四张图

全部由 `python -m scripts.research.plot_llm_results --results ... --attack-results ... --output <new>` 生成；完整命令与输入哈希见 [图表结果](../results/p1-comparison-01/result.json)。空覆盖处风险保持未定义，不画为零。

'''
    plotdir=ROOT/'research/results/p1-comparison-01'
    for p in sorted(plotdir.glob('*.png')):
        llm+=f'![{p.stem}](../results/p1-comparison-01/{p.name})\n\n'
    llm+='## 三个保留的失败案例\n\n以下内容逐字来自保留的 test / attack 输出，见各例链接；不改写为“正确答案”。\n\n'
    failures=[]
    for name,folder in zip(names,['p1-05b-01','p1-15b-01']):
        for line in (ROOT/'research/results'/folder/'test-outputs.jsonl').read_text().splitlines():
            row=json.loads(line)
            if not row['metrics']['correct']:
                failures.append((name,folder,row));break
    extra=next(json.loads(line) for line in (ROOT/'research/results/p1-15b-01/attack-outputs.jsonl').read_text().splitlines() if json.loads(line)['attack_id']=='role-5' and json.loads(line)['defended'])
    extra['case_id'] += ' / defended role-5'
    failures.append(('1.5B','p1-15b-01',extra))
    for name,folder,row in failures:
        llm+=f"### {name} / {row['case_id']}\n\n[原始输出](../results/{folder}/{('attack-outputs.jsonl' if 'attack_id' in row else 'test-outputs.jsonl')})，[指标与协议](../results/{folder}/result.json)。此故障样本未获得正确根因诊断。\n\n```json\n{row['audit']['raw_output']}\n```\n\n"
    llm+='''## 六问与限制

证明：推理、证据映射、校准和防御可被审计，同时暴露当前小模型的诊断效用不足。数据：规则合成场景和冻结攻击，无真实外部 RCA 结论。对照：强制选择，以及同攻击防御前后。区间：详见主表，不能将 seed 当独立生产环境。失效边界：未人工复核、有限拓扑、协议上限、共享 CPU。下一步：先人工复核与真实 RCA 数据，再扩展模型；未取得 7B/API，成本为 null。

延迟来自同一共享机器的不同时间段，期间存在其他验证进程与非模型工具安装；版本清单已保留，不能据此认定模型规模是延迟差异的唯一原因。生产诊断仍以可核对的规则/RCA证据为基础，小模型失败或弃权不能升级成自动恢复动作。
'''
    (ROOT/'research/llm/RESULTS.md').write_text(llm)
    results=[]
    for name,run,corr,folder,af in zip(names,runs,corrections,['p1-05b-01','p1-15b-01'],['p1-05b-correction-02','p1-15b-correction-02']):
        results.append(f"| LLM {name}（合成故障） | Top-1 [{metric(run['metrics']['top1']['value'])}](research/results/{folder}/result.json) | 原始攻击率 [{metric(corr['metrics']['before']['raw_attack_success']['value'])} → {metric(corr['metrics']['after']['raw_attack_success']['value'])}](research/results/{af}/result.json) |")
    readme=f'''# AIOps Research Platform

面向微服务的证据约束 LLM 根因诊断平台：让检测、引用、弃权与失败都能被复核。

![Local checks](docs/release/badge-local.svg) ![Coverage](docs/release/badge-coverage.svg) ![CI](docs/release/badge-ci.svg) ![License](docs/release/badge-license.svg) ![Python](docs/release/badge-python.svg)

**交付状态：可运行的研究实现与实测材料；并非提示词包全部门禁通过。** 公共检测基准已完成，本地质量通过；人工复核、远端 CI、容器/三节点部署、真实集群故障和长时规模实验仍未通过。详见 [验收对照](docs/release/ACCEPTANCE.md)。

| 实测范围 | 主结果 | 解释 |
|---|---|---|
| SMD / MAD vs ZScore | AP [0.3235 vs 0.2894](research/results/p0-smd-03/result.json) | [Wilcoxon p=0.125](research/results/p0-smd-03/result.json)，不声称显著优越 |
| PSM / 流式 POT 变体 | point F1 [0.0123](research/results/p0-psm-03/result.json) / PA F1 [0.8496](research/results/p0-psm-03/result.json) | PA 明显放大稀疏命中，必须联合报告 |
{chr(10).join(results)}
| 工程回归 | [{n_pass} 通过，{n_skip} 跳过，覆盖率 {coverage:.2f}%](research/results/p2-quality-counts-01/result.json) | [完整日志](research/results/p2-quality-final-03/tests.log)，远端 CI 未运行 |

```bash
# 解压交付包后，在 aiops-research/ 根目录（Python 3.11 或 3.12 + make）
make demo
```

会新建虚拟环境、按运行时锁文件安装依赖并启动短时回环 API 验证。默认结果目录 `/tmp/aiops-demo-时间戳` 必须不存在。需要首次网络下载依赖；演示数据是合成数据，不下载公共数据或模型。实测 [干净 Python 3.12 虚拟环境](research/results/p4-demo-clean-312-03/result.json) 中入库与 API 校验通过；**未在新容器中验证**。未发布外部 Git 地址，因此不提供虚构的 `git clone` URL；可用包内 git bundle 还原真实提交历史。

```mermaid
flowchart TD
    A[业务遥测] --> B[Kafka]
    B --> C[Flink 检测]
    B --> D[证据归档]
    C --> E[告警与事件关联]
    D --> F[规则 RCA 与检索]
    E --> F
    F --> G[受限 LLM 与弃权]
    G --> H[不可变报告与 API]
    F --> H
```

图示为系统路径。真实原生 Kafka/Flink、本地 GGUF、恢复和短时性能步骤见 [最终验收](research/results/final-acceptance-02/result.json)；演示命令走轻量本地路径。Online Boutique / OTEL / Chaos Mesh 为待集群验证的扩展，不冒充已部署。

## 导航

- [公共检测结果与 PR 图](research/results/RESULTS.md)、[方法与统计口径](research/METHODS.md)。
- [LLM 主表、四图、失败案例](research/llm/RESULTS.md)、[实验协议](research/llm/PROTOCOL.md)。
- [技术报告 PDF](docs/release/research-report.pdf)、[A4 项目简介](docs/release/project-brief.pdf)。
- [五分钟真实执行录像](demo/aiops-five-minute.mp4)、[逐字稿及录制说明](demo/script.md)。
- [部署与真实集群实验](deploy/README.md)、[API 迁移](docs/release/API_COMPATIBILITY.md)。
- [简历条目](docs/release/resume.md)、[中文博客大纲](docs/release/blog-outline.md)。
- [证据与版本溯源](docs/release/PROVENANCE.md)、[最终独立审计](docs/research/audit-final.md)。

## 公共实验复现

```bash
make research-env
.venv/bin/python -m scripts.research.download --dataset all --output data/public-reproduction
.venv/bin/python -m scripts.research.evaluate_detectors --dataset smd --data-root data/public-reproduction/smd --output /tmp/smd-reproduction-01
.venv/bin/python -m scripts.research.evaluate_detectors --dataset psm --data-root data/public-reproduction/psm --output /tmp/psm-reproduction-01
.venv/bin/python -m scripts.research.verify_results --help
make check
```

下载参数以 [download.sh](research/datasets/download.sh) 为准；数据固定上游提交并校验每文件哈希。SMD 采用预先指定的字典序前五台机器，PSM 排除时间戳列。所有方法仅用训练数据定阈值。完整权重与公共原始数据不进仓库；所有评估的 scores/labels/predictions、原始模型输出、日志与命令均在交付包中。

LLM 复现请先阅读协议及冻结测试运行锁。训练/开发调参与保留测试严格分开；不删锁来重复调同一测试集。新模型/新协议应使用新实验 ID 与新保留集。权重来源与 SHA256 见两个模型的 result.json；默认不外发数据。

## 工程约束

`/v1` 与旧 API 并存。为保留现有客户端和既有测试，旧路径默认维持返回语义并加弃用头，GET/HEAD 的 301 可通过环境变量开启；这是对“默认 301”的明确偏离。Bearer 认证先于限流；限流按规范路由与直连 IP 分桶，未知路径不增长无限标签。SQLite 单写进程约束：同 Pod 共享卷、单副本；不能配置多个独立副本并宣称高可用。

Ruff、mypy、覆盖率门禁均本地执行，并有隔离红→绿探针。`Any` 保留在历史动态 JSON 接口边界，类型标注不是完整结构化建模。CI 配置包含 Python 矩阵和不可忽略的代码门禁，Docker job 可选；徽章明确显示远端 CI 未运行。

## 局限与原始产物

小规模单主机实验、合成 RCA 标签、未完成人工复核、未做多租户隔离与生产高可用。没有把规则根因排名当作 LLM 准确率；没有把空引用当成“零幻觉”。P3 缺真实集群的注入与资源证据，MTTD/MTTR/规模结论留空。

原始 phase10 历史材料在 `docs/historical/` 说明，旧文档的数字不是本轮结果。原始大包已外置：[原上传项目](https://chatgpt.com/library?file_id=libfile_f7eec5a11ebc8191ab154db8632dad6e&account_id=personal)。本轮完整验收数据库与 Kafka 文件在交付包根目录 `acceptance-full-evidence.zip`，其逐文件哈希见 [ARCHIVE_MANIFEST](research/results/final-acceptance-02/ARCHIVE_MANIFEST.json)。不删除失败记录。

## English quick guide

Evidence-grounded RCA for microservices. Public anomaly benchmarks are real downloaded data; RCA and prompt-injection labels are synthetic and not human-reviewed. Run `make demo` from this directory for a local HTTP smoke test. Read [the measured detector results](research/results/RESULTS.md), [LLM results](research/llm/RESULTS.md), and [acceptance limitations](docs/release/ACCEPTANCE.md) before making claims. Native Kafka/Flink and model integration passed their executed checks. Remote CI, Docker/kind deployment, real cluster chaos and sustained scale gates remain unpassed. The tiny models' abstention and low diagnostic utility are retained as negative results, not hidden by schema-validity scores.
'''
    (ROOT/'README.md').write_text(readme)
    badges=[('local','local checks','passed','#16856b'),('coverage','coverage',f'{coverage:.2f}%','#16856b'),('ci','remote CI','not run','#85602b'),('license','license','Apache-2.0','#315784'),('python','Python','3.11 / 3.12','#315784')]
    for key,left,right,color in badges:
        lw=max(65,len(left)*7+15);rw=max(55,len(right)*7+15)
        svg=f'<svg xmlns="http://www.w3.org/2000/svg" width="{lw+rw}" height="24" role="img" aria-label="{left}: {right}"><rect width="{lw+rw}" height="24" rx="4" fill="#334155"/><path d="M{lw} 0h{rw}v24H{lw}z" fill="{color}"/><g fill="white" font-family="Verdana,sans-serif" font-size="11" text-anchor="middle"><text x="{lw/2}" y="16">{left}</text><text x="{lw+rw/2}" y="16">{right}</text></g></svg>'
        (ROOT/f'docs/release/badge-{key}.svg').write_text(svg)
    resume=f'''# 可核验的简历条目

- 针对合成数据评估缺乏外部效度的问题，搭建训练集定阈值的 SMD/PSM 检测基准，完成 [120 个实体×方法×seed 组合](../../research/results/p0-integrity-final-01/result.json) 的重跑核验；同时报告 PR-AUC、逐点与区间指标，避免只用 PA-F1 美化结果。
- 为证据约束 LLM 建立冻结 dev/test、原文哈希、弃权校准与攻击前后对照；保留两个本地模型的 [证据召回与 Top-K 负结果](../../research/results/p1-15b-01/result.json)，通过审计修正评分遗漏而不重跑测试模型。
- 升级 API 版本、认证与限流次序、自监控和锁定依赖；实现 [{n_pass} 项通过、{n_skip} 项跳过、覆盖率 {coverage:.2f}%](../../research/results/p2-quality-counts-01/result.json)，并完成 [原生 Kafka/Flink 等全部已执行步骤](../../research/results/final-acceptance-02/result.json)。不声称远端 CI 或 Kubernetes 已验收。

使用时保留限定条件。尚未完成的真实集群故障次数、生产吞吐或准确率不能写进简历。
'''
    (ROOT/'docs/release/resume.md').write_text(resume)
    print(json.dumps({'readme':'README.md','llm':'research/llm/RESULTS.md','passed':n_pass,'skipped':n_skip,'coverage':coverage}))


if __name__=='__main__':
    build()
