"""Generate a paginated technical report and one-page brief from retained measurements."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image, KeepTogether
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

ROOT=Path(__file__).resolve().parents[2]
W,H=A4
INK=colors.HexColor('#173344');TEAL=colors.HexColor('#147d85');GRAY=colors.HexColor('#5b6e7a');PALE=colors.HexColor('#edf5f6')
S={}

def load(path):return json.loads((ROOT/path).read_text())

def para(text,style='body'):return Paragraph(text,S[style])

def number(x,d=4):return 'null' if x is None else f'{x:.{d}f}'

def link(text,path):return f'<link href="../../{path}" color="#147d85">{escape(str(text))}</link>'

def stat(s):
    if s.get('n_families')==1:return number(s.get('value'))+'；n='+str(s['n'])+' / 1家族，CI退化'
    ci=s.get('ci95')
    return number(s.get('value')) + (' ['+','.join(number(v) for v in ci)+']' if ci else ' [null]')

def table(rows,widths,small=False):
    header=ParagraphStyle('white-header',parent=S['tiny' if small else 'cell'],textColor=colors.white)
    cells=[[Paragraph(str(c),header) for c in rows[0]]] + [[para(str(c),'tiny' if small else 'cell') for c in row] for row in rows[1:]]
    t=Table(cells,colWidths=widths,hAlign='LEFT',repeatRows=1)
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),INK),('TEXTCOLOR',(0,0),(-1,0),colors.white),('VALIGN',(0,0),(-1,-1),'TOP'),('LINEBELOW',(0,0),(-1,0),.6,TEAL),('LINEBELOW',(0,1),(-1,-1),.25,colors.HexColor('#cbdadd')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,PALE]),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7)]))
    # Paragraph colors are explicit; reset header to white.
    for c in cells[0]:c.style=ParagraphStyle('headcell',parent=c.style,textColor=colors.white)
    return t

def pic(path,width=495,maxheight=255):
    from PIL import Image as PI
    p=ROOT/path;w,h=PI.open(p).size
    scale=min(width/w,maxheight/h)
    return Image(str(p),width=w*scale,height=h*scale,hAlign='CENTER')

def foot(canvas,doc):
    canvas.saveState();canvas.setFillColor(TEAL);canvas.rect(0,H-10,W,10,fill=1,stroke=0)
    canvas.setFont('CN',8);canvas.setFillColor(GRAY)
    canvas.drawString(43,25,'AIOps Research  ·  可复核的证据与明确的验收边界')
    canvas.drawRightString(W-43,25,f'{doc.page:02d}')
    canvas.restoreState()

def architecture():
    d=Drawing(495,235)
    positions={'Kafka':(18,164),'Flink':(190,164),'Incident':(362,164),'Archive':(18,76),'RCA':(190,76),'Report':(362,76)}
    labels={'Kafka':'Kafka 遥测','Flink':'Flink 检测','Incident':'事件关联','Archive':'证据归档','RCA':'规则 RCA / LLM','Report':'版本化报告'}
    for a,b in [('Kafka','Flink'),('Flink','Incident'),('Kafka','Archive'),('Archive','RCA'),('Incident','RCA'),('RCA','Report')]:
        x,y=positions[a];u,v=positions[b]
        d.add(Line(x+54,y+20,u+54,v+20,strokeColor=TEAL,strokeWidth=1.5))
    for name,(x,y) in positions.items():
        d.add(Rect(x,y,113,42,rx=5,ry=5,fillColor=PALE,strokeColor=TEAL))
        d.add(String(x+56.5,y+16,labels[name],fontName='CN',fontSize=10,textAnchor='middle',fillColor=INK))
    d.add(String(245,24,'实线表示设计中的数据路径；部署事实另以运行记录认定。',fontName='CN',fontSize=9,textAnchor='middle',fillColor=GRAY))
    return d

def page(story,kicker,title,lead=None):
    if story:story.append(PageBreak())
    story.extend([para(kicker,'kicker'),para(title,'title')])
    if lead:story.append(para(lead,'lead'))
    story.append(Spacer(1,12))

def write_docs(font):
    pdfmetrics.registerFont(TTFont('CN',str(font)))
    pdfmetrics.registerFontFamily('CN',normal='CN',bold='CN',italic='CN',boldItalic='CN')
    for name,size,leading,color in [('body',11.0,18.3,INK),('lead',11.5,19,GRAY),('title',23,31,INK),('kicker',9,14,TEAL),('h2',13,20,TEAL),('cell',9.2,13.8,INK),('tiny',8.3,12.2,INK),('caption',8.5,13,GRAY)]:
        S[name]=ParagraphStyle(name,fontName='CN',fontSize=size,leading=leading,textColor=color,spaceAfter=9,wordWrap='CJK',alignment=TA_LEFT)
    smd=load('research/results/p0-smd-03/result.json');psm=load('research/results/p0-psm-03/result.json')
    quality=load('research/results/p2-quality-final-03/result.json')
    import re
    match=re.search(r'(\d+) passed, (\d+) skipped',(ROOT/'research/results/p2-quality-final-03/tests.log').read_text())
    passed,skipped=map(int,match.groups());coverage=quality['metrics']['coverage']['percent_covered']
    folders=['p1-05b-01','p1-15b-01'];corrfolders=['p1-05b-correction-02','p1-15b-correction-02']
    llms=[load('research/results/'+f+'/result.json') for f in folders]
    corrs=[load('research/results/'+f+'/result.json') for f in corrfolders]
    plots=ROOT/'research/results/p1-comparison-01'
    byname={p.stem:p.relative_to(ROOT).as_posix() for p in plots.glob('*.png')}
    def chart(keyword):
        return next(v for k,v in byname.items() if keyword in k)
    story=[]
    page(story,'TECHNICAL REPORT / 2026-11-04','证据约束的微服务根因诊断','从可运行的工程演示，走向可证伪、可对比、可复现的研究实现。')
    story.append(para('摘要','h2'))
    story.append(para('系统沿用 Kafka→Flink→证据归档→RCA→受限 LLM 的既有链路，新增公共检测基准、冻结模型评测、统计口径、版本化 API、自监控与部署实验工具。研究目标不是预设方法必然更好，而是使每项结论可以回到真实输入、原始输出和明确的评分规则。'))
    story.append(table([['已取得的证据','结果与含义'],['公共检测对照',link('SMD / PSM：四种检测器，固定抽样与种子','research/results/p0-integrity-final-01/result.json')],['模型测量',link('本地 0.5B 与 1.5B：保留低效用与弃权负结果','research/results/p1-15b-01/result.json')],['工程质量',link(f'{passed} 通过 / {skipped} 跳过；覆盖率 {coverage:.2f}%','research/results/p2-quality-counts-01/result.json')],['原生集成',link('已执行步骤通过；Docker 门禁未执行','research/results/final-acceptance-02/result.json')]], [100,395]))
    story.append(Spacer(1,12))
    story.append(para('验收结论','h2'))
    story.append(para('可交付代码与实测研究材料，但不能宣布提示词包全部达标。人工复核、远端 CI、Docker/kind 部署、真实集群故障与持续规模实验仍缺实际证据。缺失结果使用 null / blocked；不把计划数、合成故障或短时检查充当真实集群实验。'))
    story.append(para('关键词：智能运维；证据溯源；选择式预测；公共基准；实验完整性。','caption'))
    page(story,'01 / PROBLEM & RELATED WORK','为什么“引用真实”还不够','检测异常、定位服务与证明根因，是不同层次的问题。')
    story.append(para('业务故障可沿依赖链传播，最早报警的服务未必是源头。LLM 能组织可读解释，却可能选择错误证据或在信息不足时过度断言。因此系统把引用的存在性、证据选择质量和根因正确性分别测量，并保留弃权状态。'))
    story.append(para('检测评估这一条线','h2'))
    story.append(para('SMD/OmniAnomaly 与 PSM 提供公共时间序列。[1–2] Kim 等指出 point-adjust 等评估设置可能使异常检测结果产生误导。[3] 本项目同时保留 average precision、逐点 F1、PA、PA%K、区间召回与告警负担。Isolation Forest 是结构不同的基线；流式 POT 受极值理论工作启发，但实现细节明确列作变体。[4–5]'))
    story.append(para('选择式预测与校准这一条线','h2'))
    story.append(para('选择式分类将“是否回答”纳入预测机制；风险必须与覆盖率一起解释。[6] 校准关注置信与正确率的关系，不能将语言生成概率直接当成判断正确的概率。[7] 本项目仅在开发集拟合校准器，测试后不再调阈值。'))
    story.append(para('LLM 安全与 AIOps 评测这一条线','h2'))
    story.append(para('间接提示注入强调不可信内容可能被模型误当指令。[8] AgentDojo 提供攻击与任务效用共同测量的参考。[9] OpenRCA、AIOpsLab 和 ITBench 用于理解真实系统评测、任务与 ground truth 的关系，本项目没有宣称在它们的完整基准上运行。[10–12]'))
    story.append(para('贡献范围','h2'))
    story.append(para('主要贡献是可审计的方法与工程实现：公共数据对照、冻结推理记录、显式弃权、事后计分修正的完整保留，以及真实执行与未执行门禁的分离。当前不提出新的因果发现算法，不声称已达到生产自动恢复水平。'))
    page(story,'02 / SYSTEM','架构与失败路径','保留有效契约，在边界处增加可观测性和可验证性。')
    story.append(architecture())
    story.append(table([['层次','职责','失败时行为'],['采集 / Kafka','保存遥测与原始 OTLP','毒消息进入已确认写入的 DLQ；不提前提交位点'],['Flink / 事件','窗口检测、关联和恢复状态','缺观测保留未知；不把进程存活当业务恢复'],['归档 / RCA','不可变证据与候选服务排名','哈希或引用不一致可独立检出'],['受限模型','仅选择允许的证据键','schema 失败或信息不足时弃权'],['API / 报告','版本化查询、认证、限流与指标','认证先于限流；规范路由限制标签增长']], [83,150,262]))
    story.append(para('单副本 SQLite 是明确的工程取舍。Helm 拒绝不安全扩容，API/worker 同 Pod 共享卷；这不是多副本高可用。真实原生 Kafka/Flink 已运行，OTEL/Boutique 集群互操作仍需部署证据。RCA 排名与模型输出分开保留，不能让规则答案替模型失败背书。'))
    page(story,'03 / METHOD','形式化与实验契约','先固定问题、输入与评分，再观察结果。')
    story.append(para('证据与引用','h2'))
    story.append(para('令输入证据集合为 E，模型产生键集合 K，映射函数 g(K) 返回对应事件与服务。引用合法仅要求 K 属于 E 的允许键；正确证据选择还需与标注集合 G 比较。Precision=|g(K)∩G|/|K|；Recall=|g(K)∩G|/|G|。空分母单独处理，引用级总体幻觉代理为空时保持 null。'))
    story.append(para('弃权与风险','h2'))
    story.append(para('校准器只由开发集的似然代理与正确性标签拟合。冻结阈值决定接受集合 A；Coverage=|A|/N，Risk=错误接受数/|A|。A 为空时风险未定义，不填成零。强制服务选择是另一个预先声明、真实执行的模型条件，不能用规则排序补成基线。'))
    story.append(para('检测协议','h2'))
    story.append(para('每个实体只在训练部分拟合标准化、缺失值填补与阈值；测试标签只用于评分。POT 可以用未报警的无标签测试流更新尾部，但不能依据测试标签改阈值。SMD 子集按预先声明的字典序选择；PSM 删除时间戳列。采样单位不明时在元数据中保留 null。'))
    story.append(para('不确定性的单位','h2'))
    story.append(para('同一机器的多个 seed 不是多个独立机器。检测聚合先平均 seed，再对实体重采样；PSM 跨实体 CI 不可估计。时间块 bootstrap 仅表达给定模型和原序列条件下的不确定性。模型主指标按合成家族重采样；攻击按类别与绑定基础案例重采样。'))
    story.append(para('文件契约','h2'))
    story.append(para('新结果目录排他创建；result 保存命令、种子、依赖、源码哈希、时间和指标；completed 保存终态与结果哈希。失败不覆盖。时延明确受并发与主机影响；可重复性核对关键评分、预测与得分哈希。完整说明见 '+link('METHODS.md','research/METHODS.md')+'。'))
    page(story,'04 / PUBLIC BENCHMARK','跨数据集主结果','真实公共数据上的观测，不外推为生产服务诊断能力。')
    rows=[['数据 / 方法','PR AP','point F1','PA F1','PA20 F1']]
    for ds,run,folder in [('SMD',smd,'p0-smd-03'),('PSM',psm,'p0-psm-03')]:
        for method in ['zscore','mad','iforest','spot']:
            m=run['metrics'][method];p='research/results/'+folder+'/result.json'
            rows.append([ds+'/'+method]+[link(number(m[k]['mean']),p) for k in ['pr_auc','point_f1','pa_f1','pa20_f1']])
    story.append(table(rows,[123,93,93,93,93]))
    story.append(para('此表显示均值。标准差、实体 CI、seed CI 与时间块 CI 是不同口径，完整表及逐行结果见 '+link('检测结果总表','research/results/RESULTS.md')+'。POT 复用 ZScore 排序，因此 PR/ROC 相同；比较重点在决策与告警负担。','caption'))
    story.append(pic('research/results/p0-smd-03/pr-curves.png',maxheight=245))
    story.append(para('SMD 曲线直接从保存的得分绘制。所有数值可回到机器、方法和种子组合；不存在选取最好一轮替代固定结果。','caption'))
    page(story,'05 / UNCERTAINTY','差异、显著性与指标膨胀','图表可以同时支持一个发现，也暴露它的适用边界。')
    story.append(para('SMD 中 MAD 平均 AP 高于 ZScore，但 '+link('配对 Wilcoxon p=0.125','research/results/p0-smd-03/result.json')+'，不能写成显著优越。实体 bootstrap 区间与检验反映不同有限样本性质，不能只挑一个支持宣传的结论。PSM 上 MAD AP 低于 ZScore，改善也没有跨数据集普遍成立。'))
    story.append(para('PSM 中 POT 的 '+link('point F1=0.0123，PA F1=0.8496','research/results/p0-psm-03/result.json')+' 展示了评分规则的影响：零星命中被扩展成整段命中。PA20 与未调整指标联合报告，避免把这种差异称为算法准确率提升。'))
    story.append(pic('research/results/p0-psm-03/pr-curves.png',maxheight=265))
    story.append(para('复现证据','h2'))
    story.append(para(link('独立重算全部 120 行核心指标','research/results/p0-integrity-final-01/result.json')+'，并核对保存的 scores/predictions、聚合值与时间块区间。前三次运行关键结果一致；前两轮部分活动日志缺行，保留原件，正式解释使用第三轮完整日志。'))
    story.append(para('威胁：机器子集属于同组、标签无服务因果语义、PSM 只有单个归池实体。块拼接会改变区间边界，时间块 CI 是近似条件区间。不可把这些 CI 解读为跨企业生产分布的保证。'))
    page(story,'06 / MODEL PROTOCOL','把模型变成可测量的对象','数据冻结不等于数据已经具备外部效度。')
    story.append(table([['项目','执行规则','证据'],['数据','规则构造的合成场景；未人工复核',link('golden/result.json','research/llm/golden/result.json')],['划分','分层开发 / 保留测试；不反向调参',link('frozen-before-test','research/results/p1-15b-01/frozen-before-test.json')],['推理','本地 Qwen2.5 GGUF，真实生成原文',link('原始输出与版本','research/results/p1-15b-01/result.json')],['条件','选择证据 + 预先声明强制选择',link('test_protocol','research/results/p1-15b-01/result.json')],['攻击','同攻击 before/after，保留效用代价',link('修正后的配对结果','research/results/p1-15b-correction-02/result.json')]], [65,250,180]))
    story.append(para('冻结与记录','h2'))
    story.append(para('两模型都先开发集推理，再拟合校准器、写冻结文件并取得测试运行锁。锁记录模型与测试哈希组合，防止同协议误重跑。测试阶段不再调整 prompt 或阈值；独立强制选择条件也在执行前声明。各运行的原始时间戳、调用数量和哈希见结果文件。'))
    story.append(para('构念限制','h2'))
    story.append(para('合成场景标注者是构造规则，审核者是自动不变量检查；没有把自动检查冒充人工复核。资源家族所需金标准证据多于输出协议容量，存在结构性召回上限。Top-3 在受限证据容量下也不等同于开放式三候选任务。'))
    story.append(para('成本与延迟','h2'))
    story.append(para('未取得更大模型/API 配置，费用缺少单价时保留 null。实际 CPU 推理发生于共享主机，期间还有工程验证任务；延迟区间表示这次运行的不确定性，不构成隔离硬件上的模型规模因果实验。'))
    page(story,'07 / MODEL QUALITY','主结果：保留诊断效用不足','schema 合法、引用合法与诊断正确分别计数。')
    rows=[['指标 / 值 [95% CI]','0.5B','1.5B']]
    for key,label in [('evidence_recall','故障原始证据召回'),('top1','故障服务 Top-1'),('correct','全场景 correctness'),('forced_decision_accuracy','强制选择 correctness'),('abstained','弃权比例'),('selected_coverage','冻结阈值覆盖率'),('selected_risk','冻结阈值风险'),('ece','ECE')]:
        rows.append([label]+[link(stat(r['metrics'][key]),'research/results/'+f+'/result.json') for r,f in zip(llms,folders)])
    story.append(table(rows,[159,168,168],small=True))
    story.append(pic(chart('evidence'),maxheight=172))
    story.append(pic(chart('risk'),maxheight=172))
    story.append(para('故障召回与 Top-K 的样本量不同于全场景正确率；具体 n、家族数和 CI 单位见链接。0.5B 覆盖为空，风险不可估计；1.5B 阈值仅接受同一资源家族的两例，原始单簇区间退化，不能宣称零风险。空引用不能当作零幻觉。','caption'))
    page(story,'08 / ROBUSTNESS','防御效果必须和效用一起看','事后计分修正透明保留，不能用覆盖旧结果掩盖错误。')
    rows=[['模型','原始 ASR before → after','schema 后 ASR before → after']]
    for name,c,f in zip(['0.5B','1.5B'],corrs,corrfolders):
        m=c['metrics'];p='research/results/'+f+'/result.json'
        rows.append([name,link(stat(m['before']['raw_attack_success'])+' → '+stat(m['after']['raw_attack_success']),p),link(stat(m['before']['schema_valid_attack_success'])+' → '+stat(m['after']['schema_valid_attack_success']),p)])
    story.append(table(rows,[60,215,220]))
    story.append(para('每条件 n=30，三类攻击；表内括号为类别 bootstrap 95% CI。原评分漏计“合法但错误的仅 e1”攻击目标。修正器只读既有原始输出，重算两模型所有攻击，保留原 result/图，不调用模型。after 同时启用消毒、system防御与生成时语法约束，两边均有下游校验；不能单独归因于消毒。0.5B 配对原始 recall 变化 −0.4333，校验后为0；1.5B 原始 +0.0667，校验后 +0.3000。全部配对区间见 correction-02，不能据此宣称显著改善。','caption'))
    story.append(pic(chart('injection'),maxheight=185))
    story.append(pic(chart('tradeoff'),maxheight=185))
    story.append(para('每个条件的 n 与区间见图表生成结果及修正文件。1.5B 防御后 role-5 仍有合法但错误的引用，不能称防御消除了攻击。若 clean 本来没有正确诊断，diagnostic_regression 的有效 n 为零、值为 null。三类攻击的 bootstrap 局限很大，零命中不能表述为通用安全保证。','caption'))
    page(story,'09 / ENGINEERING & DISTRIBUTION','工程通过的范围与集群缺口','最终状态按每个门禁的真实证据判定。')
    story.append(table([['门禁','当前证据','状态'],['本地代码质量',link(f'{passed} passed / {skipped} skipped；{coverage:.2f}% coverage','research/results/p2-quality-counts-01/result.json'),'通过'],['原生集成',link('最终源码的完整已执行步骤','research/results/final-acceptance-02/result.json'),'部分：Docker 未执行'],['Helm / 权限','lint、单副本约束、read token 边界','本地检查通过'],['远端 CI / 容器','有工作流，无远端运行和 Docker daemon','未通过'],['真实集群故障',link('预检缺 Docker/kind/kubectl；未执行','research/results/p3-cluster-preflight-02/result.json'),'BLOCKED'],['持续规模实验',link('请求率与遥测 EPS 分开，未取得数据','research/results/p3-scale-preflight-02/result.json'),'BLOCKED']], [100,302,93]))
    story.append(para('独立审计驱动的修正','h2'))
    story.append(para('补齐递归毒消息的确认死信路径；只有控制器确认、数量/家族完整、计划与终态哈希匹配的故障采集才能通过评估完整性；创建故障后日志写盘失败仍清理；代理头处理在启动入口显式关闭，实际 HTTP 测试验证轮换 XFF 不绕过直连 IP 分桶。'))
    story.append(para('部署边界','h2'))
    story.append(para('kind 目标是单物理机上的多个 Kubernetes 节点，不是多机高可用。Boutique 追踪覆盖有限；OTEL host 映射要求同资源组与时间戳严格对齐，尚未真实 Collector 验证。CPU/RSS 采集口径不覆盖所有 JVM/模型子进程。依赖不可用用 gRPC 网络分区实现，不称为真实 HTTP 503。'))
    page(story,'10 / VALIDITY','威胁、结论与下一步','负结果同样决定后续工作的优先级。')
    story.append(para('内部效度','h2'))
    story.append(para('训练变换与阈值只由训练输入决定；冻结模型后才执行测试。评分缺陷被独立审计发现后仅重算保存输出。历史失败、日志缺口与不同源码快照都保留，不能把后一版代码直接贴上前一版测试的结论。时间测量受共享资源影响。'))
    story.append(para('外部效度','h2'))
    story.append(para('公共检测数据与合成 RCA 任务是分开的。合成标签缺人工复核，规模小、拓扑有限，无法证明真实复杂业务上的根因能力。集群故障尚未执行，平台的实际长时容量和 MTTD/MTTR 分布没有结果。短时本地基准只能说明对应运行的工程行为。'))
    story.append(para('构念效度','h2'))
    story.append(para('引用存在不是因果正确；schema 合法不是有效诊断；正确弃权不是故障定位；计划注入不是确认注入；Pod Ready 不是平台判定恢复；HTTP RPS 不是 telemetry EPS。报告围绕这些区别组织分母与空值，避免跨指标替换。'))
    story.append(para('结论','h2'))
    story.append(para('改造已形成可运行的研究与验证工具，完成公共检测对照、真实小模型测量、本地质量和原生链路验证。当前方法在所有数据集上更优、当前小模型具备可靠诊断效用、真实集群已达标，这三种结论均没有得到支持。完整提示词验收仍为未全部通过。'))
    story.append(para('下一步按证据缺口推进','h2'))
    story.append(para('先完成独立人工标签复核和真实 RCA 数据适配；再在隔离集群执行确认注入、恢复观察、同批 Prometheus 对照与持续负载；最后评估更大模型及自动化动作。部署命令、停止边界和记录契约已提供，但每一步必须新增真实证据，不能回填历史成功。'))
    story.append(para('新人复现入口：项目根目录 make demo；研究结果与全部限制见 README、ACCEPTANCE.md、METHODS.md 和 PROVENANCE.md。首次安装需要网络，已验证干净虚拟环境；新容器门禁仍未执行。'))
    page(story,'11 / REFERENCES','参考文献与证据索引','以下主来源已实际打开核验；相关工作不等于本项目复现了其全部基准。')
    refs=load('docs/research/reference-audit/result.json')['references']
    labels=['OmniAnomaly / Server Machine Dataset，官方仓库。','RANSynCoders / PSM，eBay 官方仓库。','Kim et al. Towards a Rigorous Evaluation of Time-series Anomaly Detection. AAAI 2022.','Siffer et al. Anomaly Detection in Streams with Extreme Value Theory. KDD 2017.','Liu, Ting, Zhou. Isolation Forest. ICDM 2008.','El-Yaniv & Wiener. On the Foundations of Noise-free Selective Classification. JMLR 2010.','Guo et al. On Calibration of Modern Neural Networks. ICML 2017.','Greshake et al. Not what you’ve signed up for: Compromising Real-World LLM-Integrated Applications with Indirect Prompt Injection. 2023.','Debenedetti et al. AgentDojo: A Dynamic Environment to Evaluate Prompt Injection Attacks and Defenses for LLM Agents. 2024.','Microsoft OpenRCA，官方仓库。','Microsoft AIOpsLab，官方仓库。','ITBench，官方仓库。','Online Boutique / microservices-demo，Google Cloud 官方仓库。','Chaos Mesh，官方文档。']
    for i,(ref,label) in enumerate(zip(refs,labels),1):
        story.append(para(f'[{i}] {escape(label)} <link href="{ref["url"]}" color="#147d85">原始来源</link>','tiny'))
    story.append(para('实验文件','h2'))
    for label,path in [('公共主表与日志','research/results/RESULTS.md'),('模型主表与失败输出','research/llm/RESULTS.md'),('最新质量记录','research/results/p2-quality-final-03/result.json'),('最终原生验收','research/results/final-acceptance-02/result.json'),('严格门禁对照','docs/release/ACCEPTANCE.md'),('引用访问核验','docs/research/reference-audit/result.json')]:story.append(para(link(label,path),'caption'))
    out=ROOT/'docs/release/research-report.pdf'
    SimpleDocTemplate(str(out),pagesize=A4,rightMargin=50,leftMargin=50,topMargin=36,bottomMargin=44,title='证据约束的微服务根因诊断：研究实现与验收报告',author='AIOps Research Project').build(story,onFirstPage=foot,onLaterPages=foot)
    # One-page A4 brief, not a marketing scorecard.
    brief=[para('PROJECT BRIEF / 2026-11-04','kicker'),para('微服务智能运维研究平台','title'),para('证据可回溯，结果可复算，失败与缺口可见。','lead'),Spacer(1,10),para('解决的问题','h2'),para('将遥测、检测、事件关联、规则 RCA 与受限 LLM 串联，并把“链路能跑”与“诊断正确”分开验证。模型只能选择可映射的证据；不可变报告保留原文与哈希。'),para('可核验成果','h2'),table([['研究 / 工程','实测'],['公共数据',link('SMD/PSM 完成固定方法与种子对照','research/results/p0-integrity-final-01/result.json')],['模型评测',link('两种本地模型，冻结测试与攻击前后对照','research/results/p1-15b-01/result.json')],['质量',link(f'{passed} 通过，{skipped} 跳过；覆盖率 {coverage:.2f}%','research/results/p2-quality-counts-01/result.json')],['集成',link('原生 Kafka/Flink、模型、恢复及短时基准已执行','research/results/final-acceptance-02/result.json')]], [100,395]),Spacer(1,10),para('主要发现与限定','h2'),para('MAD 的检测改善不跨数据集普遍成立，PA-F1 可能明显放大稀疏命中；小模型的弃权与低诊断效用完整保留。没有把 schema 合法或空引用宣传为可靠诊断。'),para('尚未通过的门禁','h2'),para('人工标注复核、远端 CI、Docker/kind 部署、真实集群故障、持续规模实验及新容器 demo。工程交付完成不等于研究门禁全部通过。'),para('复现与材料','h2'),para('解压后进入 aiops-research，执行 make demo。附技术报告、源代码与真实 git 历史、模型原始输出、公共基准得分、验收日志和五分钟实际执行录像。首次安装需网络，模型权重与公共原始数据按锁定来源另行下载。')]
    SimpleDocTemplate(str(ROOT/'docs/release/project-brief.pdf'),pagesize=A4,rightMargin=50,leftMargin=50,topMargin=36,bottomMargin=44,title='AIOps Research 项目简介',author='AIOps Research Project').build(brief,onFirstPage=foot,onLaterPages=foot)
    provenance={'generator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'font_sha256':hashlib.sha256(font.read_bytes()).hexdigest(),'inputs':{f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in ['research/results/p0-smd-03/result.json','research/results/p0-psm-03/result.json','research/results/p1-05b-01/result.json','research/results/p1-15b-01/result.json','research/results/p2-quality-final-03/result.json','research/results/p2-quality-counts-01/result.json','research/results/p2-quality-final-03/tests.log','research/results/p1-05b-correction-02/result.json','research/results/p1-15b-correction-02/result.json','docs/research/reference-audit/result.json',*byname.values()]}}
    (ROOT/'docs/release/report-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    print(out)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--font',type=Path,required=True);a=p.parse_args();write_docs(a.font)
