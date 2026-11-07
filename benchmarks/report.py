"""Stable flat CSV plus a human-readable measured-result summary."""
import csv
import json
from pathlib import Path
from .metrics import comparable


def flat_rows(result):
    for component in ('http','kafka'):
        for tier in result.get('components',{}).get(component,{}).get('tiers',[]):
            http=component=='http';latency=tier['successful_latency_ms'] if http else tier['event_to_processing_complete_ms']
            yield {'component':component,'kind':tier.get('kind','kafka_python'),'stage_id':tier['stage_id'],
                'repeat':tier['repeat'],'target_per_s':tier.get('target_per_s',tier.get('target_events_per_s')),
                'offered':tier['offered'],'started':tier['started'],
                'successful':tier.get('successes',tier.get('durably_processed')),
                'failed':tier.get('failed',tier.get('producer_failed')),'skipped':tier['load_generator_skipped'],
                'send_window_s':tier['send_window_s'],'elapsed_with_drain_s':tier['elapsed_with_drain_s'],
                'successful_per_s_including_drain':tier.get('successful_per_s_including_drain',tier.get('durable_events_per_s_including_drain')),
                'latency_p50_ms':latency['p50'],'latency_p95_ms':latency['p95'],'latency_p99_ms':latency['p99'],
                'cpu_mean_one_core_percent':tier['resources']['cpu_sum_percent_one_core']['mean'],
                'rss_peak_bytes':tier['resources']['rss_sum_bytes']['max'],
                'lag_peak':tier.get('lag',{}).get('max'),'lag_after':tier.get('lag_after')}


def write_report(output,result):
    output=Path(output)
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    rows=list(flat_rows(result))
    with (output/'metrics.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]) if rows else ['component','kind','stage_id'])
        writer.writeheader();writer.writerows(rows)
    def f(value):return '未测得' if value is None else f'{value:.2f}'
    lines=['# 第六阶段自动评估结果','',f"状态：{result['status']}。测试时间：{result['started_at_utc']}。",'',
        'HTTP 业务量单位是请求/s；HTTP ingest 与 Kafka 是事件/s。吞吐包含发送结束后的排空时间。HTTP 延迟为成功请求到响应；Kafka 为事件创建到生产处理器持久化完成。', '',
        '| 类型 | 重复 | 目标/s | 成功/s | p95 ms | 失败/跳过 | CPU均值（单核%） | RSS峰值 MiB | lag峰值→末值 |',
        '| --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for row in rows:
        lines.append(f"| {row['kind']} | {row['repeat']} | {row['target_per_s']} | {f(row['successful_per_s_including_drain'])} | {f(row['latency_p95_ms'])} | {row['failed']}/{row['skipped']} | {f(row['cpu_mean_one_core_percent'])} | {f(row['rss_peak_bytes']/1024**2 if row['rss_peak_bytes'] is not None else None)} | {row['lag_peak']}→{row['lag_after']} |")
    lines+=['','## 分项状态','']+[f"- {name}: {value.get('status')} {value.get('reason','')}" for name,value in result['components'].items()]
    lines+=['','合成标签回放的检测准确率、精确率、召回率在 evaluation/detection/result.json；RCA Top-1/Top-3 在 evaluation/rca/result.json。真实故障的独立统计在 http/result.json 的 live_quality。',
            '', '缺失数值保持 null，CSV 空值不代表0。合成窗口实验不代表生产准确率；本机单 broker/Python 消费者不代表 Flink 或集群容量。模型费用仅按实际 usage 和配置单价估算。']
    (output/'report.md').write_text('\n'.join(lines)+'\n')


def compare_runs(a,b):
    checks=comparable(a,b);valid=all(checks.values());left={r['stage_id']:r for r in flat_rows(a)};right={r['stage_id']:r for r in flat_rows(b)}
    changes=[]
    if valid:
        for stage in sorted(left.keys() & right.keys()):
            before,after=left[stage],right[stage];deltas={}
            for key in ('successful_per_s_including_drain','latency_p95_ms','cpu_mean_one_core_percent','rss_peak_bytes'):
                x,y=before.get(key),after.get(key)
                deltas[key]={'before':x,'after':y,'relative_change':(y-x)/x if x is not None and y is not None and x!=0 else None}
            changes.append({'stage_id':stage,'changes':deltas})
    return {'comparable':valid,'checks':checks,'changes':changes,
            'note':'Same metadata permits descriptive comparison, not a statistically significant capacity claim. Different configuration/source/environment requires an explicitly controlled experiment.'}
