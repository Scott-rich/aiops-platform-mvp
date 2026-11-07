"""Summarize audited attempts, including retries; prices are estimates, not invoices."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from .metrics import distribution

ROOT=Path(__file__).resolve().parents[1]


def summarize_reports(reports):
    attempts={};diagnoses={};reused=0
    for report in reports:
        audit=report.get('audit',{})
        meta=audit.get('model_metadata') or {}
        reused+=bool(meta.get('reused_from_report_id'))
        scope=audit.get('model_budget',{}).get('scope_id') or report.get('incident_id') or report.get('report_id')
        for attempt in meta.get('attempts',[]):
            key=(scope,attempt.get('ordinal'),attempt.get('started_at'),attempt.get('input_sha256'))
            attempts[key]=attempt
        if meta.get('attempts'):
            key=(scope,meta.get('input_sha256'),tuple(a.get('started_at') for a in meta['attempts']))
            diagnoses[key]=meta.get('elapsed_ms')
    rows=list(attempts.values());usage=[];costs={};known_costs=0
    for attempt in rows:
        u=attempt.get('usage')
        valid_usage=isinstance(u,dict) and all(type(u.get(k)) is int and u[k]>=0 for k in ('prompt_tokens','completion_tokens','total_tokens')) and u['total_tokens']>=u['prompt_tokens']+u['completion_tokens']
        if valid_usage:usage.append(u)
        cost=attempt.get('estimated_cost')
        if valid_usage and isinstance(cost,dict) and type(cost.get('amount')) in (int,float) and math.isfinite(cost['amount']) and cost['amount']>=0 and isinstance(cost.get('currency'),str) and cost['currency']:
            costs[cost['currency']]=costs.get(cost['currency'],0)+cost['amount'];known_costs+=1
    return {'actual_attempts':len(rows),'reused_reports':reused,
        'transport_successful_attempts':sum(a.get('outcome')=='transport_success' for a in rows),
        'outcomes':{name:sum(a.get('outcome','unknown')==name for a in rows) for name in sorted({a.get('outcome','unknown') for a in rows})},
        'attempt_latency_ms':distribution(a.get('duration_ms') for a in rows),
        'diagnosis_latency_ms':distribution(diagnoses.values()),
        'attempts_without_usage':len(rows)-len(usage),
        'reported_token_totals':{k:sum(u[k] for u in usage) for k in ('prompt_tokens','completion_tokens','total_tokens')} if usage else None,
        'attempts_without_cost':len(rows)-known_costs,
        'estimated_cost_by_currency':costs if rows and known_costs==len(rows) else None,
        'known_partial_estimated_cost_by_currency':costs or None,
        'cost_basis':'Provider-reported usage × configured prices; estimate only, excludes hardware and invoices. Missing usage/prices stay null; currencies never combined.'}


def run_model(output,enabled=False,evidence=None):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    result={'status':'not_requested','reason':'Real model calls require --real-model; no automatic paid requests',
            'metrics':summarize_reports([])}
    if not os.environ.get('LLM_BASE_URL') or not os.environ.get('LLM_MODEL'):
        result.update(status='blocked',reason='LLM_BASE_URL / LLM_MODEL not configured; no real call made')
    elif enabled:
        env=dict(os.environ)
        env.update(LLM_MAX_ATTEMPTS='1',LLM_MAX_CALLS_PER_INCIDENT='1',LLM_TIMEOUT_SECONDS='20',LLM_TOTAL_TIMEOUT_SECONDS='30')
        command=[sys.executable,'-m','scripts.verify_llm','--output',str(output/'verification')]
        if evidence:command+=['--evidence-report',str(evidence)]
        try:
            with (output/'verification.log').open('w') as log:
                child=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
                try:child.wait(timeout=50)
                finally:
                    if child.poll() is None:
                        child.terminate()
                        try:child.wait(timeout=5)
                        except subprocess.TimeoutExpired:child.kill();child.wait(timeout=3)
            detail=json.loads((output/'verification/result.json').read_text())
            result.update(status=detail['status'],reason='See verification/result.json',verification_exit_code=child.returncode)
        except subprocess.TimeoutExpired:
            result.update(status='failed',reason='Verification deadline exceeded; provider billing outcome may be unknown')
        path=output/'verification/model-enriched-report.json'
        if path.exists():result['metrics']=summarize_reports([json.loads(path.read_text())])
        else:
            result['metrics'].update(actual_attempts=None,attempts_without_usage=None,attempts_without_cost=None,
                                     audit_complete=False,reason='No final attempt audit available; do not infer zero calls or zero billing')
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return result
