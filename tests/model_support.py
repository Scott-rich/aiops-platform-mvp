"""Explicit synthetic fixtures for offline model/schema tests, never real inference."""
import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from app.contracts import now_ms


def records():
    base = {"schema_version":1,"event_time":now_ms()-1000,"service":"payment","instance_id":"p-1",
            "trace_id":"trace-test","span_id":"span-p","parent_span_id":None,"downstream":[],"status_code":503,"latency_ms":1001}
    return [{**base,"event_id":"metric-p","event_type":"request"},
            {**base,"event_id":"span-p","event_type":"span"},
            {**base,"event_id":"log-p","event_type":"log","level":"ERROR","message":"dependency unavailable"}]


def analysis(ref="span-p",service="payment",unknown=False):
    return {"analysis_schema_version":2,"diagnosis_status":"unknown" if unknown else "candidates",
        "summary":"无法确定内部根因，先核实支付服务观测" if not unknown else "无法确定",
        "involved_services":[] if unknown else [service],
        "hypotheses":[] if unknown else [{"cause":"服务异常观测，内部原因待核实","service":service,"confidence":"low",
            "confidence_basis":"观测有错误，缺少内部依赖和变更证据；不是概率",
            "supporting_evidence_ids":[ref],"against_evidence_ids":[],"uncertainty":["缺少依赖证据"],
            "verification_steps":["检查实际调用链与依赖状态"],"remediation_steps":["先人工确认再按运维手册处置"]}],
        "uncertainties":["相关性不证明因果"],"verification_steps":["补充依赖和变更证据"],"recommendations":["先验证候选原因"]}


def payload():
    rows=records()
    return {"prompt_version":"test-v2","records":rows,"evidence_set":{"id":"evidence-test"}}


@contextmanager
def http_stub(scenarios):
    """Real HTTP wire with a deterministic test double; not an LLM."""
    seen=[]
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):
            pass
        def do_POST(self):
            raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
            with lock:
                index=len(seen)
                seen.append({'path':self.path,'headers':dict(self.headers),'body':json.loads(raw)})
            spec=scenarios[min(index,len(scenarios)-1)]
            if callable(spec):spec=spec(seen[-1])
            time.sleep(spec.get('delay',0))
            value=spec.get('body')
            if value is None:
                candidate=spec.get('analysis',analysis())
                value=json.dumps({'id':'stub-response','model':'http-test-double','choices':[{'message':{'content':json.dumps(candidate)},'finish_reason':spec.get('finish_reason','stop')}],
                    'usage':{'prompt_tokens':120,'completion_tokens':80,'total_tokens':200}}).encode()
            if isinstance(value,str):value=value.encode()
            try:
                self.send_response(spec.get('status',200))
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(value)))
                for k,v in spec.get('headers',{}).items():self.send_header(k,v)
                self.end_headers()
                if spec.get('drip'):
                    for b in value:
                        self.wfile.write(bytes([b]));self.wfile.flush();time.sleep(spec['drip'])
                else:self.wfile.write(value)
            except (BrokenPipeError,ConnectionResetError):
                pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    server.daemon_threads=True
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01),daemon=True);thread.start()
    try:yield f'http://127.0.0.1:{server.server_port}/v1',seen
    finally:server.shutdown();server.server_close();thread.join(timeout=2)
