"""Execute the existing dashboard RCA renderer against a minimal DOM in Node.

This checks rendering/security contracts, not browser layout or a real HTTP run.
Node is optional for Python-only deployments; unavailable Node is an explicit skip.
"""
from pathlib import Path
import re
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / 'app/static/index.html'
NODE = shutil.which('node')

DOM = r'''
const assert=require('node:assert/strict');
class Element {
  constructor(tag){this.tagName=tag;this.children=[];this.attributes={};this.events={};this._text='';this.hidden=false;}
  set textContent(value){this._text=String(value);this.children=[];}
  get textContent(){return this._text+this.children.map(x=>x.textContent).join('');}
  set innerHTML(_value){throw new Error('unsafe HTML insertion');}
  append(...nodes){this.children.push(...nodes);}
  replaceChildren(...nodes){this._text='';this.children=[...nodes];}
  setAttribute(key,value){this.attributes[key]=String(value);}
  addEventListener(name,callback){this.events[name]=callback;}
  showModal(){this.open=true;}
}
const elements=new Map();
globalThis.document={
  getElementById(id){if(!elements.has(id))elements.set(id,new Element('div'));return elements.get(id);},
  createElement(tag){return new Element(tag);},
  createElementNS(ns,tag){const e=new Element(tag);e.namespaceURI=ns;return e;}
};
function walk(node){return [node,...node.children.flatMap(walk)];}
function byId(id){return document.getElementById(id);}
function reportFixture(){return {report_id:'report-test',snapshot:{records:[
  {event:{event_id:'trace-1',service:'frontend',message:'original record'}},
  {event:{event_id:'trace-2',service:'database-adapter',message:'observed failure'}}]},rca:{
  rca_version:'evidence-rca-v1',status:'candidates',summary:'Observed dependency and evidence',
  direct_anomalous_services:['database-adapter'],affected_services:['frontend'],
  candidates:[{service:'database-adapter',rank:1,score:4.25,confidence:'low',supporting_evidence_ids:['trace-2'],against_evidence_ids:['missing-ref'],
      affected_services:['frontend'],reasoning:['Observed child span failure'],uncertainties:['No internal resource metrics'],verification_steps:['Inspect dependency'],historical_case_matches:[],model_hypotheses:[]},
    {service:'frontend',rank:2,score:2,confidence:'low',supporting_evidence_ids:['trace-1'],against_evidence_ids:[],reasoning:['Competing explanation'],uncertainties:['Direction remains uncertain'],verification_steps:['Compare spans']}],
  graph:{nodes:[{service:'frontend'},{service:'database-adapter'}],edges:[
    {caller:'frontend',callee:'database-adapter',basis:'parent_span',evidence_ids:['trace-1','trace-2'],trace_ids:['t']},
    {caller:'database-adapter',callee:'frontend',basis:'declared_only',evidence_ids:['trace-2'],trace_ids:['t']}]},
  uncertainties:['Temporal order does not establish causation']}};}
'''


@unittest.skipUnless(NODE, 'Node required for dashboard JavaScript execution tests')
class RCAUITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = HTML.read_text()
        scripts = re.findall(r'<script>(.*?)</script>', cls.html, re.S)
        if len(scripts) != 1:
            raise AssertionError('expected one standalone dashboard script')
        cls.script = scripts[0]
        marker = "  $('refresh').addEventListener('click',refresh);"
        if marker not in cls.script:
            raise AssertionError('dashboard startup marker changed')
        cls.renderer = cls.script.split(marker)[0] + '\n globalThis.ui={renderRca,renderReport,showEvidence,verificationMessage,state};\n})();\n'

    def run_js(self, checks):
        run = subprocess.run([NODE], input=DOM+self.renderer+checks, text=True,
                             capture_output=True, cwd=ROOT, timeout=15)
        self.assertEqual(run.returncode, 0, run.stdout+run.stderr)

    def test_dashboard_javascript_parses(self):
        run = subprocess.run([NODE, '--check'], input=self.script, text=True,
                             capture_output=True, cwd=ROOT, timeout=15)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn('调用方 → 被调用方', self.html)
        self.assertIn('故障影响可能沿相反方向传播', self.html)

    def test_legacy_report_does_not_fabricate_rca(self):
        self.run_js(r'''
const report=reportFixture();ui.state.report=report;ui.renderRca(report);
assert.equal(byId('rca-candidates').children.length,2);
ui.renderRca({snapshot:{records:[]}});
assert.equal(byId('rca-content').hidden,true);
assert.equal(byId('rca-empty').hidden,false);
assert.match(byId('rca-empty').textContent,/尚无RCA版本结果/);
assert.equal(byId('rca-candidates').children.length,0);
assert.equal(byId('rca-graph').children.length,0);
''')

    def test_candidates_evidence_links_and_dependency_basis(self):
        self.run_js(r'''
const report=reportFixture();ui.state.report=report;ui.renderRca(report);
assert.equal(byId('rca-content').hidden,false);
assert.equal(byId('rca-candidates').children.length,2);
assert.match(byId('rca-candidates').textContent,/评分不是概率/);
assert.match(byId('rca-roles').textContent,/直接异常服务/);
assert.match(byId('rca-roles').textContent,/候选根因服务/);
assert.match(byId('rca-roles').textContent,/受影响服务/);
const paths=walk(byId('rca-graph')).filter(n=>n.tagName==='path' && (n.attributes.class || '').includes('rca-edge'));
assert.equal(paths.length,2);
assert.equal(paths.filter(n=>n.attributes.class.includes('rca-edge-declared')).length,1);
const buttons=walk(byId('rca-candidates')).filter(n=>n.tagName==='button');
assert.equal(buttons.length,2);assert.ok(buttons.every(n=>n.textContent!=='missing-ref'));
assert.match(byId('rca-candidates').textContent,/missing-ref（不在本报告快照中）/);
// A version change must not redirect a retained reference to another report.
ui.state.report={snapshot:{records:[{event:{event_id:'trace-2',message:'different report'}}]}};
buttons.find(b=>b.textContent==='trace-2').events.click();
assert.equal(byId('evidence-dialog').open,true);
assert.equal(byId('evidence-id').textContent,'trace-2');
assert.equal(JSON.parse(byId('evidence-json').textContent).event.message,'observed failure');
''')

    def test_untrusted_service_and_evidence_are_only_text(self):
        self.run_js(r'''
const attack='<img src=x onerror=globalThis.compromised=true>';
const report=reportFixture();report.rca.graph.nodes[0].service=attack;
report.rca.graph.edges[0].caller=attack;report.rca.graph.edges[1].callee=attack;
report.rca.candidates[0].service=attack;report.rca.candidates[0].reasoning=[attack];
report.snapshot.records[1].event.message=attack;
ui.state.report=report;ui.renderRca(report);
assert.ok(byId('rca-candidates').textContent.includes(attack));
assert.ok(walk(byId('rca-graph')).some(n=>n.tagName==='title' && n.textContent.includes(attack)));
assert.ok(walk(byId('rca-graph')).every(n=>n.tagName!=='img' && n.tagName!=='script' && n.tagName!=='foreignObject'));
assert.equal(globalThis.compromised,undefined);
const button=walk(byId('rca-candidates')).find(n=>n.tagName==='button' && n.textContent==='trace-2');button.events.click();
assert.equal(JSON.parse(byId('evidence-json').textContent).event.message,attack);
''')

    def test_graph_is_bounded_and_truncation_is_explicit(self):
        self.run_js(r'''
const report=reportFixture();report.rca.graph.nodes=Array.from({length:40},(_,i)=>({service:'service-'+i}));
report.rca.graph.edges=Array.from({length:80},(_,i)=>({caller:'service-'+i%29,callee:'service-'+(i%29+1),basis:i%2?'declared_only':'parent_span'}));
ui.renderRca(report);
const nodes=walk(byId('rca-graph'));
assert.equal(nodes.filter(n=>n.tagName==='rect').length,30);
assert.equal(nodes.filter(n=>n.tagName==='path' && (n.attributes.class || '').includes('rca-edge')).length,60);
assert.match(byId('rca-graph-count').textContent,/30\/40个节点、60\/80条边/);
assert.match(byId('rca-graph-count').textContent,/已截断/);
assert.ok(nodes.every(n=>!Object.values(n.attributes).some(v=>v.includes('NaN') || v.includes('undefined'))));
''')

    def test_integrity_message_distinguishes_legacy_and_failed_rca(self):
        self.run_js(r'''
const old=ui.verificationMessage({snapshot_sha256_valid:true,rca_sha256_valid:null,rca_evidence_references_valid:null,rca_evidence_set_valid:null});
assert.equal(old.ok,true);assert.match(old.message,/未提供RCA完整性校验结果/);
const good={snapshot_sha256_valid:true,rca_sha256_valid:true,rca_evidence_references_valid:true,rca_evidence_set_valid:true};
assert.equal(ui.verificationMessage(good).ok,true);
const bad=ui.verificationMessage({...good,rca_evidence_references_valid:false});
assert.equal(bad.ok,false);assert.match(bad.message,/RCA完整性校验未全部通过/);
assert.equal(ui.verificationMessage({...good,snapshot_sha256_valid:false}).ok,false);
''')


if __name__ == '__main__':
    unittest.main()
