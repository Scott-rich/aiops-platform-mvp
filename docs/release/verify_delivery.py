"""Verify delivery files and frozen evidence; never convert blocked research gates to passes."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any
from research.experiment import Experiment, sha256

ROOT=Path(__file__).resolve().parents[2]

def read(path: str) -> Any:
    return json.loads((ROOT/path).read_text())

def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    with Experiment(args.output,{'scope':'delivery integrity, not proof all research gates passed','verifier_sha256':sha256(Path(__file__))},[2026]) as run:
        quality=read('research/results/p2-quality-final-03/result.json')
        mismatches=[name for name,h in quality['source_sha256'].items() if not (ROOT/name).is_file() or sha256(ROOT/name)!=h]
        assert not mismatches,mismatches
        originals=subprocess.check_output(['git','ls-tree','-r','--name-only','64195e2','--','tests'],cwd=ROOT,text=True).splitlines()
        original_changed=[name for name in originals if (ROOT/name).read_bytes()!=subprocess.check_output(['git','show','64195e2:'+name],cwd=ROOT)]
        assert not original_changed,original_changed
        terminal_names=['p0-smd-03','p0-psm-03','p0-integrity-final-01','p1-05b-01','p1-15b-01','p1-05b-correction-02','p1-15b-correction-02','p1-comparison-01','p2-quality-final-03','p2-quality-counts-01','p3-cluster-preflight-02','p3-scale-preflight-02','p3-cluster-evaluation-02','p4-demo-clean-312-03']
        terminals={}
        for folder in terminal_names:
            path=ROOT/'research/results'/folder
            d=json.loads((path/'result.json').read_text());t=json.loads((path/'completed.json').read_text())
            assert t['result_sha256']==sha256(path/'result.json') and t['status']==d['status']
            terminals[folder]={'status':d['status'],'sha256':t['result_sha256']}
        native=read('research/results/final-acceptance-02/result.json')
        native_terminal=read('research/results/final-acceptance-02/completed.json')
        assert native==native_terminal
        assert len(native['steps'])==13 and all(s['status']=='passed' for s in native['steps'])
        assert native['status']=='partial' and native['full_acceptance'] is False and native['exit_code']==2
        counts=read('research/results/p2-quality-counts-01/result.json')['metrics']
        assert counts['tests_passed']==509 and counts['tests_skipped']==10 and counts['tests_failed']==0
        docs=[ROOT/'README.md',ROOT/'research/METHODS.md',ROOT/'research/llm/RESULTS.md',ROOT/'research/llm/PROTOCOL.md',ROOT/'deploy/README.md',*(ROOT/'docs/release').glob('*.md')]
        broken=[];checked=0
        for path in docs:
            for target in re.findall(r'\]\(([^\s)]+)\)',path.read_text()):
                if target.startswith(('https://','http://','#')):continue
                target=target.split('#')[0]
                if not target:continue
                checked+=1
                if not (path.parent/target).exists():broken.append([str(path.relative_to(ROOT)),target])
        assert not broken,broken
        pages={}
        for file,wanted in [('research-report.pdf',12),('project-brief.pdf',1)]:
            p=ROOT/'docs/release'/file
            info=subprocess.check_output(['pdfinfo',str(p)],text=True)
            actual=int(re.search(r'Pages:\s+(\d+)',info).group(1));assert actual==wanted
            pages[file]={'pages':actual,'sha256':sha256(p),'bytes':p.stat().st_size}
        video=ROOT/'demo/aiops-five-minute.mp4'
        probe=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_entries','format=duration:stream=codec_name,width,height,nb_frames','-of','json',str(video)]))
        assert abs(float(probe['format']['duration'])-300)<0.01
        vroot=ROOT/'research/results/p4-video-02'
        recording=json.loads((vroot/'recording.json').read_text());vr=json.loads((vroot/'run/result.json').read_text());vt=json.loads((vroot/'run/completed.json').read_text())
        assert recording['exit_code']==0 and recording['cast_sha256']==sha256(vroot/'session.cast')
        assert vr['status']=='passed' and vr['metrics']['selected_incident_final_status']=='resolved'
        assert vt['result_sha256']==sha256(vroot/'run/result.json')
        assert vr['source_sha256']['demo/video_driver.py']==sha256(ROOT/'demo/video_driver.py')
        responses=json.loads((vroot/'run/http-responses.json').read_text())
        verifies=[r['response'] for r in responses if r['path'].endswith('/verify')]
        required=['full_evidence_valid','snapshot_sha256_valid','rca_sha256_valid','rca_evidence_references_valid','rca_evidence_set_valid']
        assert len(verifies)>=2 and all(all(v.get(k) is True for k in required) for v in verifies)
        n_chinese=len(re.findall(r'[\u4e00-\u9fff]',(ROOT/'docs/release/blog-outline.md').read_text()));assert n_chinese>=4000
        commit_count=int(subprocess.check_output(['git','rev-list','--count','HEAD'],cwd=ROOT,text=True));assert commit_count>=30
        files=subprocess.check_output(['git','ls-files','-z'],cwd=ROOT).decode().split('\0');files=[ROOT/f for f in files if f]
        size=sum(p.stat().st_size for p in files);assert size<300*1024*1024
        run.result['metrics']={'frozen_quality_source_files_verified':len(quality['source_sha256']),'original_test_files_unchanged':len(originals),'terminal_hashes':terminals,'test_counts':counts,'native_steps_passed':len(native['steps']),'native_full_acceptance':False,'local_markdown_links_checked':checked,'pdfs':pages,'video':{'duration_seconds':float(probe['format']['duration']),'frames':int(probe['streams'][0]['nb_frames']),'sha256':sha256(video),'observed_payment_recovery':True,'integrity_verdicts_checked':len(verifies)},'blog_chinese_characters':n_chinese,'git_commits_at_verification':commit_count,'tracked_file_bytes_at_verification':size,'all_prompt_gates_passed':False,'unpassed_gates':['human_label_review','remote_ci','docker_build','kind_deployment','30_real_cluster_faults','sustained_eps_scale','clean_container_demo']}
        run.summary(['交付文件结构与冻结证据一致；不改变研究门禁状态。','公共检测、模型输出、工程与演示产物。','对照原始测试文件、冻结源码和终态哈希。','完整性检查不估计外部准确率。','人工复核/远端CI/真实集群等缺证据。','在具备相应条件后以新目录补验。'])

if __name__=='__main__':main()
