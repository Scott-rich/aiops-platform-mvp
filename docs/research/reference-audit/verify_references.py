import json, urllib.request, hashlib, sys, platform
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
urls = [
('smd', 'https://github.com/NetManAIOps/OmniAnomaly'),
('psm', 'https://github.com/eBay/RANSynCoders'),
('pa', 'https://arxiv.org/abs/2109.05257'),
('spot', 'https://www.kdd.org/kdd2017/papers/view/anomaly-detection-in-streams-with-extreme-value-theory'),
('iforest', 'https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf'),
('selective', 'https://jmlr.org/papers/v11/el-yaniv10a.html'),
('calibration', 'https://proceedings.mlr.press/v70/guo17a.html'),
('injection', 'https://arxiv.org/abs/2302.12173'),
('agentdojo', 'https://arxiv.org/abs/2406.13352'),
('openrca', 'https://github.com/microsoft/OpenRCA'),
('aiopslab', 'https://github.com/microsoft/AIOpsLab'),
('itbench', 'https://github.com/itbench-hub/ITBench'),
('boutique', 'https://github.com/GoogleCloudPlatform/microservices-demo'),
('chaos', 'https://chaos-mesh.org/docs/')]
def check(row):
    key,url=row
    try:
        with urllib.request.urlopen(url,timeout=30) as response:
            body=response.read()
            Path(key+'.source').write_bytes(body)
            return {'id':key,'url':url,'http_status':response.status,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'retrieved_at':datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        return {'id':key,'url':url,'error':str(exc)}
with ThreadPoolExecutor(max_workers=4) as executor: rows=list(executor.map(check,urls))
result={'config':{'urls':urls},'seeds':[],'command':'python verify_references.py','environment':{'python':sys.version,'platform':platform.platform()},'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'metrics':{'accessible':sum(r.get('http_status')==200 for r in rows),'total':len(rows)},'references':rows}
Path('result.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result['metrics']))
