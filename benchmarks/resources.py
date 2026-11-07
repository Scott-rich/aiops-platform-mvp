"""Linux /proc measurement; missing samples remain missing, CPU is per-core %."""
from importlib.metadata import version, PackageNotFoundError
import hashlib
import json
import os
from pathlib import Path
import platform
import time
from .metrics import distribution


def parse_stat(text,hz,page_size):
    # comm can contain whitespace and parentheses; fields after the last ')' start at field3.
    fields=text[text.rindex(')')+2:].split()
    return {'cpu_seconds':(int(fields[11])+int(fields[12]))/hz,
            'start_ticks':int(fields[19]),'rss_bytes':int(fields[21])*page_size}


class ProcessSampler:
    def __init__(self,pids,proc=Path('/proc')):
        self.pids=dict(pids);self.proc=proc;self.previous={};self.paths={}
        self.hz=os.sysconf('SC_CLK_TCK') if hasattr(os,'sysconf') else 100
        self.page_size=os.sysconf('SC_PAGE_SIZE') if hasattr(os,'sysconf') else 4096

    def process_path(self,pid):
        # Test fixtures inject a proc tree. Real measurements must resolve PID
        # namespaces before opening /proc/N: N may belong to an unrelated process.
        if self.proc!=Path('/proc'):return self.proc/str(pid)
        if pid==os.getpid():return self.proc/'self'
        parent=(self.proc/'self/stat').read_text().split(' ',1)[0]
        def owned(path):
            try:
                fields=dict(line.split(':',1) for line in (path/'status').read_text().splitlines() if ':' in line)
                ids=fields.get('NSpid',path.name).split()
                return fields.get('PPid','').strip()==parent and ids[-1:]==[str(pid)]
            except OSError:return False
        cached=self.paths.get(pid)
        if cached and owned(cached):return cached
        direct=self.proc/str(pid)
        if owned(direct):self.paths[pid]=direct;return direct
        # Only status identity fields are inspected; never cmdline/environ.
        for path in self.proc.iterdir():
            if path.name.isdigit() and owned(path):self.paths[pid]=path;return path
        raise ProcessLookupError('owned child not visible in proc filesystem')

    def sample(self,at=None):
        now=time.monotonic() if at is None else at
        rows=[]
        for name,pid in self.pids.items():
            try:
                path=self.process_path(pid)
                current=parse_stat((path/'stat').read_text(),self.hz,self.page_size)
                old=self.previous.get(name)
                cpu=None
                if old and old['start_ticks']==current['start_ticks'] and now>old['at']:
                    delta=current['cpu_seconds']-old['cpu_seconds']
                    if delta>=0:cpu=100*delta/(now-old['at'])
                self.previous[name]={**current,'at':now}
                rows.append({'name':name,'pid':pid,'available':True,'proc_path':str(path),'cpu_percent_one_core':cpu,**current})
            except (OSError,ValueError,IndexError):
                rows.append({'name':name,'pid':pid,'available':False,'cpu_percent_one_core':None,'rss_bytes':None})
                self.previous.pop(name,None)
        available=[r for r in rows if r['available']]
        return {'observed_at_ms':int(time.time()*1000),'monotonic_s':now,'processes':rows,
                'available_count':len(available),'expected_count':len(rows),
                'rss_sum_bytes':sum(r['rss_bytes'] for r in available) if len(available)==len(rows) else None,
                'cpu_sum_percent_one_core':sum(r['cpu_percent_one_core'] for r in available)
                 if len(available)==len(rows) and all(r['cpu_percent_one_core'] is not None for r in available) else None}


def summarize(samples):
    return {'sample_count':len(samples),'status':'measured' if any(s['cpu_sum_percent_one_core'] is not None for s in samples) and any(s['rss_sum_bytes'] is not None for s in samples) else 'unavailable',
            'cpu_sum_percent_one_core':distribution(s['cpu_sum_percent_one_core'] for s in samples),
            'rss_sum_bytes':distribution(s['rss_sum_bytes'] for s in samples),
            'note':'Sum of owned PIDs only. RSS can double-count shared pages. CPU is percent of one logical core; sum may exceed 100.'}


def environment():
    data={'python':platform.python_version(),'platform':platform.platform(),'logical_cpus_visible':os.cpu_count(),
          'cpu_affinity_count':len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None}
    for name,path in [('cgroup_memory_max','/sys/fs/cgroup/memory.max'),('cgroup_cpu_max','/sys/fs/cgroup/cpu.max')]:
        try:data[name]=Path(path).read_text().strip()
        except OSError:data[name]=None
    try:
        data['cpu_model']=next(line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name'))
    except (OSError,StopIteration):data['cpu_model']=None
    data['dependency_versions']={}
    for name in ('fastapi','httpx','kafka-python','pydantic','uvicorn'):
        try:data['dependency_versions'][name]=version(name)
        except PackageNotFoundError:data['dependency_versions'][name]=None
    data['hardware_id']=hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    return data
