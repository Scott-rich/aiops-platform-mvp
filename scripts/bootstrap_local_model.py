"""Optional isolated, pinned CPU model runtime (Linux x86_64 AVX2, no GPU).

Needs uv, C/C++ compiler and network access. No system packages are installed.
Builds llama-cpp-python from source to avoid incompatible musl CPU wheels.
Downloads pinned Apache-2.0 Qwen weights (1.5B default, 0.5B optional).
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time
import urllib.request

REVISION='9217f5db79a29953eb74d5343926648285ec7e67'
NAME='qwen2.5-0.5b-instruct-q4_k_m.gguf'
SHA256='74a4da8c9fdbcd15bd1f6d01d621410d31c6fc00986f5eb687824e7b93d7a9db'
BYTES=491400032
MODELS={
 '0.5b':{'repository':'Qwen/Qwen2.5-0.5B-Instruct-GGUF','revision':REVISION,'name':NAME,'sha256':SHA256,'bytes':BYTES},
 '1.5b':{'repository':'Qwen/Qwen2.5-1.5B-Instruct-GGUF','revision':'91cad51170dc346986eccefdc2dd33a9da36ead9',
         'name':'qwen2.5-1.5b-instruct-q4_k_m.gguf','sha256':'6a1a2eb6d15622bf3c96857206351ba97e1af16c30d7a74ee38970e434e9407e','bytes':1117320736}}


def sha256(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def fetch_model(target,spec=None):
    spec=spec or {'repository':'Qwen/Qwen2.5-0.5B-Instruct-GGUF','revision':REVISION,'name':NAME,'sha256':SHA256,'bytes':BYTES}
    expected_size,expected_sha=spec['bytes'],spec['sha256']
    if target.exists():
        if target.stat().st_size!=expected_size or sha256(target)!=expected_sha:
            raise ValueError('existing model checksum mismatch; file preserved')
        return
    temporary=target.with_suffix('.partial')
    if temporary.exists():raise FileExistsError('partial download exists; inspect before retrying')
    url=f"https://huggingface.co/{spec['repository']}/resolve/{spec['revision']}/{spec['name']}?download=true"
    started=time.monotonic();size=0
    try:
        with urllib.request.urlopen(url,timeout=60) as response,temporary.open('xb') as stream:
            while block:=response.read(1024*1024):
                size+=len(block)
                if size>expected_size or time.monotonic()-started>1800:raise TimeoutError('model download size/time limit exceeded')
                stream.write(block)
        if size!=expected_size or sha256(temporary)!=expected_sha:raise ValueError('official pinned model checksum mismatch')
        temporary.replace(target)
    except Exception:
        # Keep partial files inspectable instead of overwriting or presenting them as weights.
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir',type=Path,default=Path('/tmp/aiops-model-runtime'))
    parser.add_argument('--model-size',choices=MODELS,default='1.5b')
    args=parser.parse_args();runtime=args.runtime_dir.resolve();runtime.mkdir(parents=True,exist_ok=True)
    spec=MODELS[args.model_size]
    manifest=runtime/'model-runtime.json'
    result={'status':'running','started_at_utc':datetime.now(timezone.utc).isoformat(),
            'model':spec['repository'],'revision':spec['revision'],'sha256':spec['sha256'],
            'license':'Apache-2.0','runtime_installation':'llama-cpp-python 0.3.19 built from source'}
    try:
        if platform.system()!='Linux' or platform.machine()!='x86_64':raise RuntimeError('tested bootstrap requires Linux x86_64')
        cpu=Path('/proc/cpuinfo').read_text().lower().split()
        if not {'avx2','fma'}<=set(cpu):raise RuntimeError('this pinned build requires AVX2 and FMA')
        if any(not shutil.which(tool) for tool in ('uv','gcc','g++')):raise RuntimeError('uv, gcc and g++ are required')
        python=runtime/'venv/bin/python'
        if not python.exists():subprocess.run(['uv','venv','--python','3.11',str(runtime/'venv')],check=True,timeout=600)
        env={**os.environ,'CMAKE_ARGS':'-DGGML_NATIVE=OFF -DGGML_AVX2=ON -DGGML_FMA=ON -DGGML_OPENMP=OFF -DLLAMA_CURL=OFF -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF -DLLAMA_BUILD_SERVER=OFF',
             'CMAKE_BUILD_PARALLEL_LEVEL':'2'}
        subprocess.run(['uv','pip','install','--python',str(python),'--no-binary','llama-cpp-python',
             'llama-cpp-python==0.3.19','fastapi==0.115.12','uvicorn==0.34.2','httpx==0.28.1','pydantic==2.14.1'],env=env,check=True,timeout=1800)
        # Import and symbol validation catches incompatible pre-existing binary installations.
        subprocess.run([str(python),'-c','from llama_cpp import Llama, LogitsProcessorList; import llama_cpp; assert llama_cpp.__version__ == "0.3.19"'],check=True,timeout=30)
        model=runtime/spec['name'];fetch_model(model,spec)
        result.update(status='prepared',python=str(python),model_file=str(model),bytes=model.stat().st_size)
        result['dependencies']=subprocess.check_output(['uv','pip','freeze','--python',str(python)],text=True,timeout=30).splitlines()
    except Exception as error:
        result.update(status='blocked',error_type=type(error).__name__)
        raise
    finally:
        result['finished_at_utc']=datetime.now(timezone.utc).isoformat()
        if manifest.exists():
            backup=manifest.with_name('model-runtime-before-'+str(time.time_ns())+'.json')
            shutil.copy2(manifest,backup)
        manifest.write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':main()
