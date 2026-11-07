"""Optional real GGUF CPU inference server for isolated phase-2 verification.

Requires the separately installed llama-cpp-python runtime. Never imported by
normal API/worker startup; binds loopback and serves no hard-coded diagnoses.
"""
import argparse
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import logging
from pathlib import Path
import time
import traceback
from fastapi import FastAPI,HTTPException,Request
from fastapi.responses import JSONResponse


def inference_schema(value):
    """Use short bounded strings instead of unbounded generation loops.

    Keep field names, types, required fields, nonempty bounds and observed ID
    enums. Cap free text at 160 characters and arrays at two in this reference
    model. Enum literals retain their original constraints, including long IDs.
    The full application schema still validates every returned field.
    """
    if isinstance(value,dict):
        return {k:(min(v,2) if k=='maxItems' else
                   min(v,160) if k=='maxLength' and 'enum' not in value else inference_schema(v))
                for k,v in value.items()}
    if isinstance(value,list):return [inference_schema(v) for v in value]
    return value


def sha256_file(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def create_app(model_file,model_name,threads=4,context_size=16384):
    if type(threads) is not int or not 1<=threads<=8:
        raise ValueError('CPU threads must be between 1 and 8')
    if type(context_size) is not int or not 2048<=context_size<=16384:
        raise ValueError('context size out of bounds')
    @asynccontextmanager
    async def lifespan(app):
        from llama_cpp import Llama
        import llama_cpp
        app.state.loaded=False
        app.state.backend_version=llama_cpp.__version__
        library=Path(llama_cpp.__file__).parent/'lib/libllama.so'
        app.state.library_sha256=sha256_file(library) if library.exists() else None
        app.state.sha256=sha256_file(model_file)
        app.state.llm=Llama(model_path=str(model_file),n_ctx=context_size,n_threads=threads,n_threads_batch=threads,n_batch=256,
                            n_gpu_layers=0,seed=42,use_mmap=True,verbose=False)
        app.state.lock=asyncio.Lock();app.state.loaded=True
        try:yield
        finally:
            app.state.loaded=False
            app.state.llm.close()
    app=FastAPI(title='Real local CPU model verification server',lifespan=lifespan)
    @app.get('/health')
    def health():
        return {'ready':app.state.loaded,'execution_kind':'real_gguf_cpu_inference','model':model_name,
                'model_sha256':app.state.sha256,'threads':threads,'context_size':context_size,
                'backend_version':app.state.backend_version,'runtime_library_sha256':app.state.library_sha256,
                'decoding_array_limit':2,'decoding_text_limit':160,'repeat_penalty':1.1,
                'full_schema_validation':'platform',
                'batch_threads':threads,'inference_timeout_seconds':480}
    @app.post('/v1/chat/completions')
    async def complete(request:Request):
        raw=bytearray()
        async for block in request.stream():
            if len(raw)+len(block)>262144:raise HTTPException(413,'request too large')
            raw.extend(block)
        try:
            body=json.loads(raw)
            if body.get('model')!=model_name or body.get('stream',False):raise ValueError('unsupported model or streaming')
            maximum=body.get('max_tokens',1200)
            if type(maximum)is not int or not 64<=maximum<=4096:raise ValueError('max_tokens out of range')
            messages=body['messages']
            if not isinstance(messages,list) or not 1<=len(messages)<=8:raise ValueError('invalid messages')
            if any(not isinstance(m,dict) or m.get('role') not in ('system','user','assistant') or not isinstance(m.get('content'),str) for m in messages):raise ValueError('invalid message')
            temperature=body.get('temperature',0)
            if type(temperature)not in (int,float) or not 0<=temperature<=2:raise ValueError('invalid temperature')
            response_format=body.get('response_format')
            if response_format and response_format.get('type')=='json_schema':
                response_format={'type':'json_object','schema':inference_schema(response_format['json_schema']['schema'])}
            elif response_format and response_format.get('type')!='json_object':raise ValueError('unsupported response format')
        except (ValueError,KeyError,TypeError,AttributeError):raise HTTPException(400,'invalid completion request')
        if app.state.lock.locked():
            return JSONResponse({'error':'local model busy'},status_code=429,headers={'Retry-After':'1'})
        async with app.state.lock:
            from llama_cpp import LogitsProcessorList
            started=time.monotonic();deadline=started+480;sample_steps=0
            def deadline_guard(_ids,scores):
                nonlocal sample_steps
                sample_steps+=1
                if sample_steps==1 or sample_steps%100==0:
                    logging.getLogger(__name__).info('Local inference sample_steps=%s elapsed_seconds=%.3f',sample_steps,time.monotonic()-started)
                if time.monotonic()>deadline:raise TimeoutError('local inference deadline exceeded')
                return scores
            processors=LogitsProcessorList([deadline_guard])
            try:
                response=await asyncio.to_thread(app.state.llm.create_chat_completion,messages=messages,model=model_name,
                     max_tokens=maximum,temperature=temperature,repeat_penalty=1.1,
                     response_format=response_format,logits_processor=processors)
            except Exception as exc:
                frames=[(Path(f.filename).name,f.name,f.lineno) for f in traceback.extract_tb(exc.__traceback__)][-5:]
                logging.getLogger(__name__).warning('Local inference failed type=%s frames=%s',type(exc).__name__,frames)
                raise HTTPException(400,'inference request rejected')
        return response
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-file',type=Path,required=True)
    parser.add_argument('--model-name',default='qwen2.5-1.5b-instruct-q4_k_m')
    parser.add_argument('--port',type=int,default=18888)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--context-size',type=int,default=16384)
    args=parser.parse_args()
    if not args.model_file.is_file():parser.error('model file does not exist')
    if not 1<=args.threads<=8 or not 2048<=args.context_size<=16384:parser.error('CPU/context safety bounds exceeded')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
    import uvicorn
    uvicorn.run(create_app(args.model_file,args.model_name,args.threads,args.context_size),host='127.0.0.1',port=args.port,log_level='info',limit_concurrency=4)


if __name__=='__main__':main()
