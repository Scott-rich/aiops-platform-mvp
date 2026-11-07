import asyncio
import io
import json
import logging
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api_runtime import RequestBoundary, install_boundary
from app.llm_config import LLMConfig
from app.runtime_config import RuntimeConfig
from app.runtime_logging import SafeFormatter
from scripts.preflight import inspect_config
from scripts.local_platform import platform_environment,simulator_environment
from pathlib import Path

TOKENS={'API_AUTH_MODE':'token','API_READ_TOKEN':'r'*32,'API_CONTROL_TOKEN':'c'*32,'TELEMETRY_INGEST_TOKEN':'i'*32}

class ConfigTests(unittest.TestCase):
    def test_graceful_signal_exit_requires_completed_lifespan(self):
        from scripts.verify_runtime_http import graceful_exit
        lines='\n'.join(json.dumps({'logger':logger,'message':message}) for logger,message in
            [('app.local_api','local_pipeline_stopped'),('uvicorn.error','Application shutdown complete.')])
        self.assertTrue(graceful_exit(-15,lines))
        self.assertFalse(graceful_exit(-9,lines))
        self.assertFalse(graceful_exit(0,''))
        self.assertFalse(graceful_exit(-15,'Application shutdown complete.'))

    def test_model_config_does_not_mutate_environment(self):
        before=dict(os.environ)
        c=LLMConfig.from_env({'LLM_MODEL':'fixture','LLM_BASE_URL':'http://localhost/v1'})
        self.assertTrue(c.configured);self.assertEqual(before,dict(os.environ))
        inspect_config({},'/nonexistent')
        self.assertEqual(before,dict(os.environ))

    def test_runtime_invalids_rejected_without_echo(self):
        for key,value in [('LOG_LEVEL','raw-secret'),('API_AUTH_MODE','raw-secret'),('LOG_FORMAT','raw-secret'),
                          ('DB_TIMEOUT_SECONDS','nan'),('EVIDENCE_OPERATION_TIMEOUT_SECONDS','121'),
                          ('EVIDENCE_RETENTION_DAYS','6'),('FAST_STATE_MAX_KEYS','0'),('API_BODY_TIMEOUT_SECONDS','nan')]:
            with self.subTest(key=key),self.assertRaises(ValueError) as cm:RuntimeConfig.from_env({key:value})
            self.assertNotIn('raw-secret',str(cm.exception))

    def test_token_configuration_complete_distinct_and_private(self):
        self.assertNotIn('r'*32,repr(RuntimeConfig.from_env(TOKENS)))
        for changed in [{'API_CONTROL_TOKEN':''},{'API_CONTROL_TOKEN':'r'*32},{'API_READ_TOKEN':'short'},{'TELEMETRY_INGEST_TOKEN':'x\n'*32}]:
            with self.subTest(changed=list(changed)),self.assertRaises(ValueError):RuntimeConfig.from_env({**TOKENS,**changed})

    def test_launcher_loads_security_and_simulators_only_get_ingest(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'settings';p.write_text('\n'.join(k+'='+v for k,v in TOKENS.items()))
            env=platform_environment({},p)
            self.assertEqual({k:env[k] for k in TOKENS},TOKENS)
            child=simulator_environment(env)
            self.assertEqual(child['TELEMETRY_INGEST_TOKEN'],'i'*32)
            self.assertNotIn('API_READ_TOKEN',child);self.assertNotIn('API_CONTROL_TOKEN',child)

    def test_logs_are_json_and_no_exception_secrets(self):
        record=logging.LogRecord('example',40,'test',1,'value=%s url=http://user:password@host/path?key=unlisted',['secret-value'],None)
        with patch.dict(os.environ,{'LLM_API_KEY':'secret-value'}):
            text=SafeFormatter().format(record)
        result=json.loads(text)
        for value in ('secret-value','password','unlisted'):self.assertNotIn(value,text)
        self.assertEqual(result['level'],'ERROR')
        try:raise ValueError('exception-secret')
        except ValueError:
            import sys
            record.exc_info=sys.exc_info()
        self.assertNotIn('exception-secret',SafeFormatter().format(record))
        self.assertIn('ValueError',SafeFormatter().format(record))

class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def dispatch(self,method='GET',path='/reports',headers=(),chunks=None,env=None,delay=0):
        called=[];sent=[];reads=[]
        async def endpoint(scope,receive,send):
            called.append(True)
            if method=='POST':await receive()
            await send({'type':'http.response.start','status':204,'headers':[]})
            await send({'type':'http.response.body','body':b''})
        pending=list(chunks or [{'type':'http.request','body':b'{}','more_body':False}])
        async def receive():
            reads.append(True)
            if delay:await asyncio.sleep(delay)
            return pending.pop(0) if pending else {'type':'http.disconnect'}
        async def send(x):sent.append(x)
        scope={'type':'http','method':method,'path':path,'scheme':'http','headers':[(b'host',b'localhost'),*headers]}
        with patch.dict(os.environ,env or {},clear=True):await RequestBoundary(endpoint)(scope,receive,send)
        status=next((x['status'] for x in sent if x['type']=='http.response.start'),None)
        return status,called,sent,reads

    async def test_roles_and_anonymous_liveness(self):
        for role,method,path,expected in [('read','GET','/reports',204),('read','POST','/faults/payment',403),
            ('control','POST','/faults/payment',204),('ingest','POST','/demo/ingest',204),('ingest','GET','/reports',403),
            ('bad','GET','/reports',401)]:
            key={'read':'r','control':'c','ingest':'i','bad':'x'}[role]*32
            status,*_=await self.dispatch(method,path,[(b'authorization',('Bearer '+key).encode())],env=TOKENS)
            self.assertEqual(status,expected)
        self.assertEqual((await self.dispatch(path='/live',env=TOKENS))[0],204)

    async def test_invalid_auth_configuration_fails_closed(self):
        status,called,*_=await self.dispatch(env={'API_AUTH_MODE':'token'})
        self.assertEqual(status,503);self.assertFalse(called)

    async def test_duplicate_authorization_is_rejected(self):
        hdr=(b'authorization',('Bearer '+'c'*32).encode())
        self.assertEqual((await self.dispatch(headers=[hdr,hdr],env=TOKENS))[0],401)

    async def test_cross_origin_and_null_origin_write_denied(self):
        for origin,expected in [(b'http://evil.test',403),(b'null',403),(b'http://localhost:80',204)]:
            self.assertEqual((await self.dispatch('POST','/faults/payment',[(b'origin',origin)]))[0],expected)

    async def test_declared_oversize_rejected_before_read_or_endpoint(self):
        status,called,_,reads=await self.dispatch('POST',headers=[(b'content-length',b'1000001')])
        self.assertEqual(status,413);self.assertFalse(called);self.assertFalse(reads)

    async def test_chunked_oversize_stops_and_never_mutates(self):
        chunks=[{'type':'http.request','body':b'x'*500001,'more_body':True} for _ in range(3)]
        status,called,_,reads=await self.dispatch('POST',chunks=chunks)
        self.assertEqual(status,413);self.assertFalse(called);self.assertEqual(len(reads),2)

    async def test_body_timeout_never_mutates(self):
        status,called,*_=await self.dispatch('POST',env={'API_BODY_TIMEOUT_SECONDS':'.05'},delay=.2)
        self.assertEqual(status,408);self.assertFalse(called)

    async def test_bad_content_length(self):
        for value in (b'bad',b'-1'):
            self.assertEqual((await self.dispatch('POST',headers=[(b'content-length',value)]))[0],400)

    async def test_request_ids_generated_not_trusted(self):
        _,_,sent,_=await self.dispatch(headers=[(b'x-request-id',b'untrusted')])
        hdr=dict(sent[0]['headers']);self.assertEqual(len(hdr[b'x-request-id']),32)

class ReadinessAndErrors(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        env=patch.dict(os.environ,{'DATA_DIR':temp.name,'API_AUTH_MODE':'local','LLM_BASE_URL':'','LLM_MODEL':''})
        env.start();self.addCleanup(env.stop)

    def test_readiness_requires_current_success(self):
        from app.api import ready
        health={'running':True,'last_ok':True,'last_success':time.monotonic()}
        request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(local_pipeline_health=health)))
        with patch('app.api.storage',return_value=Mock()),patch('app.readiness.dependencies',return_value={'execution_mode':'local-http-python-rules','ready':True}):
            self.assertEqual(ready(request).status_code,200)
            health['last_ok']=False;self.assertEqual(ready(request).status_code,503)
            health['last_ok']=True;health['last_success']=0;self.assertEqual(ready(request).status_code,503)
            health['last_success']=time.monotonic();health['running']=False;self.assertEqual(ready(request).status_code,503)

    def test_liveness_does_not_access_database(self):
        from app.api import app
        with patch('app.api.storage',side_effect=AssertionError('must not access')):
            with TestClient(app) as client:
                self.assertEqual(client.get('/live').status_code,200)

    def test_422_does_not_echo_bad_input_and_keeps_detail(self):
        from app.api import app
        with TestClient(app) as client:
            response=client.post('/faults/payment',json={'kind':'do-not-echo-secret'})
            self.assertEqual(response.status_code,422)
            self.assertNotIn('do-not-echo-secret',response.text)
            self.assertIsInstance(response.json()['detail'],list)
            self.assertEqual(response.headers['x-request-id'],response.json()['request_id'])

if __name__=='__main__':unittest.main()
