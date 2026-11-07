"""Server contract tests use an explicit double; real inference is opt-in."""
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from scripts.local_llm_server import create_app,inference_schema


class LocalServerTests(unittest.TestCase):
    def test_grammar_bounds_text_and_preserves_actual_enum_literals(self):
        source={'type':'array','minItems':1,'maxItems':5,'items':{'type':'string','maxLength':3000,'enum':['actual-id']}}
        result=inference_schema(source)
        self.assertEqual(result,{'type':'array','minItems':1,'maxItems':2,'items':{'type':'string','maxLength':3000,'enum':['actual-id']}})
        self.assertIn('maxItems',source)
        self.assertEqual(inference_schema({'type':'string','minLength':1,'maxLength':3000}),
                         {'type':'string','minLength':1,'maxLength':160})

    def test_cpu_thread_bounds_are_validated_before_loading_model(self):
        for threads in (0,9,True):
            with self.subTest(threads=threads),self.assertRaises(ValueError):create_app('unused','test',threads)
        self.assertIsNotNone(create_app('unused','test',8))

    def test_real_adapter_contract_validation_and_cleanup_with_double(self):
        calls=[]
        class Double:
            def __init__(self,**kwargs):calls.append(('load',kwargs))
            def close(self):calls.append(('close',{}))
            def create_chat_completion(self,*,messages,model,max_tokens,temperature,repeat_penalty,response_format,logits_processor):
                calls.append(('completion',{'response_format':response_format,'logits_processor':logits_processor,'repeat_penalty':repeat_penalty}))
                return {'choices':[{'message':{'content':'explicit test double'}}]}
        with tempfile.TemporaryDirectory() as directory:
            model=Path(directory)/'fake.gguf';model.write_bytes(b'explicit-test-fixture')
            module=types.SimpleNamespace(Llama=Double,LogitsProcessorList=list,__version__='test-double',__file__=str(Path(directory)/'__init__.py'))
            with patch.dict(sys.modules,{'llama_cpp':module}),TestClient(create_app(model,'test')) as client:
                health=client.get('/health').json()
                self.assertTrue(health['ready']);self.assertEqual(health['decoding_text_limit'],160)
                body={'model':'test','max_tokens':100,'messages':[{'role':'user','content':'test'}],
                      'response_format':{'type':'json_schema','json_schema':{'schema':{'type':'string','maxLength':30}}}}
                self.assertEqual(client.post('/v1/chat/completions',json=body).status_code,200)
                self.assertEqual(calls[-1][1]['response_format']['schema'],{'type':'string','maxLength':30})
                self.assertEqual(calls[-1][1]['repeat_penalty'],1.1)
                for changed in ({'model':'other'},{'max_tokens':10000},{'messages':[]},{'temperature':float('inf')}):
                    response=client.post('/v1/chat/completions',content=json.dumps({**body,**changed}),headers={'Content-Type':'application/json'})
                    self.assertEqual(response.status_code,400)
                self.assertEqual(client.post('/v1/chat/completions',content='x'*262145).status_code,413)
                self.assertEqual(client.post('/v1/chat/completions',content='[]').status_code,400)
                with patch.object(client.app.state.llm,'create_chat_completion',side_effect=RuntimeError('test fixture error')):
                    self.assertEqual(client.post('/v1/chat/completions',json=body).status_code,400)
                with patch.object(client.app.state.lock,'locked',return_value=True):
                    response=client.post('/v1/chat/completions',json=body)
                    self.assertEqual(response.status_code,429);self.assertEqual(response.headers['retry-after'],'1')
            self.assertEqual(calls[-1][0],'close')

