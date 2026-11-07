"""Own every process, bind numeric loopback, preserve logs, always reap children."""
from contextlib import asynccontextmanager
import asyncio
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid
import httpx
from scripts.verify_faults import Suite, free_port
from scripts.local_platform import simulator_environment


@asynccontextmanager
async def http_lab(output):
    suite=Suite(output)
    errors=[]
    try:
        suite.proxy_port=await suite.proxy.start()
        async with httpx.AsyncClient(timeout=6,trust_env=False) as client:
            suite.client=client
            for name in ('api','payment','inventory','order','gateway'):
                suite.spawn(name);await suite.ready(name)
            yield suite
    finally:
        for name in ('gateway','order','inventory','payment','api'):
            try:await suite.stop(name)
            except Exception as exc:errors.append(name+':'+type(exc).__name__)
        try:await suite.proxy.close()
        except Exception as exc:errors.append("proxy:"+type(exc).__name__)
        for handle in suite.handles:handle.close()
        suite.result.update(status='stopped',cleanup={'errors':errors,
            'all_processes_stopped':all(p.poll() is not None for _,p in suite.spawned),
            'proxy_tasks_remaining':len(suite.proxy.tasks)})
        suite.save()
        if errors or not suite.result['cleanup']['all_processes_stopped'] or suite.proxy.tasks:
            raise RuntimeError('HTTP benchmark cleanup incomplete')


class KafkaBroker:
    def __init__(self,home,output):
        self.home=Path(home);self.output=Path(output);self.process=self.admin=self.log=None
        self.output.mkdir(parents=True,exist_ok=False)
        self.cleanup={'broker_stopped':False}

    def start(self):
        from kafka import KafkaAdminClient
        if not (self.home/'bin/kafka-server-start.sh').is_file():raise FileNotFoundError('Kafka installation not found')
        port,controller=free_port(),free_port()
        while controller==port:controller=free_port()
        self.bootstrap=f'127.0.0.1:{port}'
        config=self.output/'kafka.properties'
        config.write_text('\n'.join(['process.roles=broker,controller','node.id=1',
            f'controller.quorum.voters=1@127.0.0.1:{controller}',
            f'listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}',
            f'advertised.listeners=PLAINTEXT://{self.bootstrap}',
            'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
            'controller.listener.names=CONTROLLER','inter.broker.listener.name=PLAINTEXT',
            f'log.dirs={self.output/"kafka-data"}','num.partitions=1','offsets.topic.replication.factor=1',
            'transaction.state.log.replication.factor=1','transaction.state.log.min.isr=1',
            'group.initial.rebalance.delay.ms=0','auto.create.topics.enable=false',
            'num.network.threads=2','num.io.threads=2','log.retention.hours=1','log.segment.bytes=16777216'])+'\n')
        env=simulator_environment(os.environ)
        env.update(KAFKA_HEAP_OPTS='-Xms128m -Xmx384m',LOG_DIR=str(self.output/'logs'))
        with (self.output/'format.log').open('w') as log:
            subprocess.run([str(self.home/'bin/kafka-storage.sh'),'format','-t',uuid.uuid4().hex[:22],'-c',str(config)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=30)
        self.log=(self.output/'broker.log').open('w')
        self.process=subprocess.Popen([str(self.home/'bin/kafka-server-start.sh'),str(config)],env=env,
                                      stdout=self.log,stderr=subprocess.STDOUT,start_new_session=True)
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            if self.process.poll() is not None:raise RuntimeError('Kafka process exited')
            try:
                self.admin=KafkaAdminClient(bootstrap_servers=self.bootstrap,request_timeout_ms=3000,api_version_auto_timeout_ms=1000)
                return self
            except Exception:time.sleep(.2)
        raise TimeoutError('Kafka startup deadline')

    def close(self):
        admin_error=None
        if self.admin:
            try:self.admin.close()
            except Exception as exc:admin_error=exc
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid,signal.SIGTERM)
            try:self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid,signal.SIGKILL);self.process.wait(timeout=3)
        if self.log:self.log.close()
        self.cleanup={'broker_stopped':self.process is None or self.process.poll() is not None}
        if admin_error:raise admin_error
