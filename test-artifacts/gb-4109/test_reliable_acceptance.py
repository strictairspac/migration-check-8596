"""GB-4109 可靠受理与派发验证套件。

前置：本地 PostgreSQL（unix socket /tmp:17748）与 Redis（:16379），环境变量见 conftest。
覆盖：保存后未入队停机、消息重复、消息乱序、多执行者争抢、取消后迟到消息、
同批次顺序/跨批次并发、租约过期、受理幂等与内容防重放、目标行/凭据唯一、逻辑备份恢复。
"""
import os,subprocess,time,sys,uuid,signal
import pytest
from fastapi.testclient import TestClient
from src.config.database import Session
from src.models.identity import User
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow,Acceptance
from src.services import acceptances,dispatcher
from src.services import queue as queue_svc
from src.services.migration import digest,validate_backup
from src.utils.security import hash_password

VENV='/workspace/test-artifacts/gb-4109/venv/bin/python'
BACKEND='/workspace/backend'
BASE_ENV=dict(os.environ,PYTHONPATH=BACKEND,SWEEP_INTERVAL='0.5',REDELIVER_AFTER='1',WORKER_LEASE_SECONDS='2')

class In:
 def __init__(self,no,name,mid):self.acceptance_no=no;self.name=name;self.mapping_id=mid

@pytest.fixture(scope='session')
def client():
 from src.main import app
 with TestClient(app) as c:yield c
@pytest.fixture(scope='session')
def token(client):
 with Session() as s:
  u=s.query(User).filter_by(role='operator').first();uid=u.id
 r=client.post('/auth/login',json={'email':'operator@migration.local','password':'Migrate2026!'})
 assert r.status_code==200,r.text
 return r.json()['token']

@pytest.fixture(autouse=True)
def clean():
 # 每个用例独立队列键，避免上一例残留消息被本例的执行者误领
 key='q-'+uuid.uuid4().hex
 queue_svc.QUEUE_KEY=key
 from src.config import settings
 settings.QUEUE_KEY=key
 from src import worker as worker_mod
 worker_mod.QUEUE_KEY=key
 BASE_ENV['QUEUE_KEY']=key
 with Session() as s:
  s.query(TargetRow).delete();s.query(Acceptance).delete();s.query(Run).delete()
  s.commit()
 yield
 queue_svc.queue.delete(key)

@pytest.fixture()
def mapping():
 with Session() as s:
  b=Batch(name='B-'+uuid.uuid4().hex[:10],source_file='t.json',rows=[
   {'委托号':'JS-T-01','检测项':'拉伸强度','结果':'420.1','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
   {'委托号':'JS-T-02','检测项':'拉伸强度','结果':'421.2','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
   {'委托号':'JS-T-03','检测项':'弯曲强度','结果':'bad','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'}])
  s.add(b);s.flush()
  rules=[{'source':a,'target':t,'type':k,'required':True} for a,t,k in
   [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),('单位','unit','text'),('时间','measured_at','datetime')]]
  m=Mapping(name='M-'+uuid.uuid4().hex[:10],batch_id=b.id,rules=rules,chunk_size=1)
  s.add(m);s.commit();return m.id,b.id

def wait_status(run_id,targets,timeout=40,workers=()):
 deadline=time.time()+timeout
 while time.time()<deadline:
  with Session() as s:
   r=s.get(Run,run_id)
   if r.status in targets:return r.status
  time.sleep(0.25)
 detail='\n'.join(w.tail() for w in workers)
 raise AssertionError(f'run {run_id} 未进入 {targets}\nworker 日志:\n{detail}')

def wait_acceptance(acc_id,targets,timeout=40):
 deadline=time.time()+timeout
 while time.time()<deadline:
  with Session() as s:
   a=s.get(Acceptance,acc_id)
   if a.status in targets:return a.status
  time.sleep(0.25)
 raise AssertionError(f'acceptance {acc_id} 未进入 {targets}')

class Worker:
 def __init__(self,**extra):
  self.env=dict(BASE_ENV);self.env.update({k:str(v) for k,v in extra.items()})
  self.hb=f'/tmp/gb4109-hb-{uuid.uuid4().hex[:8]}'
  self.log=f'/tmp/gb4109-worker-{uuid.uuid4().hex[:6]}.log'
  self.env['WORKER_HEARTBEAT_FILE']=self.hb
  self.proc=subprocess.Popen([VENV,'-m','src.worker'],cwd=BACKEND,env=self.env,
   stdout=open(self.log,'w'),stderr=subprocess.STDOUT,start_new_session=True)
  deadline=time.time()+10
  while time.time()<deadline:
   if os.path.exists(self.hb):break
   if self.proc.poll() is not None:
    raise RuntimeError('worker 启动失败: '+open(self.log).read())
   time.sleep(0.1)
  else:raise RuntimeError('worker 心跳超时: '+open(self.log).read())
 def stop(self):
  try:os.killpg(os.getpgid(self.proc.pid),signal.SIGTERM)
  except ProcessLookupError:pass
  try:self.proc.wait(timeout=5)
  except subprocess.TimeoutExpired:os.killpg(os.getpgid(self.proc.pid),signal.SIGKILL)
  finally:
   try:os.remove(self.hb)
   except OSError:pass
 def tail(self):
  try:return open(self.log).read()[-2000:]
  except OSError:return ''

# ---------- 1. 保存后未入队即停机：受理保留，恢复后自动派发 ----------
def test_persist_before_enqueue_then_auto_dispatch(mapping):
 mid,_=mapping
 # 队列通知完全不可用：受理仍成功
 old=queue_svc.publish
 queue_svc.publish=lambda rid:False
 try:
  a=acceptances.create(In('ACC-DOWN-'+uuid.uuid4().hex[:8],'r-down-'+uuid.uuid4().hex[:8],mid))
 finally:queue_svc.publish=old
 with Session() as s:
  r=s.get(Run,a['run_id'])
  assert r.status=='queued' and r.notified_at is None
  assert s.get(Acceptance,a['id']).status=='pending'
 assert queue_svc.queue.llen(queue_svc.QUEUE_KEY)==0
 w=Worker()
 try:
  assert wait_status(a['run_id'],('completed','failed'),workers=(w,))=='completed'
 finally:w.stop()
 with Session() as s:
  assert s.query(TargetRow).filter_by(run_id=a['run_id']).count()==2

# ---------- 2. 同受理号重试不新增运行；换内容拒绝 ----------
def test_idempotent_acceptance_and_content_conflict(mapping,token,client):
 mid,bid=mapping;no='ACC-IDEM-'+uuid.uuid4().hex[:8];name='r-idem-'+uuid.uuid4().hex[:8]
 a1=acceptances.create(In(no,name,mid))
 a2=acceptances.create(In(no,name,mid))
 assert a1['id']==a2['id'] and a1['run_id']==a2['run_id']
 with Session() as s:assert s.query(Acceptance).filter_by(acceptance_no=no).count()==1
 # 换内容（改名称）→ 拒绝
 with pytest.raises(Exception) as ei:
  acceptances.create(In(no,name+'-changed',mid))
 assert getattr(ei.value,'status_code',None)==422
 # API 层同样保证幂等
 h={'Authorization':'Bearer '+token}
 r=client.post('/acceptances',headers=h,json={'acceptance_no':no,'name':name,'mapping_id':mid})
 assert r.status_code==200 and r.json()['id']==a1['id']
 r=client.post('/acceptances',headers=h,json={'acceptance_no':no,'name':name+'-x','mapping_id':mid})
 assert r.status_code==422

# ---------- 3. 消息重复：同一 run_id 多次入队只跑一次，目标行/凭据一份 ----------
def test_duplicate_messages_single_execution(mapping):
 mid,_=mapping
 a=acceptances.create(In('ACC-DUP-'+uuid.uuid4().hex[:8],'r-dup-'+uuid.uuid4().hex[:8],mid))
 queue_svc.queue.delete(queue_svc.QUEUE_KEY)  # 去掉正常通知，手工灌重复消息
 for _ in range(5):queue_svc.queue.rpush(queue_svc.QUEUE_KEY,a['run_id'])
 w=Worker()
 try:wait_status(a['run_id'],('completed',))
 finally:w.stop()
 with Session() as s:
  r=s.get(Run,a['run_id'])
  rows=s.query(TargetRow).filter_by(run_id=a['run_id']).all()
  keys=[x.key_digest for x in rows]
  assert len(rows)==2 and len(keys)==len(set(keys))
  assert r.result['accepted_count']==2
  assert digest([x.payload for x in rows])==r.result['checksum']

# ---------- 4. 消息乱序：同批次仍严格按受理顺序 ----------
def test_out_of_order_messages_batch_ordering(mapping):
 mid,bid=mapping
 ids=[]
 for i in range(3):
  a=acceptances.create(In(f'ACC-ORD-{uuid.uuid4().hex[:6]}-{i}',f'r-ord-{uuid.uuid4().hex[:6]}-{i}',mid));ids.append(a['run_id'])
 queue_svc.queue.delete(queue_svc.QUEUE_KEY)
 for rid in reversed(ids):queue_svc.queue.rpush(queue_svc.QUEUE_KEY,rid)  # 逆序灌入
 started=[];completed=[]
 w=Worker(MIGRATE_PROGRESS_DELAY='0.4')
 try:
  deadline=time.time()+45
  while time.time()<deadline and len(completed)<3:
   with Session() as s:
    for rid in ids:
     st=s.get(Run,rid).status
     if st in ('running','completed','failed','cancelled') and rid not in started:started.append(rid)
     if st in ('completed','failed','cancelled') and rid not in completed:completed.append(rid)
   time.sleep(0.1)
 finally:w.stop()
 assert len(completed)==3
 assert started==ids, f'实际启动顺序 {started}，应为受理顺序 {ids}'
 with Session() as s:
  for idx,rid in enumerate(ids):
   acc=s.query(Acceptance).filter_by(run_id=rid).one()
   assert acc.seq==idx+1

# ---------- 4b. 认领处顺序兜底：前项未终结，后项消息先到也不启动 ----------
def test_claim_blocks_until_predecessor_terminal(mapping):
 mid,_=mapping
 a1=acceptances.create(In('ACC-OB-1-'+uuid.uuid4().hex[:6],'r-ob1-'+uuid.uuid4().hex[:6],mid))
 a2=acceptances.create(In('ACC-OB-2-'+uuid.uuid4().hex[:6],'r-ob2-'+uuid.uuid4().hex[:6],mid))
 # 无工作进程：直接模拟后项消息先到的认领
 assert dispatcher.claim(a2['run_id'],'probe') is None
 with Session() as s:assert s.get(Run,a2['run_id']).status=='queued'
 assert dispatcher.claim(a1['run_id'],'probe') is not None
 dispatcher.finish(a1['run_id'],'completed',result={'accepted_count':0,'checksum':digest([]),'row_hashes':[]},target_rows=[])
 assert dispatcher.claim(a2['run_id'],'probe') is not None
 dispatcher.finish(a2['run_id'],'cancelled',error='probe')

# ---------- 5. 多执行者争抢：只有一个认领，目标行一份 ----------
def test_multi_worker_contention(mapping):
 mid,_=mapping
 a=acceptances.create(In('ACC-LOCK-'+uuid.uuid4().hex[:8],'r-lock-'+uuid.uuid4().hex[:8],mid))
 queue_svc.queue.delete(queue_svc.QUEUE_KEY)
 for _ in range(4):queue_svc.queue.rpush(queue_svc.QUEUE_KEY,a['run_id'])
 ws=[Worker() for _ in range(3)]
 try:
  wait_status(a['run_id'],('completed',),timeout=45,workers=ws)
 finally:
  for w in ws:w.stop()
 with Session() as s:
  r=s.get(Run,a['run_id'])
  assert r.lease_owner is None and r.status=='completed', ws[0].tail()
  assert s.query(TargetRow).filter_by(run_id=a['run_id']).count()==2

# ---------- 6. 不同批次并发，故障/慢批次不拖住其他批次 ----------
def test_cross_batch_concurrency_no_head_of_line_blocking(mapping):
 mid1,bid1=mapping
 with Session() as s:
  b=Batch(name='B2-'+uuid.uuid4().hex[:10],source_file='t2.json',rows=[
   {'委托号':'JS-O-01','检测项':'拉伸强度','结果':'1.0','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'}])
  s.add(b);s.flush()
  rules=[{'source':a,'target':t,'type':k,'required':True} for a,t,k in
   [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),('单位','unit','text'),('时间','measured_at','datetime')]]
  m2=Mapping(name='M2-'+uuid.uuid4().hex[:10],batch_id=b.id,rules=rules,chunk_size=1);s.add(m2);s.commit();mid2=m2.id
 # 批次1 两个受理（慢）；批次2 一个受理，应不受批次1后项阻塞
 slow1=acceptances.create(In('ACC-CB1A-'+uuid.uuid4().hex[:6],'r-cb1a-'+uuid.uuid4().hex[:6],mid1))
 slow2=acceptances.create(In('ACC-CB1B-'+uuid.uuid4().hex[:6],'r-cb1b-'+uuid.uuid4().hex[:6],mid1))
 other=acceptances.create(In('ACC-CB2-'+uuid.uuid4().hex[:6],'r-cb2-'+uuid.uuid4().hex[:6],mid2))
 # 验证批次1第二项在第一项未完成时处于等待
 dispatcher.reconcile()
 with Session() as s:
  assert s.get(Acceptance,slow2['id']).wait_reason=='waiting_predecessor'
  assert s.get(Acceptance,other['id']).wait_reason in ('waiting_dispatch',None,'newly_accepted')
 ws=[Worker(MIGRATE_PROGRESS_DELAY='0.6') for _ in range(2)]
 try:
  assert wait_status(other['run_id'],('completed',),timeout=30,workers=ws)=='completed'
  # 此时批次1第二项可能仍在排队——证明它没有挡住批次2
  with Session() as s:assert s.get(Run,slow2['run_id']).status in ('queued','running')
  for a in (slow1,slow2):wait_status(a['run_id'],('completed','failed'),timeout=45,workers=ws)
 finally:
  for w in ws:w.stop()

# ---------- 7. 待派发取消后，迟到消息不启动迁移 ----------
def test_cancel_then_late_message_never_starts(mapping):
 mid,_=mapping
 a=acceptances.create(In('ACC-CAN-'+uuid.uuid4().hex[:8],'r-can-'+uuid.uuid4().hex[:8],mid))
 queue_svc.queue.delete(queue_svc.QUEUE_KEY)
 acceptances.cancel(a['id'])
 with Session() as s:
  assert s.get(Run,a['run_id']).status=='cancelled'
  assert s.get(Acceptance,a['id']).status=='cancelled'
 # 迟到/重复消息
 for _ in range(3):queue_svc.queue.rpush(queue_svc.QUEUE_KEY,a['run_id'])
 w=Worker()
 try:time.sleep(3)
 finally:w.stop()
 with Session() as s:
  assert s.get(Run,a['run_id']).status=='cancelled'
  assert s.query(TargetRow).filter_by(run_id=a['run_id']).count()==0

# ---------- 8. 租约过期：执行者崩溃判失败，不自动重试、无双写 ----------
def test_lease_expiry_marks_failed_no_double_write(mapping):
 mid,_=mapping
 a=acceptances.create(In('ACC-LEASE-'+uuid.uuid4().hex[:8],'r-lease-'+uuid.uuid4().hex[:8],mid))
 queue_svc.queue.delete(queue_svc.QUEUE_KEY);queue_svc.queue.rpush(queue_svc.QUEUE_KEY,a['run_id'])
 # 慢迁移执行者持租约后在块间睡眠；第二个执行者用独立队列键，只做扫描回收、不抢消息
 slow=Worker(WORKER_LEASE_SECONDS='1',MIGRATE_PROGRESS_DELAY='2.5')
 reaper=Worker(QUEUE_KEY='reaper-'+uuid.uuid4().hex,WORKER_LEASE_SECONDS='1')
 try:
  assert wait_status(a['run_id'],('failed',),timeout=30,workers=(slow,reaper))=='failed'
 finally:
  slow.stop();reaper.stop()
 with Session() as s:
  r=s.get(Run,a['run_id'])
  assert '租约过期' in (r.error or '')
  assert r.lease_owner is None
  assert s.query(TargetRow).filter_by(run_id=a['run_id']).count()==0
 # 迟到的原消息再投递也不会复活
 queue_svc.queue.rpush(queue_svc.QUEUE_KEY,a['run_id'])
 w2=Worker()
 try:time.sleep(3)
 finally:w2.stop()
 with Session() as s:assert s.get(Run,a['run_id']).status=='failed'

# ---------- 9. 完成凭据与目标行只落一份；备份恢复照常 ----------
def test_single_proof_and_backup_restore_roundtrip(mapping):
 mid,_=mapping
 a=acceptances.create(In('ACC-BK-'+uuid.uuid4().hex[:8],'r-bk-'+uuid.uuid4().hex[:8],mid))
 w=Worker()
 try:wait_status(a['run_id'],('completed',))
 finally:w.stop()
 with Session() as s:
  r=s.get(Run,a['run_id']);rid=r.id
  rows=s.query(TargetRow).filter_by(run_id=rid).order_by(TargetRow.ordinal).all()
  payloads=[x.payload for x in rows]
  assert len(rows)==r.result['accepted_count']==2
  assert r.result['row_hashes']==[digest(x) for x in payloads]
  assert r.result['checksum']==digest(payloads)
  backup=r.result['backup']
 # 备份可校验并恢复
 validate_backup(backup,backup['checksum'],rid)
 class Req:pass
 req=Req();req.backup=dict(backup)
 proof=__import__('src.services.runs',fromlist=['restore']).restore(rid,req)
 assert proof['verified'] and proof['restored_count']==2
 # 篡改备份被拒
 bad=dict(backup);bad['rows']=[dict(backup['rows'][0])];bad['rows'][0]['result_value']='999.000000'
 with pytest.raises(Exception) as ei:
  rq=Req();rq.backup=bad;__import__('src.services.runs',fromlist=['restore']).restore(rid,rq)
 assert getattr(ei.value,'status_code',None)==422

# ---------- 10. 页面数据：受理、等待原因、完成状态可核对 ----------
def test_acceptance_visibility_fields(mapping):
 mid,_=mapping
 a1=acceptances.create(In('ACC-VIS-A-'+uuid.uuid4().hex[:6],'r-vis-a-'+uuid.uuid4().hex[:6],mid))
 a2=acceptances.create(In('ACC-VIS-B-'+uuid.uuid4().hex[:6],'r-vis-b-'+uuid.uuid4().hex[:6],mid))
 dispatcher.reconcile()
 items={x['id']:x for x in acceptances.list_items()}
 assert items[a1['id']]['wait_reason'] in ('waiting_dispatch','newly_accepted',None)
 assert items[a2['id']]['wait_reason']=='waiting_predecessor'
 assert items[a1['id']]['seq']==1 and items[a2['id']]['seq']==2
 w=Worker()
 try:
  wait_acceptance(a1['id'],('completed',));wait_acceptance(a2['id'],('completed',))
 finally:w.stop()
 items={x['id']:x for x in acceptances.list_items()}
 assert items[a1['id']]['status']=='completed' and items[a1['id']]['wait_reason'] is None
 assert items[a2['id']]['status']=='completed'
