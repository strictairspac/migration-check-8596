from fastapi import HTTPException
from src.config.database import Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,Acceptance
from src.services.migration import digest
from src.services import queue as queue_svc
from src.utils.serialization import record
def list_items():
 with Session() as s:
  rows=s.query(Acceptance).order_by(Acceptance.id.desc()).limit(100).all()
  return [record(a) for a in rows]
def detail(item_id):
 with Session() as s:
  a=s.get(Acceptance,item_id)
  if not a:raise HTTPException(404,'未找到受理记录')
  return record(a)
def create(data):
 """受理即落库：同一受理号重放且内容一致返回原受理不新增运行；内容变化一律 422 拒绝。
 Redis 不可用不影响受理，扫描线程恢复后自动派发。"""
 with Session() as s:
  m=s.get(Mapping,data.mapping_id)
  if not m:raise HTTPException(404,'未找到字段映射')
  b=s.get(Batch,m.batch_id)
  if not b:raise HTTPException(404,'未找到源批次')
  existing=s.query(Acceptance).filter_by(acceptance_no=data.acceptance_no).with_for_update().first()
  snapshot={'batch':record(b),'mapping':record(m)}
  request_digest=digest({'name':data.name,'acceptance_no':data.acceptance_no,'batch_id':b.id,'mapping_id':m.id,'snapshot':snapshot})
  if existing:
   if existing.request_digest!=request_digest:
    raise HTTPException(422,'受理号已存在但提交内容（名称/批次/映射快照）不一致，拒绝重复受理')
   return record(existing)
  if s.query(Acceptance).filter_by(name=data.name).first():
   raise HTTPException(409,'受理名称已存在，请更换名称')
  if s.query(Run).filter_by(name=data.name).first():
   raise HTTPException(409,'运行名称已存在，请更换名称')
  last=s.query(Acceptance).filter_by(batch_id=b.id).with_for_update().all()
  seq=(max((x.seq for x in last),default=0)+1)
  run=Run(name=data.name,batch_id=b.id,mapping_id=m.id,status='queued',snapshot={'batch':snapshot['batch'],'mapping':snapshot['mapping']})
  s.add(run);s.flush()
  acc=Acceptance(acceptance_no=data.acceptance_no,name=data.name,batch_id=b.id,mapping_id=m.id,seq=seq,status='pending',wait_reason='newly_accepted',request_digest=request_digest,run_id=run.id,snapshot=snapshot)
  s.add(acc);s.commit();s.refresh(acc);out=record(acc);run_id=run.id
 # 事务提交后再尽力通知；通知失败或进程在此刻停机都不影响受理，扫描线程恢复后自动派发
 queue_svc.publish(run_id)
 return out
def cancel(item_id):
 """取消受理：待派发直接终结；迟到消息因运行非 queued 不会启动迁移。锁顺序 Run → Acceptance。"""
 with Session() as s:
  a=s.query(Acceptance).filter_by(id=item_id).with_for_update().first()
  if not a:raise HTTPException(404,'未找到受理记录')
  r=s.query(Run).filter_by(id=a.run_id).with_for_update().first()
  if r is None or r.status in ('completed','failed','cancelled'):
   raise HTTPException(409,'运行已终结，无法取消')
  a.cancel_requested=True
  r.cancel_requested=True
  if r.status=='queued':
   r.status='cancelled';r.error='派发前取消，未启动迁移'
   a.status='cancelled';a.wait_reason=None
  s.commit();return record(a)
