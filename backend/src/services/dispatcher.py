import logging
from datetime import timedelta
from src.config.database import Session
from src.config.settings import REDELIVER_AFTER,WORKER_LEASE_SECONDS
from src.models.run import Run,TargetRow,Acceptance
from src.services.migration import key_digest
from src.services import queue as queue_svc
from src.utils.timeutil import utcnow
TERMINAL=('completed','failed','cancelled')
def _due(run,now):
 return run.notified_at is None or (now-run.notified_at).total_seconds()>=REDELIVER_AFTER
def reconcile():
 """事实来源在数据库：扫描全部待决受理，补发丢失/过期的通知，并按批次受理顺序决定等待原因。
 批次之间互不阻塞；同一批次前项未终结，后项一律 waiting_predecessor。
 统一锁顺序 Run → Acceptance，避免与认领/终局事务死锁。"""
 notified=[];now=utcnow()
 with Session() as s:
  pending=s.query(Acceptance).filter(Acceptance.status.notin_(TERMINAL)).order_by(Acceptance.batch_id,Acceptance.seq).all()
  groups={}
  for a in pending:groups.setdefault(a.batch_id,[]).append(a)
  # 先按固定顺序锁定本批涉及的运行行，再处理受理行
  run_ids=sorted({a.run_id for items in groups.values() for a in items if a.run_id is not None})
  runs_by_id={r.id:r for r in s.query(Run).filter(Run.id.in_(run_ids)).with_for_update().all()} if run_ids else {}
  for items in groups.values():
   blocked=False
   for a in items:
    r=runs_by_id.get(a.run_id)
    if r is None or r.status in TERMINAL:
     a.status=r.status if r else 'cancelled';a.wait_reason=None
     continue
    if r.status=='running':
     a.status='running';a.wait_reason=None;blocked=True;continue
    if r.cancel_requested:
     r.status='cancelled';r.error='派发前取消，未启动迁移'
     a.status='cancelled';a.wait_reason=None
     continue
    if blocked:
     a.wait_reason='waiting_predecessor';continue
    if _due(r,now):
     r.notified_at=now;notified.append(r.id)
    a.status='pending';a.wait_reason='waiting_dispatch';blocked=True
  s.commit()
 for run_id in notified:queue_svc.publish(run_id)
 return notified
def reap_expired_leases():
 """执行者崩溃后租约过期：运行判失败并留痕，不自动重试，避免双写目标行。"""
 now=utcnow();reaped=[]
 with Session() as s:
  rows=s.query(Run).filter(Run.status=='running',Run.lease_until<now).with_for_update(skip_locked=True).all()
  for r in rows:
   r.status='failed';r.error='执行者租约过期，疑似进程中断，请人工复核后新建受理'
   r.lease_owner=None;r.lease_until=None;reaped.append(r.id)
  s.commit()
 return reaped
def claim(run_id,owner):
 """多执行者争抢：行锁+SKIP LOCKED，只有一个能把 queued 改成 running。重复/迟到消息返回 None。
 同批次受理顺序在认领处兜底：前项未终结，后项即便消息先到也不得启动。"""
 now=utcnow()
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update(skip_locked=True).first()
  if not r or r.status!='queued' or r.cancel_requested:return None
  a=s.query(Acceptance).filter_by(run_id=run_id).with_for_update().first()
  if a:
   prior=s.query(Acceptance).filter(Acceptance.batch_id==a.batch_id,Acceptance.seq<a.seq).order_by(Acceptance.seq).all()
   for p in prior:
    pr=s.query(Run).filter_by(id=p.run_id).first()
    if pr is None or pr.status not in TERMINAL:return None
  r.status='running';r.lease_owner=owner;r.lease_until=now+timedelta(seconds=WORKER_LEASE_SECONDS)
  if a:a.status='running';a.wait_reason=None
  snapshot=r.snapshot;s.commit();return snapshot
def finish(run_id,status,error=None,result=None,target_rows=None):
 """终局一次提交：目标行、运行状态、受理状态在同一事务内落库，只落一份。"""
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update().first()
  if not r or r.status!='running':
   logging.warning('运行已非迁移中，放弃终局提交: run=%s status=%s',run_id,r.status if r else None)
   return False
  if target_rows:
   for i,row in enumerate(target_rows):s.add(TargetRow(run_id=run_id,ordinal=i,key_digest=key_digest(row),payload=row))
  r.status=status;r.error=error;r.result=result;r.lease_owner=None;r.lease_until=None
  a=s.query(Acceptance).filter_by(run_id=run_id).with_for_update().first()
  if a:a.status=status;a.wait_reason=None
  s.commit();return True
