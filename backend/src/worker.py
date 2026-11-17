import time,logging,uuid,os
from pathlib import Path
import redis
from src.config.settings import REDIS_URL,QUEUE_KEY,SWEEP_INTERVAL,WORKER_LEASE_SECONDS
from src.config.database import Session
# 显式导入全部模型模块，确保 SQLAlchemy metadata 注册 batches/mappings/runs 等表及外键
from src.models import batch as _b, mapping as _m, run as _r
_ = (_b, _m, _r)
from src.models.run import Run
from src.services.migration import migrate,make_backup,Cancelled
from src.services import dispatcher
from src.services.audit import log
from src.utils.timeutil import utcnow
from datetime import timedelta
PROGRESS_DELAY=float(os.getenv('MIGRATE_PROGRESS_DELAY','0'))
HEARTBEAT_FILE=os.getenv('WORKER_HEARTBEAT_FILE','/tmp/worker-heartbeat')
q=redis.Redis.from_url(REDIS_URL)
WORKER_ID=uuid.uuid4().hex[:12]
def heartbeat():
 Path(HEARTBEAT_FILE).touch()
def progress(run_id):
 def _cb(info):
  heartbeat()
  if PROGRESS_DELAY:time.sleep(PROGRESS_DELAY)
  with Session() as s:
   r=s.query(Run).filter_by(id=run_id).with_for_update().first()
   if not r or r.cancel_requested or r.status!='running':raise Cancelled()
   r.progress=info
   r.lease_until=utcnow()+timedelta(seconds=WORKER_LEASE_SECONDS)
   s.commit()
 return _cb
def handle(run_id):
 snapshot=dispatcher.claim(run_id,WORKER_ID)
 if snapshot is None:
  logging.info('消息忽略（重复/乱序/已取消/被其他执行者认领）: run=%s',run_id)
  return
 try:
  result=migrate(snapshot['batch']['rows'],snapshot['mapping']['rules'],snapshot['mapping']['chunk_size'],progress(run_id))
  result['backup']=make_backup(result,run_id,snapshot['batch']['name'])
  ok=dispatcher.finish(run_id,'completed',result=result,target_rows=result['accepted_rows'])
  if ok:log('worker:'+WORKER_ID,'run.completed',str(run_id))
 except Cancelled:
  if dispatcher.finish(run_id,'cancelled',error='用户取消，未提交目标行'):log('worker:'+WORKER_ID,'run.cancelled',str(run_id))
 except Exception as exc:
  logging.exception('migration failed')
  dispatcher.finish(run_id,'failed',error=str(exc)[:1000])
def main():
 last_sweep=0.0
 while True:
  heartbeat()
  now=time.monotonic()
  if now-last_sweep>=SWEEP_INTERVAL:
   last_sweep=now
   try:
    for rid in dispatcher.reap_expired_leases():
     logging.error('租约过期判失败: run=%s',rid);log('worker:'+WORKER_ID,'run.lease_expired',str(rid))
    dispatcher.reconcile()
   except Exception:logging.exception('reconcile loop')
  try:
   item=q.blpop(QUEUE_KEY,timeout=2)
   if item:handle(int(item[1]))
  except redis.RedisError:
   time.sleep(SWEEP_INTERVAL)  # 通知流断开：扫描继续兜底，恢复后自动派发
  except Exception:logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':main()
