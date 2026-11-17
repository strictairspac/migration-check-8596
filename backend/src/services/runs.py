from datetime import datetime,timezone
from fastapi import HTTPException
from src.config.database import Session
from src.models.run import Run,TargetRow,Acceptance
from src.utils.serialization import record
from src.services.migration import digest,key_digest,validate_backup
from src.services import acceptances
def list_items():
 with Session() as s:return [record(r) for r in s.query(Run).order_by(Run.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  r=s.get(Run,item_id)
  if not r:raise HTTPException(404,'未找到对账运行')
  return record(r)
def create(data):
 # 兼容旧入口：统一走可靠受理，受理号由运行名派生保证可重放
 class _A:pass
 payload=_A();payload.acceptance_no='A-'+digest(data.name)[:24];payload.name=data.name;payload.mapping_id=data.mapping_id
 return acceptances.create(payload)
def cancel(item_id):
 with Session() as s:
  a=s.query(Acceptance).filter_by(run_id=item_id).with_for_update().first()
 if not a:raise HTTPException(404,'未找到受理记录')
 return acceptances.cancel(a.id)
def targets(item_id):
 detail(item_id)
 with Session() as s:rows=[r.payload for r in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
 return {'rows':rows,'count':len(rows),'checksum':digest(rows)}
def backup(item_id):
 r=detail(item_id)
 if r['status']!='completed':raise HTTPException(409,'仅完成运行可以备份')
 return r['result']['backup']
def restore(item_id,data):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status!='completed':raise HTTPException(409,'仅完成运行可以恢复')
  try:rows=validate_backup(data.backup,r.result['checksum'],item_id)
  except ValueError as e:raise HTTPException(422,str(e))
  before=s.query(TargetRow).filter_by(run_id=item_id).count()
  s.query(TargetRow).filter_by(run_id=item_id).delete()
  for i,row in enumerate(rows):s.add(TargetRow(run_id=item_id,ordinal=i,key_digest=key_digest(row),payload=row))
  s.flush()
  actual=[x.payload for x in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
  proof={'restored_at':datetime.now(timezone.utc).isoformat(),'previous_count':before,'restored_count':len(actual),'checksum':digest(actual),'verified':digest(actual)==r.result['checksum']}
  if not proof['verified']:raise HTTPException(409,'恢复后校验失败，已回滚')
  r.recovery=(r.recovery or [])+[proof];s.commit();return proof
