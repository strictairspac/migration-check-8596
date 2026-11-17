import os,time,sys
from src.config.database import Session
from src.models.run import Run,TargetRow,Acceptance
from src.models.mapping import Mapping
from src.services import acceptances
class In:
 def __init__(self,no,name,mid):self.acceptance_no=no;self.name=name;self.mapping_id=mid
with Session() as s:mid=s.query(Mapping).first().id
a=acceptances.create(In('ACC-SMOKE-001','smoke-运行-001',mid))
print('accepted',a['id'],a['acceptance_no'],a['seq'],a['status'],a['wait_reason'])
# 同受理号重放
a2=acceptances.create(In('ACC-SMOKE-001','smoke-运行-001',mid))
assert a2['id']==a['id'], '重放不得新增受理'
print('replay idempotent ok')
# 同批次第二个受理，验证等待原因
b=acceptances.create(In('ACC-SMOKE-002','smoke-运行-002',mid))
for _ in range(30):
 time.sleep(0.5)
 with Session() as s:
  r1=s.get(Run,a['run_id']);r2=s.get(Run,b['run_id'])
  a2b=s.get(Acceptance,b['id'])
  if r1.status=='completed':break
print('run1',r1.status,'run2',r2.status,'acc2 wait:',a2b.wait_reason)
for _ in range(40):
 time.sleep(0.5)
 with Session() as s:
  r2=s.get(Run,b['run_id'])
  if r2.status in ('completed','failed','cancelled'):break
with Session() as s:
 r1=s.get(Run,a['run_id']);r2=s.get(Run,b['run_id'])
 t1=s.query(TargetRow).filter_by(run_id=r1.id).count()
 t2=s.query(TargetRow).filter_by(run_id=r2.id).count()
 print('final',r1.status,t1,'|',r2.status,t2)
 print('checksum match:',r1.result['checksum'] if r1.result else None)
assert r1.status=='completed' and r2.status=='completed'
print('SMOKE OK')
