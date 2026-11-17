from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey,Boolean,UniqueConstraint
from src.config.database import Base
class Run(Base):
 __tablename__='runs'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 batch_id=Column(Integer,ForeignKey('batches.id'),nullable=False)
 mapping_id=Column(Integer,ForeignKey('mappings.id'),nullable=False)
 status=Column(String(20),nullable=False,default='queued')
 cancel_requested=Column(Boolean,nullable=False,default=False)
 snapshot=Column(JSON,nullable=False)
 progress=Column(JSON,nullable=True)
 result=Column(JSON,nullable=True)
 recovery=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 # 可靠派发：通知时间与执行者租约，扫描据此补发与识别失联执行者
 notified_at=Column(DateTime,nullable=True)
 lease_owner=Column(String(120),nullable=True)
 lease_until=Column(DateTime,nullable=True)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
class TargetRow(Base):
 __tablename__='target_rows'
 __table_args__=(UniqueConstraint('run_id','key_digest'),)
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,ForeignKey('runs.id'),nullable=False)
 ordinal=Column(Integer,nullable=False)
 key_digest=Column(String(64),nullable=False)
 payload=Column(JSON,nullable=False)
class Acceptance(Base):
 """可靠受理：提交即落库，受理号幂等；批次内序号约束同批次受理顺序。"""
 __tablename__='acceptances'
 __table_args__=(UniqueConstraint('batch_id','seq'),)
 id=Column(Integer,primary_key=True)
 acceptance_no=Column(String(80),unique=True,nullable=False)
 name=Column(String(120),unique=True,nullable=False)
 batch_id=Column(Integer,ForeignKey('batches.id'),nullable=False)
 mapping_id=Column(Integer,ForeignKey('mappings.id'),nullable=False)
 seq=Column(Integer,nullable=False)
 status=Column(String(20),nullable=False,default='pending')
 wait_reason=Column(String(60),nullable=True)
 request_digest=Column(String(64),nullable=False)
 run_id=Column(Integer,ForeignKey('runs.id'),nullable=True)
 cancel_requested=Column(Boolean,nullable=False,default=False)
 snapshot=Column(JSON,nullable=False)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
