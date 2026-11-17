-- Schema v2: 可靠受理与派发
-- 全新部署由 SQLAlchemy metadata 自动建表；已有部署按下述语句幂等升级。
ALTER TABLE runs ADD COLUMN IF NOT EXISTS notified_at TIMESTAMP NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(120) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NULL;
CREATE TABLE IF NOT EXISTS acceptances (
 id SERIAL PRIMARY KEY,
 acceptance_no VARCHAR(80) NOT NULL UNIQUE,
 name VARCHAR(120) NOT NULL UNIQUE,
 batch_id INTEGER NOT NULL REFERENCES batches(id),
 mapping_id INTEGER NOT NULL REFERENCES mappings(id),
 seq INTEGER NOT NULL,
 status VARCHAR(20) NOT NULL DEFAULT 'pending',
 wait_reason VARCHAR(60) NULL,
 request_digest VARCHAR(64) NOT NULL,
 run_id INTEGER NULL REFERENCES runs(id),
 cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
 snapshot JSON NOT NULL,
 created_at TIMESTAMP DEFAULT now(),
 updated_at TIMESTAMP DEFAULT now(),
 UNIQUE(batch_id,seq)
);
-- 历史 running 数据若来自崩溃进程，保持人工复核策略，不自动改写。
