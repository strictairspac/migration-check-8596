import os
os.environ.setdefault('JWT_SECRET','local-test-secret')
os.environ.setdefault('DATABASE_URL','postgresql+psycopg://migrate:migratepass@localhost:17748/migrationcheck?host=/tmp')
os.environ.setdefault('REDIS_URL','redis://localhost:16379/0')
os.environ.setdefault('SWEEP_INTERVAL','0.5')
os.environ.setdefault('REDELIVER_AFTER','1')
os.environ.setdefault('WORKER_LEASE_SECONDS','2')
