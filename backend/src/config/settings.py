import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://migrate:migratepass@db:5432/migrationcheck')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
# Redis 仅作通知流，受理与运行的事实来源始终是 PostgreSQL
QUEUE_KEY=os.getenv('QUEUE_KEY','migration-runs')
WORKER_LEASE_SECONDS=int(os.getenv('WORKER_LEASE_SECONDS','30'))
SWEEP_INTERVAL=float(os.getenv('SWEEP_INTERVAL','5'))
REDELIVER_AFTER=float(os.getenv('REDELIVER_AFTER','15'))
