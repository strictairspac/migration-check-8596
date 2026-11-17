# GB-4109 可靠受理与派发验证

针对「队列故障下可靠受理」改造的验证套件与运行环境说明。源码不在本目录，测试通过 `PYTHONPATH` 引用 `/workspace/backend`。

## 被测保证
1. 保存后未入队即停机：受理已落库，恢复后扫描自动派发。
2. 同一受理号重试不新增运行；提交内容变化返回 422。
3. 消息重复：同一运行只执行一次，目标行与完成凭据只落一份。
4. 消息乱序：同批次严格按受理顺序启动；认领处有顺序兜底。
5. 多执行者争抢：`FOR UPDATE SKIP LOCKED` 下只有一个认领成功。
6. 不同批次并发，慢/故障批次不阻塞其他批次；同批次后项可见 `waiting_predecessor`。
7. 待派发取消后，迟到消息不会启动迁移，目标行数为 0。
8. 执行者租约过期判 failed、写审计、不自动重试、迟到消息不复活。
9. 逻辑 JSON 备份恢复往返一致；篡改/错摘要备份被 422 拒绝。
10. 受理台账可核对受理号、批次序号、等待原因与完成状态。

## 本地依赖（无 root 环境的准备方式）
- Python 3.11 虚拟环境：`virtualenv` 建 `venv/`，`pip install -r /workspace/backend/requirements.txt httpx pytest`。
- PostgreSQL：Debian arm64 包（postgresql-15/client/common/libpq5/libllvm14）解包到本地目录，`initdb` 后以 unix socket `/tmp:17748` 启动，库 `migrationcheck`，用户 `migrate/migratepass`。
- Redis：arm64 redis-server/tools + libjemalloc2/liblzf1 解包，端口 16379 启动。
- 建表：`python -m scripts.seed_demo`（`Base.metadata.create_all`）；旧库执行 `backend/migrations/002_reliable_acceptance.sql`。

## 运行
```sh
export JWT_SECRET=local-test-secret
export DATABASE_URL='postgresql+psycopg://migrate:migratepass@localhost:17748/migrationcheck?host=/tmp'
export REDIS_URL='redis://localhost:16379/0'
PYTHONPATH=/workspace/backend ./venv/bin/python -m pytest test_reliable_acceptance.py -q
```
每个用例使用独立 Redis 队列键并清空 runs/acceptances/target_rows；子进程执行者通过各自心跳文件确认就绪后才开始计时。`MIGRATE_PROGRESS_DELAY` 可人为拉长块间耗时，用于保序与租约场景。

## 结果
11 项全部通过，连续三轮稳定；无执行者进程泄漏。
