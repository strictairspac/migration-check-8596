# Schema v2: 可靠受理与派发

## 背景
v1 的后台任务只靠 Redis 单消费者列表：保存运行后若 rpush 失败会被置 failed；进程崩溃留下 running 需人工新建；无受理幂等键、无批次内顺序、无多执行者租约。

## 变更
1. `acceptances` 表：受理号 `acceptance_no` 唯一、同批次 `(batch_id, seq)` 唯一；`request_digest` 保存提交时批次/映射快照与名称的规范 JSON SHA-256；`run_id` 关联运行；`status/wait_reason/cancel_requested/snapshot` 供页面核对受理、等待原因与完成状态。
2. `runs` 增加 `notified_at`（最近通知时间，扫描据此补发）、`lease_owner/lease_until`（执行者租约）、`updated_at`。
3. Redis 列表降级为纯通知流：消息重复/乱序/丢失均由数据库 `reconcile` 扫描与 `SELECT ... FOR UPDATE SKIP LOCKED` 认领兜底。

## 升级
全新容器卷由 `Base.metadata.create_all` 建表；已有库执行同目录 `002_reliable_acceptance.sql`（幂等）。

## 语义
- 同受理号重放且摘要一致：返回原受理，不新增运行；摘要不一致：422 拒绝。
- 同批次按 seq 顺序，前项未终结（completed/failed/cancelled）后项 `waiting_predecessor`；不同批次互不阻塞。
- 待派发取消立即终结为 cancelled；迟到消息因运行非 queued 不会启动迁移。
- 租约过期判 failed 并留审计，不自动重试，目标行与完成凭据仍只在终局事务落一份。
