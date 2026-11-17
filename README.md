# 见山 · 检验结果迁移核对站

见山质量档案组13人用源批次、字段映射、后台对账运行完成旧字段检验结果的有证据交接。

## 场景与业务
上游实验室交付旧字段JSON检验记录；过去人工改字段名、复制到新表并凭总行数判断成功。痛点：非法数值默默丢失、重复委托号覆盖、备份无法证明能恢复。本站登记原始行，字段映射规定类型和必填，Python逐块转换并保留拒绝原行，真实PG目标表一次提交。下游质量复核人下载对账凭据，管理员用JSON逻辑备份恢复本次运行目标并核验。

四业务页：源批次、字段映射、可靠受理、迁移执行（兼容入口，可取消）、差异核对（拒绝区/目标行/备份）；管理员另有审计和回传恢复。三核心实体batches/mappings/runs，另持久化target_rows与acceptances受理台账。全部实体按模型/schema/service/controller/routes/前端api/store/page贯穿。

## 启动与账号
```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
前端 http://localhost:18248 ，后端 http://localhost:19248/docs ，PostgreSQL localhost:17748（migrate / migratepass / migrationcheck）。结束docker compose down -v --remove-orphans会清除本地数据。

| 角色 | 账号 | 权限 / 页面 | 典型动作 |
|---|---|---|---|
| admin | admin@migration.local | 全部、审计、恢复 | 回传备份恢复目标并校验 |
| operator | operator@migration.local | 创建/编辑/执行/取消/导出 | 配置映射并迁移 |
| viewer | viewer@migration.local | 只读、下载凭据/备份 | 复核拒绝区和真实目标表 |

密码统一Migrate2026!；PBKDF2密码与8小时JWT。本地演示凭据与JWT密钥，部署前更换。

## 一天的交接剧本
1. 09:00 operator签入，查看见山日班检验结果12条正常记录。
2. 09:05 登记lab-results-20261117.json，委托号JS-20261117-001结果420.1MPa。
3. 09:10 新建shift-20261117批次，复制旧字段。
4. 09:15 字段映射：委托号→sample_code、检测项→test_name、结果→result_value、单位→unit、时间→measured_at，每块4行。
5. 09:20 提交shift-20261117对账，或运行旧字段异常样本查看重复/非法数字/空值/缺列拒绝。
6. 09:25 查看接受/拒绝与分块证据，读取真实PG目标表。
7. 09:30 下载reconciliation-<id>.json与backup-<id>.json，viewer复核。
8. 10:00 admin回传该备份，恢复并核对摘要，查看审计完成事件。

seed含三账号、三批次（三种正常/异常/空输入）、三映射。固定数值、自然键upsert式补缺，不覆盖用户编辑；连续执行不新增。

## 迁移、校验、取消与恢复
字段类型为text/decimal/datetime。decimal使用Decimal精确转换为6位小数字符串，范围±1e9；无穷/NaN拒绝。空值/空串按required隔离（只有unit可非必填）；缺源字段隔离。时间统一UTC，无时区按Asia/Shanghai(+08:00)解释。自然键sample_code+test_name按规范JSON SHA-256计算；同次运行重复键只接受首条合法记录。每行与全部接受行用排序key/紧凑JSON/UTF-8算SHA-256。拒绝行保存原行和原因。

最多2000源行、每行20列、单元格500字符；每块1-500行。逐块更新进度；取消在块间检查，最终提交前锁定run再次检查，成功目标一次提交；取消/异常没有部分目标数据。

## 可靠受理与派发（v2）
队列故障不再让已保存的迁移失败。受理与运行的事实来源是PostgreSQL，Redis列表只作通知流，消息重复/乱序/丢失均由数据库扫描与行锁兜底：
- **断线仍受理**：`POST /acceptances`在同一事务落库运行（queued）、冻结批次/映射快照与受理台账；Redis不可用或进程在通知前停机，受理仍保留。执行者每5秒（`SWEEP_INTERVAL`）扫描待决受理，对未通知或超过15秒（`REDELIVER_AFTER`）未确认的运行补发通知，恢复后自动派发。
- **受理号幂等**：`acceptance_no`唯一；同号重放且提交内容（名称/批次/映射快照）的规范JSON SHA-256一致时返回原受理、不新增运行；摘要不一致一律422拒绝。旧入口`POST /runs`按运行名派生受理号，语义不变。
- **同批次保序、跨批次并发**：受理在批次内分配连续`seq`（`UNIQUE(batch_id,seq)`）；前项未终结（completed/failed/cancelled）后项保持`waiting_predecessor`，认领处再做一次顺序兜底，消息逆序到达也不会抢先启动。不同批次互不阻塞，故障/慢批次不会拖住其他批次。
- **取消与迟到消息**：待派发取消立即终结为cancelled；任何迟到/重复消息在认领时因运行非queued被忽略，迁移不会启动。运行中取消仍在块间与终局提交前检查。
- **多执行者与租约**：`SELECT ... FOR UPDATE SKIP LOCKED`保证同一运行只有一个执行者认领，认领即写`lease_owner/lease_until`并在每个块续租（默认30秒`WORKER_LEASE_SECONDS`）。执行者崩溃后租约过期，扫描将运行判failed并写审计，不自动重试，避免双写；目标行与完成凭据只在终局事务落一份。
- **页面核对**：可靠受理页展示受理号、批次内序号、受理状态、等待原因（刚受理/等待派发/等待前项）、关联运行与取消入口，轮询自动刷新；差异核对页的完成状态、拒绝区、目标行与备份恢复维持不变。

### 验证（test-artifacts/gb-4109）
`test_reliable_acceptance.py`（pytest，11项）用真实PostgreSQL/Redis与多子进程执行者覆盖：保存后未入队即停机后自动派发、同号重放不新增与换内容422、消息重复（目标行/凭据只一份）、消息乱序仍按受理顺序、认领处顺序兜底、三执行者争抢、跨批次并发不被故障批次拖住、取消后迟到消息不启动、租约过期判失败且不复活、备份恢复往返与篡改422、受理台账字段可见。运行方式见该目录说明。

下载备份为本工具目标行的逻辑JSON快照(version1)，含run_id/rows/row_hashes/checksum，不是pg_dump或物理备份，不含WAL/索引/权限。管理员恢复只允许该运行已保存checksum完全匹配的备份；先核验逐行/总体摘要、schema和自然键，再事务性替换该运行target_rows，回读重新计算摘要并保存recovery记录。篡改、错运行、未完成运行拒绝，恢复事务回滚；不能用任意新数据覆盖目标。

受控场景演示旧字段到新目标结构的逻辑迁移，数据库真实保存源/映射/目标/对账。不能连接远端数据库，不宣称生产异构迁移或灾备能力。

## 工程与开发
Python3.11/FastAPI/SQLAlchemy/psycopg/Redis/PostgreSQL16，原生HTML/CSS/JS，nginx1.27。exact五服务frontend/backend/worker/db/redis均healthcheck，数据库/Redis/共享产物命名卷；依赖健康后启动。worker可水平扩容多副本，SKIP LOCKED认领保证不重复执行。请求追踪、全局错误、JWT授权和业务审计横切。

backend/src按models/schemas/services/controllers/routes/middlewares/config/constants/utils；backend/scripts/seed_demo.py；backend/migrations记录元数据版本。frontend/src按api/stores/pages/components/common/hooks/router/types/constants/utils；共享table/status/feedback；forms/poll hooks。角色/状态/类型枚举在backend/src/constants/enums.py与frontend/src/constants/enums.js。墨绿邮政凭单视觉，Noto Sans SC/Space Mono双字体来自Google Fonts，离线回退；桌面/平板/手机响应。

.env.example定义COMPOSE_PROJECT_NAME/三端口/JWT_SECRET/SEED_DEMO；DATABASE_URL/REDIS_URL由Compose注入。启动SEED_DEMO失败只记日志。本地开发pip install -r backend/requirements.txt，设数据库/Redis/JWT环境变量，在backend运行uvicorn src.main:app --reload；前端需/api代理，不能file://。测试/截图/日志放批次test-artifacts/gb-4109，源码不混入。MIT License。
