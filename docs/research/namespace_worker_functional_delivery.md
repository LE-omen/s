# Worker 功能交付：独立入口、原生协议、通用 DDL

起点：`ae890cffe`（V18）。用户目标是完整实现以下 1～3；单独增加监听端口或通过少量 SQL 探针不代表完成。

## 1. 独立客户端入口与共享存储服务

- worker 暂时各自监听不同端口，复用原生 NIO、认证、协议处理器及结果发送。
- 共享存储可以独立接收和处理 worker 发起的请求，不依赖共享端先收到用户 SQL。
- 接通连接、会话、事务、扫描的创建、使用、关闭和异常清理。
- 验证两个 worker 并发读写、断连、超时、worker 死亡；严格禁止共享进程执行 SQL。
- 多端口是临时入口；未来入口变化不改变 SQL 执行与存储接口。

## 2. 原生协议、会话与权限

- 文本查询、二进制预处理、参数绑定、长参数、多结果、重置连接、切库、取消。
- 字符集、TLS、普通用户和权限校验。
- 复用原生协议及执行逻辑；在底层服务接通后移除对应原型限制。
- 验证 CLI、常见驱动、连接池复用，以及会话隔离、资源回收和认证失败。
- BEGIN、savepoint、原生内部 SQL 和同 session 嵌套执行继续回归。

## 3. 通用 DDL、表与普通索引

- 创建/删除数据库，创建/修改/删除表，创建/删除索引。
- 复合主键、普通/唯一索引、NULL、字符串、数字、日期时间和大字段。
- schema 版本、计划失效及 DDL 后立即访问。
- 从空目录启动，建库表及索引，读写事务与回滚，修改 schema，再重启验证恢复。
- 验证约束与索引一致性；管理回调发起的 SQL 仍在 worker 执行。

## 实现约束

- 先完成上述功能；本轮不做内存调优，不增加重复的进程级对象缓存。
- 存储接口传递值和受生命周期约束的句柄，不传递跨进程指针。
- 复用现有跨平台网络、线程和进程设施；本轮不宣称已完成远程 worker 或所有平台验收。
- 不把 namespace 全对象 fork 和跨平台部署的后续工作算入 1～3，也不把 1～3 缩成小探针。

## 进展与证据

- 源码确认：V18 的共享存储处理在 `Exchange::next()` 内，由共享端发起 SQL 的线程驱动。这是独立 worker 监听前必须拆除的依赖。
- 源码确认：原生 `ObSrvXlator` 已覆盖协议命令，原生 Rust NIO 支持独立端口；优先接通这条已有路径。
- 已把共享存储请求从 `Exchange::next()` 移到已有共享 runtime 的请求线程池。IPC reader 只提交任务；同一请求的存储操作串行，关闭时等待已提交任务结束再释放借用的 session 和扫描对象。
- `source ~/.bashrc && make -C build_release -j80 seekdb`：通过，日志 `/data/1/tmp/namespace-v19-dispatch-build.log`。
- 严格模式空目录 bootstrap、崩溃重启、系统表及配置虚拟表：通过，日志 `/data/1/tmp/namespace-v19-dispatch-bootstrap.log`，目录 `/tmp/namespace_fork_PROTOTYPE_bootstrap_v18_6vkovm54`。
- 同 session 嵌套 SQL、事务及 worker 死亡回滚：通过，日志 `/data/1/tmp/namespace-v19-dispatch-nested.log`，目录 `/tmp/namespace_fork_PROTOTYPE_nested_session_v17_92hpayij`。
- **1～3 尚未完成**：独立存储请求和原生监听已初步接通，协议权限、通用 DDL/索引及完整验收仍需继续。

### 独立入口推进记录（进行中）

- 分支：`codex/namespace-worker-direct-v19`。共享端调度拆分已提交为 `fa6d23e24`。
- 增加 worker 主动发起的存储路由：请求编号最高位区分方向，复用同一 IPC。每个直连连接拥有一个路由，沿用当前原型的 32 个路由上限；扩展并发准入仍待完成。
- 共享端 `DirectStorageContext` 持有该连接的原生事务/快照状态；`e` 完成一次请求并释放扫描，`v` 关闭连接并回滚剩余事务。worker 异常退出时将对象清理提交到已有共享请求线程池。
- 独立路由探针随严格 bootstrap 和重启通过：`SEEKDB_NAMESPACE_SQL_WORKER_DIRECT_PROBE=1`，日志 `/data/1/tmp/namespace-v19-direct-storage-bootstrap.log`，目录 `/tmp/namespace_fork_PROTOTYPE_bootstrap_v18_vr69t3fc`。归档内 worker 日志包含 `PROTOTYPE_V19_DIRECT_STORAGE_PROBE ns=1 ret=0`。
- 原生监听正在通过 `SEEKDB_NAMESPACE_SQL_WORKER_LISTEN=1` 接通。worker 初始化现有 runtime 请求调度器，复用 NIO、协议处理器、session manager 和 packet sender；连接绑定持有原生 session 指针，避免逐请求查 session 表。
- 原生直连测试：`tools/obtest/namespace_worker_direct_prototype.py`。最新通过握手、SELECT、BEGIN/ROLLBACK 状态、切库及 8 个连接的会话隔离；日志 `/data/1/tmp/namespace-v19-direct-ingress.log`，目录 `/tmp/namespace_fork_PROTOTYPE_direct_v19_l3o1ff7f`。这不代表目标 1～3 验收通过。
- 已定位并补齐两项原有完整启动路径的依赖：握手随机串初始化；共享端和 worker 的逻辑服务地址。后者通过启动帧 `B` 传递，与 worker 独立的客户端监听端口分开，防止原生事务状态把 worker 误判成事务路由转移节点。原始 IPC 测试入口也已发送启动帧。
- 编译时必须等待完成后才能编辑 `.ipp`；最新构建日志 `/data/1/tmp/namespace-v19-listener-build.log`。

仍需交付的完整内容：

1. 完成并验证原生直连入口（当前仍保留原共享客户端转发路径，需收口）；两个 namespace worker 并发读写与断连/取消/死亡；消除不合理的连接准入上限。
2. 接通真实用户、权限和系统变量元数据，不能把 worker 的 bootstrap schema 当成最终认证数据；预处理/长参数/多结果/重置连接/字符集/TLS/连接池回归。
3. `query::ObIRootCommandService` 的远程适配已实现，复用原有类型接口传输 DDL、安全管理及配置命令；当前仅接入 namespace 1。建库、建表已在原生直连探针通过；需继续完善 schema guard 版本与生命周期、计划失效、通用索引/DML/LOB，并解决 fork namespace 的 DDL 元数据更新，完成重启验收。
4. `RemoteTransactionService::submit_commit_tx` 已接通共享端原生 commit，完成后调用 worker 的原生回调。公共 packet sender 的借用引用释放和回调清理顺序已修复，`/data/1/tmp/namespace-v19-direct-native-matrix.log` 的 `direct_insert_committed` 验证原生自动提交成功；后续显式事务触发独立内部 session 的路由问题，仍在推进。

本轮故障定位证据：

- 冷启动建表最初查询了不存在的 namespace 注册表。旧 `check_ddl` 漏了其他注册钩子已有的就绪检查，现已补齐。
- 接着建表等待超时。`/data/1/tmp/namespace-v19-ddl-worker-stack.log` 确认阻塞于 `wait_local_schema_visible`：元数据来自共享端，刷新版本却读取 worker 的 bootstrap 版本。已在 schema 服务边界转发刷新/发布版本读取，`/data/1/tmp/namespace-v19-direct-schema-version.log` 包含 `direct_table_created`。
- 同一探针的 INSERT 在存储完成写入、提交前返回 -4018 并回滚。`/data/1/tmp/namespace-v19-session-lifetime-stack.log` 确认 session 节点仍存在，但引用数已低于生存基线：公共发送器清除连接指针后，清理错误递减了借用引用。
- BEGIN 后 UPDATE 触发优化器统计读取。`/data/1/tmp/namespace-v19-nested-route2-stack.log` 确认新建的内部 session 5 借用了用户 session 2 的存储路由，因已有活动事务而失败。已在原生 session 切换处加入 `StorageSessionScope`：相同 session 复用路由，独立 session 持有独立路由，执行/迭代/销毁期间切换并恢复调用者。构建 `/data/1/tmp/namespace-v19-inner-storage-scope-build.log` 进行中。
- `/data/1/tmp/namespace-v19-direct-inner-scope.log` 已通过建库建表、自动提交、显式事务/回滚/savepoint、8 客户端隔离、CLI、多结果、UTF-8、二进制预处理及 360 KB 长参数。尚未通过连接重置；不能把长参数的纯表达式验证等同于存储 LOB 支持。
- 连接重置的原生锁清理会创建 session ID 为 0 的临时 session，并先直接调用事务接口，再执行内部 SQL。已允许内部存储路由使用原生匿名 session ID，并在公共显式事务开始/结束边界使用同一 `StorageSessionScope`。构建 `/data/1/tmp/namespace-v19-internal-trans-scope-build.log` 通过。
- 原生直连完整探针通过：`/data/1/tmp/namespace-v19-direct-internal-trans.log`，包括连接重置清空用户变量、回滚未提交事务，以及重置后继续查询。
- 本轮回归通过：严格空目录 bootstrap 和崩溃重启 `/data/1/tmp/namespace-v19-native-bootstrap-regression.log`；同 session 嵌套 SQL、取消及 worker 死亡回滚 `/data/1/tmp/namespace-v19-native-nested-regression.log`；IPC 句柄复用、取消及并发 `/data/1/tmp/namespace-v19-native-handles-regression.log`。这些验证不代表真实权限、通用 DDL 或 namespace 多 worker 全部完成。

以上都是目标 1～3 的剩余工作，不是可省略的后续建议。内存优化、完整 namespace fork 对象覆盖和跨平台部署验收继续维持原边界。

### 统一元数据读取（进行中）

- 原生直连入口及前述回归已提交为 `b3f2949b6`。
- worker 的 schema guard 现在记录共享端的快照版本，表、库、用户及系统变量读取携带该版本。共享端使用原生历史版本接口，若返回版本不同则报 `OB_SCHEMA_EAGAIN`，避免混用版本；仍需 DDL 并发、历史版本及计划失效验收。
- 用户密码、锁定状态和系统变量已从共享端按 guard 生命周期读取，不新增进程级元数据缓存。
- 握手在 session 创建前读取全局 autocommit。公共元数据接口为没有 SQL session 的调用使用临时路由，覆盖原生握手和后台读取。当前为同步 RPC，后续入口性能验收需覆盖这一点。
- 构建 `/data/1/tmp/namespace-v19-metadata-no-session-build.log` 通过。`/data/1/tmp/namespace-v19-direct-metadata-no-session.log` 已通过原直连矩阵、真实用户创建、错误密码/不存在用户拒绝；随后在表级授权用户切库时报 1044，确认权限读取仍在使用 worker 启动时的本地权限表。
- 正在统一原生 `ObPrivMgr` 读取接口：共享端执行原生查找，worker 按 guard 生命周期持有返回值，原生权限判断及错误处理仍在 worker。权限用例覆盖只读授权、拒绝写入、撤权即时生效和列授权；通用 DDL 用例已补复合主键、decimal/date/NULL、普通/唯一索引和 schema 修改，尚未全部通过。
- `ObPrivMgr` 的 30 个底层读取接口已接入远程适配，原生权限聚合逻辑继续复用。构建 `/data/1/tmp/namespace-v19-privilege-reader-build2.log` 通过。
- `/data/1/tmp/namespace-v19-direct-privileges-ddl.log` 已通过真实用户表级只读授权、拒绝写入、已有连接撤权即时生效、列授权，以及直连断连回滚解锁、KILL QUERY 后连接复用、全局 autocommit 修改后的新连接握手。
- 通用表最初在 DECIMAL 读取时报 4016。`/data/1/tmp/namespace-v19-direct-column-diagnosis.log` 验证同一表字符串/date/NULL 均可读取，仅 DECIMAL 失败；扫描和重复键返回代码直接取原始列类型，遗漏精度与小数位。已改用原生读写计划的完整列描述，构建 `/data/1/tmp/namespace-v19-native-column-types-build.log` 通过，完整矩阵重新验证中。
- `/data/1/tmp/namespace-v19-direct-native-columns.log` 已通过复合主键、字符串、DECIMAL、DATE 和 NULL 读写，随后 CREATE INDEX 返回 -4002。
- 分阶段调试确认 CREATE INDEX 的参数、加锁、schema 生成及版本分配、schema 持久化和 tablet 创建均成功；索引任务插入因 trace ID 为空失败。`/data/1/tmp/namespace-v19-index-trace-stack.log` 显示任务已初始化但 trace 四个字均为 0。共享 runtime 的 `OB_TASK` 不会像 MySQL 请求自动建立 trace，上轮存储调度拆分遗漏了这项上下文。
- 公共 worker 存储请求现携带 trace ID，共享任务使用原生 `ObTraceIdGuard` 在调用期间恢复；没有 SQL trace 的后台元数据读取和本地清理建立独立 trace。修复覆盖两个请求方向，未修改原生索引任务规则。构建 `/data/1/tmp/namespace-v19-storage-trace-build.log` 通过。
- `/data/1/tmp/namespace-v19-direct-storage-trace.log` 原有协议、权限、事务和类型用例通过，CREATE INDEX 转为等待阶段超时。`/data/1/tmp/namespace-v19-index-progress-worker.log` 发现 IPC 线程均等待任务队列锁；嵌套任务的最后 session 引用可能在重新加锁后销毁，销毁又发起 RPC 并重入同一等待循环。现把嵌套任务销毁移至加锁之前，构建 `/data/1/tmp/namespace-v19-nested-owner-build.log` 通过；尚需长时间并发销毁回归。
- `/data/1/tmp/namespace-v19-index-progress2-probe.log` 确认索引任务已经持久化并进入状态 3（REDEFINITION），用户等待却立即报超时。worker 跳过完整 `ObServer::start()`，`stop_` 仍为构造时的 true；原生 DDL 等待认为服务已停机。worker 启动/停止现同步发布原生运行状态，构建 `/data/1/tmp/namespace-v19-serving-state-build.log` 通过。
- 修复运行状态后，原有直连矩阵仍通过；CREATE INDEX 进入原生回填而非立即超时。`/data/1/tmp/namespace-v19-index-backfill-sql.log` 捕获实际回填语句为带 `enable_parallel_dml` / `use_px` 的 INSERT … SELECT，内部写返回 -4006 并被原生任务重试，尚未解决。
- `/data/1/tmp/namespace-v19-backfill-phase-stack.log` 另外捕获到协作式嵌套任务在深层 SQL 栈上构造 `RequestWorker` 时栈溢出。公共任务执行入口现复用 `SMART_CALL_LARGE`，在必要时使用原生栈扩展；构建 `/data/1/tmp/namespace-v19-nested-stack-build.log` 通过，回填阶段继续定位中。
- 诊断时额外发现：引用不存在列触发原生外部符号查找后，worker 会等待永远未更新的 `sys_package_ready_`。这是运行时状态尚未统一的另一处，需通过已有本地管理/元数据服务接通，不能简单置 true；尚未修复。
- `/data/1/tmp/namespace-v19-backfill-filtered-stack.log` 将回填的 -4006 定位到原生 `ObPxCoordOp::inner_open`：worker 漏掉了中断管理和 PX 数据通道/调度依赖。已补齐原生 interrupt、shared timer、DTL、Dfc、PX pools、DTL intermediate result manager 的组合启动；构建 `/data/1/tmp/namespace-v19-px-runtime-timer-build.log` 通过。
- `/data/1/tmp/namespace-v19-direct-px-runtime.log` 继续通过原有协议、权限、事务和通用类型用例，CREATE INDEX 进入 PX 后返回 -4007。归档 worker 输出确认 `get_tx_exec_result`、`add_tx_exec_result`、`merge_tx_state` 仍是未接通的事务服务接口。
- 正在接通这组原生事务状态操作：PX 私有描述符按原生 shadow 语义解码和释放，worker 内复用原生描述符汇总方法，主事务的执行结果由共享端原生事务服务接收。编译及索引回归尚未完成，仍需检查 PX 线程的独立存储路由和写状态回传。
- 事务状态适配构建 `/data/1/tmp/namespace-v19-px-tx-state-build2.log` 通过，`/data/1/tmp/namespace-v19-ddl-tx-state.log` 已越过未支持接口，CREATE INDEX 改为 -4016。`/data/1/tmp/namespace-v19-px-sqc-stack.log` 和 `/data/1/tmp/namespace-v19-direct-insert2-stack.log` 定位到 `ObDirectInsertOrchestrator::start`，尚未进入实际 PX 扫描任务。原生 `ObIndependentDag::basic_init` 要求存储 DAG scheduler，worker 没有该服务；应把 DirectInsert 会话/写入接口接入共享存储，而非在 worker 启动另一套存储组件。
- 增加用户 `parallel(2)` 聚合查询回归，先独立收齐 PX 执行链路。`/data/1/tmp/namespace-v19-parallel-route-stack.log` 捕获到 DAS 范围估算服务指针为空导致 worker 崩溃；现通过原生 `ObIRangeService` 远程执行范围估算/切分，边界只传 tablet/range 值，返回 rowkey 使用调用方 allocator，未增加进程级对象缓存。构建 `/data/1/tmp/namespace-v19-range-service-build2.log` 通过。
- `/data/1/tmp/namespace-v19-parallel-task-stack.log` 随后定位到实际 PX 扫描缺少存储路由。扫描、事务和 DML 公共接口现按执行 session 绑定路由；首次使用 PX session 时，共享端通过原生 `acquire_tx(serialized)` 导入 shadow 描述符。清理 shadow 只释放副本，不回滚主事务；未绑定路由的存储请求直接返回错误，不发送无效请求标签。
- 构建 `/data/1/tmp/namespace-v19-px-session-routes-build.log` 通过。`/data/1/tmp/namespace-v19-parallel-session-routes.log` 已通过复合主键及类型读取、两路 PX 聚合（`direct_parallel_scan_verified`），随后 CREATE INDEX 仍在 DirectInsert 准备阶段失败。并行 DML 提交/回滚及完整原生客户端矩阵正在回归，DirectInsert 远程适配尚未实现。
- `/data/1/tmp/namespace-v19-direct-parallel-matrix.log` 已通过原有协议、权限、会话、事务与通用类型用例，以及 PX 查询、PX INSERT … SELECT 的显式回滚和自动提交；共享 SQL 拒绝计数为 0。矩阵仅在 CREATE INDEX 的 DirectInsert 准备阶段失败，仍不代表目标 1～3 完成。
- 冷启动通过，但 `/data/1/tmp/namespace-v19-px-bootstrap-regression.log` 的崩溃重启返回 `OB_SCHEMA_EAGAIN`。定向诊断 `/data/1/tmp/namespace-v19-restart-diagnosis.log` 对应共享日志显示请求版本 1、刷新版本 1，却取得持久化 baseline 版本：将当前 core 版本作为显式历史版本请求，触发了原生 baseline 提升。元数据和权限读取现统一通过 `catalog_schema_guard`：当前版本用原生无版本参数入口，历史版本保持原生历史入口，取得后仍严格比对版本；诊断日志已移除。构建及恢复回归进行中。
- 恢复修复构建 `/data/1/tmp/namespace-v19-recovery-catalog-guard-build.log` 通过。严格空目录 bootstrap 和崩溃重启 `/data/1/tmp/namespace-v19-px-bootstrap-regression2.log`、同 session 多层嵌套 SQL/取消/worker 死亡回滚 `/data/1/tmp/namespace-v19-px-nested-regression.log`、IPC 句柄与取消并发 `/data/1/tmp/namespace-v19-px-handles-regression.log` 全部通过。下一项仍是 DirectInsert 远程接口以及通用索引/DDL 验收；1～3 尚未完成。
- 上述元数据、权限和原生 PX 服务已提交为 `6a8d82c0b`。
- DirectInsert 现增加可绑定的原生/远程服务入口，session 结束通过虚函数释放所属实现。共享端保存原生回填 DAG、slice writer；worker 只持执行期句柄，通过原生 SQC/PX session 的已有路由调用，批数据按最多 32 行传输。不同 PX 路由能并发完成，最终释放排除活动调用；后台 DAG 引用已有共享 session 的生存期对象，没有新增进程级对象缓存。
- DirectInsert 初版构建 `/data/1/tmp/namespace-v19-direct-insert-service-build.log` 通过，`/data/1/tmp/namespace-v19-direct-insert-ddl.log` 的并行读写继续通过，但建索引导致 worker IPC 退出。源码确认新增回复类型未加入接收白名单，现改用已有通用存储回复类型；正在重新构建验收。诊断重跑另一次在 CREATE DATABASE 等待超时，`/data/1/tmp/namespace-v19-insert-preparation.log` 仅确认等待共享 root command，尚未定位，不能据此宣称通用 DDL 稳定。
- worker 已注册原生 DDL slice store，其调度信息持久化通过现有 inner SQL 完成，无需再加 RPC。构建 `/data/1/tmp/namespace-v19-native-slice-registration-build.log` 通过；`/data/1/tmp/namespace-v19-native-slice-ddl.log` 已完成普通/唯一索引回填及一致性、ALTER ADD COLUMN、DROP INDEX/TABLE，最终仅在 DROP DATABASE 被旧原型统一保护拦截。
- 数据库 DDL 保护已改为检查现有 namespace catalog 引用，保留编码 ID 和已登记数据库保护，调用者传播实际错误。补充登记前 ALTER/DROP 和登记后拒绝的回归；构建 `/data/1/tmp/namespace-v19-database-ddl-guard-build.log` 通过。该保护变更仍待完整验收，不能视为 namespace > 1 的通用 DDL 已实现。
- `/data/1/tmp/namespace-v19-database-ddl-native-matrix.log` 的协议、权限和生命周期通过，但 CREATE DATABASE 再次等待超时。双进程诊断 `/data/1/tmp/namespace-v19-create-db-snapshot-{shared,worker}.log` 捕获同类 GRANT 等待：用户命令持有 worker 的 `root_service_serial_mutex`，共享端发布 schema 等待内部查询；内部查询等待 metadata RPC 时，协作调度嵌入无关系统包 DDL，后者等待用户持有的锁，使已就绪的外层回复无法被消费。不是单一 DDL 语句缺少支持。
- 协作等待现仅选择同一调用标识的内部任务。共享端向内部连接/SQL 请求传递原生 trace，worker 将其作为本次调用标识跨存储回调保留，并在嵌套完成后恢复；普通语句可保留自身 trace。构建 `/data/1/tmp/namespace-v19-inner-call-chain-build.log` 进行中，随后须验证原完整矩阵、单执行线程 bootstrap 和嵌套事务。此时尚未宣称死锁消除或数据库保护验收通过。
