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
