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
- **1～3 尚未完成**：worker 主动发起存储请求、原生监听入口、协议权限接通、通用 DDL/索引及完整验收仍需继续。
