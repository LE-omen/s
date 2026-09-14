# V16 方向调整：复用原生 SQL 请求执行链

状态：已核对当前调用链，确定下一轮重构范围；运行代码仍为 V15。

本轮目标是让 SQL worker 完整复用原生请求执行代码，替换当前手写的精简执行循环。目标架构中，namespace SQL 的唯一执行入口是对应 worker；共享端口负责客户端接入。重试是复用这条调用链后的验收项目。

此前“普通入口”指当前原型保留的一体化旧执行路径：非 namespace-worker 连接仍由原有 `ObMPQuery` 在共享进程执行 SQL。它在迁移期间用于回归对照，不属于目标架构中的第二个 namespace SQL 执行入口。

## 当前缺口来自哪里

| 原生路径 | 当前 worker | 影响 |
| --- | --- | --- |
| `ObMPQuery::process_single_stmt` 管理请求上下文和重试循环 | 自己建立 session/context，直接调用 `stmt_query` | 请求生命周期有两套维护点 |
| `ObSyncPlanDriver` / `ObSyncCmdDriver` 打开、输出、关闭结果，按原有时序处理失败与重试 | 自己调用 `open/get_next_row/close`，发送 S/H/R 帧 | 原有 driver 的行为不会自动带入 worker |
| `ObQueryDriver` 统一处理结果字段、字符集和 LOB 等 | worker 逐项适配行值 | 结果协议行为也在逐项补齐 |
| SQL 事务控制经 data_plane 调用事务/存储实现 | 部分接口代理，事务描述符使用兼容视图 | 入口复用后仍需补齐进程间接口，不能宣称全部 SQL 自动可用 |

相关源码：

- [原生请求入口](../../src/observer/mysql/obmp_query.cpp)：`process_single_stmt`、`do_process`、`response_result`。
- [计划执行 driver](../../src/observer/mysql/ob_sync_plan_driver.cpp)：先决定重试，再设置 `will_retry`、关闭结果；已发送结果后的重试限制也在这里。
- [命令执行 driver](../../src/observer/mysql/ob_sync_cmd_driver.cpp) 与 [结果处理](../../src/observer/mysql/ob_query_driver.cpp)。
- [已有发送接口](../../src/query/api/query/protocol/ob_mysql_packet_sender.h)：`ObIMPPacketSender` 已存在，但 driver 构造及成员仍依赖 `ObMPPacketSender` 具体类型。
- [worker 手写执行循环](../../src/observer/namespace_sql_worker_prototype.ipp) 与 [ResultSet 中的 namespace 分支](../../src/sql/ob_result_set.cpp)。

## 目标结构

worker 复用的执行模块接收已绑定的 session、SQL、请求期限及结果输出接口。内部负责原有请求上下文、解析优化、执行、重试决策、结果处理和事务收尾。网络请求对象及连接所有权由共享端的接入层管理。

```text
客户端 → 共享端口/路由 → 对应 namespace 的 worker
                        原生 SQL 请求执行链 + DAS
                                  ↓ IPC
                        共享引擎：真实事务、存储
```

共享端的网络接入与共享引擎属于同一共享进程。SQL 与 DAS 在 worker 内执行；共享引擎持有真实事务和存储对象。IPC 传递接口需要的值、句柄及批量数据。

结果输出优先使用现有 `ObIMPPacketSender` 接口，为 worker 接入 IPC 实现。迁移期间旧执行路径仍可使用原有发送实现，以验证复用代码的行为一致性。输出适配必须保留“发送开始后不得重试”和“事务收尾完成后才确认成功”的既有约束；跨进程完成通知与连接所有权在输出适配处衔接。

## 迁移顺序及完成条件

1. **从原生执行链提取可供 worker 使用的执行模块。** 同步 plan/cmd driver 改用已有发送接口。迁移期间原有 `ObMPQuery` 也调用提取后的实现，验证旧路径行为保持一致。这里的代码共用是迁移方法；提取时搬移现有实现，避免复制出第三份流程。
2. **worker 切换到同一模块。** IPC 只适配请求投递、结果输出和数据平面调用。删除 worker 中独立的执行、取行、结果转换与关闭循环。原型范围检查集中保留，仍然拒绝尚未接通的能力。
3. **在数据平面接口处收敛剩余适配。** 统一审计事务、扫描、写上下文和 DML 接口的生命周期及参数。把 `ObResultSet` 中的 namespace 专用行为迁到合适的事务/快照适配处，再删除上层分支。显式事务接入时由会话持有共享端事务，避免在 SQL 类型分支中延长对象生命周期。

每一步有可运行结果；尚未完成第 2 步时，不能把“提取了模块”报告成“worker 已复用完整链路”。

## 验收标准

- worker 确实复用原生请求执行实现；迁移期旧执行路径用于同源回归。目标架构的 namespace SQL 统一进入 worker。
- worker 中不再维护独立的 `open/get_next_row/close` 和错误重试循环；原有结果转换逻辑得到复用。
- 并发锁冲突和快照冲突使用原有重试决策；测试不再靠客户端补重试通过。超时和已经发送结果的请求遵守原有退出规则。
- V15 的跨批次写入、主键修改、回滚、隔离、恢复，以及原有会话、共享 IPC、取消、原生路径回归继续通过。
- 新增 SQL 验收若暴露缺口，应落到共用执行模块或明确的数据平面接口契约，避免再修改 worker 的 SQL 类型分支以实现业务语义。
- 不增加重复的完整对象缓存、session 查找链路或独立清理线程。共用执行代码不意味着在两侧各自维护一份完整 SQL 会话状态。

V15 的两列整数表、自动提交等限制仍有效。上述结构收敛完成后，再按数据平面接口补齐能力，以 SQL 用例验证覆盖。
