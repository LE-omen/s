// Throwaway V14 SQL-only composition. Included by ob_server.cpp so the prototype
// can reuse the existing composition owner without a second server object graph.
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/engine/ob_sql_memory_manager.h"
#include "sql/ob_result_set.h"
#include "observer/omt/ob_srs_service.h"
#include "observer/omt/ob_server_module_lifecycle.h"
#include "sql/optimizer/stat/ob_opt_stat_monitor_manager.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/das/ob_das_context.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include <memory>
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "rpc/obmysql/ob_sql_nio_server.h"
#include "observer/namespace_worker_sql_request_prototype.ipp"

namespace oceanbase { namespace observer {
int ObServer::namespace_sql_worker_prototype(const char *query)
{
  namespace_worker_prototype::worker_process = true;
  scramble_rand_.init(static_cast<uint64_t>(start_time_), static_cast<uint64_t>(start_time_ / 2));
  using namespace sql;
  using namespace common;
  using namespace share;
  int ret = OB_SUCCESS;
  namespace_worker_prototype::Frame bootstrap;
  ret = namespace_worker_prototype::worker_read_wire(bootstrap);
  if (ret || bootstrap.type() != 'B') { return ret ? ret : OB_INVALID_ARGUMENT; }
  const uint64_t logical_port = bootstrap.number();
  const ObString logical_ip = bootstrap.string();
  const std::string logical_host(logical_ip.ptr(), logical_ip.length());
  if (!bootstrap.consumed() || logical_port == 0 || logical_port > UINT16_MAX
      || !self_addr_.set_ip_addr(logical_host.c_str(), static_cast<int>(logical_port))) { return OB_INVALID_ARGUMENT; }
  namespace_worker_prototype::RemoteTabletScan remote_scan;
  int64_t concurrency = 2;
  if (const char *value = std::getenv("SEEKDB_NAMESPACE_SQL_WORKER_THREADS")) {
    char *end = nullptr; concurrency = std::strtol(value, &end, 10);
    if (!*value || !end || *end || concurrency < 1 || concurrency > 8) { return OB_INVALID_ARGUMENT; }
  }
  lib::Worker worker;
  lib::Worker::set_worker_to_thread_local(&worker);
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg);
  OB_LOGGER.set_file_name("worker-prototype.log", true, false);
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_enable_async_log(false);
  const int64_t budget = 512L * 1024 * 1024;
  // reload_config derives cache sizing from memory_budget; memory_limit is ignored.
  config_.memory_budget.set_value("512M");
  config_.cpu_count.set_value(std::to_string(concurrency).c_str());
  config_.enable_async_syslog.set_value("false");
  config_._pushdown_storage_level.set_value("0");
  config_._rowsets_max_rows.set_value("32");
  config_.enable_sql_operator_dump.set_value("false");
  // Client endpoints differ; these processes execute on one logical storage
  // server. Native transaction routing compares this identity with its owner.
  config_.self_addr_ = self_addr_;
  lib::update_mini_mode(budget, concurrency);
  set_memory_budget(budget);
  g_bootstrap_server_runtime.init();
  g_bootstrap_server_runtime.set_memory_size(budget);
  g_bootstrap_server_runtime.set_min_cpu(concurrency);
  g_bootstrap_server_runtime.set_max_cpu(concurrency);
  g_bootstrap_server_runtime.set_role(ObServerRole::PRIMARY_ROLE);
  // Each step is reported outside the protocol while bootstrapping is proved.
#define WORKER_STEP(expr) do { if (OB_SUCC(ret)) { ret = (expr); \
  fprintf(stderr, "worker bootstrap: %s => %d\n", #expr, ret); } } while (0)
  WORKER_STEP(GMEMCONF.reload_config(config_));
  WORKER_STEP(init_pre_setting());
  WORKER_STEP(init_global_context());
  WORKER_STEP(init_interrupt());
  WORKER_STEP(ObTimerService::get_instance().start());
  WORKER_STEP(init_config_module(""));
  WORKER_STEP(init_tz_info_mgr());
  WORKER_STEP(ObQueryRetryCtrl::init());
  WORKER_STEP(sql::init_sql_factories());
  WORKER_STEP(sql::init_sql_executor_singletons());
  WORKER_STEP(sql::init_sql_expr_static_var());
  WORKER_STEP(ObPreProcessSysVars::init_sys_var(ObServerOptions::KeyValueArray()));
  WORKER_STEP(ObBasicSessionInfo::init_sys_vars_cache_base_values());
  WORKER_STEP(init_global_kvcache());
  WORKER_STEP(init_sql_proxy());
  WORKER_STEP(schema_status_proxy_.init());
  WORKER_STEP(init_schema());
  WORKER_STEP(session_mgr_.init());
  WORKER_STEP(server_module_new_default(mods_plan_cache_));
  WORKER_STEP(server_module_new_default(mods_ps_cache_));
  WORKER_STEP(server_module_new_default(mods_sql_memory_manager_));
  WORKER_STEP(server_module_new_default(mods_srs_service_));
  WORKER_STEP(server_module_new_default(mods_opt_stat_monitor_manager_));
  WORKER_STEP(server_module_new_default(mods_data_access_service_));
  WORKER_STEP(server_module_new_default(mods_shared_timer_));
  WORKER_STEP(dtl::ObDfc::server_module_new(mods_dfc_));
  WORKER_STEP(server_module_new_default(mods_px_pools_));
  WORKER_STEP(server_module_new_default(mods_dtl_interm_result_manager_));
  WORKER_STEP(storage::ObLobManager::server_module_new(mods_lob_manager_));
  bind_server_service<ObSQLSessionMgr>(&session_mgr_);
  bind_server_service<ObVTIterCreator>(&vt_data_service_.get_vt_iter_factory().get_vt_iter_creator());
  bind_server_service<ObPlanCache>(mods_plan_cache_);
  bind_server_service<ObPsCache>(mods_ps_cache_);
  bind_server_service<ObSqlMemoryManager>(mods_sql_memory_manager_);
  bind_server_service<ObOptStatMonitorManager>(mods_opt_stat_monitor_manager_);
  bind_server_service<ObDataAccessService>(mods_data_access_service_);
  bind_server_service<share::ObISharedTimer>(mods_shared_timer_);
  bind_server_service<dtl::ObDfc>(mods_dfc_);
  bind_server_service<omt::ObPxPools>(mods_px_pools_);
  bind_server_service<dtl::ObDTLIntermResultManager>(mods_dtl_interm_result_manager_);
  WORKER_STEP(omt::ObSharedTimer::server_module_init(mods_shared_timer_));
  WORKER_STEP(omt::ObSharedTimer::server_module_start(mods_shared_timer_));
  WORKER_STEP(DTL.init());
  WORKER_STEP(dtl::ObDfc::server_module_init(mods_dfc_));
  WORKER_STEP(omt::ObPxPools::server_module_init(mods_px_pools_));
  WORKER_STEP(dtl::ObDTLIntermResultManager::server_module_init(mods_dtl_interm_result_manager_));
  WORKER_STEP(dtl::ObDTLIntermResultManager::server_module_start(mods_dtl_interm_result_manager_));
  WORKER_STEP(ObOptStatMonitorManager::server_module_init(mods_opt_stat_monitor_manager_));
  WORKER_STEP(ObPlanCache::server_module_init(mods_plan_cache_, *this));
  WORKER_STEP(ObPsCache::server_module_init(mods_ps_cache_));
  WORKER_STEP(ObSqlMemoryManager::server_module_init(mods_sql_memory_manager_));
  WORKER_STEP(ObOptStatManager::get_instance().init(&sql_proxy_, &config_));
  namespace_worker_prototype::RemoteRootCommands remote_commands;
  WORKER_STEP(sql_engine_.init(&ObOptStatManager::get_instance(), &remote_scan,
      self_addr_, *mods_plan_cache_, *mods_ps_cache_, pl_engine_, *this, *this,
      remote_commands, ob_service_, *this, *this, *this,
      *mods_srs_service_, *mods_lob_manager_));
  gctx_.status_ = SS_SERVING;
  g_server_modules_ready = OB_SUCC(ret);
  using namespace namespace_worker_prototype;
  if (ret != OB_SUCCESS) { return ret; }
  if (query[0] != '@') { return OB_NOT_SUPPORTED; }
  RemoteTransactionService remote_transactions;
  RemoteDmlService remote_dml;
  RemoteWriteContext remote_write_context;
  RemoteRangeService remote_ranges;
  RemoteDirectInsertService remote_direct_insert;
  {
    bind_server_service<ObITabletScan>(&remote_scan);
    bind_server_service<ObIVirtualTableScan>(&remote_scan);
    bind_server_service<data_plane::ObITransactionService>(&remote_transactions);
    bind_server_service<data_plane::ObIRangeService>(&remote_ranges);
    bind_server_service<data_plane::IDirectInsertService>(&remote_direct_insert);
    // The native slice store persists scheduling metadata through inner SQL,
    // whose storage path is already remote in this worker.
    sql::register_ddl_slice_store(this);
    bind_server_service<data_plane::ObIDmlService>(&remote_dml);
    bind_server_service<data_plane::ObIWriteContextService>(&remote_write_context);
    char *end = nullptr;
    worker_namespace = std::strtoull(query + 1, &end, 10);
    if (!end || *end || worker_namespace == 0 || worker_namespace >= (1ULL << 30)) { return OB_INVALID_ARGUMENT; }
    worker_catalog_fetch = fetch_catalog;
  }
  struct SessionOwner {
    ObArenaAllocator allocator{ObMemAttr("NsSQLSession")};
    ObSQLSessionInfo session; // Destroyed before its allocator.
    std::atomic<bool> running{false};
    common::sqlclient::ObISQLConnectionGuard inner;
  };
  struct SessionSlot {
    std::shared_ptr<SessionOwner> owner;
    uint64_t generation = 1;
  };
  std::mutex sessions_mutex;
  std::vector<SessionSlot> slots;
  std::vector<uint64_t> free_slots;
  uint64_t active_sessions = 0;
  auto initialize = [&](SessionOwner &owner, uint32_t sid, uint32_t capabilities, Frame *state, bool internal) -> int {
    int ret = OB_SUCCESS;
    ObSQLSessionInfo &session = owner.session;
    WORKER_STEP(session.test_init(1, sid, &owner.allocator));
    WORKER_STEP(session.load_default_sys_variable(false, false));
    WORKER_STEP(session.set_user(ObString::make_string("root"), ObString::make_string("%"), OB_SYS_USER_ID));
    session.set_capability(obmysql::ObMySQLCapabilityFlags(capabilities));
    session.set_user_priv_set(OB_PRIV_SELECT | OB_PRIV_INSERT | OB_PRIV_UPDATE | OB_PRIV_DELETE);
    if (worker_namespace == 1) { session.set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT); }
    session.set_session_manager(&session_mgr_);
    if (!ret && internal) { ret = ObInnerSQLConnection::init_session_info(&session, false, false); }
    if (!ret && state) {
      ret = apply_session_state(session, *state);
      if (!ret && !state->consumed()) {
        const uint64_t user_id = state->number();
        const ObString user_name = state->string();
        const ObString host_name = state->string();
        if (!state->consumed() || state->ret || user_id > UINT64_MAX) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          ret = session.set_user(user_name, host_name, user_id);
          if (!ret) {
            session.set_user_priv_set(user_id == OB_SYS_USER_ID
                ? OB_PRIV_ALL | OB_PRIV_GRANT
                : 0);
          }
        }
      }
      const uint64_t db = session.get_database_id();
      const share::schema::ObDatabaseSchema *database = nullptr;
      if (!ret && !internal && db != OB_INVALID_ID) {
        ObSchemaGetterGuard guard;
        if (worker_namespace == 1) {
          ret = schema_service_.get_runtime_schema_guard(guard);
          if (!ret) { ret = guard.get_database_schema(db, database); }
          if (!ret && !database) { ret = OB_ERR_BAD_DATABASE; }
          if (!ret) { ret = session.set_default_database(database->get_database_name_str()); }
        } else if (((db & ~(1ULL << 62)) >> 32) != worker_namespace) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          // The worker starts with only the core native schema. The encoded
          // database is resolved by the shared fork catalog when a statement
          // is planned; requiring a local database schema here races the first
          // catalog fetch and turns a valid connection into 1146.
          ret = OB_SUCCESS;
        }
      }
    }
    if (!ret && internal) { ret = ObInnerSQLConnection::create_connection_with_external_session(&session, owner.inner); }
    return ret;
  };
  // Native expression/operator checkpoints dispatch through THIS_WORKER. The
  // base lib::Worker used by a plain thread does not check SQL timeout/cancel.
  class RequestWorker final : public lib::Worker {
  public:
    int check_status() override {
      int ret = worker_request ? worker_request->status() : OB_SUCCESS;
      if (!ret && get_session()) { get_session()->is_terminate(ret); }
      if (!ret && is_timeout()) { ret = OB_TIMEOUT; }
      return ret ? ret : lib::Worker::check_status();
    }
  };
  auto execute = [&](SessionOwner &owner, const ObString &text, int64_t deadline) -> int {
    ObSQLSessionInfo &session = owner.session;
    const int64_t started = ObTimeUtility::current_time();
    THIS_WORKER.set_timeout_ts(deadline);
    THIS_WORKER.set_session(&session);
    ObCurTraceId::init(self_addr_);
    session.set_query_start_time(started);
    session.reset_warnings_buf();
    ob_setup_tsi_warning_buffer(&session.get_warnings_buffer());
    struct FinishRequest {
      ObSQLSessionInfo &session;
      ~FinishRequest() {
        session.set_session_in_retry(SESS_NOT_IN_RETRY);
        session.reset_query_string(); session.reset_warnings_buf(); session.set_session_sleep();
        ob_setup_tsi_warning_buffer(nullptr);
        THIS_WORKER.set_session(nullptr);
      }
    } finish{session};
    int ret = THIS_WORKER.check_status();
    if (!ret) { ret = session.check_and_init_retry_info(*ObCurTraceId::get_trace_id(), text); }
    {
      ObArenaAllocator allocator(ObMemAttr("NsSQLPolicy"));
      if (!ret) { ret = check_worker_sql(text, session, allocator); }
    }
    if (ret) { return ret; }
    ObQueryRetryCtrl retry;
    do {
      retry.clear_state_before_each_retry(session.get_retry_info_for_update());
      session.reset_warnings_buf();
      ret = THIS_WORKER.check_status();
      if (ret) { break; }
      ObArenaAllocator allocator(ObMemAttr("WorkerProto"));
      share::schema::ObSchemaGetterGuard guard;
      ObSqlCtx context;
      context.session_info_ = &session; context.schema_guard_ = &guard;
      context.retry_times_ = retry.get_retry_times();
      auto result = std::make_unique<ObMySQLResultSet>(session, allocator, sql_engine_.get_plan_cache_access_service());
      WorkerPacketSender sender;
      // The worker has no long-lived observer schema refresh task of its own.
      // Honor the session DDL fence before taking the per-statement guard;
      // unlike an unconditional refresh this does not contend on every query.
      int64_t local_schema_version = 0;
      const int64_t ddl_schema_version = session.get_last_ddl_schema_version();
      if (!ret) { ret = schema_service_.get_runtime_refreshed_schema_version(local_schema_version); }
      if (!ret && ddl_schema_version > local_schema_version) {
        ret = schema_service_.async_refresh_schema(ddl_schema_version);
      }
      // Use the native version-fenced path: it reads the current schema
      // version from inner tables and refreshes before returning a guard.
      if (!ret) { ret = schema_service_.get_runtime_schema_guard_with_version_in_inner_table(guard); }
      if (!ret) { ret = session.update_query_sensitive_system_variable(guard); }
      if (!ret) { ret = result->init(); }
      int64_t version = 0;
      if (!ret) { ret = guard.get_schema_version(version); }
      if (!ret) {
        retry.set_current_local_schema_version(version);
        retry.set_current_global_schema_version(version);
        auto &task = result->get_exec_context().get_sql_exec_ctx();
        task.schema_service_ = &schema_service_; task.set_query_begin_schema_version(version);
        // DML operators open nested SQL through the execution context. Keep
        // the worker's routed proxy attached so that path uses the same
        // worker/shared storage bridge instead of observing a null proxy.
        result->get_exec_context().set_sql_proxy(&sql_proxy_);
        session.set_current_execution_id(sql_engine_.get_execution_id());
        session.reset_plsql_exec_time(); session.reset_plsql_compile_time(); session.set_stmt_type(stmt::T_NONE);
        ret = session.set_session_active(text, started, started, obmysql::COM_QUERY);
      }
      if (!ret) {
        ret = sql_engine_.stmt_query(text, context, *result);
        if (ret) {
          int client_ret = ret;
          retry.test_and_save_retry_state(gctx_, context, *result, ret, client_ret);
          ret = client_ret;
        } else if (!(ret = check_worker_plan(*result))) {
          result->set_end_trans_async(false);
          result->get_exec_context().set_plan_start_time(ObTimeUtility::current_time());
          if (result->get_physical_plan()) {
            ObSyncPlanDriver driver(gctx_, context, session, retry, sender);
            ret = driver.response_result(*result);
          } else {
            ObSyncCmdDriver driver(gctx_, context, session, retry, sender);
            ret = driver.response_result(*result);
          }
        }
      }
      session.set_session_in_retry(retry.need_retry());
      fprintf(stderr, "PROTOTYPE_V16_NATIVE_EXECUTE session=%u attempt=%lld retry=%d ret=%d\n",
          session.get_server_sid(), (long long)context.retry_times_, static_cast<int>(retry.get_retry_type()), ret);
      if (!retry.need_retry() && !ret) { ret = sender.complete(); }
    } while (retry.get_retry_type() == RETRY_TYPE_LOCAL);
    return ret;
  };
  auto execute_inner = [&](SessionOwner &owner, Frame &input) -> int {
    auto *connection = static_cast<ObInnerSQLConnection *>(owner.inner.get_ptr());
    if (!connection) { return OB_INVALID_ARGUMENT; }
    THIS_WORKER.set_session(&owner.session);
    struct SessionScope { ~SessionScope() { THIS_WORKER.set_session(nullptr); } } scope;
    const int64_t deadline = input.number();
    THIS_WORKER.set_timeout_ts(deadline);
    ObSessionDDLInfo ddl; input.read(ddl); owner.session.set_ddl_info(ddl);
    const uint64_t operation = input.number();
    int ret = input.ret;
    int64_t affected = 0;
    if (!ret && (operation == 'R' || operation == 'W')) {
      const bool user_sql = input.number() != 0;
      const ObString text = input.string();
      if (!input.consumed()) { return OB_INVALID_ARGUMENT; }
      if (operation == 'W') { ret = connection->execute_write(text, affected, user_sql); }
      else {
        ObISQLClient::ReadResult result;
        ret = connection->execute_read(text, result, user_sql);
        if (!ret) {
          auto *native = static_cast<ObInnerSQLResult *>(result.get_result());
          const auto *fields = native->result_set().get_field_columns();
          if (!fields) { return OB_ERR_UNEXPECTED; }
          Frame metadata('m'); metadata.number(fields->count());
          for (int64_t i = 0; i < fields->count(); ++i) { metadata.string(fields->at(i).cname_); }
          ret = worker_send(metadata);
          while (!ret && !(ret = native->next())) {
            const ObNewRow *row = native->get_row();
            if (!row) { ret = OB_ERR_UNEXPECTED; break; }
            Frame values('r'); values.number(row->get_count());
            for (int64_t i = 0; i < row->get_count(); ++i) { values.append(row->get_cell(i)); }
            ret = worker_send(values);
          }
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
          const int close_ret = native->close();
          if (!ret) { ret = close_ret; }
        }
      }
    } else if (!ret && operation == 'B') {
      const bool snapshot = input.number() != 0;
      ret = input.consumed() ? connection->start_transaction(snapshot) : OB_INVALID_ARGUMENT;
    } else if (!ret && operation == 'C') {
      ret = input.consumed() ? connection->commit() : OB_INVALID_ARGUMENT;
    } else if (!ret && operation == 'X') {
      ret = input.consumed() ? connection->rollback() : OB_INVALID_ARGUMENT;
    } else if (!ret && operation == 'S') {
      const ObString name = input.string(); ObObj value; input.read(value);
      if (!input.consumed()) { ret = OB_INVALID_ARGUMENT; }
      else { ret = value.is_int() ? connection->set_session_variable(name, value.get_int())
                                 : connection->set_session_variable(name, value.get_string()); }
    } else if (!ret) { ret = OB_NOT_SUPPORTED; }
    if (!ret && operation != 'R') { Frame result('o'); result.number(affected); ret = worker_send(result); }
    fprintf(stderr, "PROTOTYPE_V18_INNER_EXECUTE session=%u operation=%c ret=%d\n", owner.session.get_server_sid(), char(operation), ret);
    return ret;
  };
  struct Job {
    Frame input;
    std::shared_ptr<PendingRequest> request;
    std::shared_ptr<SessionOwner> owner;
  };
  RequestRoutes requests;
  std::mutex jobs_mutex;
  std::condition_variable jobs_changed;
  std::deque<Job> jobs;
  bool stopping = false;
  auto complete = [&](const std::shared_ptr<PendingRequest> &request, int result, SessionOwner *owner = nullptr) {
    if (owner && !result) { result = request->status(); }
    Frame done('D'); done.tag(request->tag); done.number(result);
    if (owner) {
      if (append_session_state(owner->session, done)) { std::_Exit(1); }
      owner->running = false;
    }
    // Release before publishing D: the peer may immediately reuse this slot.
    requests.release(request->tag);
    fprintf(stderr, "PROTOTYPE_V13_DONE request=%llu generation=%llu ret=%d\n",
        (unsigned long long)request->tag.slot, (unsigned long long)request->tag.generation, result);
    if (worker_send_wire(std::move(done))) { std::_Exit(1); }
  };
  auto execute_job = [&](Job &job) -> int {
    if (!job.request->call_trace.is_valid()) { job.request->call_trace.init(self_addr_); }
    ObTraceIdGuard trace_guard(job.request->call_trace);
    struct CallTraceScope {
      const ObCurTraceId::TraceId *previous = worker_call_trace;
      explicit CallTraceScope(const ObCurTraceId::TraceId &trace) { worker_call_trace = &trace; }
      ~CallTraceScope() { worker_call_trace = previous; }
    } call_trace_scope(job.request->call_trace);
    lib::Worker *previous_worker = &THIS_WORKER;
    PendingRequest *previous_request = worker_request;
    RequestWorker request_worker;
    lib::Worker::set_worker_to_thread_local(&request_worker);
      worker_request = job.request.get();
      worker_request->sql_session = job.owner ? &job.owner->session : nullptr;
      THIS_WORKER.set_timeout_ts(job.request->deadline);
      Frame &input = job.input;
      int result = OB_SUCCESS;
      if (input.type() == 'H') {
        // Exercise a worker-originated route with no gateway SQL exchange
        // serving its storage frames. Used by the direct-ingress regression.
        SessionBinding *binding = nullptr;
        result = begin_direct_request(1, binding);
        Frame schema;
        if (!result) { result = fetch_catalog('d', worker_namespace, ObString::make_string("oceanbase"), OB_INVALID_VERSION, schema); }
        const int finish_ret = finish_direct_request();
        if (!result) { result = finish_ret; }
        close_session(binding);
        worker_request = job.request.get();
        fprintf(stderr, "PROTOTYPE_V19_DIRECT_STORAGE_PROBE ns=%llu ret=%d\n", (unsigned long long)worker_namespace, result);
      } else if (input.type() == 'A' || input.type() == 'a') {
        const uint64_t sid = input.number(), capabilities = input.number();
        auto owner = std::make_shared<SessionOwner>();
        result = input.ret || sid == 0 || sid > UINT32_MAX || capabilities > UINT32_MAX
            ? OB_INVALID_ARGUMENT : initialize(*owner, sid, capabilities, &input, input.type() == 'a');
        if (!result) {
          Frame opened('a');
          {
            std::lock_guard<std::mutex> guard(sessions_mutex);
            uint64_t index;
            if (free_slots.empty()) { index = slots.size(); slots.emplace_back(); }
            else { index = free_slots.back(); free_slots.pop_back(); }
            slots[index].owner = owner; ++active_sessions;
            opened.number(index); opened.number(slots[index].generation);
            fprintf(stderr, "PROTOTYPE_V11_SESSION_OPEN ns=%llu slot=%llu generation=%llu active=%llu slots=%zu capacity=%zu\n",
                (unsigned long long)worker_namespace, (unsigned long long)index,
                (unsigned long long)slots[index].generation, (unsigned long long)active_sessions, slots.size(), slots.capacity());
          }
          result = worker_send(opened);
        }
      } else if (input.type() == 'I') {
        result = execute_inner(*job.owner, input);
      } else {
        const int64_t deadline = static_cast<int64_t>(input.number());
        ObString sql = input.string();
        std::string use_statement;
        if (!input.consumed()) { result = OB_INVALID_ARGUMENT; }
        if (!result && input.type() == 'U') {
          use_statement = "USE `";
          for (int64_t i = 0; i < sql.length(); ++i) {
            if (sql[i] == '`') { use_statement.push_back('`'); }
            use_statement.push_back(sql[i]);
          }
          use_statement.push_back('`');
          sql.assign_ptr(use_statement.data(), use_statement.size());
        }
        fprintf(stderr, "PROTOTYPE_V12_EXECUTE_BEGIN request=%llu generation=%llu session=%u\n",
            (unsigned long long)job.request->tag.slot, (unsigned long long)job.request->tag.generation,
            job.owner->session.get_server_sid());
        if (!result) { result = execute(*job.owner, sql, deadline); }
        fprintf(stderr, "PROTOTYPE_V12_EXECUTE_END request=%llu generation=%llu session=%u ret=%d\n",
            (unsigned long long)job.request->tag.slot, (unsigned long long)job.request->tag.generation,
            job.owner->session.get_server_sid(), result);
      }
      complete(job.request, result, job.owner.get());
    worker_request = previous_request;
    lib::Worker::set_worker_to_thread_local(previous_worker);
    return OB_SUCCESS;
  };
  auto run_job = [&](Job &job) {
    // A cooperative nested job can enter from deep inside native SQL. Reuse
    // native stack extension before constructing another request context.
    const int error = SMART_CALL_LARGE(execute_job(job));
    if (error) { complete(job.request, error, job.owner.get()); }
  };
  PrototypeThreads executors(concurrency, [&] {
    lib::set_thread_name("NsSQLExecute");
    int depth = 0;
    if (worker_namespace == 1) {
      // Native internal callers can start another query before closing a streamed
      // result. Only nest work from that call chain: unrelated SQL may need a lock
      // held by the suspended caller and prevent its ready reply from being read.
      // The pipe reader still only dispatches; no polling or extra thread.
      worker_wait = [&](PendingRequest &waiting, bool credit, bool draining) {
        auto ready = [&] {
          std::lock_guard<std::mutex> guard(waiting.mutex);
          return waiting.error || (credit ? waiting.credit : bool(waiting.incoming || waiting.terminal))
              || (!draining && waiting.status());
        };
        std::unique_lock<std::mutex> lock(jobs_mutex);
        while (!stopping && !ready()) {
          auto internal = std::find_if(jobs.begin(), jobs.end(), [](const Job &job) {
            return (job.input.type() == 'a' || job.input.type() == 'I')
                && worker_call_trace && job.request->call_trace == *worker_call_trace;
          });
          if (internal != jobs.end()) {
            {
              Job nested = std::move(*internal); jobs.erase(internal);
              lock.unlock();
              if (depth >= 8) { complete(nested.request, OB_SIZE_OVERFLOW, nested.owner.get()); }
              else { ++depth; run_job(nested); --depth; }
              // Releasing the last session owner can issue storage RPCs and
              // reenter this wait loop. Keep destruction outside jobs_mutex.
            }
            lock.lock();
          } else if (draining || waiting.deadline == INT64_MAX) {
            jobs_changed.wait(lock);
          } else {
            jobs_changed.wait_until(lock,
                std::chrono::system_clock::time_point(std::chrono::microseconds(waiting.deadline)));
          }
        }
      };
    }
    for (;;) {
      Job job;
      {
        std::unique_lock<std::mutex> lock(jobs_mutex);
        jobs_changed.wait(lock, [&] { return stopping || !jobs.empty(); });
        if (stopping) { break; }
        job = std::move(jobs.front()); jobs.pop_front();
      }
      run_job(job);
    }
    worker_wait = {};
  });
  if ((ret = executors.start())) { return ret; }
  fprintf(stderr, "PROTOTYPE_V12_EXECUTORS count=%lld max_requests=%zu\n", (long long)concurrency, MAX_REQUESTS);
  const bool direct_listener = std::getenv("SEEKDB_NAMESPACE_SQL_WORKER_LISTEN") != nullptr;
  if (direct_listener) {
    WORKER_STEP(server_runtime_controller_.init_sql_worker_runtime());
    bind_server_service<omt::ObServerRuntimeController>(&server_runtime_controller_);
    WORKER_STEP(conn_res_mgr_.init(schema_service_, server_gtimer_));
    session_mgr_.bind_lifecycle_services(*mods_ps_cache_, debug_sync_broadcaster_, conn_res_mgr_);
    WORKER_STEP(server_gtimer_.schedule(session_mgr_, ObSQLSessionMgr::SCHEDULE_PERIOD, true));
    config_.mysql_port_mode.set_value("random");
    config_.sql_net_thread_count.set_value("1");
    WORKER_STEP(net_frame_.init());
    WORKER_STEP(net_frame_.start());
    if (ret) { std::_Exit(1); }
  }
  Frame ready('Y'); ready.number(worker_namespace);
  if (direct_listener) { ready.number(obmysql::get_sql_nio_bound_tcp_port()); }
  // Publish the same native serving state as ObServer::start(). SQL executors
  // use it to distinguish an asynchronous DDL wait from server shutdown.
  prepare_stop_ = false;
  stop_ = false;
  has_stopped_ = false;
  ret = worker_send_wire(ready);
  while (!ret) {
    Frame input;
    if ((ret = worker_read_wire(input))) { break; }
    const RequestTag tag = input.tag();
    if (tag.slot & WORKER_REQUEST) {
      auto request = worker_storage_routes.find(tag);
      if (!request || (input.type() != 'K' && input.type() != 'l' && input.type() != 'c'
          && input.type() != 's' && input.type() != 'w' && input.type() != 'g')) { ret = OB_INVALID_ARGUMENT; break; }
      ret = request->post(std::move(input));
      if (worker_namespace == 1) { std::lock_guard<std::mutex> guard(jobs_mutex); jobs_changed.notify_all(); }
      continue;
    }
    if (input.ret || !tag.generation || tag.slot >= MAX_REQUESTS) { ret = OB_INVALID_ARGUMENT; break; }
    if (input.type() == 'Z') {
      const int reason = static_cast<int>(input.number());
      if (!input.consumed() || (reason != OB_TIMEOUT && reason != OB_ERR_QUERY_INTERRUPTED)) {
        ret = OB_INVALID_ARGUMENT; break;
      }
      auto request = requests.find(tag);
      if (!request || !request->cancellable) { continue; }
      request->cancel(reason);
      std::optional<Job> queued;
      {
        std::lock_guard<std::mutex> guard(jobs_mutex);
        for (auto it = jobs.begin(); it != jobs.end(); ++it) {
          if (it->request == request) { queued = std::move(*it); jobs.erase(it); break; }
        }
      }
      // Queued cancellation must not wait for an execution thread to become free.
      if (queued) { complete(request, reason, queued->owner.get()); }
      jobs_changed.notify_all();
      continue;
    }
    if (input.type() == 'K' || input.type() == 'c' || input.type() == 's' || input.type() == 'w' || input.type() == 'g') {
      auto request = requests.find(tag);
      if (request) { ret = request->post(std::move(input)); }
      if (worker_namespace == 1) { std::lock_guard<std::mutex> guard(jobs_mutex); jobs_changed.notify_all(); }
      continue;
    }
    auto request = requests.accept(tag);
    if (!request) { ret = OB_INVALID_ARGUMENT; break; }
    if (input.type() == 'a' || input.type() == 'I') {
      input.read(request->call_trace);
      if (input.ret || !request->call_trace.is_valid()) { ret = OB_INVALID_ARGUMENT; break; }
    }
    if (input.type() == 'P') { complete(request, input.consumed() ? OB_SUCCESS : OB_INVALID_ARGUMENT); continue; }
    int result = OB_SUCCESS;
    std::shared_ptr<SessionOwner> owner;
    if (input.type() == 'Q' || input.type() == 'U' || input.type() == 'C' || input.type() == 'I') {
      const uint64_t index = input.number(), generation = input.number();
      {
        std::lock_guard<std::mutex> guard(sessions_mutex);
        if (input.ret || index >= slots.size() || generation != slots[index].generation || !slots[index].owner) {
          result = OB_ERR_SESSION_INTERRUPTED;
        } else if (input.type() == 'C') {
          if (!input.consumed()) { result = OB_INVALID_ARGUMENT; }
          else {
            owner = std::move(slots[index].owner); --active_sessions;
            if (slots[index].generation != UINT64_MAX) { ++slots[index].generation; free_slots.push_back(index); }
            fprintf(stderr, "PROTOTYPE_V11_SESSION_CLOSE ns=%llu slot=%llu generation=%llu active=%llu slots=%zu\n",
                (unsigned long long)worker_namespace, (unsigned long long)index,
                (unsigned long long)generation, (unsigned long long)active_sessions, slots.size());
          }
        } else if (slots[index].owner->running.exchange(true)) { result = OB_EAGAIN; }
        else { owner = slots[index].owner; }
      }
      if (input.type() == 'C') {
        // An admitted query owns a reference; closing/reusing its slot cannot
        // destroy the old session until that query has finished using it.
        owner.reset(); complete(request, result); continue;
      }
    } else if (input.type() != 'A' && input.type() != 'a' && input.type() != 'H') { result = OB_NOT_SUPPORTED; }
    if (!result && owner) {
      const int64_t saved = input.pos;
      const uint64_t deadline = input.number();
      if (input.ret || !deadline || deadline > INT64_MAX) { result = OB_INVALID_ARGUMENT; }
      else { request->deadline = static_cast<int64_t>(deadline); request->cancellable = true; }
      input.pos = saved;
      if (!result) { result = request->status(); }
    }
    if (result) { complete(request, result, owner.get()); continue; }
    {
      std::lock_guard<std::mutex> guard(jobs_mutex);
      jobs.push_back(Job{std::move(input), request, owner});
    }
    jobs_changed.notify_all();
  }
  prepare_stop_ = true;
  stop_ = true;
  requests.fail();
  worker_storage_routes.fail();
  // The worker is a process lifetime boundary. Stop it before unwinding the
  // SQL/storage adapters that its native network tasks can still reference.
  if (direct_listener) { std::_Exit(ret == OB_SUCCESS ? 0 : 1); }
  { std::lock_guard<std::mutex> guard(jobs_mutex); stopping = true; }
  jobs_changed.notify_all(); executors.stop(); executors.wait();
  has_stopped_ = true;

#undef WORKER_STEP
  return ret;
}
} }
