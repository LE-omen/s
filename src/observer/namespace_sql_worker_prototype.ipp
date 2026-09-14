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
#include <memory>
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "observer/mysql/ob_query_driver.h"

namespace oceanbase { namespace observer {
int ObServer::namespace_sql_worker_prototype(const char *query)
{
  using namespace sql;
  using namespace common;
  using namespace share;
  int ret = OB_SUCCESS;
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
  self_addr_.set_ip_addr("127.0.0.1", 1);
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
  WORKER_STEP(ObTimerService::get_instance().start());
  WORKER_STEP(init_config_module(""));
  WORKER_STEP(init_tz_info_mgr());
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
  WORKER_STEP(storage::ObLobManager::server_module_new(mods_lob_manager_));
  bind_server_service<ObSQLSessionMgr>(&session_mgr_);
  bind_server_service<ObPlanCache>(mods_plan_cache_);
  bind_server_service<ObPsCache>(mods_ps_cache_);
  bind_server_service<ObSqlMemoryManager>(mods_sql_memory_manager_);
  bind_server_service<ObOptStatMonitorManager>(mods_opt_stat_monitor_manager_);
  bind_server_service<ObDataAccessService>(mods_data_access_service_);
  WORKER_STEP(ObOptStatMonitorManager::server_module_init(mods_opt_stat_monitor_manager_));
  WORKER_STEP(ObPlanCache::server_module_init(mods_plan_cache_, *this));
  WORKER_STEP(ObPsCache::server_module_init(mods_ps_cache_));
  WORKER_STEP(ObSqlMemoryManager::server_module_init(mods_sql_memory_manager_));
  WORKER_STEP(ObOptStatManager::get_instance().init(&sql_proxy_, &config_));
  WORKER_STEP(sql_engine_.init(&ObOptStatManager::get_instance(), &vt_data_service_,
      self_addr_, *mods_plan_cache_, *mods_ps_cache_, pl_engine_, *this, *this,
      local_management_service_, ob_service_, *this, *this, *this,
      *mods_srs_service_, *mods_lob_manager_));
  gctx_.status_ = SS_SERVING;
  g_server_modules_ready = OB_SUCC(ret);
  using namespace namespace_worker_prototype;
  if (ret != OB_SUCCESS) { return ret; }
  const bool serve = query[0] == '@';
  RemoteTabletScan remote_scan;
  RemoteTransactionService remote_transactions;
  RemoteDmlService remote_dml;
  RemoteWriteContext remote_write_context;
  if (serve) {
    bind_server_service<ObITabletScan>(&remote_scan);
    bind_server_service<data_plane::ObITransactionService>(&remote_transactions);
    bind_server_service<data_plane::ObIDmlService>(&remote_dml);
    bind_server_service<data_plane::ObIWriteContextService>(&remote_write_context);
    char *end = nullptr;
    worker_namespace = std::strtoull(query + 1, &end, 10);
    if (!end || *end || worker_namespace <= 1 || worker_namespace >= (1ULL << 30)) { return OB_INVALID_ARGUMENT; }
    worker_catalog_fetch = fetch_catalog;
  }
  struct SessionOwner {
    ObArenaAllocator allocator{ObMemAttr("NsSQLSession")};
    ObSQLSessionInfo session; // Destroyed before its allocator.
    std::atomic<bool> running{false};
  };
  struct SessionSlot {
    std::shared_ptr<SessionOwner> owner;
    uint64_t generation = 1;
  };
  std::mutex sessions_mutex;
  std::vector<SessionSlot> slots;
  std::vector<uint64_t> free_slots;
  uint64_t active_sessions = 0;
  auto initialize = [&](SessionOwner &owner, uint32_t sid, Frame *state) -> int {
    int ret = OB_SUCCESS;
    ObSQLSessionInfo &session = owner.session;
    WORKER_STEP(session.test_init(1, sid, &owner.allocator));
    WORKER_STEP(session.load_default_sys_variable(false, false));
    WORKER_STEP(session.set_user(ObString::make_string("root"), ObString::make_string("%"), OB_SYS_USER_ID));
    session.set_user_priv_set(OB_PRIV_SELECT | OB_PRIV_INSERT | OB_PRIV_UPDATE | OB_PRIV_DELETE);
    session.set_session_manager(&session_mgr_);
    if (!ret && state) {
      ret = apply_session_state(session, *state);
      if (!ret && !state->consumed()) { ret = OB_INVALID_ARGUMENT; }
      const uint64_t db = session.get_database_id();
      const share::schema::ObDatabaseSchema *database = nullptr;
      if (!ret && db != OB_INVALID_ID) {
        if (((db & ~(1ULL << 62)) >> 32) != worker_namespace) { ret = OB_INVALID_ARGUMENT; }
        else { ret = storage::NamespaceForkKernelPrototype::database_by_id(db, database); }
        if (!ret && !database) { ret = OB_ERR_BAD_DATABASE; }
        if (!ret) { ret = session.set_default_database(database->get_database_name_str()); }
      }
    }
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
  auto execute = [&](SessionOwner &owner, const ObString &text, uint64_t snapshot, int64_t deadline) -> int {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObMemAttr("WorkerProto"));
    ObSQLSessionInfo *session = &owner.session;
    session->set_query_start_time(ObTimeUtility::current_time());
    THIS_WORKER.set_timeout_ts(deadline);
    THIS_WORKER.set_session(session);
    ret = THIS_WORKER.check_status();
    struct FinishRequest {
      ObSQLSessionInfo &session;
      ~FinishRequest() {
        session.reset_query_string(); session.reset_warnings_buf(); session.set_session_sleep();
        THIS_WORKER.set_session(nullptr);
      }
    } finish{*session};
    // Reject commands outside the SELECT/session/simple autocommit DML slice.
    ObParser parser(allocator, session->get_sql_mode(), session->get_charsets4parser());
    ObSEArray<ObString, 2> statements;
    ObMPParseStat parse_stat;
    WORKER_STEP(parser.split_multiple_stmt(text, statements, parse_stat));
    if (!ret && (parse_stat.parse_fail_ || statements.count() != 1)) { ret = OB_NOT_SUPPORTED; }
    ParseResult parsed;
    WORKER_STEP(parser.parse(text, parsed));
    if (!ret) {
      const ParseNode *node = parsed.result_tree_;
      if (node && node->type_ == T_STMT_LIST && node->num_child_ == 1) { node = node->children_[0]; }
      if (!node || (node->type_ != T_SELECT && node->type_ != T_INSERT && node->type_ != T_UPDATE
                    && node->type_ != T_DELETE && node->type_ != T_VARIABLE_SET
                    && node->type_ != T_USE_DATABASE)) { ret = OB_NOT_SUPPORTED; }
      if (!ret && node->type_ == T_INSERT && (node->num_child_ != 4 || node->children_[3])) { ret = OB_NOT_SUPPORTED; }
      // IGNORE needs additional savepoint operations; keep it outside this slice.
      if (!ret && node->type_ == T_UPDATE && (node->num_child_ != 11 || node->children_[8])) { ret = OB_NOT_SUPPORTED; }
      if (!ret && node->type_ == T_DELETE && (node->num_child_ != 10 || node->children_[9])) { ret = OB_NOT_SUPPORTED; }
      std::vector<const ParseNode *> pending;
      if (!ret) { pending.push_back(node); }
      while (!ret && !pending.empty()) {
        const ParseNode *part = pending.back(); pending.pop_back();
        if (part->type_ == T_INTO_OUTFILE || part->type_ == T_INTO_DUMPFILE || part->type_ == T_INTO_VARIABLES) {
          ret = OB_NOT_SUPPORTED;
        }
        for (int i = 0; !ret && i < part->num_child_; ++i) {
          if (part->children_[i]) { pending.push_back(part->children_[i]); }
        }
      }
    }
    share::schema::ObSchemaGetterGuard guard;
    WORKER_STEP(schema_service_.get_runtime_schema_guard(guard));
    ObSqlCtx context;
    context.session_info_ = session; context.schema_guard_ = &guard;
    std::unique_ptr<ObResultSet> result(new ObResultSet(*session, allocator, *this));
    WORKER_STEP(result->init());
    WORKER_STEP(sql_engine_.stmt_query(text, context, *result));
    if (!ret && result->get_stmt_type() == stmt::T_SELECT) {
      if (!result->get_physical_plan() || !result->get_physical_plan()->is_plain_select()) { ret = OB_NOT_SUPPORTED; }
    } else if (!ret && result->get_stmt_type() == stmt::T_INSERT) {
      if (!serve || !result->get_physical_plan() || !result->get_physical_plan()->is_plain_insert()) { ret = OB_NOT_SUPPORTED; }
    } else if (!ret && (result->get_stmt_type() == stmt::T_UPDATE || result->get_stmt_type() == stmt::T_DELETE)) {
      if (!serve || !result->get_physical_plan()) { ret = OB_NOT_SUPPORTED; }
    } else if (!ret && result->get_stmt_type() == stmt::T_VARIABLE_SET) {
      auto *command = static_cast<ObVariableSetStmt *>(result->get_cmd());
      if (!command || command->has_global_variable()) { ret = OB_NOT_SUPPORTED; }
      for (int64_t i = 0; !ret && i < command->get_variables_size(); ++i) {
        ObVariableSetStmt::VariableSetNode node;
        ret = command->get_variable_node(i, node);
        if (!ret && !node.set_names_stmt_) {
          if (node.set_scope_ != ObSetVar::SET_SCOPE_SESSION
              || (node.value_expr_ && node.value_expr_->has_flag(CNT_SUB_QUERY))) { ret = OB_NOT_SUPPORTED; }
          // Keep explicit transactions and global/storage-affecting SET outside this slice.
          if (node.is_system_variable_ && node.variable_name_.case_compare("sql_mode") != 0
              && node.variable_name_.case_compare("ob_query_timeout") != 0
              && node.variable_name_.case_compare("character_set_client") != 0
              && node.variable_name_.case_compare("character_set_connection") != 0
              && node.variable_name_.case_compare("character_set_results") != 0
              && node.variable_name_.case_compare("collation_connection") != 0) { ret = OB_NOT_SUPPORTED; }
        }
      }
    } else if (!ret && result->get_stmt_type() == stmt::T_USE_DATABASE) {
      auto *command = static_cast<ObUseDatabaseStmt *>(result->get_cmd());
      if (!command || ((static_cast<uint64_t>(command->get_db_id()) & ~(1ULL << 62)) >> 32) != worker_namespace) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (!ret) { ret = OB_NOT_SUPPORTED; }
    if (!ret && serve && result->get_stmt_type() == stmt::T_SELECT) {
      SCN scn; scn.convert_for_tx(snapshot);
      result->get_exec_context().get_das_ctx().get_snapshot().init_weak_read(scn);
    }
    WORKER_STEP(result->open());
    const bool with_rows = result->is_with_rows();
    if (serve) {
      Frame state('S'); state.number(with_rows); state.number(result->get_affected_rows());
      state.number(result->get_warning_count());
      int state_ret = append_session_state(*session, state);
      if (!state_ret) { state_ret = worker_send(state); }
      if (state_ret) { ret = state_ret; }
    }
    const ColumnsFieldIArray *fields = result->get_field_columns();
    if (!ret && serve && with_rows) {
      Frame header('H'); header.number(fields ? fields->count() : 0);
      if (!fields || fields->count() > 64) { ret = OB_NOT_SUPPORTED; }
      for (int64_t i = 0; !ret && i < fields->count(); ++i) { header.append(fields->at(i)); }
      if (!ret) { ret = worker_send(header); }
    }
    const ObNewRow *row = nullptr;
    while (!ret && with_rows && OB_SUCCESS == (ret = result->get_next_row(row))) {
      if (serve) {
        Frame output('R'); output.number(row->get_count());
        for (int64_t i = 0; i < row->get_count(); ++i) {
          ObObj value = row->get_cell(i);
          if (!value.is_null() && !ob_is_numeric_type(value.get_type()) && !ob_is_string_type(value.get_type())) { ret = OB_NOT_SUPPORTED; break; }
          // REPEAT and other large string expressions return LOB values with an
          // internal header. Reuse the normal MySQL result conversion before IPC.
          if (value.is_lob()) {
            ret = ObQueryDriver::process_lob_locator_results(value, &allocator, session, &result->get_exec_context());
          }
          if (!ret) { output.append(value); }
        }
        if (!ret) { ret = worker_send(output); }
      } else {
        char row_text[4096]; row->to_string(row_text, sizeof(row_text)); fprintf(stdout, "row: %s\n", row_text);
      }
    }
    if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    // close(int &) reports to the caller; set_errcode supplies rollback's input.
    result->set_errcode(ret);
    const int close_ret = result->close(ret);
    if (!ret) { ret = close_ret; }
    fprintf(stderr, "PROTOTYPE_V12_EXECUTE_END request=%llu generation=%llu session=%u ret=%d\n",
        (unsigned long long)(worker_request ? worker_request->tag.slot : 0),
        (unsigned long long)(worker_request ? worker_request->tag.generation : 0), session->get_server_sid(), ret);
    return ret;
  };
  if (!serve) {
    SessionOwner owner;
    ret = initialize(owner, 1, nullptr);
    int64_t timeout = 0;
    if (!ret) { ret = owner.session.get_query_timeout(timeout); }
    RequestWorker request_worker;
    lib::Worker::set_worker_to_thread_local(&request_worker);
    if (!ret) { ret = execute(owner, ObString::make_string(query), 0, ObTimeUtility::current_time() + timeout); }
    lib::Worker::set_worker_to_thread_local(&worker);
    return ret;
  }
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
  PrototypeThreads executors(concurrency, [&] {
    lib::set_thread_name("NsSQLExecute");
    lib::Worker *previous = &THIS_WORKER;
    RequestWorker request_worker;
    lib::Worker::set_worker_to_thread_local(&request_worker);
    for (;;) {
      Job job;
      {
        std::unique_lock<std::mutex> lock(jobs_mutex);
        jobs_changed.wait(lock, [&] { return stopping || !jobs.empty(); });
        if (stopping) { break; }
        job = std::move(jobs.front()); jobs.pop_front();
      }
      worker_request = job.request.get();
      THIS_WORKER.set_timeout_ts(job.request->deadline);
      Frame &input = job.input;
      int result = OB_SUCCESS;
      if (input.type() == 'A') {
        const uint64_t sid = input.number();
        auto owner = std::make_shared<SessionOwner>();
        result = input.ret || sid == 0 || sid > UINT32_MAX ? OB_INVALID_ARGUMENT : initialize(*owner, sid, &input);
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
      } else {
        const uint64_t snapshot = input.number();
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
        if (!result) { result = execute(*job.owner, sql, snapshot, deadline); }
      }
      complete(job.request, result, job.owner.get());
      worker_request = nullptr;
      THIS_WORKER.set_timeout_ts(INT64_MAX);
    }
    lib::Worker::set_worker_to_thread_local(previous);
  });
  if ((ret = executors.start())) { return ret; }
  fprintf(stderr, "PROTOTYPE_V12_EXECUTORS count=%lld max_requests=%zu\n", (long long)concurrency, MAX_REQUESTS);
  Frame ready('Y'); ready.number(worker_namespace); ret = worker_send_wire(ready);
  while (!ret) {
    Frame input;
    if ((ret = worker_read_wire(input))) { break; }
    const RequestTag tag = input.tag();
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
      continue;
    }
    if (input.type() == 'K' || input.type() == 'c' || input.type() == 's' || input.type() == 'w') {
      auto request = requests.find(tag);
      if (request) { ret = request->post(std::move(input)); }
      continue;
    }
    auto request = requests.accept(tag);
    if (!request) { ret = OB_INVALID_ARGUMENT; break; }
    if (input.type() == 'P') { complete(request, input.consumed() ? OB_SUCCESS : OB_INVALID_ARGUMENT); continue; }
    int result = OB_SUCCESS;
    std::shared_ptr<SessionOwner> owner;
    if (input.type() == 'Q' || input.type() == 'U' || input.type() == 'C') {
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
    } else if (input.type() != 'A') { result = OB_NOT_SUPPORTED; }
    if (!result && owner) {
      const int64_t saved = input.pos;
      input.number(); // snapshot
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
    jobs_changed.notify_one();
  }
  requests.fail();
  { std::lock_guard<std::mutex> guard(jobs_mutex); stopping = true; }
  jobs_changed.notify_all(); executors.stop(); executors.wait();

#undef WORKER_STEP
  return ret;
}
} }
