// Throwaway V10 SQL-only composition. Included by ob_server.cpp so the prototype
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

namespace oceanbase { namespace observer {
int ObServer::namespace_sql_worker_prototype(const char *query)
{
  using namespace sql;
  using namespace common;
  using namespace share;
  int ret = OB_SUCCESS;
  lib::Worker worker;
  lib::Worker::set_worker_to_thread_local(&worker);
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg);
  OB_LOGGER.set_file_name("worker-prototype.log", true, false);
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_enable_async_log(false);
  const int64_t budget = 512L * 1024 * 1024;
  config_.memory_limit.set_value("512M");
  config_.cpu_count.set_value("1");
  config_.enable_async_syslog.set_value("false");
  config_._pushdown_storage_level.set_value("0");
  config_._rowsets_max_rows.set_value("32");
  config_.enable_sql_operator_dump.set_value("false");
  self_addr_.set_ip_addr("127.0.0.1", 1);
  lib::update_mini_mode(budget, 1);
  set_memory_budget(budget);
  g_bootstrap_server_runtime.init();
  g_bootstrap_server_runtime.set_memory_size(budget);
  g_bootstrap_server_runtime.set_min_cpu(1);
  g_bootstrap_server_runtime.set_max_cpu(1);
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
  if (serve) {
    bind_server_service<ObITabletScan>(&remote_scan);
    char *end = nullptr;
    worker_namespace = std::strtoull(query + 1, &end, 10);
    if (!end || *end || worker_namespace <= 1 || worker_namespace >= (1ULL << 30)) { return OB_INVALID_ARGUMENT; }
    worker_catalog_fetch = fetch_catalog;
  }
  auto execute = [&](const ObString &text, uint64_t db, uint64_t snapshot) -> int {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObMemAttr("WorkerProto"));
    std::unique_ptr<ObSQLSessionInfo> session(new ObSQLSessionInfo());
    WORKER_STEP(session->test_init(1, 1, &allocator));
    WORKER_STEP(session->load_default_sys_variable(false, false));
    WORKER_STEP(session->set_user(ObString::make_string("root"), ObString::make_string("%"), OB_SYS_USER_ID));
    session->set_user_priv_set(OB_PRIV_SELECT);
    session->set_query_start_time(ObTimeUtility::current_time());
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30L * 1000 * 1000);
    const share::schema::ObDatabaseSchema *database = nullptr;
    if (serve && db != OB_INVALID_ID) {
      WORKER_STEP(storage::NamespaceForkKernelPrototype::database_by_id(db, database));
      if (!ret && !database) { ret = OB_ERR_BAD_DATABASE; }
      if (!ret) { ret = session->set_default_database(database->get_database_name_str()); session->set_database_id(db); }
    }
    // Reject commands before resolution/execution. No local DDL or writes exist.
    ObParser parser(allocator, session->get_sql_mode(), session->get_charsets4parser());
    ParseResult parsed;
    WORKER_STEP(parser.parse(text, parsed));
    if (!ret) {
      const ParseNode *node = parsed.result_tree_;
      if (node && node->type_ == T_STMT_LIST && node->num_child_ == 1) { node = node->children_[0]; }
      if (!node || node->type_ != T_SELECT) { ret = OB_NOT_SUPPORTED; }
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
    context.session_info_ = session.get(); context.schema_guard_ = &guard;
    std::unique_ptr<ObResultSet> result(new ObResultSet(*session, allocator, *this));
    WORKER_STEP(result->init());
    WORKER_STEP(sql_engine_.stmt_query(text, context, *result));
    if (!ret && (!result->get_physical_plan() || !result->get_physical_plan()->is_plain_select())) { ret = OB_NOT_SUPPORTED; }
    if (!ret && serve) {
      SCN scn; scn.convert_for_tx(snapshot);
      result->get_exec_context().get_das_ctx().get_snapshot().init_weak_read(scn);
    }
    WORKER_STEP(result->open());
    const ColumnsFieldIArray *fields = result->get_field_columns();
    if (!ret && serve) {
      Frame header('H'); header.number(fields ? fields->count() : 0);
      if (!fields || fields->count() > 64) { ret = OB_NOT_SUPPORTED; }
      for (int64_t i = 0; !ret && i < fields->count(); ++i) { header.append(fields->at(i)); }
      if (!ret) { ret = worker_send(header); }
    }
    const ObNewRow *row = nullptr;
    while (!ret && OB_SUCCESS == (ret = result->get_next_row(row))) {
      if (serve) {
        Frame output('R'); output.number(row->get_count());
        for (int64_t i = 0; i < row->get_count(); ++i) {
          const ObObj &value = row->get_cell(i);
          if (!value.is_null() && !ob_is_numeric_type(value.get_type())) { ret = OB_NOT_SUPPORTED; break; }
          output.append(value);
        }
        if (!ret) { ret = worker_send(output); }
      } else {
        char row_text[4096]; row->to_string(row_text, sizeof(row_text)); fprintf(stdout, "row: %s\n", row_text);
      }
    }
    if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    const int close_ret = result->close(ret);
    if (!ret) { ret = close_ret; }
    fprintf(stderr, "worker query finished: %d\n", ret);
    return ret;
  };
  if (!serve) { return execute(ObString::make_string(query), OB_INVALID_ID, 0); }
  Frame ready('Y'); ready.number(worker_namespace); ret = worker_send(ready);
  while (!ret) {
    Frame input;
    if ((ret = worker_read(input))) { break; }
    if (input.type() == 'P') { ret = worker_send(Frame('P')); continue; }
    if (input.type() != 'Q') { ret = OB_INVALID_ARGUMENT; break; }
    const uint64_t db = input.number(), snapshot = input.number();
    const ObString sql = input.string();
    if (!input.consumed()) { ret = OB_INVALID_ARGUMENT; break; }
    const int query_ret = execute(sql, db, snapshot);
    Frame done('D'); done.number(query_ret); ret = worker_send(done);
  }
#undef WORKER_STEP
  return ret;
}
} }
