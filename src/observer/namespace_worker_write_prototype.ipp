// Throwaway V15: DAS stays in the SQL worker; transactions and writes stay here.
#include "data_plane/ob_i_dml_service.h"
#include "data_plane/ob_i_write_context_service.h"
#include "data_plane/access/ob_dml_table_plan.h"
#include "data_plane/blocksstable/ob_datum_row_iterator.h"
#include "data_plane/transaction/ob_i_transaction_service.h"
#include "storage/tx/ob_trans_define_v4.h"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace data_plane;
using namespace transaction;
using namespace blocksstable;

bool cleanup_write(Frame &request) {
  const int64_t position = request.pos;
  const uint64_t op = request.number();
  request.pos = position;
  return (request.type() == 'W' && op == 'X')
      || (request.type() == 'T' && (op == 'B' || op == 'R' || op == 'U' || op == 'E' || op == 'V'));
}
int write_rpc(Frame &request, Frame &reply) {
  int ret = worker_send(request, cleanup_write(request));
  if (!ret) { ret = worker_read(reply); }
  if (!ret && reply.type() != 'w') { ret = OB_INVALID_ARGUMENT; }
  if (!ret) { ret = static_cast<int>(reply.number()); }
  return ret ? ret : reply.ret;
}

struct EngineWrite {
  ObArenaAllocator allocator{ObMemAttr("NsRemoteWrite")};
  ObSchemaGetterGuard guard;
  ObDmlTablePlan plan{allocator};
  ObTimeZoneInfo timezone;
  ObWriteContext context;
  ObDmlExecution execution; // Released before context, plan and allocator.
  ObSEArray<uint64_t, 2> columns;
  ObTabletID tablet;
  const ObTableSchema *schema = nullptr;

  int prepare(uint64_t ns, ObTxDesc &tx, Frame &request) {
    const uint64_t table = request.number();
    ObDmlWriteSpec spec;
    spec.schema_version_ = request.number();
    spec.timeout_ = std::min<int64_t>(request.number(), THIS_WORKER.get_timeout_ts());
    spec.sql_mode_ = request.number();
    spec.branch_id_ = request.number();
    spec.is_total_quantity_log_ = request.number() != 0;
    spec.prelock_ = request.number() != 0;
    spec.is_batch_stmt_ = request.number() != 0;
    spec.is_main_table_in_fts_ddl_ = request.number() != 0;
    spec.check_schema_version_ = request.number() != 0;
    spec.access_vector_id_as_master_table_ = request.number() != 0;
    request.read(timezone); spec.tz_info_ = &timezone;
    ObTxReadSnapshot snapshot;
    concurrent_control::ObWriteFlag flag;
    request.read(snapshot); request.read(flag);
    const uint64_t count = request.number();
    if (request.ret || count == 0 || count > OB_MAX_COLUMN_NUMBER || !owns_table(ns, table)) { return OB_NOT_SUPPORTED; }
    int ret = storage_schema(ns, table, guard, schema);
    if (ret) { return ret; }
    if (!schema || schema->get_schema_version() != spec.schema_version_) { return OB_SCHEMA_EAGAIN; }
    for (uint64_t i = 0; !ret && i < count; ++i) {
      const uint64_t id = request.number();
      const auto *column = schema->get_column_schema(id);
      if (!column || has_exist_in_array(columns, id)) { ret = OB_INVALID_ARGUMENT; }
      else { ret = columns.push_back(id); }
    }
    if (ret || !request.consumed()) { return ret ? ret : OB_INVALID_ARGUMENT; }
    tablet = schema->get_tablet_id();
    ret = plan.build(schema, spec.schema_version_, columns);
    if (!ret) { ret = share::server_service<ObIWriteContextService>()->acquire_write_context(
        spec.timeout_, tx, snapshot, spec.branch_id_, flag, context); }
    if (!ret) { ret = share::server_service<ObIDmlService>()->prepare_execution(
        spec, plan, snapshot, allocator, context, flag, execution); }
    return ret;
  }

  int batch(char operation, ObTxDesc &tx, Frame &request, int64_t &affected, Frame &returned) {
    const uint64_t tablet_id = request.number(), count = request.number();
    const bool update = operation == 'U';
    if (request.ret || tablet_id != tablet.id() || count == 0
        || count > (update ? 64 : 32) || (update && count % 2)) { return OB_INVALID_ARGUMENT; }
    int64_t lock_timeout = 0;
    ObRowLockMode lock_mode = ObRowLockMode::NONE;
    if (operation == 'L') {
      lock_timeout = request.number(); lock_mode = static_cast<ObRowLockMode>(request.number());
      if (request.ret || (lock_mode != ObRowLockMode::NONE && lock_mode != ObRowLockMode::WRITE)) { return OB_INVALID_ARGUMENT; }
    }
    ObSEArray<uint64_t, 2> updated_columns;
    const uint64_t updated_count = request.number();
    if (request.ret || updated_count > uint64_t(columns.count())
        || (update || operation == 'f' ? updated_count == 0 : updated_count != 0)) { return OB_INVALID_ARGUMENT; }
    for (uint64_t i = 0; i < updated_count; ++i) {
      const uint64_t column = request.number();
      if (request.ret || !has_exist_in_array(columns, column)
          || has_exist_in_array(updated_columns, column)) { return OB_INVALID_ARGUMENT; }
      int ret = updated_columns.push_back(column);
      if (ret) { return ret; }
    }
    auto duplicate_mode = operation == 'f' ? static_cast<ObDuplicateReturnMode>(request.number()) : ObDuplicateReturnMode::ALL;
    if (duplicate_mode != ObDuplicateReturnMode::ALL && duplicate_mode != ObDuplicateReturnMode::ONE) { return OB_INVALID_ARGUMENT; }
    // Decode and validate a bounded batch before entering native storage.
    std::vector<ObObj> cells(count * columns.count());
    for (auto &cell : cells) {
      request.read(cell);
      if (request.ret) { return OB_INVALID_ARGUMENT; }
    }
    if (!request.consumed()) { return OB_INVALID_ARGUMENT; }
    class Rows final : public ObDatumRowIterator {
    public:
      std::vector<ObObj> &cells;
      int64_t width;
      size_t position = 0;
      // UPDATE's old row must remain alive while storage obtains its new row.
      ObDatumRow rows[2];
      Rows(std::vector<ObObj> &values, int64_t columns) : cells(values), width(columns) {}
      int get_next_row(ObDatumRow *&out) override {
        if (position == cells.size()) { return OB_ITER_END; }
        ObDatumRow &row = rows[(position / width) % 2];
        int ret = OB_SUCCESS;
        for (int64_t i = 0; !ret && i < width; ++i) { ret = row.storage_datums_[i].from_obj_enhance(cells[position++]); }
        row.row_flag_.set_flag(DF_INSERT); out = &row; return ret;
      }
    } rows(cells, columns.count());
    int ret = rows.rows[0].init(columns.count());
    if (!ret) { ret = rows.rows[1].init(columns.count()); }
    auto *service = share::server_service<ObIDmlService>();
    if (!ret && operation == 'f') {
      ObDatumRowIterator *duplicates = nullptr;
      ret = service->insert_rows_fetch_duplicates(tablet, tx, execution, columns, updated_columns,
                                                 &rows, duplicate_mode, affected, duplicates);
      struct ReleaseDuplicates { ObIDmlService *service; ObDatumRowIterator *rows;
        ~ReleaseDuplicates() { if (rows) { service->free_duplicate_rows_iterator(rows); } }
      } release{service, duplicates};
      if (!ret || ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
        const int storage_ret = ret;
        Frame values; int64_t count = 0; ret = OB_SUCCESS;
        ObDatumRow *row = nullptr;
        while (duplicates && !ret && !(ret = duplicates->get_next_row(row))) {
          if (!row || row->get_column_count() != updated_columns.count() || count >= 32) { ret = OB_SIZE_OVERFLOW; break; }
          for (int64_t i = 0; !ret && i < updated_columns.count(); ++i) {
            ObObj cell;
            ret = row->storage_datums_[i].to_obj_enhance(cell, schema->get_column_schema(updated_columns.at(i))->get_meta_type());
            if (!ret) { values.append(cell); ret = values.ret; }
          }
          ++count;
        }
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
        if (!ret) {
          returned.number(count);
          returned.data.insert(returned.data.end(), values.data.begin() + Frame::HEADER_SIZE, values.data.end());
          ret = storage_ret;
        }
      }
    } else if (!ret && update) { ret = service->update_rows(tablet, tx, execution, columns, updated_columns, &rows, affected); }
    else if (!ret && operation == 'D') { ret = service->delete_rows(tablet, tx, execution, columns, &rows, affected); }
    else if (!ret && operation == 'L') { ret = service->lock_rows(tablet, tx, execution, lock_timeout, lock_mode, &rows, affected); }
    else if (!ret && operation == 'p') { ret = service->put_rows(tablet, tx, execution, columns, &rows, affected); }
    else if (!ret) { ret = service->insert_rows(tablet, tx, execution, columns, &rows, affected); }
    fprintf(stderr, "PROTOTYPE_V15_WRITE_BATCH op=%c tx=%lld rows=%llu affected=%lld ret=%d\n",
        operation, (long long)tx.get_tx_id().get_id(), (unsigned long long)(update ? count / 2 : count), (long long)affected, ret);
    return ret;
  }
};

// The existing gateway session owns the real descriptor. Its native disconnect
// handling can interrupt it; no extra transaction registry or cleanup thread.
struct EngineWrites {
  uint64_t ns;
  sql::ObSQLSessionInfo &session;
  uint32_t sid;
  ObTxDesc *&tx;
  uint64_t sequence = 0;
  std::map<uint64_t, std::unique_ptr<EngineWrite>> writes;
  explicit EngineWrites(uint64_t namespace_id, sql::ObSQLSessionInfo &s)
      : ns(namespace_id), session(s), sid(s.get_server_sid()), tx(s.get_tx_desc()) {}
  int check_finished() const {
    return writes.empty() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  }
  void reset() {
    session.reset_reserved_snapshot_version();
    writes.clear();
    if (tx) {
      auto *service = query_transaction_service();
      const bool rollback = tx->is_in_tx() && !tx->is_tx_end();
      if (rollback) { service->rollback_tx(*tx); }
      service->release_tx(*tx);
      tx = nullptr;
      fprintf(stderr, "PROTOTYPE_V14_TX_RELEASED session=%u rollback=%d\n", sid, rollback);
    }
  }
  ~EngineWrites() { reset(); }
  int process(Frame &request, Frame &reply) {
    auto *service = query_transaction_service();
    const uint64_t operation = request.number();
    const uint64_t txid = request.number();
    int ret = request.ret;
    Frame values;
    if (!ret && !tx && request.type() == 'T' && (operation == 'A' || operation == 'S')) {
      ret = service->acquire_tx(tx, sid);
    }
    if (!ret && (!tx || static_cast<uint64_t>(tx->get_tx_id().get_id()) != txid)) { ret = OB_INVALID_ARGUMENT; }
    if (!ret && request.type() == 'T') {
      if (operation == 'A') {
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
      } else if (operation == 'V') {
        if (!request.consumed() || !writes.empty()) { ret = OB_INVALID_ARGUMENT; }
        else { reset(); }
      } else if (operation == 'H') {
        ObTxParam param; request.read(param);
        if (!request.consumed() || !param.is_valid()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->start_tx(*tx, param); }
      } else if (operation == 'S' || operation == 'N' || operation == 'U') {
        if (!request.consumed() || (operation != 'S' && !writes.empty())) { ret = OB_INVALID_ARGUMENT; }
        else if (operation == 'S') { ret = service->prepare_tx_for_statement(*tx); }
        else if (operation == 'N') { ret = service->prepare_tx_for_autocommit_retry(*tx); }
        else { ret = service->reuse_tx(*tx); }
      } else if (operation == 'P') {
        ObTxParam param; ObTxSEQ savepoint;
        request.read(param); const bool release = request.number() != 0;
        if (!request.consumed() || !param.is_valid()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->create_implicit_savepoint(*tx, param, savepoint, release); }
        values.append(savepoint);
      } else if (operation == 'G') {
        auto isolation = static_cast<ObTxIsolationLevel>(request.number());
        const int64_t deadline = request.number();
        ObTxReadSnapshot snapshot;
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->get_read_snapshot(*tx, isolation, std::min(deadline, THIS_WORKER.get_timeout_ts()), snapshot); }
        if (!ret && (!session.get_reserved_snapshot_version().is_valid()
            || snapshot.core_.version_ < session.get_reserved_snapshot_version())) {
          session.set_reserved_snapshot_version(snapshot.core_.version_);
        }
        values.append(snapshot);
      } else if (operation == 'C') {
        const int64_t deadline = request.number();
        if (!request.consumed() || !writes.empty()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->commit_tx(*tx, std::min(deadline, THIS_WORKER.get_timeout_ts())); }
        fprintf(stderr, "PROTOTYPE_V14_COMMIT session=%u tx=%llu ret=%d\n", sid, (unsigned long long)txid, ret);
      } else if (operation == 'R') {
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else if (!writes.empty()) { ret = OB_ERR_UNEXPECTED; }
        else { ret = service->rollback_tx(*tx); }
      } else if (operation == 'B') {
        ObTxSEQ savepoint; request.read(savepoint);
        const int64_t deadline = request.number();
        const bool touched = request.number() != 0;
        auto policy = static_cast<ObTxCleanPolicy>(request.number());
        if (!request.consumed() || (policy != FAST_ROLLBACK && policy != ROLLBACK && policy != KEEP)) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->rollback_to_implicit_savepoint(*tx, savepoint, deadline, touched, policy); }
      } else if (operation == 'J' || operation == 'I') {
        ObTxSEQ savepoint;
        const int16_t branch = operation == 'J' ? request.number() : 0;
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else if (operation == 'J') { ret = service->create_branch_savepoint(*tx, branch, savepoint); }
        else { ret = service->create_in_txn_implicit_savepoint(*tx, savepoint); }
        values.append(savepoint);
      } else if (operation == 'F' || operation == 'L' || operation == 'D' || operation == 'K') {
        const ObString name = request.string();
        const int64_t deadline = operation == 'L' ? request.number() : 0;
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else if (operation == 'F') { ret = service->create_explicit_savepoint(*tx, name); }
        else if (operation == 'L') { ret = service->rollback_to_explicit_savepoint(*tx, name, deadline); }
        else if (operation == 'D') { ret = service->release_explicit_savepoint(*tx, name); }
        else { ret = service->create_stash_savepoint(*tx, name); }
      } else if (operation == 'E') {
        ObTxExecResult result;
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->collect_tx_exec_result(*tx, result); }
        values.append(result);
      } else { ret = OB_NOT_SUPPORTED; }
    } else if (!ret && request.type() == 'W') {
      if (operation == 'P') {
        if (writes.size() >= 32) { ret = OB_SIZE_OVERFLOW; }
        auto prepared = std::make_unique<EngineWrite>();
        if (!ret) { ret = prepared->prepare(ns, *tx, request); }
        if (!ret) { writes.emplace(++sequence, std::move(prepared)); values.number(sequence); }
      } else {
        const uint64_t handle = request.number();
        auto it = writes.find(handle);
        if (request.ret || it == writes.end()) { ret = OB_INVALID_ARGUMENT; }
        else if (operation == 'X' && request.consumed()) { writes.erase(it); }
        else if (operation == 'I' || operation == 'U' || operation == 'D' || operation == 'L' || operation == 'p' || operation == 'f') {
          int64_t affected = 0; Frame returned;
          ret = it->second->batch(operation, *tx, request, affected, returned); values.number(affected);
          values.data.insert(values.data.end(), returned.data.begin() + Frame::HEADER_SIZE, returned.data.end());
        } else { ret = OB_INVALID_ARGUMENT; }
      }
    }
    reply = Frame('w'); reply.number(ret);
    if (!ret || (request.type() == 'W' && operation == 'f' && ret == OB_ERR_PRIMARY_KEY_DUPLICATE)) {
      if ((request.type() == 'T' && operation != 'V') || (request.type() == 'W' && operation == 'X')) { reply.append(*tx); }
      reply.data.insert(reply.data.end(), values.data.begin() + Frame::HEADER_SIZE, values.data.end());
      if (values.ret) { reply.ret = values.ret; }
    }
    fprintf(stderr, "PROTOTYPE_V14_RPC type=%c op=%c tx=%llu ret=%d wire=%d bytes=%zu\n",
        request.type(), static_cast<char>(operation), (unsigned long long)txid, ret, reply.ret, reply.data.size());
    return reply.ret;
  }
};

// Compatibility view for query's existing descriptor accessors. Constructing
// and decoding this value does not start a transaction service, register a
// transaction, or allocate a storage context in the worker. Engine owns all
// authoritative transaction state; this view is refreshed by transaction RPC.
int tx_rpc(char operation, ObTxDesc &tx, Frame &request, Frame &reply) {
  Frame message('T'); message.number(operation); message.number(tx.get_tx_id().get_id());
  message.data.insert(message.data.end(), request.data.begin() + Frame::HEADER_SIZE, request.data.end());
  message.ret = request.ret;
  int ret = write_rpc(message, reply);
  if (!ret) { reply.read(tx); ret = reply.ret; }
  return ret;
}

class RemoteTransactionService final : public ObITransactionService {
public:
  int acquire_tx(transaction::ObTxDesc *&tx,
                         uint32_t session_id) override {
    if (tx) { return OB_INVALID_ARGUMENT; }
    auto owned = std::make_unique<ObTxDesc>();
    Frame request, reply;
    int ret = tx_rpc('A', *owned, request, reply);
    if (!ret && !reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
    if (!ret) { tx = owned.release(); }
    return ret; }
  int acquire_tx(const char *buf,
                         int64_t len,
                         int64_t &pos,
                         transaction::ObTxDesc *&tx) override {
    if (tx) { return OB_INVALID_ARGUMENT; }
    auto owned = std::make_unique<ObTxDesc>();
    int ret = owned->deserialize(buf, len, pos);
    if (!ret) { tx = owned.release(); }
    return ret; }
  int start_tx(transaction::ObTxDesc &tx,
                       const transaction::ObTxParam &tx_param) override {
    Frame request, reply; request.append(tx_param); return tx_rpc('H', tx, request, reply); }
  int abort_tx(transaction::ObTxDesc &tx, int cause) override { return rollback_tx(tx); }
  int rollback_tx(transaction::ObTxDesc &tx) override { Frame request, reply; return tx_rpc('R', tx, request, reply); }
  int commit_tx(transaction::ObTxDesc &tx,
                        int64_t expire_ts) override { Frame request, reply; request.number(expire_ts); return tx_rpc('C', tx, request, reply); }
  int submit_commit_tx(transaction::ObTxDesc &tx,
                               int64_t expire_ts,
                               transaction::ObITxCallback &callback) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX submit_commit_tx\n"); return OB_NOT_SUPPORTED; }
  int release_tx(transaction::ObTxDesc &tx) override {
    int ret = OB_SUCCESS;
    if (worker_request) {
      Frame request('T'), reply; request.number('V'); request.number(tx.get_tx_id().get_id());
      ret = write_rpc(request, reply);
      if (!ret && !reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
    }
    delete &tx; return ret; }
  int reuse_tx(transaction::ObTxDesc &tx) override { Frame request, reply; return tx_rpc('U', tx, request, reply); }
  int prepare_tx_for_statement(transaction::ObTxDesc &tx) override { Frame request, reply; return tx_rpc('S', tx, request, reply); }
  int prepare_tx_for_autocommit_retry(transaction::ObTxDesc &tx) override { Frame request, reply; return tx_rpc('N', tx, request, reply); }
  int interrupt(transaction::ObTxDesc &tx, int cause) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX interrupt\n"); return OB_NOT_SUPPORTED; }
  int get_read_snapshot(transaction::ObTxDesc &tx,
                                transaction::ObTxIsolationLevel isolation_level,
                                int64_t expire_ts,
                                transaction::ObTxReadSnapshot &snapshot) override {
    Frame request, reply; request.number(static_cast<int>(isolation_level)); request.number(expire_ts);
    int ret = tx_rpc('G', tx, request, reply);
    if (!ret) { reply.read(snapshot); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
    return ret; }
  int get_read_snapshot_version(int64_t expire_ts,
                                        share::SCN &snapshot_version) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX get_read_snapshot_version\n"); return OB_NOT_SUPPORTED; }
  int get_weak_read_snapshot_version(int64_t max_read_stale_time,
                                             share::SCN &snapshot_version) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX get_weak_read_snapshot_version\n"); return OB_NOT_SUPPORTED; }
  int register_tx_snapshot_verify(
      transaction::ObTxReadSnapshot &snapshot) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX register_tx_snapshot_verify\n"); return OB_NOT_SUPPORTED; }
  int create_implicit_savepoint(transaction::ObTxDesc &tx,
                                        const transaction::ObTxParam &tx_param,
                                        transaction::ObTxSEQ &savepoint,
                                        bool release) override {
    Frame request, reply; request.append(tx_param); request.number(release);
    int ret = tx_rpc('P', tx, request, reply);
    if (!ret) { reply.read(savepoint); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
    return ret; }
  int create_branch_savepoint(transaction::ObTxDesc &tx,
                                      int16_t branch,
                                      transaction::ObTxSEQ &savepoint) override {
    Frame request, reply; request.number(branch); int ret = tx_rpc('J', tx, request, reply);
    if (!ret) { reply.read(savepoint); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
    return ret; }
  int create_in_txn_implicit_savepoint(transaction::ObTxDesc &tx,
                                               transaction::ObTxSEQ &savepoint) override {
    Frame request, reply; int ret = tx_rpc('I', tx, request, reply);
    if (!ret) { reply.read(savepoint); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
    return ret; }
  int create_explicit_savepoint(transaction::ObTxDesc &tx,
                                        const common::ObString &savepoint) override {
    Frame request, reply; request.string(savepoint); return tx_rpc('F', tx, request, reply); }
  int rollback_to_implicit_savepoint(
      transaction::ObTxDesc &tx,
      transaction::ObTxSEQ savepoint,
      int64_t expire_ts,
      bool touched_storage,
      transaction::ObTxCleanPolicy clean_policy) override {
    Frame request, reply; request.append(savepoint); request.number(expire_ts);
    request.number(touched_storage); request.number(clean_policy);
    return tx_rpc('B', tx, request, reply); }
  int rollback_to_explicit_savepoint(transaction::ObTxDesc &tx,
                                             const common::ObString &savepoint,
                                             int64_t expire_ts) override {
    Frame request, reply; request.string(savepoint); request.number(expire_ts); return tx_rpc('L', tx, request, reply); }
  int release_explicit_savepoint(transaction::ObTxDesc &tx,
                                         const common::ObString &savepoint) override {
    Frame request, reply; request.string(savepoint); return tx_rpc('D', tx, request, reply); }
  int create_stash_savepoint(transaction::ObTxDesc &tx,
                                     const common::ObString &name) override {
    Frame request, reply; request.string(name); return tx_rpc('K', tx, request, reply); }
  int merge_tx_state(transaction::ObTxDesc &to,
                             const transaction::ObTxDesc &from) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX merge_tx_state\n"); return OB_NOT_SUPPORTED; }
  int get_tx_exec_result(transaction::ObTxDesc &tx,
                                 transaction::ObTxExecResult &exec_info) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX get_tx_exec_result\n"); return OB_NOT_SUPPORTED; }
  int add_tx_exec_result(transaction::ObTxDesc &tx,
                                 const transaction::ObTxExecResult &exec_info) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX add_tx_exec_result\n"); return OB_NOT_SUPPORTED; }
  int collect_tx_exec_result(transaction::ObTxDesc &tx,
                                     transaction::ObTxExecResult &result) override {
    Frame request, reply; int ret = tx_rpc('E', tx, request, reply);
    if (!ret) { reply.read(result); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
    return ret; }
  bool can_elr() const override { return false; }
};

class RemoteWriteContext final : public ObIWriteContextService {
public:
  int acquire_write_context(int64_t, ObTxDesc &tx, const ObTxReadSnapshot &, int16_t,
                            concurrent_control::ObWriteFlag &, ObWriteContext &context) override {
    // Deferred acquisition is combined with prepare_execution in one RPC.
    context.bind(&tx, nullptr); return OB_SUCCESS;
  }
};

struct RemoteExecution final : public ObIDmlExecutionState {
  uint64_t handle = 0, txid = 0;
  ObTxDesc *tx = nullptr; // Borrowed query view; never sent across IPC.
  int64_t deadline = 0;
  std::vector<ObObjMeta> types;
  std::vector<uint64_t> columns;
  void destroy() override {
    if (handle) {
      Frame request('W'), reply; request.number('X'); request.number(txid); request.number(handle);
      int ret = write_rpc(request, reply);
      // Native revert_store_ctx merges write state into the engine descriptor
      // only when its write context is released. Refresh after that point so
      // SQL autocommit sees the completed write, including partial failures.
      if (!ret) { reply.read(*tx); if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; } }
      if (ret && worker_request) { worker_request->cancel(ret); }
    }
    delete this;
  }
  int64_t timeout() const override { return deadline; }
  void set_skip_flush_redo(bool) override {} // No index writes in this slice.
};

class DuplicateRows final : public ObDatumRowIterator {
public:
  std::vector<Frame> batches;
  size_t current = 0;
  uint64_t remaining = 0;
  int64_t width;
  ObDatumRow row;
  explicit DuplicateRows(int64_t n) : width(n) {}
  int get_next_row(ObDatumRow *&out) override {
    while (!remaining) {
      if (current == batches.size()) { return OB_ITER_END; }
      remaining = batches[current].number();
      if (!remaining) { ++current; }
    }
    Frame &batch = batches[current];
    int ret = row.is_valid() ? OB_SUCCESS : row.init(width);
    for (int64_t i = 0; !ret && i < width; ++i) {
      ObObj value; batch.read(value);
      ret = batch.ret ? batch.ret : row.storage_datums_[i].from_obj_enhance(value);
    }
    if (!--remaining) {
      if (!batch.consumed()) { ret = OB_INVALID_ARGUMENT; }
      ++current;
    }
    out = &row; return ret;
  }
};
class RemoteDmlService final : public ObIDmlService {
public:
  int prepare_execution(
      const ObDmlWriteSpec &write_spec,
      const ObDmlTablePlan &table_plan,
      const transaction::ObTxReadSnapshot &snapshot,
      common::ObIAllocator &allocator,
      const ObWriteContext &write_context,
      const concurrent_control::ObWriteFlag &write_flag,
      ObDmlExecution &execution) override {
    const auto view = table_plan.get_data_table();
    const auto &columns = table_plan.get_col_descs();
    if (!write_context.is_valid() || columns.empty() || columns.count() > OB_MAX_COLUMN_NUMBER || !write_spec.tz_info_) { return OB_NOT_SUPPORTED; }
    auto prepared = std::make_unique<RemoteExecution>();
    auto &tx = *static_cast<ObTxDesc *>(write_context.native_handle());
    prepared->tx = &tx;
    prepared->txid = tx.get_tx_id().get_id(); prepared->deadline = write_spec.timeout_;
    Frame request('W'), reply; request.number('P'); request.number(prepared->txid);
    request.number(view.get_table_id()); request.number(write_spec.schema_version_);
    request.number(write_spec.timeout_); request.number(write_spec.sql_mode_); request.number(write_spec.branch_id_);
    request.number(write_spec.is_total_quantity_log_); request.number(write_spec.prelock_);
    request.number(write_spec.is_batch_stmt_); request.number(write_spec.is_main_table_in_fts_ddl_);
    request.number(write_spec.check_schema_version_); request.number(write_spec.access_vector_id_as_master_table_);
    request.append(*write_spec.tz_info_);
    request.append(snapshot); request.append(write_flag); request.number(columns.count());
    for (int64_t i = 0; i < columns.count(); ++i) {
      request.number(columns.at(i).col_id_); prepared->types.push_back(columns.at(i).col_type_);
      prepared->columns.push_back(columns.at(i).col_id_);
    }
    execution.reset();
    int ret = write_rpc(request, reply);
    if (!ret) {
      prepared->handle = reply.number();
      if (!reply.consumed() || !prepared->handle) { ret = OB_INVALID_ARGUMENT; }
      else { bind_execution(execution, prepared.release()); }
    }
    return ret; }
  int delete_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    return write_rows('D', tablet_id, tx_desc, execution, &column_ids, nullptr, row_iter, affected_rows); }
  int put_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    return write_rows('p', tablet_id, tx_desc, execution, &column_ids, nullptr, row_iter, affected_rows); }
  int insert_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    return write_rows('I', tablet_id, tx_desc, execution, &column_ids, nullptr, row_iter, affected_rows); }
  int write_rows(char operation, const ObTabletID &tablet_id, ObTxDesc &tx_desc,
      const ObDmlExecution &execution, const ObIArray<uint64_t> *column_ids,
      const ObIArray<uint64_t> *updated_column_ids, ObDatumRowIterator *row_iter, int64_t &affected_rows,
      int64_t lock_timeout = 0, ObRowLockMode lock_mode = ObRowLockMode::NONE,
      DuplicateRows *duplicates = nullptr, ObDuplicateReturnMode duplicate_mode = ObDuplicateReturnMode::ALL) {
    auto *state = static_cast<RemoteExecution *>(execution_state(execution));
    if (!state || !row_iter || (column_ids && column_ids->count() != static_cast<int64_t>(state->columns.size()))
        || state->txid != static_cast<uint64_t>(tx_desc.get_tx_id().get_id())) { return OB_INVALID_ARGUMENT; }
    const int64_t width = state->columns.size();
    for (int64_t i = 0; column_ids && i < width; ++i) {
      if (column_ids->at(i) != state->columns[i]) { return OB_INVALID_ARGUMENT; }
    }
    affected_rows = 0;
    int ret = OB_SUCCESS;
    bool end = false, duplicated = false;
    while (!ret && !end) {
      Frame cells;
      int64_t rows = 0;
      // Keep old/new pairs in the same frame: at most 32 logical writes.
      while (!ret && rows < (operation == 'U' ? 64 : 32)) {
        ObDatumRow *row = nullptr;
        ret = THIS_WORKER.check_status();
        if (!ret) { ret = row_iter->get_next_row(row); }
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; end = true; break; }
        if (!ret && (!row || row->get_column_count() != width)) { ret = OB_INVALID_ARGUMENT; }
        for (int64_t i = 0; !ret && i < width; ++i) {
          ObObj value;
          ret = row->storage_datums_[i].to_obj_enhance(value, state->types[i]);
          if (!ret) { cells.append(value); ret = cells.ret; }
        }
        if (!ret) { ++rows; }
      }
      if (!ret && operation == 'U' && rows % 2) { ret = OB_INVALID_ARGUMENT; }
      if (!ret && rows) {
        Frame request('W'), reply; request.number(operation); request.number(state->txid);
        request.number(state->handle); request.number(tablet_id.id()); request.number(rows);
        if (operation == 'L') { request.number(lock_timeout); request.number(static_cast<int>(lock_mode)); }
        request.number(updated_column_ids ? updated_column_ids->count() : 0);
        if (updated_column_ids) {
          for (int64_t i = 0; i < updated_column_ids->count(); ++i) { request.number(updated_column_ids->at(i)); }
        }
        if (duplicates) { request.number(static_cast<int>(duplicate_mode)); }
        request.data.insert(request.data.end(), cells.data.begin() + Frame::HEADER_SIZE, cells.data.end());
        ret = write_rpc(request, reply);
        if (duplicates && ret == OB_ERR_PRIMARY_KEY_DUPLICATE) { duplicated = true; ret = OB_SUCCESS; }
        if (!ret) {
          affected_rows += reply.number();
          if (duplicates && !reply.ret) { duplicates->batches.push_back(std::move(reply)); }
          else if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
        }
      }
    }
    return ret ? ret : duplicated ? OB_ERR_PRIMARY_KEY_DUPLICATE : OB_SUCCESS; }
  int insert_rows_fetch_duplicates(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      const ObDuplicateReturnMode return_mode,
      int64_t &affected_rows,
      blocksstable::ObDatumRowIterator *&duplicated_rows) override {
    if (duplicated_rows) { return OB_INVALID_ARGUMENT; }
    auto result = std::make_unique<DuplicateRows>(duplicated_column_ids.count());
    const int ret = write_rows('f', tablet_id, tx_desc, execution, &column_ids, &duplicated_column_ids,
                              row_iter, affected_rows, 0, ObRowLockMode::NONE, result.get(), return_mode);
    if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) { duplicated_rows = result.release(); }
    return ret;
  }
  void free_duplicate_rows_iterator(
      blocksstable::ObDatumRowIterator *iterator) override { delete iterator; }
  int update_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &updated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    return write_rows('U', tablet_id, tx_desc, execution, &column_ids, &updated_column_ids, row_iter, affected_rows); }
  int lock_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const int64_t abs_lock_timeout,
      const ObRowLockMode lock_mode,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    return write_rows('L', tablet_id, tx_desc, execution, nullptr, nullptr, row_iter, affected_rows, abs_lock_timeout, lock_mode); }
};
} } }
