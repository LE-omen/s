// Throwaway V14: DAS stays in the SQL worker; transactions and writes stay here.
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

int write_rpc(Frame &request, Frame &reply) {
  int ret = worker_send(request);
  if (!ret) { ret = worker_read(reply); }
  if (!ret && reply.type() != 'w') { ret = OB_INVALID_ARGUMENT; }
  if (!ret) { ret = static_cast<int>(reply.number()); }
  return ret ? ret : reply.ret;
}

struct EngineWrite {
  ObArenaAllocator allocator{ObMemAttr("NsRemoteWrite")};
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
    if (request.ret || count != 2 || ((table & ~(1ULL << 62)) >> 32) != ns) { return OB_NOT_SUPPORTED; }
    int ret = NamespaceForkKernelPrototype::schema_by_id(table, schema);
    if (ret) { return ret; }
    // Namespace catalog currently admits only these simple integer schemas.
    if (!schema || schema->get_schema_version() != spec.schema_version_
        || schema->get_column_count() != 2 || schema->get_index_tid_count() != 0) { return OB_NOT_SUPPORTED; }
    for (uint64_t i = 0; !ret && i < count; ++i) {
      const uint64_t id = request.number();
      const auto *column = schema->get_column_schema(id);
      if (!column || !ob_is_integer_type(column->get_data_type())
          || (i == 0 && !column->is_rowkey_column())
          || (i && id == columns.at(0))) { ret = OB_NOT_SUPPORTED; }
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

  int insert(ObTxDesc &tx, Frame &request, int64_t &affected) {
    const uint64_t tablet_id = request.number(), count = request.number();
    if (request.ret || tablet_id != tablet.id() || count == 0 || count > 32) { return OB_INVALID_ARGUMENT; }
    // Decode and validate a bounded batch before entering native storage.
    std::vector<ObObj> cells(count * 2);
    for (auto &cell : cells) {
      request.read(cell);
      if (request.ret || (!cell.is_null() && !ob_is_integer_type(cell.get_type()))) { return OB_INVALID_ARGUMENT; }
    }
    if (!request.consumed()) { return OB_INVALID_ARGUMENT; }
    class Rows final : public ObDatumRowIterator {
    public:
      std::vector<ObObj> &cells;
      size_t position = 0;
      ObDatumRow row;
      explicit Rows(std::vector<ObObj> &values) : cells(values) {}
      int get_next_row(ObDatumRow *&out) override {
        if (position == cells.size()) { return OB_ITER_END; }
        int ret = OB_SUCCESS;
        for (int i = 0; !ret && i < 2; ++i) { ret = row.storage_datums_[i].from_obj_enhance(cells[position++]); }
        row.row_flag_.set_flag(DF_INSERT); out = &row; return ret;
      }
    } rows(cells);
    int ret = rows.row.init(2);
    if (!ret) { ret = share::server_service<ObIDmlService>()->insert_rows(
        tablet, tx, execution, columns, &rows, affected); }
    fprintf(stderr, "PROTOTYPE_V14_INSERT_BATCH tx=%lld rows=%llu affected=%lld ret=%d\n",
        (long long)tx.get_tx_id().get_id(), (unsigned long long)count, (long long)affected, ret);
    return ret;
  }
};

// ponytail: one autocommit statement per request; explicit transactions need a
// session-owned engine transaction. No transaction map or background collector.
struct EngineWrites {
  uint64_t ns;
  uint32_t sid;
  ObTxDesc *tx = nullptr;
  uint64_t sequence = 0;
  std::unique_ptr<EngineWrite> write;
  explicit EngineWrites(uint64_t namespace_id, uint32_t session_id) : ns(namespace_id), sid(session_id) {}
  int check_finished() const {
    return write || (tx && tx->is_in_tx() && !tx->is_tx_end()) ? OB_ERR_UNEXPECTED : OB_SUCCESS;
  }
  ~EngineWrites() {
    write.reset();
    if (tx) {
      auto *service = query_transaction_service();
      const bool rollback = tx->is_in_tx() && !tx->is_tx_end();
      if (rollback) { service->rollback_tx(*tx); }
      service->release_tx(*tx);
      fprintf(stderr, "PROTOTYPE_V14_TX_RELEASED session=%u rollback=%d\n", sid, rollback);
    }
  }
  int process(Frame &request, Frame &reply) {
    auto *service = query_transaction_service();
    const uint64_t operation = request.number();
    const uint64_t txid = request.number();
    int ret = request.ret;
    Frame values;
    if (!ret && !tx && request.type() == 'T' && (operation == 'A' || operation == 'P')) {
      ret = service->acquire_tx(tx, sid);
    }
    if (!ret && (!tx || static_cast<uint64_t>(tx->get_tx_id().get_id()) != txid)) { ret = OB_INVALID_ARGUMENT; }
    if (!ret && request.type() == 'T') {
      if (operation == 'A') {
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
      } else if (operation == 'P') {
        ObTxParam param; ObTxSEQ savepoint;
        request.read(param); const bool release = request.number() != 0;
        if (!request.consumed() || !param.is_valid() || param.isolation_ != ObTxIsolationLevel::RC) { ret = OB_NOT_SUPPORTED; }
        else { ret = service->create_implicit_savepoint(*tx, param, savepoint, release); }
        values.append(savepoint);
      } else if (operation == 'G') {
        auto isolation = static_cast<ObTxIsolationLevel>(request.number());
        const int64_t deadline = request.number();
        ObTxReadSnapshot snapshot;
        if (!request.consumed() || isolation != ObTxIsolationLevel::RC) { ret = OB_NOT_SUPPORTED; }
        else { ret = service->get_read_snapshot(*tx, isolation, std::min(deadline, THIS_WORKER.get_timeout_ts()), snapshot); }
        values.append(snapshot);
      } else if (operation == 'C') {
        const int64_t deadline = request.number();
        if (!request.consumed() || write) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->commit_tx(*tx, std::min(deadline, THIS_WORKER.get_timeout_ts())); }
        fprintf(stderr, "PROTOTYPE_V14_COMMIT session=%u tx=%llu ret=%d\n", sid, (unsigned long long)txid, ret);
      } else if (operation == 'R') {
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else { write.reset(); ret = service->rollback_tx(*tx); }
      } else if (operation == 'B') {
        ObTxSEQ savepoint; request.read(savepoint);
        const int64_t deadline = request.number();
        const bool touched = request.number() != 0;
        auto policy = static_cast<ObTxCleanPolicy>(request.number());
        if (!request.consumed() || (policy != FAST_ROLLBACK && policy != ROLLBACK && policy != KEEP)) { ret = OB_INVALID_ARGUMENT; }
        else { write.reset(); ret = service->rollback_to_implicit_savepoint(*tx, savepoint, deadline, touched, policy); }
      } else if (operation == 'E') {
        ObTxExecResult result;
        if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
        else { ret = service->collect_tx_exec_result(*tx, result); }
        values.append(result);
      } else { ret = OB_NOT_SUPPORTED; }
    } else if (!ret && request.type() == 'W') {
      if (operation == 'P') {
        if (write) { ret = OB_NOT_SUPPORTED; }
        auto prepared = std::make_unique<EngineWrite>();
        if (!ret) { ret = prepared->prepare(ns, *tx, request); }
        if (!ret) { write = std::move(prepared); values.number(++sequence); }
      } else {
        const uint64_t handle = request.number();
        if (request.ret || !write || handle != sequence) { ret = OB_INVALID_ARGUMENT; }
        else if (operation == 'X' && request.consumed()) { write.reset(); }
        else if (operation == 'I') {
          int64_t affected = 0;
          ret = write->insert(*tx, request, affected); values.number(affected);
        } else { ret = OB_INVALID_ARGUMENT; }
      }
    }
    reply = Frame('w'); reply.number(ret);
    if (!ret) {
      if (request.type() == 'T' || (request.type() == 'W' && operation == 'X')) { reply.append(*tx); }
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
                       const transaction::ObTxParam &tx_param) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX start_tx\n"); return OB_NOT_SUPPORTED; }
  int abort_tx(transaction::ObTxDesc &tx, int cause) override { return rollback_tx(tx); }
  int rollback_tx(transaction::ObTxDesc &tx) override { Frame request, reply; return tx_rpc('R', tx, request, reply); }
  int commit_tx(transaction::ObTxDesc &tx,
                        int64_t expire_ts) override { Frame request, reply; request.number(expire_ts); return tx_rpc('C', tx, request, reply); }
  int submit_commit_tx(transaction::ObTxDesc &tx,
                               int64_t expire_ts,
                               transaction::ObITxCallback &callback) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX submit_commit_tx\n"); return OB_NOT_SUPPORTED; }
  int release_tx(transaction::ObTxDesc &tx) override { delete &tx; return OB_SUCCESS; }
  int reuse_tx(transaction::ObTxDesc &tx) override { tx.~ObTxDesc(); new (&tx) ObTxDesc(); return OB_SUCCESS; }
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
                                      transaction::ObTxSEQ &savepoint) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX create_branch_savepoint\n"); return OB_NOT_SUPPORTED; }
  int create_in_txn_implicit_savepoint(transaction::ObTxDesc &tx,
                                               transaction::ObTxSEQ &savepoint) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX create_in_txn_implicit_savepoint\n"); return OB_NOT_SUPPORTED; }
  int create_explicit_savepoint(transaction::ObTxDesc &tx,
                                        const common::ObString &savepoint) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX create_explicit_savepoint\n"); return OB_NOT_SUPPORTED; }
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
                                             int64_t expire_ts) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX rollback_to_explicit_savepoint\n"); return OB_NOT_SUPPORTED; }
  int release_explicit_savepoint(transaction::ObTxDesc &tx,
                                         const common::ObString &savepoint) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX release_explicit_savepoint\n"); return OB_NOT_SUPPORTED; }
  int create_stash_savepoint(transaction::ObTxDesc &tx,
                                     const common::ObString &name) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_TX create_stash_savepoint\n"); return OB_NOT_SUPPORTED; }
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
    if (!write_context.is_valid() || columns.count() != 2 || !write_spec.tz_info_) { return OB_NOT_SUPPORTED; }
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
      if (!ob_is_integer_type(columns.at(i).col_type_.get_type())) { return OB_NOT_SUPPORTED; }
      request.number(columns.at(i).col_id_); prepared->types.push_back(columns.at(i).col_type_);
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
      int64_t &affected_rows) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_DML delete_rows\n"); return OB_NOT_SUPPORTED; }
  int put_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_DML put_rows\n"); return OB_NOT_SUPPORTED; }
  int insert_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override {
    auto *state = static_cast<RemoteExecution *>(execution_state(execution));
    if (!state || !row_iter || column_ids.count() != 2
        || state->txid != static_cast<uint64_t>(tx_desc.get_tx_id().get_id())) { return OB_INVALID_ARGUMENT; }
    affected_rows = 0;
    int ret = OB_SUCCESS;
    bool end = false;
    while (!ret && !end) {
      Frame cells;
      int64_t rows = 0;
      while (!ret && rows < 32) {
        ObDatumRow *row = nullptr;
        ret = THIS_WORKER.check_status();
        if (!ret) { ret = row_iter->get_next_row(row); }
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; end = true; break; }
        if (!ret && (!row || row->get_column_count() != 2)) { ret = OB_INVALID_ARGUMENT; }
        for (int i = 0; !ret && i < 2; ++i) {
          ObObj value;
          ret = row->storage_datums_[i].to_obj_enhance(value, state->types[i]);
          if (!ret) { cells.append(value); ret = cells.ret; }
        }
        if (!ret) { ++rows; }
      }
      if (!ret && rows) {
        Frame request('W'), reply; request.number('I'); request.number(state->txid);
        request.number(state->handle); request.number(tablet_id.id()); request.number(rows);
        request.data.insert(request.data.end(), cells.data.begin() + Frame::HEADER_SIZE, cells.data.end());
        ret = write_rpc(request, reply);
        if (!ret) {
          affected_rows += reply.number();
          if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
        }
      }
    }
    return ret; }
  int insert_rows_fetch_duplicates(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &duplicated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      const ObDuplicateReturnMode return_mode,
      int64_t &affected_rows,
      blocksstable::ObDatumRowIterator *&duplicated_rows) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_DML insert_rows_fetch_duplicates\n"); return OB_NOT_SUPPORTED; }
  void free_duplicate_rows_iterator(
      blocksstable::ObDatumRowIterator *iterator) override { delete iterator; }
  int update_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const common::ObIArray<uint64_t> &column_ids,
      const common::ObIArray<uint64_t> &updated_column_ids,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_DML update_rows\n"); return OB_NOT_SUPPORTED; }
  int lock_rows(
      const common::ObTabletID &tablet_id,
      transaction::ObTxDesc &tx_desc,
      const ObDmlExecution &execution,
      const int64_t abs_lock_timeout,
      const ObRowLockMode lock_mode,
      blocksstable::ObDatumRowIterator *row_iter,
      int64_t &affected_rows) override { fprintf(stderr, "PROTOTYPE_V14_UNSUPPORTED_DML lock_rows\n"); return OB_NOT_SUPPORTED; }
};
} } }
