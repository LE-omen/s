// V10 bounded, read-only tablet RPC. SQL expressions stay in the worker.
#include "data_plane/access/ob_table_scan_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "data_plane/access/ob_table_param.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
int worker_send(const Frame &);
int worker_read(Frame &);
struct EngineScan {
  ObArenaAllocator allocator{ObMemAttr("NsRemoteScan")};
  ObTableParam table{allocator};
  ObTableScanParam param;
  std::vector<ObObj> keys;
  ObNewRowIterator *iter = nullptr;
  const ObTableSchema *schema = nullptr;
  ~EngineScan() { if (iter) { share::server_service<ObITabletScan>()->revert_scan_iter(iter); } }
  int open(uint64_t ns, uint64_t snapshot, Frame &request) {
    const uint64_t table_id = request.number(), tablet_id = request.number();
    const bool reverse = request.number() != 0, get = request.number() != 0;
    param.limit_param_.limit_ = static_cast<int64_t>(request.number());
    param.limit_param_.offset_ = static_cast<int64_t>(request.number());
    const uint64_t count = request.number();
    if (request.ret || count == 0 || count > 8 || ((table_id & ~(1ULL << 62)) >> 32) != ns) { return OB_NOT_SUPPORTED; }
    int ret = OB_SUCCESS;
    ret = NamespaceForkKernelPrototype::schema_by_id(table_id, schema);
    if (ret) { return ret; }
    if (!schema || schema->get_tablet_id().id() != tablet_id) { return OB_INVALID_ARGUMENT; }
    for (uint64_t i = 0; !ret && i < count; ++i) {
      const uint64_t column = request.number();
      if (!schema->get_column_schema(column)) { ret = OB_NOT_SUPPORTED; }
      else { ret = param.column_ids_.push_back(column); }
    }
    const uint64_t ranges = request.number();
    if (ret || request.ret || ranges > 256) { return ret ? ret : OB_NOT_SUPPORTED; }
    keys.resize(ranges * 2);
    for (uint64_t i = 0; !ret && i < ranges; ++i) {
      ObNewRange range; range.table_id_ = table_id;
      range.border_flag_.set_data(request.number());
      request.read(keys[i * 2]); request.read(keys[i * 2 + 1]);
      range.start_key_.assign(&keys[i * 2], 1); range.end_key_.assign(&keys[i * 2 + 1], 1);
      ret = param.key_ranges_.push_back(range);
    }
    if (ret || !request.consumed()) { return ret ? ret : OB_INVALID_ARGUMENT; }
    param.index_id_ = table_id; param.tablet_id_ = ObTabletID(tablet_id);
    param.schema_version_ = schema->get_schema_version();
    param.runtime_schema_version_ = schema->get_schema_version();
    param.timeout_ = THIS_WORKER.get_timeout_ts();
    param.is_get_ = get;
    param.scan_flag_ = ObQueryFlag(reverse ? ObQueryFlag::Reverse : ObQueryFlag::Forward,
        false, false, false, true, false, false, false);
    param.allocator_ = &allocator; param.scan_allocator_ = &allocator;
    param.reserved_cell_count_ = count;
    SCN scn; scn.convert_for_tx(snapshot); param.snapshot_.init_weak_read(scn);
    ret = table.convert(*schema, param.column_ids_, sql::ObStoragePushdownFlag());
    param.table_param_ = &table;
    if (!ret) { ret = share::server_service<ObITabletScan>()->table_scan(param, iter); }
    fprintf(stderr, "PROTOTYPE_V10_SCAN_OPEN ns=%llu table=%llu ret=%d\n",
        static_cast<unsigned long long>(ns), static_cast<unsigned long long>(table_id), ret);
    return ret;
  }
  int fetch(Frame &reply) {
    Frame rows('s'); rows.number(0); rows.number(0); rows.number(0);
    uint64_t count = 0; bool end = false; int ret = OB_SUCCESS;
    for (; count < 32; ++count) {
      blocksstable::ObDatumRow *row = nullptr;
      ret = static_cast<ObTableScanIterator *>(iter)->get_next_row(row);
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; end = true; break; }
      if (ret) { break; }
      if (!row || row->get_column_count() != param.column_ids_.count()) { ret = OB_ERR_UNEXPECTED; break; }
      for (int64_t i = 0; !ret && i < row->get_column_count(); ++i) {
        ObObj value;
        ret = row->storage_datums_[i].to_obj_enhance(value,
            schema->get_column_schema(param.column_ids_.at(i))->get_meta_type());
        if (!ret) { rows.append(value); }
      }
      if (ret) { break; }
      if (rows.ret) { ret = rows.ret; break; }
    }
    reply = Frame('s'); reply.number(ret); reply.number(end); reply.number(count);
    if (!ret) { reply.data.insert(reply.data.end(), rows.data.begin() + 25, rows.data.end()); }
    return OB_SUCCESS;
  }
};
struct ReadScans {
  uint64_t ns, snapshot;
  std::map<uint64_t, std::unique_ptr<EngineScan>> scans;
  uint64_t sequence = 0;
  ReadScans(uint64_t n, uint64_t s) : ns(n), snapshot(s) {}
  int process(Frame &request, Frame &reply) {
    int ret = OB_SUCCESS;
    reply = Frame('s');
    if (request.type() == 'O') {
      if (scans.size() >= 4) { ret = OB_NOT_SUPPORTED; }
      auto scan = std::make_unique<EngineScan>();
      if (!ret) { ret = scan->open(ns, snapshot, request); }
      reply.number(ret); reply.number(ret ? 0 : ++sequence);
      if (!ret) { scans.emplace(sequence, std::move(scan)); }
    } else {
      const uint64_t id = request.number();
      auto it = scans.find(id);
      if (!request.consumed() || it == scans.end()) { reply.number(OB_INVALID_ARGUMENT); }
      else if (request.type() == 'X') { scans.erase(it); reply.number(0); }
      else if (request.type() == 'F') { ret = it->second->fetch(reply); }
      else { reply.number(OB_NOT_SUPPORTED); }
    }
    return ret;
  }
};
class RemoteScanIterator final : public ObNewRowIterator {
public:
  ObVTableScanParam &param;
  uint64_t handle = 0, row_index = 0;
  int64_t qualified = 0, returned = 0;
  bool end = false;
  std::vector<ObObj> cells;
  ObNewRow row;
  explicit RemoteScanIterator(ObVTableScanParam &p) : param(p) {}
  ~RemoteScanIterator() override { reset(); }
  int open() {
    const sql::ObStoragePushdownFlag flags(param.pd_storage_flag_);
    if (param.for_update_ || !param.op_ || !param.output_exprs_
        || param.output_exprs_->count() != param.column_ids_.count()
        || (param.aggregate_exprs_ && !param.aggregate_exprs_->empty())
        || flags.is_filter_pushdown() || flags.is_aggregate_pushdown() || flags.is_group_by_pushdown()) { return OB_NOT_SUPPORTED; }
    Frame request('O'); request.number(param.index_id_); request.number(param.tablet_id_.id());
    request.number(param.scan_flag_.is_reverse_scan()); request.number(param.is_get_);
    // Legacy op_filters are SQL callbacks even when storage pushdown is off.
    // Apply both those predicates and the scan limit in this worker, in order.
    request.number(static_cast<uint64_t>(-1)); request.number(0);
    request.number(param.column_ids_.count());
    for (int64_t i = 0; i < param.column_ids_.count(); ++i) { request.number(param.column_ids_.at(i)); }
    request.number(param.key_ranges_.count());
    for (int64_t i = 0; i < param.key_ranges_.count(); ++i) {
      const ObNewRange &range = param.key_ranges_.at(i);
      if (range.start_key_.get_obj_cnt() != 1 || range.end_key_.get_obj_cnt() != 1) { return OB_NOT_SUPPORTED; }
      request.number(range.border_flag_.get_data());
      request.append(range.start_key_.get_obj_ptr()[0]); request.append(range.end_key_.get_obj_ptr()[0]);
    }
    Frame reply; int ret = exchange(request, reply);
    if (!ret) { handle = reply.number(); if (!reply.consumed() || handle == 0) { ret = OB_INVALID_ARGUMENT; } }
    return ret;
  }
  int get_next_row(ObNewRow *&out) override {
    int ret = OB_SUCCESS;
    const size_t columns = param.column_ids_.count();
    if (row_index == cells.size()) {
      if (end) { return OB_ITER_END; }
      Frame request('F'), reply; request.number(handle);
      if ((ret = exchange(request, reply))) { return ret; }
      end = reply.number() != 0; const uint64_t rows = reply.number();
      if (reply.ret || rows > 32 || (!rows && !end)) { return OB_INVALID_ARGUMENT; }
      cells.resize(rows * columns); row_index = 0;
      for (auto &cell : cells) {
        reply.read(cell);
        if (!cell.is_null() && !ob_is_integer_type(cell.get_type())) { return OB_NOT_SUPPORTED; }
      }
      if (!reply.consumed()) { return OB_INVALID_ARGUMENT; }
      if (!rows) { return OB_ITER_END; }
    }
    row.cells_ = cells.data() + row_index; row.count_ = columns; row_index += columns; out = &row;
    return ret;
  }
  int get_next_row() override {
    if (param.limit_param_.limit_ >= 0 && returned >= param.limit_param_.limit_) { return OB_ITER_END; }
    int ret = OB_SUCCESS;
    for (;;) {
      if ((ret = THIS_WORKER.check_status())) { return ret; }
      ObNewRow *row = nullptr;
      if ((ret = get_next_row(row))) { return ret; }
      auto &ctx = param.op_->get_eval_ctx();
      param.op_->clear_datum_eval_flag();
      for (int64_t i = 0; !ret && i < row->count_; ++i) {
        sql::ObExpr *expr = param.output_exprs_->at(i);
        ret = expr->locate_expr_datum(ctx).from_obj(row->cells_[i]);
        expr->set_evaluated_projected(ctx);
        expr->set_evaluated_flag(ctx);
      }
      bool filtered = false;
      if (!ret && param.op_filters_) { ret = sql::ObOperator::filter_row(ctx, *param.op_filters_, filtered); }
      if (ret) { return ret; }
      if (!filtered && ++qualified > param.limit_param_.offset_) { ++returned; return OB_SUCCESS; }
    }
  }
  int get_next_rows(int64_t &count, int64_t capacity) override {
    count = 0;
    auto &ctx = param.op_->get_eval_ctx();
    sql::ObEvalCtx::BatchInfoScopeGuard batch(ctx);
    const int64_t limit = std::min<int64_t>(32, capacity);
    if (limit <= 0) { return OB_INVALID_ARGUMENT; }
    batch.set_batch_size(limit);
    int ret = OB_SUCCESS;
    while (count < limit) {
      batch.set_batch_idx(count);
      ret = get_next_row();
      if (ret) { break; }
      ++count;
    }
    return ret == OB_ITER_END && count ? OB_SUCCESS : ret;
  }
  void reset() override {
    if (handle) { Frame request('X'), reply; request.number(handle); exchange(request, reply); handle = 0; }
    cells.clear(); row_index = 0; end = false; qualified = 0; returned = 0;
  }
private:
  int exchange(const Frame &request, Frame &reply) {
    int ret = worker_send(request);
    if (!ret) { ret = worker_read(reply); }
    if (!ret && reply.type() != 's') { ret = OB_INVALID_ARGUMENT; }
    if (!ret) { ret = static_cast<int>(reply.number()); }
    return ret ? ret : reply.ret;
  }
};
class RemoteTabletScan final : public ObITabletScan {
public:
  int table_scan(ObVTableScanParam &param, ObNewRowIterator *&iter) override {
    if (iter) { return OB_INVALID_ARGUMENT; }
    auto scan = std::make_unique<RemoteScanIterator>(param);
    int ret = scan->open(); if (!ret) { iter = scan.release(); } return ret;
  }
  int revert_scan_iter(ObNewRowIterator *iter) override { delete iter; return OB_SUCCESS; }
  int table_rescan(ObVTableScanParam &, ObNewRowIterator *iter) override {
    auto *scan = static_cast<RemoteScanIterator *>(iter);
    if (!scan) { return OB_INVALID_ARGUMENT; }
    scan->reset(); return scan->open();
  }
};
} } }
