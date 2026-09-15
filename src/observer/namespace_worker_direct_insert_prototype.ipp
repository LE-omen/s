// Native direct-insert DAGs and slice writers stay beside shared tablets.
#include "data_plane/ddl/ob_direct_insert.h"
#include "query/engine/vector/ob_i_vector.h"
#include <shared_mutex>
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace data_plane;

struct StorageSessionState {
  ObArenaAllocator allocator{ObMemAttr("NsStorageSess")};
  sql::ObSQLSessionInfo session;
};

struct DirectInsertOwner final : ObIDirectInsertWorkerContext {
  ObArenaAllocator allocator{ObMemAttr("NsDirectInsert")};
  std::shared_ptr<StorageSessionState> context;
  ObIDirectInsertSession *session = nullptr;
  RequestTag origin;
  uint64_t generation;
  int64_t deadline;
  std::shared_mutex mutex;
  std::atomic<int64_t> writers{0};
  DirectInsertOwner(std::shared_ptr<StorageSessionState> state, RequestTag tag, uint64_t id)
      : context(std::move(state)), origin(tag), generation(id), deadline(THIS_WORKER.get_timeout_ts()) {}
  ~DirectInsertOwner() { ObDirectInsertOrchestrator::finish(session); }
  void bind_current_thread() override {
    // Pin the existing storage session, without retaining its route or channel.
    THIS_WORKER.set_session(&context->session);
    THIS_WORKER.set_timeout_ts(deadline);
  }
  bool matches(RequestTag tag, uint64_t id) const {
    return origin.slot == tag.slot && origin.generation == tag.generation && generation == id;
  }
};

struct DirectInsertWriterOwner {
  ObArenaAllocator allocator{ObMemAttr("NsDirectWriter")};
  std::shared_ptr<DirectInsertOwner> owner;
  ObIDirectInsertWriter *writer = nullptr;
  explicit DirectInsertWriterOwner(std::shared_ptr<DirectInsertOwner> session) : owner(std::move(session)) {
    ++owner->writers;
  }
  ~DirectInsertWriterOwner() {
    ObIDirectInsertWriterFactory::destroy(writer);
    --owner->writers;
  }
};

struct DirectInsertRoute {
  std::shared_ptr<DirectInsertOwner> owner;
  std::map<uint64_t, std::unique_ptr<DirectInsertWriterOwner>> writers;
  uint64_t session_generation = 0, writer_generation = 0;
  void reset() { writers.clear(); owner.reset(); }

  int process(uint64_t ns, RequestTag tag, RequestRoutes &routes,
      const std::shared_ptr<StorageSessionState> &context, Frame &request, Frame &reply) {
    const uint64_t operation = request.number();
    const RequestTag parent{request.number(), request.number()};
    const uint64_t generation = request.number();
    int ret = request.ret;
    Frame output;
    if (!ret && ns != 1) { ret = OB_NOT_SUPPORTED; }
    if (!ret && operation == 'S') {
      ObDirectInsertStartParam param;
      param.ddl_task_id_ = request.number(); param.execution_id_ = request.number();
      param.table_id_ = request.number(); param.worker_count_ = request.number();
      const uint64_t count = request.number();
      if (request.ret || !count || count > MAX_FRAME / 8 || owner || parent.slot || parent.generation
          || generation || session_generation == UINT64_MAX) { ret = OB_INVALID_ARGUMENT; }
      for (uint64_t i = 0; !ret && i < count; ++i) {
        const ObTabletID tablet(request.number());
        ret = request.ret ? request.ret : !tablet.is_valid() ? OB_INVALID_ARGUMENT : param.participants_.push_back(tablet);
      }
      if (!ret && (!request.consumed() || !param.is_valid() || !owns_table(ns, param.table_id_))) { ret = OB_INVALID_ARGUMENT; }
      auto pending = ret ? nullptr : routes.find(tag);
      if (!ret && !pending) { ret = OB_STATE_NOT_MATCH; }
      if (!ret) {
        auto staged = std::make_shared<DirectInsertOwner>(context, tag, ++session_generation);
        ret = ObDirectInsertOrchestrator::start(staged->allocator, param, *staged, staged->session);
        if (!ret) {
          owner = std::move(staged);
          { std::lock_guard<std::mutex> guard(pending->mutex); pending->direct_insert = owner; }
          output.number(tag.slot); output.number(tag.generation); output.number(owner->generation);
        }
      }
    } else if (!ret) {
      if (!owner || !owner->matches(parent, generation)) {
        if (!writers.empty()) { ret = OB_STATE_NOT_MATCH; }
        else {
          owner.reset();
          auto pending = routes.find(parent);
          if (pending) { std::lock_guard<std::mutex> guard(pending->mutex); owner = pending->direct_insert.lock(); }
          if (!owner || !owner->matches(parent, generation)) { owner.reset(); ret = OB_STATE_NOT_MATCH; }
        }
      }
      if (!ret && operation == 'F') {
        if (!request.consumed() || parent.slot != tag.slot || parent.generation != tag.generation) { ret = OB_INVALID_ARGUMENT; }
        else {
          std::unique_lock<std::shared_mutex> guard(owner->mutex);
          if (owner->writers) { ret = OB_STATE_NOT_MATCH; }
          else { ret = ObDirectInsertOrchestrator::finish(owner->session); }
        }
        if (!owner->session) {
          auto pending = routes.find(tag);
          if (pending) { std::lock_guard<std::mutex> guard(pending->mutex); pending->direct_insert.reset(); }
          owner.reset();
        }
      } else if (!ret) {
        // PX tasks use their own existing routes and can finish concurrently.
        // Only final teardown excludes active native calls.
        std::shared_lock<std::shared_mutex> guard(owner->mutex);
        auto *session = owner->session;
        if (!session) { ret = OB_NOT_INIT; }
        else if (operation == 'I' || operation == 'P' || operation == 'C') {
          if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
          else if (operation == 'I') { output.number(session->is_final()); }
          else if (operation == 'P') { ret = session->prepare_ordered_input(); }
          else { ret = session->complete_px_worker(); }
        } else if (operation == 'R') {
          const uint64_t flags = request.number();
          ObDirectInsertPlanFacts facts; ObDirectInsertWritePolicy policy;
          facts.regenerate_heap_table_pk_ = flags & 1; facts.vector_rowkey_vid_ = flags & 2;
          facts.has_table_autoinc_ = flags & 4; facts.rowkey_doc_id_ = flags & 8;
          facts.data_table_without_pk_ = flags & 16;
          if (!request.consumed() || flags > 31) { ret = OB_INVALID_ARGUMENT; }
          else { ret = session->resolve_write_policy(facts, policy); }
          if (!ret) { output.number(policy.vector_generated_id_ | (policy.idempotent_tablet_autoinc_ << 1)
              | (policy.idempotent_table_autoinc_ << 2) | (policy.idempotent_doc_id_ << 3)); }
        } else if (operation == 'A') {
          const uint64_t scope = request.number();
          const ObTabletID tablet(request.number()); const int64_t slice = request.number();
          ObDirectInsertAutoincParam param;
          if (!request.consumed() || scope > DIRECT_INSERT_TABLET_AUTOINC) { ret = OB_INVALID_ARGUMENT; }
          else { ret = session->build_autoinc_param(static_cast<ObDirectInsertAutoincScope>(scope), tablet, slice, param); }
          if (!ret) { output.number(param.enabled_); output.number(param.slice_count_);
            output.number(param.slice_index_); output.number(param.range_interval_); }
        } else if (operation == 'T') {
          const ObTabletID tablet(request.number()), target(request.number());
          const int64_t slice = request.number(), rows = request.number();
          ret = !request.consumed() ? OB_INVALID_ARGUMENT : session->sync_tablet_autoinc(tablet, target, slice, rows);
        } else if (operation == 'W') {
          ObDirectInsertWriterRequest param;
          const uint64_t layout = request.number();
          param.layout_ = static_cast<ObDirectInsertWriterLayout>(layout);
          param.tablet_id_ = ObTabletID(request.number()); param.slice_index_ = request.number();
          param.parallel_count_ = request.number(); param.autoinc_column_index_ = request.number();
          const uint64_t idempotent = request.number(); param.idempotent_tablet_autoinc_ = idempotent;
          // Input vectors are sent as bounded row batches; storage uses its
          // native row writer and never receives a query spool-factory pointer.
          if (!request.consumed() || layout > DIRECT_INSERT_ORDERED_WRITER || idempotent > 1
              || !param.is_valid() || writer_generation == UINT64_MAX) { ret = OB_INVALID_ARGUMENT; }
          auto staged = ret ? nullptr : std::make_unique<DirectInsertWriterOwner>(owner);
          if (!ret) { ret = session->get_writer_factory().create(staged->allocator, param, staged->writer); }
          if (!ret) { const uint64_t id = ++writer_generation; writers.emplace(id, std::move(staged)); output.number(id); }
        } else if (operation == 'B' || operation == 'E' || operation == 'X') {
          const uint64_t id = request.number();
          auto entry = writers.find(id);
          if (request.ret || entry == writers.end()) { ret = OB_STATE_NOT_MATCH; }
          else if (operation == 'B') {
            const uint64_t rows = request.number(), columns = request.number();
            if (request.ret || !rows || rows > 32 || !columns || columns > OB_MAX_COLUMN_NUMBER) { ret = OB_INVALID_ARGUMENT; }
            std::vector<ObDatum> cells;
            if (!ret) { cells.resize(rows * columns); }
            for (auto &cell : cells) {
              request.read(cell);
              // Native datum decoding borrows the frame; validate its payload
              // boundary before invoking any writer or decoding another datum.
              if (request.ret || request.pos > static_cast<int64_t>(request.data.size())) {
                ret = OB_INVALID_ARGUMENT; break;
              }
            }
            if (!ret && !request.consumed()) { ret = OB_INVALID_ARGUMENT; }
            std::vector<ObDatum *> row;
            if (!ret) { row.resize(columns); }
            for (uint64_t i = 0; !ret && i < rows; ++i) {
              for (uint64_t j = 0; j < columns; ++j) { row[j] = &cells[i * columns + j]; }
              ret = entry->second->writer->append_row(ObDirectInsertRowView(row.data(), columns));
            }
          } else if (!request.consumed()) { ret = OB_INVALID_ARGUMENT; }
          else if (operation == 'E') { ret = entry->second->writer->close(); }
          if (!ret && operation != 'X') { output.number(entry->second->writer->get_row_count()); }
          if (!ret && operation == 'X') { writers.erase(entry); }
        } else { ret = OB_INVALID_ARGUMENT; }
      }
    }
    reply = Frame('g'); reply.number(ret);
    if (!ret) { reply.data.insert(reply.data.end(), output.data.begin() + Frame::HEADER_SIZE, output.data.end()); }
    return output.ret ? output.ret : reply.ret;
  }
};

class RemoteDirectInsertSession final : public ObIDirectInsertSession, public ObIDirectInsertWriterFactory {
public:
  ObIAllocator &allocator;
  sql::ObSQLSessionInfo *sqc_session;
  RequestTag origin;
  uint64_t generation;
  mutable std::atomic<int> error{OB_SUCCESS};
  RemoteDirectInsertSession(ObIAllocator &a, sql::ObSQLSessionInfo *session, RequestTag tag, uint64_t id)
      : allocator(a), sqc_session(session), origin(tag), generation(id) {}
  int call(char operation, const Frame &payload, Frame &reply,
      sql::ObSQLSessionInfo *session = nullptr, bool cleanup = false) const {
    StorageSessionScope scope(session ? session : THIS_WORKER.get_session());
    int ret = scope.error() ? scope.error() : cleanup ? OB_SUCCESS : error.load();
    Frame request('J'); request.number(operation); request.number(origin.slot);
    request.number(origin.generation); request.number(generation);
    request.data.insert(request.data.end(), payload.data.begin() + Frame::HEADER_SIZE, payload.data.end());
    if (!ret) { ret = payload.ret ? payload.ret : request.data.size() + 64 > request.limit ? OB_SIZE_OVERFLOW : worker_send(request, cleanup); }
    if (!ret) { ret = worker_read(reply); }
    if (!ret) { ret = reply.type() == 'g' ? static_cast<int>(reply.number()) : OB_INVALID_ARGUMENT; }
    if (ret) { int expected = OB_SUCCESS; error.compare_exchange_strong(expected, ret); }
    return ret;
  }
  int empty_call(char operation) const {
    Frame payload, reply; int ret = call(operation, payload, reply);
    return ret ? ret : reply.consumed() ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
  bool is_final() const override {
    Frame payload, reply; int ret = call('I', payload, reply);
    const uint64_t final = ret ? 0 : reply.number();
    if (!ret && (!reply.consumed() || final > 1)) { error = OB_INVALID_ARGUMENT; }
    return !error && final;
  }
  int prepare_ordered_input() override { return empty_call('P'); }
  int complete_px_worker() override { return empty_call('C'); }
  int resolve_write_policy(const ObDirectInsertPlanFacts &facts, ObDirectInsertWritePolicy &policy) const override {
    Frame payload, reply;
    payload.number(facts.regenerate_heap_table_pk_ | (facts.vector_rowkey_vid_ << 1)
        | (facts.has_table_autoinc_ << 2) | (facts.rowkey_doc_id_ << 3) | (facts.data_table_without_pk_ << 4));
    int ret = call('R', payload, reply);
    const uint64_t flags = ret ? 0 : reply.number();
    if (!ret && (!reply.consumed() || flags > 15)) { ret = OB_INVALID_ARGUMENT; }
    if (!ret) { policy.vector_generated_id_ = flags & 1; policy.idempotent_tablet_autoinc_ = flags & 2;
      policy.idempotent_table_autoinc_ = flags & 4; policy.idempotent_doc_id_ = flags & 8; }
    return ret;
  }
  int build_autoinc_param(ObDirectInsertAutoincScope scope, const ObTabletID &tablet,
      int64_t slice, ObDirectInsertAutoincParam &param) override {
    Frame payload, reply; payload.number(scope); payload.number(tablet.id()); payload.number(slice);
    int ret = call('A', payload, reply);
    if (!ret) {
      const uint64_t enabled = reply.number(); param.enabled_ = enabled;
      param.slice_count_ = reply.number(); param.slice_index_ = reply.number(); param.range_interval_ = reply.number();
      if (!reply.consumed() || enabled > 1 || !param.is_valid()) { ret = OB_INVALID_ARGUMENT; }
    }
    return ret;
  }
  int sync_tablet_autoinc(const ObTabletID &tablet, const ObTabletID &target, int64_t slice, int64_t rows) override {
    Frame payload, reply; payload.number(tablet.id()); payload.number(target.id()); payload.number(slice); payload.number(rows);
    int ret = call('T', payload, reply);
    return ret ? ret : reply.consumed() ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
  ObIDirectInsertWriterFactory &get_writer_factory() override { return *this; }
  int create(ObIAllocator &, const ObDirectInsertWriterRequest &, ObIDirectInsertWriter *&) override;
private:
  int finish_and_destroy() override {
    Frame payload, reply; int ret = call('F', payload, reply, sqc_session, true);
    if (!ret && !reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
    if (error) { ret = error; }
    auto &a = allocator; this->~RemoteDirectInsertSession(); a.free(this);
    return ret;
  }
};

class RemoteDirectInsertWriter final : public ObIDirectInsertWriter {
public:
  ObIAllocator &allocator;
  RemoteDirectInsertSession &session;
  sql::ObSQLSessionInfo *sql_session;
  uint64_t id;
  ObTabletID tablet;
  int64_t slice, rows = 0;
  RemoteDirectInsertWriter(ObIAllocator &a, RemoteDirectInsertSession &s, uint64_t handle,
      const ObDirectInsertWriterRequest &request)
      : allocator(a), session(s), sql_session(THIS_WORKER.get_session()), id(handle),
        tablet(request.tablet_id_), slice(request.slice_index_) {}
  int send_rows(Frame &payload) {
    Frame reply; int ret = session.call('B', payload, reply, sql_session);
    if (!ret) { rows = reply.number(); if (!reply.consumed() || rows < 0) { ret = OB_INVALID_ARGUMENT; } }
    return ret;
  }
  int append_row(const ObDirectInsertRowView &row) override {
    if (!row.is_valid() || row.datum_count_ > OB_MAX_COLUMN_NUMBER) { return OB_INVALID_ARGUMENT; }
    Frame payload; payload.number(id); payload.number(1); payload.number(row.datum_count_);
    for (int64_t i = 0; i < row.datum_count_; ++i) {
      if (!row.datums_[i]) { return OB_INVALID_ARGUMENT; }
      payload.append(*row.datums_[i]);
    }
    return send_rows(payload);
  }
  int append_batch(const ObDirectInsertBatchView &batch) override {
    if (!batch.is_valid() || batch.vector_count_ > OB_MAX_COLUMN_NUMBER) { return OB_INVALID_ARGUMENT; }
    int ret = OB_SUCCESS;
    for (int64_t first = 0; !ret && first < batch.row_count_; first += 32) {
      const int64_t count = std::min<int64_t>(32, batch.row_count_ - first);
      Frame payload; payload.number(id); payload.number(count); payload.number(batch.vector_count_);
      for (int64_t i = first; i < first + count; ++i) {
        const int64_t index = batch.selection_type_ == ObDirectInsertBatchView::CONTIGUOUS_SELECTION
            ? batch.offset_ + i : batch.indices_[i];
        for (int64_t col = 0; col < batch.vector_count_; ++col) {
          if (!batch.vectors_[col]) { return OB_INVALID_ARGUMENT; }
          bool is_null = false; const char *value = nullptr; ObLength length = 0;
          batch.vectors_[col]->get_payload(index, is_null, value, length);
          ObDatum datum; if (is_null) { datum.set_null(); } else { datum.set_string(value, length); }
          payload.append(datum);
        }
      }
      ret = send_rows(payload);
    }
    return ret;
  }
  int close() override {
    Frame payload, reply; payload.number(id);
    int ret = session.call('E', payload, reply, sql_session);
    if (!ret) { rows = reply.number(); if (!reply.consumed() || rows < 0) { ret = OB_INVALID_ARGUMENT; } }
    return ret;
  }
  int64_t get_row_count() const override { return rows; }
  const ObTabletID &get_tablet_id() const override { return tablet; }
  int64_t get_slice_index() const override { return slice; }
private:
  void destroy_self() override {
    Frame payload, reply; payload.number(id);
    const int ret = session.call('X', payload, reply, sql_session, true);
    if (!ret && !reply.consumed()) { session.error = OB_INVALID_ARGUMENT; }
    auto &a = allocator; this->~RemoteDirectInsertWriter(); a.free(this);
  }
};
int RemoteDirectInsertSession::create(ObIAllocator &a, const ObDirectInsertWriterRequest &request,
    ObIDirectInsertWriter *&writer) {
  writer = nullptr;
  if (!request.is_valid()) { return OB_INVALID_ARGUMENT; }
  auto *storage = a.alloc(sizeof(RemoteDirectInsertWriter));
  if (!storage) { return OB_ALLOCATE_MEMORY_FAILED; }
  Frame payload, reply;
  payload.number(request.layout_); payload.number(request.tablet_id_.id()); payload.number(request.slice_index_);
  payload.number(request.parallel_count_); payload.number(request.autoinc_column_index_); payload.number(request.idempotent_tablet_autoinc_);
  int ret = call('W', payload, reply);
  const uint64_t id = ret ? 0 : reply.number();
  if (!ret && (!reply.consumed() || !id)) { ret = OB_INVALID_ARGUMENT; }
  if (!ret) { writer = new (storage) RemoteDirectInsertWriter(a, *this, id, request); }
  else { a.free(storage); }
  return ret;
}

class RemoteDirectInsertService final : public IDirectInsertService {
public:
  int start(ObIAllocator &allocator, const ObDirectInsertStartParam &param,
      ObIDirectInsertWorkerContext &context, ObIDirectInsertSession *&session) override {
    session = nullptr;
    if (!param.is_valid()) { return OB_INVALID_ARGUMENT; }
    auto *memory = allocator.alloc(sizeof(RemoteDirectInsertSession));
    if (!memory) { return OB_ALLOCATE_MEMORY_FAILED; }
    auto *previous = THIS_WORKER.get_session();
    context.bind_current_thread();
    auto *sqc_session = THIS_WORKER.get_session();
    StorageSessionScope scope(sqc_session);
    Frame request('J'), reply; request.number('S'); request.number(0); request.number(0); request.number(0);
    request.number(param.ddl_task_id_); request.number(param.execution_id_); request.number(param.table_id_);
    request.number(param.worker_count_); request.number(param.participants_.count());
    for (int64_t i = 0; i < param.participants_.count(); ++i) { request.number(param.participants_.at(i).id()); }
    int ret = scope.error();
    if (!ret) { ret = worker_send(request); }
    if (!ret) { ret = worker_read(reply); }
    if (!ret) { ret = reply.type() == 'g' ? static_cast<int>(reply.number()) : OB_INVALID_ARGUMENT; }
    if (!ret) {
      const RequestTag origin{reply.number(), reply.number()}; const uint64_t generation = reply.number();
      if (!reply.consumed() || !generation || !origin.generation || !(origin.slot & WORKER_REQUEST)) { ret = OB_INVALID_ARGUMENT; }
      else { session = new (memory) RemoteDirectInsertSession(allocator, sqc_session, origin, generation); }
    }
    THIS_WORKER.set_session(previous);
    if (ret) { allocator.free(memory); }
    return ret;
  }
};
} } }
