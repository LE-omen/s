// Included in the Observer composition unit. Shared storage serves SQL workers.
#include "observer/namespace_worker_protocol_prototype.h"
#include "rootserver/fork_table/namespace_fork_kernel_prototype.h"
#include <map>
#include <mutex>
#include "rpc/ob_sql_request_operator.h"
#include "share/rpc/ob_server_task.h"
#include "rpc/frame/ob_req_processor.h"
extern "C" {
void *namespace_proto_spawn(uint64_t, uint64_t, uint32_t *);
int namespace_proto_send(void *, const char *, size_t);
int namespace_proto_receive(void *, const char **, size_t *, uint64_t);
void namespace_proto_stop(void *);
void namespace_proto_interrupt(void *);
int namespace_proto_dispatch(void *, void (*)(void *, const char *, size_t), void *);
int namespace_proto_worker_read(void (*)(void *, const char *, size_t), void *);
int namespace_proto_worker_write(const char *, size_t);
}
#include "observer/namespace_worker_multiplex_prototype.ipp"
#include "observer/namespace_worker_scan_prototype.ipp"
#include "observer/namespace_worker_write_prototype.ipp"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace common;
using namespace share::schema;
using storage::NamespaceForkKernelPrototype;
bool enabled() {
  const char *value = std::getenv("SEEKDB_NAMESPACE_SQL_WORKER_PROTOTYPE");
  return value && !std::strcmp(value, "1") && NamespaceForkKernelPrototype::metadata_gc_mode();
}
bool bootstrap_enabled() {
  const char *value = std::getenv("SEEKDB_NAMESPACE_SQL_WORKER_BOOTSTRAP_PROTOTYPE");
  return value && !std::strcmp(value, "1");
}
int check_sql_execution_role() {
  if (bootstrap_enabled() && !worker_process) {
    fprintf(stderr, "PROTOTYPE_V18_SHARED_SQL_REJECT\n");
    return OB_NOT_SUPPORTED;
  }
  return OB_SUCCESS;
}
int admin_set_config(obcall::ObAdminSetConfigArg &arg) {
  if (worker_namespace != 1) { return OB_NOT_SUPPORTED; }
  Frame request('M'), reply; request.number(1); request.append(arg);
  int ret = worker_send(request);
  if (!ret) { ret = worker_read(reply); }
  if (!ret && reply.type() != 'g') { ret = OB_INVALID_ARGUMENT; }
  if (!ret) { ret = static_cast<int>(reply.number()); }
  if (!ret && !reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
  return ret;
}
bool is_storage_request(char type) {
  return type == 'd' || type == 'b' || type == 't' || type == 'i' || type == 'j'
      || type == 'O' || type == 'F' || type == 'X' || type == 'M' || type == 'T' || type == 'W';
}
// One admitted storage RPC at a time per SQL request. Native request workers
// execute it; the pipe reader only submits the task. No per-session thread.
struct StorageDispatch : std::enable_shared_from_this<StorageDispatch> {
  std::mutex mutex;
  std::mutex execution_mutex;
  std::condition_variable changed;
  size_t active = 0;
  bool closed = false;
  std::function<int(Frame &)> process;
  void finish() {
    std::lock_guard<std::mutex> guard(mutex);
    --active; changed.notify_all();
  }
  void close() {
    std::unique_lock<std::mutex> guard(mutex);
    closed = true;
    changed.wait(guard, [&] { return active == 0; });
    process = {};
  }
  struct Task final : rpc::ObSrvTask {
    struct Processor final : rpc::frame::ObReqProcessor {
      std::shared_ptr<StorageDispatch> owner;
      Frame input;
      bool finished = false;
      Processor(std::shared_ptr<StorageDispatch> context, Frame frame)
          : owner(std::move(context)), input(std::move(frame)) {}
      ~Processor() { if (!finished) { owner->finish(); } }
      int run() override {
        std::lock_guard<std::mutex> guard(owner->execution_mutex);
        const int ret = owner->process(input);
        owner->finish(); finished = true;
        return ret;
      }
    } processor;
    Task(std::shared_ptr<StorageDispatch> context, Frame frame)
        : processor(std::move(context), std::move(frame)) {}
    rpc::frame::ObReqProcessor &get_processor() override { return processor; }
  };
  int submit(Frame frame) {
    {
      std::lock_guard<std::mutex> guard(mutex);
      if (closed) { return OB_CONNECT_ERROR; }
      // The successor may arrive after reply publication but before run()
      // returns. Keep one such slot and serialize access to native owners.
      if (active >= 2) { return OB_SIZE_OVERFLOW; }
      ++active;
    }
    auto *task = OB_NEW(Task, "NsStorageRPC", shared_from_this(), std::move(frame));
    if (!task) { finish(); return OB_ALLOCATE_MEMORY_FAILED; }
    int ret = share::check_server_runtime_ready();
    if (!ret) { ret = static_cast<omt::ObServerRuntime *>(share::server_runtime())->recv_request(*task); }
    if (ret) { ob_delete(task); }
    return ret;
  }
};
struct Channel {
  void *handle = nullptr;
  uint64_t generation = 0;
  uint32_t pid = 0;
  std::atomic<bool> closed{false};
  RequestRoutes routes;
  std::mutex bindings_mutex;
  SessionBinding *bindings = nullptr;
  ~Channel() { fail(); namespace_proto_stop(handle); }
  void fail();
  int send(const Frame &frame) {
    if (closed) { return OB_CONNECT_ERROR; }
    const int ret = frame.ret ? frame.ret : namespace_proto_send(handle, frame.data.data(), frame.data.size()) == 0
        ? OB_SUCCESS : OB_CONNECT_ERROR;
    if (ret) { fail(); }
    return ret;
  }
  int receive(Frame &frame, uint64_t timeout = 30000) {
    const char *data = nullptr; size_t n = 0;
    const int result = namespace_proto_receive(handle, &data, &n, timeout);
    if (result) { return result == 1 ? OB_TIMEOUT : OB_CONNECT_ERROR; }
    if (n < Frame::HEADER_SIZE) { return OB_INVALID_ARGUMENT; }
    frame = Frame(); frame.data.assign(data, data + n); return OB_SUCCESS;
  }
  static void receive_frame(void *context, const char *data, size_t size) {
    auto &channel = *static_cast<Channel *>(context);
    if (channel.closed) { return; }
    if (!data || size < Frame::HEADER_SIZE) { channel.fail(); return; }
    Frame frame; frame.data.assign(data, data + size);
    auto request = channel.routes.find(frame.tag());
    // The Rust pipe reader only dispatches. It never waits for a request's
    // consumer, executes storage work, or calls a client's packet sender.
    if (request) {
      const int ret = is_storage_request(frame.type()) && request->dispatch_storage
          ? request->dispatch_storage(std::move(frame)) : request->post(std::move(frame));
      if (ret) { channel.fail(); }
    }
  }

};
struct Child { std::mutex mutex; std::shared_ptr<Channel> current; };
std::mutex children_mutex;
std::map<uint64_t, std::shared_ptr<Child>> children;
uint64_t next_generation = 0;
struct SessionBinding {
  std::shared_ptr<Channel> channel;
  uint64_t ns = 0, slot = 0, slot_generation = 0;
  sql::ObSQLSessionInfo *gateway = nullptr;
  bool internal = false;
  std::unique_ptr<EngineWrites> writes;
  SessionBinding *previous = nullptr, *next = nullptr;
  bool linked = false;
  ~SessionBinding() {
    if (linked) {
      std::lock_guard<std::mutex> guard(channel->bindings_mutex);
      if (previous) { previous->next = next; } else { channel->bindings = next; }
      if (next) { next->previous = previous; }
    }
    writes.reset();
    if (gateway && !internal) { share::server_service<sql::ObSQLSessionMgr>()->revert_session(gateway); }
  }
};
void Channel::fail() {
  if (!closed.exchange(true)) {
    routes.fail(); namespace_proto_interrupt(handle);
    // Shutdown only schedules native connection teardown. The IPC reader does
    // not wait for a session lock, execute rollback, or run another SQL engine.
    std::lock_guard<std::mutex> guard(bindings_mutex);
    for (auto *binding = bindings; binding; binding = binding->next) {
      auto &socket = binding->gateway->get_sock_desc();
      if (socket.sock_desc_) { SQL_REQ_OP.disconnect_by_sql_sock_desc(socket); }
    }
  }
}
sql::ObSQLSessionInfo *bound_session(SessionBinding *binding) { return binding ? binding->gateway : nullptr; }
int attach(uint64_t ns, std::shared_ptr<Child> &child) {
  if (!enabled() || ns == 0 || ns >= (1ULL << 30)) { return OB_NOT_SUPPORTED; }
  std::lock_guard<std::mutex> guard(children_mutex);
  auto it = children.find(ns);
  if (it == children.end()) {
    if (children.size() >= 2) { return OB_SIZE_OVERFLOW; }
    it = children.emplace(ns, std::make_shared<Child>()).first;
  }
  child = it->second; return OB_SUCCESS;
}
// These scalar mirrors are needed by gateway protocol encoding. SQL variables
// and their allocator remain in the worker; no complete session is serialized.
const share::ObSysVarClassType state_vars[] = {
  share::SYS_VAR_CHARACTER_SET_CLIENT, share::SYS_VAR_CHARACTER_SET_CONNECTION,
  share::SYS_VAR_CHARACTER_SET_RESULTS, share::SYS_VAR_COLLATION_CONNECTION,
  share::SYS_VAR_COLLATION_DATABASE, share::SYS_VAR_SQL_MODE, share::SYS_VAR_OB_QUERY_TIMEOUT,
  share::SYS_VAR_AUTOCOMMIT
};
int append_session_state(sql::ObSQLSessionInfo &session, Frame &frame) {
  frame.number(session.get_database_id()); frame.string(session.get_database_name());
  int ret = OB_SUCCESS;
  for (auto id : state_vars) {
    ObObj value;
    if ((ret = session.get_sys_variable(id, value))) { return ret; }
    frame.number(value.is_uint64() ? value.get_uint64() : static_cast<uint64_t>(value.get_int()));
  }
  return frame.ret;
}
int apply_session_state(sql::ObSQLSessionInfo &session, Frame &frame) {
  const uint64_t db = frame.number(); const ObString name = frame.string();
  int ret = frame.ret;
  if (!ret) { ret = session.set_default_database(name); session.set_database_id(db); }
  for (auto id : state_vars) {
    const uint64_t value = frame.number();
    if (!ret) { ret = frame.ret ? frame.ret : session.update_sys_variable(id, static_cast<int64_t>(value)); }
  }
  return ret ? ret : frame.ret;
}
int catalog(uint64_t ns, Frame &request, Frame &reply) {
  int ret = OB_SUCCESS;
  const uint64_t id = request.number();
  const ObString name = request.string();
  const ObDatabaseSchema *database = nullptr;
  const ObTableSchema *table = nullptr;
  const uint64_t owner = (id & ~(1ULL << 62)) >> 32;
  ObSchemaGetterGuard guard;
  if (ns == 1 && request.consumed() && (request.type() == 'd' || owns_table(ns, id))) {
    ret = ObMultiVersionSchemaService::get_instance().get_runtime_schema_guard(guard);
    if (!ret && request.type() == 'd') { ret = guard.get_database_schema(name, database); }
    else if (!ret && request.type() == 'b') { ret = guard.get_database_schema(id, database); }
    else if (!ret && (request.type() == 't' || request.type() == 'j')) { ret = guard.get_table_schema(id, name, request.type() == 'j', table); }
    else if (!ret && request.type() == 'i') { ret = guard.get_table_schema(id, table); }
  } else if (!request.consumed() || (request.type() == 'd' ? id != ns : owner != ns)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (request.type() == 'd') {
    ret = NamespaceForkKernelPrototype::database_in_namespace(ns, name, database);
  } else if (request.type() == 'b') {
    ret = NamespaceForkKernelPrototype::database_by_id(id, database);
  } else if (request.type() == 't') {
    ret = NamespaceForkKernelPrototype::schema_by_name(id, name, table);
  } else if (request.type() == 'i') {
    ret = NamespaceForkKernelPrototype::schema_by_id(id, table);
  } else { ret = OB_NOT_SUPPORTED; }
  reply = Frame('c'); reply.number(ret); reply.number(database || table ? 1 : 0);
  if (!ret && database) { reply.append(*database); }
  if (!ret && table) { reply.append(*table); }
  return reply.ret;
}
// SQL results retain credit-based streaming. Storage RPCs execute independently
// through the native shared runtime, including while the result consumer sleeps.
struct Exchange {
  Channel &channel;
  uint64_t ns;
  ReadScans *scans;
  EngineWrites *writes;
  bool query, finished = false;
  int error = OB_SUCCESS;
  std::atomic<int> cancelled{OB_SUCCESS};
  std::shared_ptr<PendingRequest> pending;
  std::shared_ptr<StorageDispatch> storage;
  Exchange(Channel &c, uint64_t n, Frame request, ReadScans *s, int64_t deadline, EngineWrites *w)
      : channel(c), ns(n), scans(s), writes(w),
        query(request.type() == 'Q' || request.type() == 'U' || request.type() == 'I') {
    pending = channel.routes.allocate(!query);
    if (!pending) { error = channel.closed ? OB_CONNECT_ERROR : OB_EAGAIN; return; }
    pending->deadline = query ? deadline : ObTimeUtility::current_time() + 30L * 1000000;
    storage = std::make_shared<StorageDispatch>();
    storage->process = [this](Frame &input) { return serve(input); };
    pending->dispatch_storage = [context = storage](Frame input) { return context->submit(std::move(input)); };
    request.tag(pending->tag); error = channel.send(request);
  }
  ~Exchange() {
    if (pending) { channel.routes.release(pending->tag, true); }
    // The borrowed native session and scan owners outlive this Exchange.
    // On pipe failure, drain an already admitted task before releasing either.
    if (storage) { storage->close(); }
  }
  int cancel(int reason) {
    if (cancelled || finished || !pending) { return error; }
    cancelled = reason;
    Frame message('Z'); message.tag(pending->tag);
    message.number(reason == OB_TIMEOUT ? OB_TIMEOUT : OB_ERR_QUERY_INTERRUPTED);
    fprintf(stderr, "PROTOTYPE_V13_CANCEL request=%llu generation=%llu ret=%d\n",
        (unsigned long long)pending->tag.slot, (unsigned long long)pending->tag.generation, reason);
    return error = channel.send(message);
  }
  int serve(Frame &input) {
    const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
    auto *saved_session = THIS_WORKER.get_session();
    THIS_WORKER.set_timeout_ts(pending->deadline);
    THIS_WORKER.set_session(writes ? &writes->session : nullptr);
    Frame result;
    const int state = cancelled.load();
    int ret = OB_SUCCESS;
    if (input.type() == 'd' || input.type() == 'b' || input.type() == 't' || input.type() == 'i' || input.type() == 'j') {
      result = Frame('c');
      if (state) { result.number(state); }
      else { ret = catalog(ns, input, result); }
    } else if (scans && (input.type() == 'O' || input.type() == 'F' || input.type() == 'X')) {
      result = Frame('s');
      if (state && input.type() != 'X') { result.number(state); }
      else { ret = scans->process(input, result, writes ? writes->tx : nullptr, writes ? &writes->session : nullptr); }
    } else if (input.type() == 'M') {
      result = Frame('g');
      const uint64_t operation = input.number();
      obcall::ObAdminSetConfigArg arg;
      input.read(arg);
      int command_ret = state ? state : ns != 1 || operation != 1 ? OB_NOT_SUPPORTED
          : !input.consumed() || !arg.is_valid() ? OB_INVALID_ARGUMENT : OB_SUCCESS;
      if (!command_ret) { command_ret = ObServer::get_instance().get_local_management_service().admin_set_config(arg); }
      result.number(command_ret);
    } else if (writes && (input.type() == 'T' || input.type() == 'W')) {
      result = Frame('w');
      if (state && !cleanup_write(input)) { result.number(state); }
      else { ret = writes->process(input, result); }
    } else { ret = OB_INVALID_ARGUMENT; }
    THIS_WORKER.set_session(saved_session);
    THIS_WORKER.set_timeout_ts(saved_timeout);
    result.tag(pending->tag);
    // Send credit before the RPC reply; the SQL worker needs both before its
    // next send. Dispatch serializes a successor arriving during publication.
    if (!ret) {
      Frame credit('K'); credit.tag(pending->tag);
      ret = channel.send(credit);
    }
    if (!ret) { ret = channel.send(result); }
    if (ret) { channel.fail(); }
    return ret;
  }
  int next(Frame &reply) {
    if (finished) { return OB_ITER_END; }
    while (!error) {
      error = pending->take(reply, cancelled != OB_SUCCESS);
      if (error == OB_TIMEOUT && query && !cancelled) { cancel(error); continue; }
      if (error) { break; }
      if (reply.type() == 'D') { finished = true; return OB_SUCCESS; }
      Frame credit('K'); credit.tag(pending->tag);
      if ((error = channel.send(credit))) { break; }
      if (!cancelled) { return OB_SUCCESS; }
    }
    channel.fail(); return error;
  }
};
int exchange(Channel &channel, uint64_t ns, Frame request, ReadScans *scans,
             const std::function<int(Frame &)> &response, int64_t deadline = INT64_MAX,
             EngineWrites *writes = nullptr) {
  Exchange pump(channel, ns, std::move(request), scans, deadline, writes);
  Frame reply;
  int ret = OB_SUCCESS;
  while (!(ret = pump.next(reply))) {
    if (reply.type() == 'D') {
      const int query_ret = static_cast<int>(reply.number());
      if (pump.query && !reply.ret && !reply.consumed()) { ret = response(reply); }
      if (!ret && !reply.consumed()) { ret = OB_INVALID_ARGUMENT; }
      if (ret) { channel.fail(); return ret; }
      const int transaction_ret = writes ? writes->check_finished() : OB_SUCCESS;
      return pump.cancelled ? pump.cancelled.load() : query_ret ? query_ret : transaction_ret;
    }
    if ((ret = response(reply))) {
      if (!pump.query || channel.closed || pump.cancel(ret)) { channel.fail(); return ret; }
    }
  }
  return ret;
}
int open_session(uint64_t ns, sql::ObSQLSessionInfo &gateway, SessionBinding *&binding, bool internal) {
  binding = nullptr;
  std::unique_ptr<SessionBinding> owned(new SessionBinding());
  std::shared_ptr<Child> child;
  int ret = attach(ns, child);
  if (ret) { return ret; }
  {
    // Only worker activation is serialized; existing queries continue running.
    std::lock_guard<std::mutex> guard(child->mutex);
    if (child->current && !child->current->closed) {
      const int ping = exchange(*child->current, ns, Frame('P'), nullptr, [](Frame &) { return OB_INVALID_ARGUMENT; });
      if (ping == OB_EAGAIN) { return ping; }
      if (ping) { child->current->fail(); }
    }
    if (!child->current || child->current->closed) {
      auto channel = std::make_shared<Channel>();
      { std::lock_guard<std::mutex> lock(children_mutex); channel->generation = ++next_generation; }
      channel->handle = namespace_proto_spawn(ns, channel->generation, &channel->pid);
      Frame ready;
      if (!channel->handle || channel->receive(ready) || ready.type() != 'Y' || ready.number() != ns || !ready.consumed()) {
        channel->fail(); return OB_CONNECT_ERROR;
      }
      if (namespace_proto_dispatch(channel->handle, Channel::receive_frame, channel.get())) {
        channel->fail(); return OB_CONNECT_ERROR;
      }
      child->current = channel;
      fprintf(stderr, "PROTOTYPE_V10_WORKER_READY ns=%llu generation=%llu pid=%u\n",
          (unsigned long long)ns, (unsigned long long)channel->generation, channel->pid);
    }
    owned->channel = child->current;
  }
  owned->internal = internal;
  if (internal) { owned->gateway = &gateway; }
  else if ((ret = share::server_service<sql::ObSQLSessionMgr>()->get_session(gateway.get_server_sid(), owned->gateway))) { return ret; }
  owned->ns = ns;
  owned->writes = std::make_unique<EngineWrites>(ns, gateway);
  Frame request(internal ? 'a' : 'A'); request.number(gateway.get_server_sid());
  request.number(gateway.get_capability().capability_);
  if ((ret = append_session_state(gateway, request))) { return ret; }
  bool opened = false;
  ret = exchange(*owned->channel, ns, request, nullptr, [&](Frame &reply) {
    if (reply.type() != 'a' || opened) { return OB_INVALID_ARGUMENT; }
    owned->slot = reply.number(); owned->slot_generation = reply.number();
    opened = reply.consumed() && owned->slot_generation != 0;
    return opened ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  });
  if (!ret && !opened) { owned->channel->fail(); ret = OB_INVALID_ARGUMENT; }
  if (!ret) {
    std::lock_guard<std::mutex> guard(owned->channel->bindings_mutex);
    if (owned->channel->closed) { ret = OB_CONNECT_ERROR; }
    else {
      owned->next = owned->channel->bindings;
      if (owned->next) { owned->next->previous = owned.get(); }
      owned->channel->bindings = owned.get(); owned->linked = true;
      binding = owned.release();
    }
  }
  return ret;
}
void close_session(SessionBinding *binding) {
  std::unique_ptr<SessionBinding> owned(binding);
  if (!owned) { return; }
  auto close = [&] {
    // Disconnect runs independently of the query task. Wait before destroying
    // either its bound transaction or the binding borrowed by that task.
    if (!owned->channel->closed) {
      Frame request('C'); request.number(owned->slot); request.number(owned->slot_generation);
      const int ret = exchange(*owned->channel, owned->ns, request, nullptr, [](Frame &) { return OB_INVALID_ARGUMENT; });
      if (ret) { owned->channel->fail(); }
    }
    owned->writes.reset();
  };
  if (owned->internal) { close(); } // The native inner connection already owns this lock.
  else { sql::ObSQLSessionInfo::LockGuard lock(owned->gateway->get_query_lock()); close(); }
}
int query(SessionBinding &binding, const ObString &sql, bool change_database,
          const std::function<int(Frame &)> &response) {
  if (binding.channel->closed) { return OB_CONNECT_ERROR; }
  EngineWrites &writes = *binding.writes;
  ReadScans scans(binding.ns); // Release scans before their borrowed transaction.
  Frame request(change_database ? 'U' : 'Q', MAX_SQL_MESSAGE);
  request.number(binding.slot); request.number(binding.slot_generation);
  const int64_t deadline = THIS_WORKER.get_timeout_ts();
  request.number(deadline); request.string(sql);
  if (request.ret) { return request.ret; }
  int response_ret = OB_SUCCESS;
  const int ret = exchange(*binding.channel, binding.ns, request, &scans, [&](Frame &frame) {
    if (frame.type() == 'D') { return apply_session_state(*binding.gateway, frame); }
    if (!response_ret) {
      response_ret = response(frame);
      if (response_ret) {
        fprintf(stderr, "PROTOTYPE_V11_RESPONSE_DRAIN ns=%llu slot=%llu ret=%d\n",
            (unsigned long long)binding.ns, (unsigned long long)binding.slot, response_ret);
      }
    }
    // Worker::is_timeout uses the cached clock, which can lag the Rust writer's
    // real clock. Use the same absolute deadline when classifying its failure.
    return response_ret && ObTimeUtility::current_time() >= deadline ? OB_TIMEOUT : response_ret;
  }, deadline, &writes);
  // The native SQL result has completed its cleanup before D. Keep the session
  // transaction across requests, but release this statement's snapshot pin.
  scans.scans.clear();
  binding.gateway->reset_reserved_snapshot_version();
  if (writes.check_finished()) { binding.channel->fail(); }
  if (ret == OB_TIMEOUT) { return ret; }
  return response_ret ? response_ret : ret;
}
void stop_all() {
  std::map<uint64_t, std::shared_ptr<Child>> detached;
  { std::lock_guard<std::mutex> guard(children_mutex); detached.swap(children); }
  for (auto &entry : detached) {
    std::lock_guard<std::mutex> guard(entry.second->mutex);
    if (entry.second->current) { entry.second->current->fail(); }
  }
}
int worker_send_wire(Frame frame) {
  return frame.ret ? frame.ret : namespace_proto_worker_write(frame.data.data(), frame.data.size()) == 0
      ? OB_SUCCESS : OB_CONNECT_ERROR;
}
int worker_read_wire(Frame &frame) {
  frame = Frame();
  const int ret = namespace_proto_worker_read([](void *context, const char *data, size_t size) {
    auto &frame = *static_cast<Frame *>(context);
    frame.data.assign(data, data + size);
  }, &frame);
  return ret ? OB_CONNECT_ERROR : frame.data.size() >= Frame::HEADER_SIZE ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}
int worker_send(const Frame &frame, bool cleanup) {
  if (!worker_request) { return worker_send_wire(frame); }
  const int ret = worker_request->take_credit(cleanup);
  if (ret) { return ret; }
  Frame output = frame; output.tag(worker_request->tag);
  return worker_send_wire(std::move(output));
}
int worker_read(Frame &frame) {
  // Once an RPC is sent, consume its reply even after cancellation so cleanup
  // cannot mistake an earlier operation's reply for its own.
  return worker_request ? worker_request->take(frame, true) : OB_ERR_UNEXPECTED;
}
int fetch_catalog(char type, uint64_t id, const ObString &name, Frame &reply) {
  Frame request(type); request.number(id); request.string(name);
  int ret = worker_send(request);
  if (!ret) { ret = worker_read(reply); }
  if (!ret && reply.type() != 'c') { ret = OB_INVALID_ARGUMENT; }
  if (!ret) { ret = static_cast<int>(reply.number()); }
  return ret ? ret : reply.ret;
}
} } }
