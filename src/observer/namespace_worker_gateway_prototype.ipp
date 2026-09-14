// Included in the Observer composition unit. The gateway remains the sole engine.
#include "observer/namespace_worker_protocol_prototype.h"
#include "rootserver/fork_table/namespace_fork_kernel_prototype.h"
#include <map>
#include <mutex>
extern "C" {
void *namespace_proto_spawn(uint64_t, uint64_t, uint32_t *);
int namespace_proto_send(void *, const char *, size_t);
int namespace_proto_receive(void *, const char **, size_t *, uint64_t);
void namespace_proto_stop(void *);
void namespace_proto_interrupt(void *);
int namespace_proto_dispatch(void *, void (*)(void *, const char *, size_t), void *);
int namespace_proto_worker_read(char *, size_t, size_t *);
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
struct Channel {
  void *handle = nullptr;
  uint64_t generation = 0;
  uint32_t pid = 0;
  std::atomic<bool> closed{false};
  RequestRoutes routes;
  ~Channel() { fail(); namespace_proto_stop(handle); }
  void fail() {
    if (!closed.exchange(true)) { routes.fail(); namespace_proto_interrupt(handle); }
  }
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
    if (request && request->post(std::move(frame))) { channel.fail(); }
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
  ~SessionBinding() {
    if (gateway) { share::server_service<sql::ObSQLSessionMgr>()->revert_session(gateway); }
  }
};
sql::ObSQLSessionInfo *bound_session(SessionBinding *binding) { return binding ? binding->gateway : nullptr; }
int attach(uint64_t ns, std::shared_ptr<Child> &child) {
  if (!enabled() || ns <= 1 || ns >= (1ULL << 30)) { return OB_NOT_SUPPORTED; }
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
  share::SYS_VAR_COLLATION_DATABASE, share::SYS_VAR_SQL_MODE, share::SYS_VAR_OB_QUERY_TIMEOUT
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
  if (!request.consumed() || (request.type() == 'd' ? id != ns : owner != ns)) {
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
int exchange(Channel &channel, uint64_t ns, Frame request, ReadScans *scans,
             const std::function<int(Frame &)> &response, int64_t deadline = INT64_MAX,
             EngineWrites *writes = nullptr) {
  const bool query = request.type() == 'Q' || request.type() == 'U';
  auto pending = channel.routes.allocate(!query);
  if (!pending) { return channel.closed ? OB_CONNECT_ERROR : OB_EAGAIN; }
  pending->deadline = query ? deadline : ObTimeUtility::current_time() + 30L * 1000000;
  struct Release { RequestRoutes &routes; RequestTag tag; ~Release() { routes.release(tag, true); } } release{channel.routes, pending->tag};
  request.tag(pending->tag);
  int ret = channel.send(request);
  int cancelled = OB_SUCCESS;
  auto cancel = [&](int reason) {
    cancelled = reason;
    Frame message('Z'); message.tag(pending->tag);
    message.number(reason == OB_TIMEOUT ? OB_TIMEOUT : OB_ERR_QUERY_INTERRUPTED);
    fprintf(stderr, "PROTOTYPE_V13_CANCEL request=%llu generation=%llu ret=%d\n",
        (unsigned long long)pending->tag.slot, (unsigned long long)pending->tag.generation, reason);
    return channel.send(message);
  };
  while (!ret) {
    Frame reply;
    ret = pending->take(reply, cancelled != OB_SUCCESS);
    if (ret == OB_TIMEOUT && query && !cancelled) { ret = cancel(ret); continue; }
    if (ret) { break; }
    if (reply.type() == 'D') {
      const int query_ret = static_cast<int>(reply.number());
      // Terminal scalar state also covers SET that completed just as cancellation
      // arrived. It must be applied even when result delivery was interrupted.
      if (query && !reply.ret && !reply.consumed()) { ret = response(reply); }
      if (ret) { break; }
      if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; break; }
      const int transaction_ret = writes && !query_ret ? writes->check_finished() : OB_SUCCESS;
      return cancelled ? cancelled : query_ret ? query_ret : transaction_ret;
    }
    // One buffered reply per request, independent of every other request. Give
    // its credit back after taking ownership, before doing SQL/storage work.
    Frame credit('K'); credit.tag(pending->tag);
    if ((ret = channel.send(credit))) { break; }
    if (cancelled) { continue; } // Keep ownership until D; no new storage work.
    if (reply.type() == 'd' || reply.type() == 'b' || reply.type() == 't' || reply.type() == 'i') {
      Frame result; ret = catalog(ns, reply, result); result.tag(pending->tag);
      if (!ret) { ret = channel.send(result); }
    } else if (scans && (reply.type() == 'O' || reply.type() == 'F' || reply.type() == 'X')) {
      Frame result; ret = scans->process(reply, result); result.tag(pending->tag);
      if (!ret) { ret = channel.send(result); }
    } else if (writes && (reply.type() == 'T' || reply.type() == 'W')) {
      Frame result; ret = writes->process(reply, result); result.tag(pending->tag);
      if (!ret) { ret = channel.send(result); }
    } else { ret = response(reply); }
    if (ret && query && !channel.closed) { ret = cancel(ret); }
  }
  channel.fail(); return ret;
}
int open_session(uint64_t ns, sql::ObSQLSessionInfo &gateway, SessionBinding *&binding) {
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
  if ((ret = share::server_service<sql::ObSQLSessionMgr>()->get_session(gateway.get_server_sid(), owned->gateway))) { return ret; }
  owned->ns = ns;
  Frame request('A'); request.number(gateway.get_server_sid());
  if ((ret = append_session_state(gateway, request))) { return ret; }
  bool opened = false;
  ret = exchange(*owned->channel, ns, request, nullptr, [&](Frame &reply) {
    if (reply.type() != 'a' || opened) { return OB_INVALID_ARGUMENT; }
    owned->slot = reply.number(); owned->slot_generation = reply.number();
    opened = reply.consumed() && owned->slot_generation != 0;
    return opened ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  });
  if (!ret && !opened) { owned->channel->fail(); ret = OB_INVALID_ARGUMENT; }
  if (!ret) { binding = owned.release(); }
  return ret;
}
void close_session(SessionBinding *binding) {
  std::unique_ptr<SessionBinding> owned(binding);
  if (!owned || owned->channel->closed) { return; }
  Frame request('C'); request.number(owned->slot); request.number(owned->slot_generation);
  const int ret = exchange(*owned->channel, owned->ns, request, nullptr, [](Frame &) { return OB_INVALID_ARGUMENT; });
  // If even reserved control admission cannot progress, retaining an unreachable
  // remote session is unsafe. Fail this activation and wake all its callers.
  if (ret) { owned->channel->fail(); }
}
int query(SessionBinding &binding, uint64_t snapshot, const ObString &sql, bool change_database,
          const std::function<int(Frame &)> &response) {
  if (binding.channel->closed) { return OB_CONNECT_ERROR; }
  ReadScans scans(binding.ns, snapshot);
  EngineWrites writes(binding.ns, binding.gateway->get_server_sid());
  Frame request(change_database ? 'U' : 'Q');
  request.number(binding.slot); request.number(binding.slot_generation);
  const int64_t deadline = THIS_WORKER.get_timeout_ts();
  request.number(snapshot); request.number(deadline); request.string(sql);
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
  frame = Frame(); frame.data.resize(MAX_FRAME); size_t size = 0;
  if (namespace_proto_worker_read(frame.data.data(), frame.data.size(), &size)) { return OB_CONNECT_ERROR; }
  frame.data.resize(size); return size >= Frame::HEADER_SIZE ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}
int worker_send(const Frame &frame) {
  if (!worker_request) { return worker_send_wire(frame); }
  const int ret = worker_request->take_credit();
  if (ret) { return ret; }
  Frame output = frame; output.tag(worker_request->tag);
  return worker_send_wire(std::move(output));
}
int worker_read(Frame &frame) {
  return worker_request ? worker_request->take(frame) : OB_ERR_UNEXPECTED;
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
