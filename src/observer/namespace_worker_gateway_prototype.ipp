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
int namespace_proto_worker_read(char *, size_t, size_t *);
int namespace_proto_worker_write(const char *, size_t);
}
#include "observer/namespace_worker_scan_prototype.ipp"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace common;
using namespace share::schema;
using storage::NamespaceForkKernelPrototype;
bool enabled() {
  const char *value = std::getenv("SEEKDB_NAMESPACE_SQL_WORKER_PROTOTYPE");
  return value && !std::strcmp(value, "1") && NamespaceForkKernelPrototype::metadata_gc_mode();
}
struct Child {
  std::mutex mutex;
  void *handle = nullptr;
  uint64_t generation = 0;
  uint32_t pid = 0;
  ~Child() { namespace_proto_stop(handle); }
  void stop() { namespace_proto_stop(handle); handle = nullptr; }
  int send(const Frame &frame) {
    return frame.ret ? frame.ret : namespace_proto_send(handle, frame.data.data(), frame.data.size()) == 0
        ? OB_SUCCESS : OB_CONNECT_ERROR;
  }
  int receive(Frame &frame) {
    const char *data = nullptr; size_t n = 0;
    if (namespace_proto_receive(handle, &data, &n, 30000)) { return OB_CONNECT_ERROR; }
    frame = Frame(); frame.data.assign(data, data + n); return OB_SUCCESS;
  }
};
std::mutex children_mutex;
std::map<uint64_t, std::shared_ptr<Child>> children;
uint64_t next_generation = 0;
struct SessionBinding {
  std::shared_ptr<Child> child;
  uint64_t ns = 0, generation = 0, slot = 0, slot_generation = 0;
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
  child = it->second;
  return OB_SUCCESS;
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
int exchange(Child &child, uint64_t ns, Frame &request, ReadScans *scans,
             const std::function<int(Frame &)> &response) {
  int ret = child.send(request);
  while (!ret) {
    Frame reply;
    if ((ret = child.receive(reply))) { break; }
    if (reply.type() == 'd' || reply.type() == 'b' || reply.type() == 't' || reply.type() == 'i') {
      Frame result; ret = catalog(ns, reply, result);
      if (!ret) { ret = child.send(result); }
    } else if (scans && (reply.type() == 'O' || reply.type() == 'F' || reply.type() == 'X')) {
      Frame result; ret = scans->process(reply, result);
      if (!ret) { ret = child.send(result); }
    } else if (reply.type() == 'D') {
      const int query_ret = static_cast<int>(reply.number());
      if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; break; }
      return query_ret;
    } else { ret = response(reply); }
  }
  child.stop(); return ret;
}
int open_session(uint64_t ns, sql::ObSQLSessionInfo &gateway, SessionBinding *&binding) {
  binding = nullptr;
  std::unique_ptr<SessionBinding> owned(new SessionBinding());
  int ret = attach(ns, owned->child);
  if (ret) { return ret; }
  Child &child = *owned->child;
  std::lock_guard<std::mutex> guard(child.mutex);
  if (child.handle) {
    Frame ping('P'), pong;
    if (child.send(ping) || child.receive(pong) || pong.type() != 'P') { child.stop(); }
  }
  if (!child.handle) {
    { std::lock_guard<std::mutex> lock(children_mutex); child.generation = ++next_generation; }
    child.handle = namespace_proto_spawn(ns, child.generation, &child.pid);
    Frame ready;
    if (!child.handle || child.receive(ready) || ready.type() != 'Y' || ready.number() != ns || !ready.consumed()) {
      child.stop(); return OB_CONNECT_ERROR;
    }
    fprintf(stderr, "PROTOTYPE_V10_WORKER_READY ns=%llu generation=%llu pid=%u\n",
        (unsigned long long)ns, (unsigned long long)child.generation, child.pid);
  }
  // One connection-owned reference. Rust's request gate drains users before
  // transferring this binding to ObDisconnectTask; ordinary requests borrow it.
  if ((ret = share::server_service<sql::ObSQLSessionMgr>()->get_session(gateway.get_server_sid(), owned->gateway))) {
    return ret;
  }
  owned->ns = ns; owned->generation = child.generation;
  Frame request('A'); request.number(gateway.get_server_sid());
  if ((ret = append_session_state(gateway, request))) { return ret; }
  bool opened = false;
  ret = exchange(child, ns, request, nullptr, [&](Frame &reply) {
    if (reply.type() != 'a' || opened) { return OB_INVALID_ARGUMENT; }
    owned->slot = reply.number(); owned->slot_generation = reply.number();
    opened = reply.consumed() && owned->slot_generation != 0;
    return opened ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  });
  if (!ret && !opened) { child.stop(); ret = OB_INVALID_ARGUMENT; }
  if (!ret) { binding = owned.release(); }
  return ret;
}
void close_session(SessionBinding *binding) {
  std::unique_ptr<SessionBinding> owned(binding);
  if (!owned) { return; }
  Child &child = *owned->child;
  std::lock_guard<std::mutex> guard(child.mutex);
  if (child.handle && child.generation == owned->generation) {
    Frame request('C'); request.number(owned->slot); request.number(owned->slot_generation);
    exchange(child, owned->ns, request, nullptr, [](Frame &) { return OB_INVALID_ARGUMENT; });
  }
}
int query(SessionBinding &binding, uint64_t snapshot, const ObString &sql, bool change_database,
          const std::function<int(Frame &)> &response) {
  // Route pointer was bound at login; no namespace map lookup on this path.
  Child &child = *binding.child;
  std::unique_lock<std::mutex> guard(child.mutex, std::try_to_lock);
  if (!guard.owns_lock()) { return OB_EAGAIN; }
  if (!child.handle || child.generation != binding.generation) { return OB_CONNECT_ERROR; }
  ReadScans scans(binding.ns, snapshot);
  Frame request(change_database ? 'U' : 'Q');
  request.number(binding.slot); request.number(binding.slot_generation);
  request.number(snapshot); request.string(sql);
  if (request.ret) { return request.ret; }
  int response_ret = OB_SUCCESS;
  const int ret = exchange(child, binding.ns, request, &scans, [&](Frame &frame) {
    // A lost client does not invalidate framing or other sessions. Drain this
    // response through D before admitting the next command on the shared pipe.
    if (!response_ret) {
      response_ret = response(frame);
      if (response_ret) {
        fprintf(stderr, "PROTOTYPE_V11_RESPONSE_DRAIN ns=%llu slot=%llu ret=%d\n",
            (unsigned long long)binding.ns, (unsigned long long)binding.slot, response_ret);
      }
    }
    return OB_SUCCESS;
  });
  return response_ret ? response_ret : ret;
}
void stop_all() {
  std::map<uint64_t, std::shared_ptr<Child>> detached;
  { std::lock_guard<std::mutex> guard(children_mutex); detached.swap(children); }
  for (auto &entry : detached) { std::lock_guard<std::mutex> guard(entry.second->mutex); entry.second->stop(); }
}
int worker_send(const Frame &frame) {
  return frame.ret ? frame.ret : namespace_proto_worker_write(frame.data.data(), frame.data.size()) == 0
      ? OB_SUCCESS : OB_CONNECT_ERROR;
}
int worker_read(Frame &frame) {
  frame = Frame(); frame.data.resize(MAX_FRAME); size_t size = 0;
  if (namespace_proto_worker_read(frame.data.data(), frame.data.size(), &size)) { return OB_CONNECT_ERROR; }
  frame.data.resize(size); return OB_SUCCESS;
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
