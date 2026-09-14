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
std::shared_ptr<Child> find_child(uint64_t ns) {
  std::lock_guard<std::mutex> guard(children_mutex);
  auto it = children.find(ns); return it == children.end() ? nullptr : it->second;
}
int attach(uint64_t ns, uint64_t &generation) {
  if (!enabled() || ns <= 1 || ns >= (1ULL << 30)) { return OB_NOT_SUPPORTED; }
  std::shared_ptr<Child> child;
  {
    std::lock_guard<std::mutex> guard(children_mutex);
    auto it = children.find(ns);
    if (it == children.end()) {
      // Two workers suffice for this experiment; never silently grow the pool.
      if (children.size() >= 2) { return OB_SIZE_OVERFLOW; }
      it = children.emplace(ns, std::make_shared<Child>()).first;
    }
    child = it->second;
  }
  std::lock_guard<std::mutex> guard(child->mutex);
  if (child->handle) {
    Frame ping('P'), pong;
    if (child->send(ping) || child->receive(pong) || pong.type() != 'P') { child->stop(); }
  }
  if (!child->handle) {
    { std::lock_guard<std::mutex> guard(children_mutex); child->generation = ++next_generation; }
    child->handle = namespace_proto_spawn(ns, child->generation, &child->pid);
    Frame ready;
    if (!child->handle || child->receive(ready) || ready.type() != 'Y' || ready.number() != ns || !ready.consumed()) {
      child->stop(); return OB_CONNECT_ERROR;
    }
    fprintf(stderr, "PROTOTYPE_V10_WORKER_READY ns=%llu generation=%llu pid=%u\n",
        static_cast<unsigned long long>(ns), static_cast<unsigned long long>(child->generation), child->pid);
  }
  generation = child->generation; return OB_SUCCESS;
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
int query(uint64_t ns, uint64_t generation, uint64_t db, uint64_t snapshot,
          const ObString &sql, const std::function<int(Frame &)> &response) {
  auto child = find_child(ns);
  if (!child) { return OB_CONNECT_ERROR; }
  // Functional transport: one in-flight query per worker, no per-client IPC thread.
  std::unique_lock<std::mutex> guard(child->mutex, std::try_to_lock);
  if (!guard.owns_lock()) { return OB_EAGAIN; }
  if (!child->handle || child->generation != generation) { return OB_CONNECT_ERROR; }
  ReadScans scans(ns, snapshot);
  Frame request('Q'); request.number(db); request.number(snapshot); request.string(sql);
  if (request.ret) { return request.ret; }
  int ret = child->send(request);
  while (!ret) {
    Frame reply;
    if ((ret = child->receive(reply))) { break; }
    if (reply.type() == 'd' || reply.type() == 'b' || reply.type() == 't' || reply.type() == 'i') {
      Frame result;
      ret = catalog(ns, reply, result);
      if (!ret) { ret = child->send(result); }
    } else if (reply.type() == 'O' || reply.type() == 'F' || reply.type() == 'X') {
      Frame result; ret = scans.process(reply, result);
      if (!ret) { ret = child->send(result); }
    } else if (reply.type() == 'D') {
      const int query_ret = static_cast<int>(reply.number());
      if (!reply.consumed()) { ret = OB_INVALID_ARGUMENT; break; }
      return query_ret;
    } else { ret = response(reply); }
  }
  // Once framing/response fails, never reuse the uncertain stream.
  child->stop(); return ret;
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
