// Throwaway V13 wire protocol. Q/U carry one absolute deadline; Z cancels its tag.
#ifndef SEEKDB_NAMESPACE_WORKER_PROTOCOL_PROTOTYPE_H_
#define SEEKDB_NAMESPACE_WORKER_PROTOCOL_PROTOTYPE_H_
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>
namespace oceanbase { namespace sql { class ObSQLSessionInfo; } }
namespace oceanbase { namespace obcall { struct ObAdminSetConfigArg; } }
namespace oceanbase { namespace share { namespace schema { class ObPrivMgr; } } }
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
constexpr size_t MAX_FRAME = 256 * 1024;
constexpr size_t MAX_SQL_MESSAGE = 64 * 1024 * 1024;
struct RequestTag { uint64_t slot = 0, generation = 0; };
constexpr uint64_t WORKER_REQUEST = 1ULL << 63;
struct Frame {
  static constexpr int64_t HEADER_SIZE = 17; // type + request slot + generation
  std::vector<char> data;
  int64_t pos = HEADER_SIZE;
  int ret = common::OB_SUCCESS;
  size_t limit = MAX_SQL_MESSAGE;
  explicit Frame(char type = '?', size_t max_size = MAX_SQL_MESSAGE) : data(HEADER_SIZE, 0), limit(max_size) { data[0] = type; }
  char type() const { return data.empty() ? '?' : data[0]; }
  RequestTag tag() {
    if (data.size() < HEADER_SIZE) { ret = common::OB_INVALID_ARGUMENT; return {}; }
    const int64_t saved = pos; pos = 1;
    RequestTag result{number(), number()}; pos = saved; return result;
  }
  void tag(RequestTag route) {
    for (unsigned i = 0; i < 8; ++i) {
      data[1 + i] = static_cast<char>(route.slot >> (8 * i));
      data[9 + i] = static_cast<char>(route.generation >> (8 * i));
    }
  }
  void number(uint64_t n) {
    if (data.size() + 8 > limit) { ret = common::OB_SIZE_OVERFLOW; return; }
    for (unsigned i = 0; i < 8; ++i) { data.push_back(static_cast<char>(n >> (8 * i))); }
  }
  uint64_t number() {
    uint64_t n = 0;
    if (pos + 8 > static_cast<int64_t>(data.size())) { ret = common::OB_INVALID_ARGUMENT; return 0; }
    for (unsigned i = 0; i < 8; ++i) { n |= uint64_t(static_cast<unsigned char>(data[pos++])) << (8 * i); }
    return n;
  }
  void string(const common::ObString &s) {
    number(s.length());
    if (ret || s.length() < 0 || data.size() + s.length() > limit) { ret = common::OB_SIZE_OVERFLOW; return; }
    if (!s.empty()) { data.insert(data.end(), s.ptr(), s.ptr() + s.length()); }
  }
  common::ObString string() {
    const uint64_t n = number();
    if (ret || n > data.size() - pos) { ret = common::OB_INVALID_ARGUMENT; return {}; }
    common::ObString s(static_cast<int32_t>(n), data.data() + pos); pos += n; return s;
  }
  template<class T> void append(const T &value) {
    const int64_t n = value.get_serialize_size();
    int64_t p = data.size();
    if (ret || n < 0 || data.size() + n > limit) { ret = common::OB_SIZE_OVERFLOW; return; }
    data.resize(p + n); ret = value.serialize(data.data(), data.size(), p); data.resize(p);
  }
  template<class T> void read(T &value) {
    if (!ret) { ret = value.deserialize(data.data(), data.size(), pos); }
  }
  bool consumed() const { return !ret && pos == static_cast<int64_t>(data.size()); }
};
using CatalogFetch = int (*)(char, uint64_t, const common::ObString &, int64_t, Frame &);
inline CatalogFetch worker_catalog_fetch = nullptr;
inline uint64_t worker_namespace = 0;
inline bool worker_process = false;
bool bootstrap_enabled();
int check_sql_execution_role();
int fetch_schema_version(bool published, bool core_version, int64_t &version);
share::schema::ObPrivMgr *make_remote_priv_mgr(int64_t version);
int admin_set_config(obcall::ObAdminSetConfigArg &arg);
bool enabled();
struct SessionBinding;
struct PendingRequest;
// Native inner SQL can switch sessions while keeping the same execution stack.
// Keep its storage route with that session and restore the caller on return.
class StorageSessionScope {
public:
  explicit StorageSessionScope(sql::ObSQLSessionInfo *session, bool create = true);
  ~StorageSessionScope();
  int error() const { return error_; }
  void close(SessionBinding *&binding);
private:
  PendingRequest *previous_;
  bool switched_ = false;
  int error_ = common::OB_SUCCESS;
  StorageSessionScope(const StorageSessionScope &) = delete;
  StorageSessionScope &operator=(const StorageSessionScope &) = delete;
};
int begin_direct_request(uint32_t sid, SessionBinding *&binding, bool internal = false);
int finish_direct_request();
int bind_direct_session(SessionBinding *binding, sql::ObSQLSessionInfo &session);
int open_session(uint64_t namespace_id, sql::ObSQLSessionInfo &gateway, SessionBinding *&binding, bool internal = false);
sql::ObSQLSessionInfo *bound_session(SessionBinding *binding);
int append_session_state(sql::ObSQLSessionInfo &session, Frame &frame, bool identity = false);
int apply_session_state(sql::ObSQLSessionInfo &session, Frame &frame);
void close_session(SessionBinding *binding);
int query(SessionBinding &binding, const common::ObString &sql, bool change_database,
          const std::function<int(Frame &)> &response);
void stop_all();
} } }
#endif
