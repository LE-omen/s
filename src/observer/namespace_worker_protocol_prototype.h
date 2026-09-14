// Throwaway V10 wire protocol. Values only; all frames have a hard size limit.
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
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
constexpr size_t MAX_FRAME = 256 * 1024;
struct Frame {
  std::vector<char> data;
  int64_t pos = 1;
  int ret = common::OB_SUCCESS;
  explicit Frame(char type = '?') : data(1, type) {}
  char type() const { return data.empty() ? '?' : data[0]; }
  void number(uint64_t n) {
    if (data.size() + 8 > MAX_FRAME) { ret = common::OB_SIZE_OVERFLOW; return; }
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
    if (ret || s.length() < 0 || data.size() + s.length() > MAX_FRAME) { ret = common::OB_SIZE_OVERFLOW; return; }
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
    if (ret || n < 0 || data.size() + n > MAX_FRAME) { ret = common::OB_SIZE_OVERFLOW; return; }
    data.resize(p + n); ret = value.serialize(data.data(), data.size(), p); data.resize(p);
  }
  template<class T> void read(T &value) {
    if (!ret) { ret = value.deserialize(data.data(), data.size(), pos); }
  }
  bool consumed() const { return !ret && pos == static_cast<int64_t>(data.size()); }
};
using CatalogFetch = int (*)(char, uint64_t, const common::ObString &, Frame &);
inline CatalogFetch worker_catalog_fetch = nullptr;
inline uint64_t worker_namespace = 0;
bool enabled();
struct SessionBinding;
int open_session(uint64_t namespace_id, sql::ObSQLSessionInfo &gateway, SessionBinding *&binding);
sql::ObSQLSessionInfo *bound_session(SessionBinding *binding);
int append_session_state(sql::ObSQLSessionInfo &session, Frame &frame);
int apply_session_state(sql::ObSQLSessionInfo &session, Frame &frame);
void close_session(SessionBinding *binding);
int query(SessionBinding &binding, uint64_t snapshot, const common::ObString &sql, bool change_database,
          const std::function<int(Frame &)> &response);
void stop_all();
} } }
#endif
