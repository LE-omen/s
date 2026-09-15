// PROTOTYPE: real immutable B+ tree pages stored in engine tables, one-engine transactions.
// Fixed two-integer-column schemas; mode 6 adds explicit metadata page reclamation.
#define USING_LOG_PREFIX STORAGE
#include "rootserver/fork_table/namespace_fork_kernel_prototype.h"
#include "observer/namespace_worker_protocol_prototype.h"
#include "rootserver/ob_tablet_creator.h"
#include "rootserver/ddl_task/ob_ddl_task_util.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/ob_debug_sync.h"
#include "share/tablet/ob_tablet_mapping_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/rc/ob_server_runtime.h"
#include "storage/compaction/ob_freeze_info_mgr.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_tablet_fork_task.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/time/ob_time_utility.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <unordered_set>

namespace oceanbase {
namespace storage {
using namespace common;
using namespace share;
using namespace share::schema;
namespace {
// Encodes (owner, local id): owner is the V2 database or V5 namespace.
// Both parts are checked; ids with the marker are reserved for this isolated experiment.
constexpr uint64_t ID_MARK = 1ULL << 62;
constexpr size_t FANOUT = 8;
const char *ROOTS = "__fork_proto_meta.roots";
const char *PAGES = "__fork_proto_meta.pages";
const char *NAMESPACES = "__fork_proto_meta.namespaces";
const char *SNAPSHOTS = "__fork_proto_meta.snapshots";
// Only the native source DROP uses this scoped internal DDL capability.
std::atomic<const ObISQLClient *> source_drop_trans{nullptr};
// ponytail: one global count, so DROP can wait for unrelated long scans/DAGs.
// No per-namespace registry/cache; use worker-local draining for production isolation.
std::atomic<int64_t> active_accesses{0};
// ponytail: manual GC excludes all metadata operations while marking/sweeping.
// One lock and nesting depth per thread, no resident page/reader registry. Use
// versioned reader epochs if measurements justify concurrent marking later.
std::shared_timed_mutex metadata_mutex;
thread_local int metadata_depth = 0;
class MetadataReadGuard final {
public:
  MetadataReadGuard() : held_(false), ret_(OB_SUCCESS) {
    if (NamespaceForkKernelPrototype::metadata_gc_mode()) {
      if (!metadata_depth) {
        while (!metadata_mutex.try_lock_shared_for(std::chrono::milliseconds(1))) {
          if (OB_SUCCESS != (ret_ = THIS_WORKER.check_status())) { return; }
        }
      }
      ++metadata_depth; held_ = true;
    }
  }
  ~MetadataReadGuard() { if (held_ && --metadata_depth == 0) { metadata_mutex.unlock_shared(); } }
  int error() const { return ret_; }
private:
  bool held_;
  int ret_;
  MetadataReadGuard(const MetadataReadGuard &) = delete;
  MetadataReadGuard &operator=(const MetadataReadGuard &) = delete;
};
struct Ref { uint64_t page = 0; int64_t cap = 0; };
struct Value { std::string data; int64_t cap = 0; };
struct Node {
  bool leaf = true;
  std::vector<std::string> keys;
  std::vector<Value> values;
  std::vector<Ref> children;
};
struct Roots {
  uint64_t source = 0;
  Ref catalog, directory;
  int64_t snapshot = 0, schema_version = 0;
  uint64_t snapshot_ref = 0;
  uint64_t parent_ref = 0; int64_t ref_count = 0; // Only canonical V8 snapshot rows.
  int64_t state = 0; // 0 LIVE, 1 DELETING, 2 DELETED; names/ids are not reused.
};
struct SchemaHolder {
  ObArenaAllocator allocator;
  ObTableSchema schema;
  SchemaHolder() : allocator("ForkProtoSchema"), schema(&allocator) {}
};
std::mutex schema_mutex;
// Immutable schemas remain alive for existing schema guards and cached plans.
// ponytail: process-lifetime schema cache; bounded by tables opened in this disposable instance.
std::map<uint64_t, std::unique_ptr<SchemaHolder>> schemas;
struct DatabaseHolder {
  ObArenaAllocator allocator;
  ObDatabaseSchema schema;
  ObSimpleDatabaseSchema simple;
  DatabaseHolder() : allocator("ForkProtoDB"), schema(&allocator), simple(&allocator) {}
};
// Schemas stay alive for old guards/plans; storage admission rejects deleted namespaces.
std::map<uint64_t, std::unique_ptr<DatabaseHolder>> database_schemas;

// V10: the SQL-only process owns these decoded schemas. The engine sends values
// over IPC; neither its schema pointers nor SQL proxy cross the process boundary.
int remote_database(char op, uint64_t id, const ObString &name, const ObDatabaseSchema *&schema) {
  using namespace observer::namespace_worker_prototype;
  schema = nullptr; Frame reply;
  int ret = worker_catalog_fetch(op, id, name, OB_INVALID_VERSION, reply);
  if (ret || reply.number() == 0) { return ret ? ret : reply.ret; }
  std::unique_ptr<DatabaseHolder> holder(new DatabaseHolder());
  reply.read(holder->schema);
  if (!reply.consumed()) { return reply.ret ? reply.ret : OB_INVALID_ARGUMENT; }
  holder->simple.set_database_id(holder->schema.get_database_id());
  holder->simple.set_schema_version(holder->schema.get_schema_version());
  holder->simple.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
  if ((ret = holder->simple.set_database_name(holder->schema.get_database_name_str()))) { return ret; }
  std::lock_guard<std::mutex> lock(schema_mutex);
  auto &slot = database_schemas[holder->schema.get_database_id()];
  if (!slot) { slot = std::move(holder); }
  schema = &slot->schema; return OB_SUCCESS;
}
int remote_table(char op, uint64_t id, const ObString &name, const ObTableSchema *&schema) {
  using namespace observer::namespace_worker_prototype;
  schema = nullptr; Frame reply;
  int ret = worker_catalog_fetch(op, id, name, OB_INVALID_VERSION, reply);
  if (ret || reply.number() == 0) { return ret ? ret : reply.ret; }
  std::unique_ptr<SchemaHolder> holder(new SchemaHolder());
  reply.read(holder->schema);
  if (!reply.consumed()) { return reply.ret ? reply.ret : OB_INVALID_ARGUMENT; }
  std::lock_guard<std::mutex> lock(schema_mutex);
  auto &slot = schemas[holder->schema.get_table_id()];
  if (!slot) { slot = std::move(holder); }
  schema = &slot->schema; return OB_SUCCESS;
}

int64_t cap_min(int64_t a, int64_t b) { return a == 0 ? b : b == 0 ? a : std::min(a, b); }
uint64_t encoded(uint64_t db, uint64_t local) {
  return NamespaceForkKernelPrototype::namespace_mode()
      ? NamespaceObjectKey{db, local}.storage_id() : ID_MARK | (db << 32) | local;
}
uint64_t database_of(uint64_t id) { return (id & ~ID_MARK) >> 32; }
uint64_t local_of(uint64_t id) { return id & 0xffffffffULL; }
std::string key_of(uint64_t id) {
  char buf[32]; snprintf(buf, sizeof(buf), "%020lu", id); return buf;
}
std::string hex(const std::string &s) {
  const char *digits = "0123456789abcdef";
  std::string out; out.reserve(s.size() * 2);
  for (unsigned char c : s) { out += digits[c >> 4]; out += digits[c & 15]; }
  return out;
}
void number(std::string &s, uint64_t n) {
  for (int i = 0; i < 8; ++i) { s += static_cast<char>(n >> (8 * i)); }
}
bool number(const std::string &s, size_t &pos, uint64_t &n) {
  if (pos > s.size() || s.size() - pos < 8) { return false; }
  n = 0;
  for (int i = 0; i < 8; ++i) { n |= uint64_t(static_cast<unsigned char>(s[pos++])) << (8 * i); }
  return true;
}
void bytes(std::string &s, const std::string &v) { number(s, v.size()); s += v; }
bool bytes(const std::string &s, size_t &pos, std::string &v) {
  uint64_t size = 0;
  if (!number(s, pos, size) || size > s.size() - pos) { return false; }
  v.assign(s, pos, size); pos += size; return true;
}
std::string entry(uint64_t schema_object, uint64_t local_table, uint64_t local_tablet,
                  uint64_t bound_tablet = 0) {
  std::string s; number(s, schema_object); number(s, local_table); number(s, local_tablet);
  number(s, bound_tablet); return s;
}
bool entry(const std::string &s, uint64_t &object, uint64_t &table, uint64_t &tablet, uint64_t &bound) {
  size_t p = 0;
  return number(s, p, object) && number(s, p, table) && number(s, p, tablet)
      && number(s, p, bound) && p == s.size();
}
int write_sql(ObISQLClient &sql, const ObSqlString &statement) {
  int64_t affected = 0; return sql.write(statement.ptr(), affected);
}
int blob(ObISQLClient &sql, uint64_t id, std::string &out) {
  int ret = OB_SUCCESS;
  ObSqlString q; ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
  ObString value;
  if (OB_FAIL(q.assign_fmt("SELECT payload FROM %s WHERE id=%lu", PAGES, id))) {
  } else if (OB_FAIL(sql.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_varchar(0L, value))) {
  } else {
    out.assign(value.ptr(), value.length());
    if (ob_crc64(out.data(), out.size()) != id) { ret = OB_CHECKSUM_ERROR; }
  }
  return ret;
}
int save_blob(ObISQLClient &sql, const std::string &data, uint64_t &id) {
  int ret = OB_SUCCESS;
  id = ob_crc64(data.data(), data.size());
  std::string existing;
  if (id == 0 || data.size() > 60000) { ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(blob(sql, id, existing))) {
    if (ret == OB_ITER_END) {
      ObSqlString q;
      if (OB_FAIL(q.assign_fmt("INSERT INTO %s VALUES(%lu,UNHEX('%s'))", PAGES, id, hex(data).c_str()))) {
      } else { ret = write_sql(sql, q); }
    }
  } else if (existing != data) { ret = OB_CHECKSUM_ERROR; }
  return ret;
}
int read_node(ObISQLClient &sql, Ref ref, Node &node) {
  if (!ref.page) { node = Node(); return OB_SUCCESS; }
  std::string s; int ret = blob(sql, ref.page, s);
  if (ret != OB_SUCCESS) { return ret; }
  uint64_t version = 0, leaf = 0, count = 0, n = 0; size_t p = 0;
  if (!number(s, p, version) || version != 1 || !number(s, p, leaf) || leaf > 1
      || !number(s, p, count) || count > FANOUT) { return OB_CHECKSUM_ERROR; }
  node = Node(); node.leaf = leaf; node.keys.resize(count);
  for (auto &key : node.keys) { if (!bytes(s, p, key)) { return OB_CHECKSUM_ERROR; } }
  if (!std::is_sorted(node.keys.begin(), node.keys.end())
      || std::adjacent_find(node.keys.begin(), node.keys.end()) != node.keys.end()) { return OB_CHECKSUM_ERROR; }
  if (node.leaf) {
    node.values.resize(count);
    for (auto &value : node.values) {
      if (!bytes(s, p, value.data) || !number(s, p, n)) { return OB_CHECKSUM_ERROR; }
      value.cap = cap_min(n, ref.cap);
    }
  } else {
    node.children.resize(count + 1);
    for (auto &child : node.children) {
      if (!number(s, p, child.page) || !child.page || !number(s, p, n)) { return OB_CHECKSUM_ERROR; }
      child.cap = cap_min(n, ref.cap);
    }
  }
  return p == s.size() ? OB_SUCCESS : OB_CHECKSUM_ERROR;
}
int save_node(ObISQLClient &sql, const Node &node, Ref &ref) {
  std::string s; number(s, 1); number(s, node.leaf); number(s, node.keys.size());
  for (auto &key : node.keys) { bytes(s, key); }
  if (node.leaf) {
    for (auto &v : node.values) { bytes(s, v.data); number(s, v.cap); }
  } else {
    for (auto &c : node.children) { number(s, c.page); number(s, c.cap); }
  }
  ref.cap = 0; return save_blob(sql, s, ref.page);
}
int find(ObISQLClient &sql, Ref ref, const std::string &key, Value &value) {
  for (int depth = 0; depth < 32; ++depth) {
    if (!ref.page) { return OB_ENTRY_NOT_EXIST; }
    Node node; int ret = read_node(sql, ref, node);
    if (ret != OB_SUCCESS) { return ret; }
    if (node.leaf) {
      auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
      if (it == node.keys.end() || *it != key) { return OB_ENTRY_NOT_EXIST; }
      value = node.values[it - node.keys.begin()]; return OB_SUCCESS;
    }
    ref = node.children[std::upper_bound(node.keys.begin(), node.keys.end(), key) - node.keys.begin()];
  }
  return OB_SIZE_OVERFLOW;
}
struct Split { Ref left, right; std::string separator; };
int put_path(ObISQLClient &sql, Ref root, const std::string &key, Value value, Split &out, int depth) {
  if (depth > 32) { return OB_SIZE_OVERFLOW; }
  Node node; int ret = read_node(sql, root, node);
  if (ret != OB_SUCCESS) { return ret; }
  if (node.leaf) {
    size_t i = std::lower_bound(node.keys.begin(), node.keys.end(), key) - node.keys.begin();
    if (i < node.keys.size() && node.keys[i] == key) { node.values[i] = std::move(value); }
    else { node.keys.insert(node.keys.begin() + i, key); node.values.insert(node.values.begin() + i, std::move(value)); }
  } else {
    size_t i = std::upper_bound(node.keys.begin(), node.keys.end(), key) - node.keys.begin();
    Split child;
    if ((ret = put_path(sql, node.children[i], key, std::move(value), child, depth + 1)) != OB_SUCCESS) { return ret; }
    node.children[i] = child.left;
    if (child.right.page) {
      node.keys.insert(node.keys.begin() + i, child.separator);
      node.children.insert(node.children.begin() + i + 1, child.right);
    }
  }
  if (node.keys.size() <= FANOUT) { return save_node(sql, node, out.left); }
  const size_t mid = node.keys.size() / 2;
  Node right; right.leaf = node.leaf; out.separator = node.keys[mid];
  if (node.leaf) {
    right.keys.assign(node.keys.begin() + mid, node.keys.end());
    right.values.assign(node.values.begin() + mid, node.values.end());
    node.keys.resize(mid); node.values.resize(mid);
  } else {
    right.keys.assign(node.keys.begin() + mid + 1, node.keys.end());
    right.children.assign(node.children.begin() + mid + 1, node.children.end());
    node.keys.resize(mid); node.children.resize(mid + 1);
  }
  if ((ret = save_node(sql, node, out.left)) == OB_SUCCESS) { ret = save_node(sql, right, out.right); }
  return ret;
}
int put(ObISQLClient &sql, Ref root, const std::string &key, Value value, Ref &next) {
  Split split; int ret = put_path(sql, root, key, std::move(value), split, 0);
  if (ret != OB_SUCCESS) { return ret; }
  if (!split.right.page) { next = split.left; return OB_SUCCESS; }
  Node node; node.leaf = false; node.keys.push_back(split.separator);
  node.children = {split.left, split.right}; return save_node(sql, node, next);
}
int roots(ObISQLClient &sql, uint64_t db, Roots &root, bool lock = false, bool inactive = false) {
  int ret = OB_SUCCESS; ObSqlString q; ObMySQLProxy::MySQLResult res;
  sqlclient::ObMySQLResult *r = nullptr;
  const bool ns = NamespaceForkKernelPrototype::namespace_mode();
  const bool life = NamespaceForkKernelPrototype::lifetime_mode();
  if (OB_FAIL(q.assign_fmt("SELECT source_id,catalog_page,catalog_cap,directory_page,directory_cap,snapshot,schema_version%s FROM %s WHERE %s=%lu%s%s",
      life ? ",snapshot_ref,state" : "", ns ? NAMESPACES : ROOTS, ns ? "namespace_id" : "database_id", db,
      life && !inactive ? " AND state=0" : "", lock ? " FOR UPDATE" : ""))) {
  } else if (OB_FAIL(sql.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_uint(0L, root.source)) || OB_FAIL(r->get_uint(1L, root.catalog.page))
      || OB_FAIL(r->get_int(2L, root.catalog.cap)) || OB_FAIL(r->get_uint(3L, root.directory.page))
      || OB_FAIL(r->get_int(4L, root.directory.cap)) || OB_FAIL(r->get_int(5L, root.snapshot))
      || OB_FAIL(r->get_int(6L, root.schema_version))) {
  } else if (life && (OB_FAIL(r->get_uint(7L, root.snapshot_ref)) || OB_FAIL(r->get_int(8L, root.state)))) {
  }
  return ret;
}
int snapshot_roots(ObISQLClient &sql, uint64_t id, Roots &root, bool lock = false) {
  ObSqlString q; ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
  const bool lineage = NamespaceForkKernelPrototype::lineage_mode();
  int ret = q.assign_fmt("SELECT catalog_page,directory_page,snapshot,schema_version%s FROM %s WHERE snapshot_id=%lu%s",
      lineage ? ",catalog_cap,directory_cap,parent_ref,ref_count" : "", SNAPSHOTS, id, lock ? " FOR UPDATE" : "");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_uint(0L, root.catalog.page)) || OB_FAIL(r->get_uint(1L, root.directory.page))
      || OB_FAIL(r->get_int(2L, root.snapshot)) || OB_FAIL(r->get_int(3L, root.schema_version))) {
  } else if (root.snapshot <= 0 || uint64_t(root.snapshot) != id) { ret = OB_CHECKSUM_ERROR;
  } else {
    root.catalog.cap = root.directory.cap = root.snapshot; root.snapshot_ref = id;
    if (lineage) {
      if (OB_FAIL(r->get_int(4L, root.catalog.cap)) || OB_FAIL(r->get_int(5L, root.directory.cap))
          || OB_FAIL(r->get_uint(6L, root.parent_ref)) || OB_FAIL(r->get_int(7L, root.ref_count))) {
      } else if (root.parent_ref >= id || root.ref_count <= 0 || root.catalog.cap <= 0
          || root.directory.cap <= 0 || root.catalog.cap > root.snapshot || root.directory.cap > root.snapshot) {
        ret = OB_CHECKSUM_ERROR;
      }
    }
  }
  return ret;
}
int save_roots(ObISQLClient &sql, uint64_t db, const Roots &root) {
  if (NamespaceForkKernelPrototype::namespace_mode()) {
    ObSqlString q;
    int ret = q.assign_fmt("UPDATE %s SET source_id=%lu,catalog_page=%lu,catalog_cap=%ld,directory_page=%lu,directory_cap=%ld,snapshot=%ld,schema_version=%ld WHERE namespace_id=%lu",
        NAMESPACES, root.source, root.catalog.page, root.catalog.cap, root.directory.page,
        root.directory.cap, root.snapshot, root.schema_version, db);
    return ret == OB_SUCCESS ? write_sql(sql, q) : ret;
  }
  ObSqlString q; int ret = q.assign_fmt("REPLACE INTO %s VALUES(%lu,%lu,%lu,%ld,%lu,%ld,%ld,%ld)",
      ROOTS, db, root.source, root.catalog.page, root.catalog.cap, root.directory.page,
      root.directory.cap, root.snapshot, root.schema_version);
  return ret == OB_SUCCESS ? write_sql(sql, q) : ret;
}
bool supported(const ObTableSchema &s) {
  for (int64_t i = 0; i < s.get_foreign_key_infos().count(); ++i) {
    const auto &fk = s.get_foreign_key_infos().at(i);
    if (fk.is_parent_table_mock_ || fk.parent_table_id_ >= (1ULL << 32)
        || fk.child_table_id_ >= (1ULL << 32)
        || fk.fk_ref_type_ != FK_REF_TYPE_PRIMARY_KEY) { return false; }
  }
  const auto *id = s.get_column_schema("id"), *v = s.get_column_schema("v");
  return s.get_table_type() == USER_TABLE && !s.is_partitioned_table() && s.get_index_tid_count() == 0
      && !s.has_lob_aux_table() && s.get_trigger_list().empty()
      && !s.has_generated_column() && s.get_autoinc_column_id() == 0 && s.get_column_count() == 2
      && s.get_rowkey_column_num() == 1 && id && v && id->is_rowkey_column()
      && ob_is_integer_type(id->get_data_type()) && ob_is_integer_type(v->get_data_type());
}
int schema_from_value(uint64_t db, const Value &value, const ObTableSchema *&schema) {
  uint64_t object = 0, table = 0, tablet = 0, bound = 0;
  if (!entry(value.data, object, table, tablet, bound)) { return OB_CHECKSUM_ERROR; }
  const uint64_t id = encoded(db, table);
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto it = schemas.find(id); if (it != schemas.end()) { schema = &it->second->schema; return OB_SUCCESS; } }
  std::string data; int ret = blob(*GCTX.sql_proxy_, object, data);
  std::unique_ptr<SchemaHolder> holder(new SchemaHolder());
  ObTableSchema *copy = &holder->schema; int64_t pos = 0;
  if (ret != OB_SUCCESS) { return ret; }
  if ((ret = copy->deserialize(data.data(), data.size(), pos)) != OB_SUCCESS) { return ret; }
  if (pos != int64_t(data.size()) || !supported(*copy)) { return OB_NOT_SUPPORTED; }
  copy->set_database_id(NamespaceForkKernelPrototype::namespace_mode() ? encoded(db, copy->get_database_id()) : db);
  copy->set_table_id(id); copy->set_tablet_id(ObTabletID(encoded(db, tablet)));
  for (int64_t i = 0; i < copy->get_foreign_key_infos().count(); ++i) {
    auto &fk = copy->get_foreign_key_infos().at(i);
    fk.table_id_ = id;
    fk.child_table_id_ = encoded(db, fk.child_table_id_);
    fk.parent_table_id_ = encoded(db, fk.parent_table_id_);
    if (fk.ref_cst_id_ != OB_INVALID_ID) { fk.ref_cst_id_ = encoded(db, fk.ref_cst_id_); }
  }
  for (int64_t i = 0; i < copy->get_column_count(); ++i) {
    const_cast<ObColumnSchemaV2 *>(copy->get_column_schema_by_idx(i))->set_table_id(id);
  }
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto &slot = schemas[id]; if (!slot) { slot = std::move(holder); } schema = &slot->schema; }
  return OB_SUCCESS;
}
int namespace_named(ObISQLClient &sql, const ObString &name, uint64_t &id) {
  ObSqlString q; ObMySQLProxy::MySQLResult res; int ret = OB_SUCCESS;
  sqlclient::ObMySQLResult *r = nullptr;
  if (OB_FAIL(q.assign_fmt("SELECT namespace_id FROM %s WHERE name=UNHEX('%s')", NAMESPACES,
      hex(std::string(name.ptr(), name.length())).c_str()))) {
  } else if (OB_FAIL(sql.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else { ret = r->get_uint(0L, id); }
  return ret;
}
int namespace_registry_ready(bool &ready) {
  ready = false; ObSchemaGetterGuard guard; uint64_t db = OB_INVALID_ID;
  const ObSimpleTableSchemaV2 *schema = nullptr;
  int ret = GSCHEMASERVICE.get_runtime_schema_guard(guard);
  if (ret == OB_SUCCESS) { ret = guard.get_database_id(ObString("__fork_proto_meta"), db); }
  if (ret == OB_SUCCESS && db != OB_INVALID_ID) {
    ret = guard.get_simple_table_schema(db, ObString("namespaces"), false, schema);
    ready = ret == OB_SUCCESS && schema != nullptr;
    if (ready && NamespaceForkKernelPrototype::lifetime_mode()) {
      ret = guard.get_simple_table_schema(db, ObString("snapshots"), false, schema);
      ready = ret == OB_SUCCESS && schema != nullptr;
    }
  }
  return ret;
}
int database_from_value(uint64_t ns, const Value &value, const ObDatabaseSchema *&schema) {
  uint64_t object = 0, db = 0, unused = 0, bound = 0;
  if (!entry(value.data, object, db, unused, bound) || unused || bound
      || !NamespaceObjectKey{ns, db}.is_valid()) { return OB_CHECKSUM_ERROR; }
  const uint64_t id = encoded(ns, db);
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto it = database_schemas.find(id);
    if (it != database_schemas.end()) { schema = &it->second->schema; return OB_SUCCESS; } }
  std::string data; int ret = blob(*GCTX.sql_proxy_, object, data); int64_t pos = 0;
  std::unique_ptr<DatabaseHolder> holder(new DatabaseHolder());
  if (ret != OB_SUCCESS) { return ret; }
  if ((ret = holder->schema.deserialize(data.data(), data.size(), pos)) != OB_SUCCESS) { return ret; }
  if (pos != int64_t(data.size()) || holder->schema.get_database_id() != db) { return OB_CHECKSUM_ERROR; }
  holder->schema.set_database_id(id);
  holder->simple.set_database_id(id);
  holder->simple.set_schema_version(holder->schema.get_schema_version());
  holder->simple.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
  if ((ret = holder->simple.set_database_name(holder->schema.get_database_name_str())) != OB_SUCCESS) { return ret; }
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto &slot = database_schemas[id]; if (!slot) { slot = std::move(holder); } schema = &slot->schema; }
  return OB_SUCCESS;
}
// Find an owned binding, or a snapshot binding retained by a LIVE descendant.
// The caller registers its activity before this lookup when it will read storage.
// ponytail: temporary graph walk per background lookup; no resident lineage index.
int bound_value(ObISQLClient &sql, const ObTabletID &tablet, Roots &root, Value &value) {
  const uint64_t local = local_of(tablet.id());
  int ret = roots(sql, database_of(tablet.id()), root);
  auto matches = [&](Ref directory) -> int {
    int result = find(sql, directory, key_of(local), value);
    if (result != OB_SUCCESS) { return result; }
    uint64_t object = 0, table = 0, source = 0, bound = 0;
    if (!entry(value.data, object, table, source, bound) || source != local) { return OB_CHECKSUM_ERROR; }
    return bound == tablet.id() ? OB_SUCCESS : OB_ENTRY_NOT_EXIST;
  };
  if (ret == OB_SUCCESS) { return matches(root.directory); }
  if (ret != OB_ITER_END) { return ret; }
  std::vector<uint64_t> pending, visited;
  {
    ObSqlString q; ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
    if (OB_FAIL(q.assign_fmt("SELECT snapshot_ref FROM %s WHERE state=0 AND snapshot_ref>0", NAMESPACES))) {
    } else if (OB_FAIL(sql.read(res, q.ptr()))) {
    } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret = r->next())) {
        uint64_t id = 0;
        if (OB_FAIL(r->get_uint(0L, id))) { break; }
        pending.push_back(id);
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  while (ret == OB_SUCCESS && !pending.empty()) {
    uint64_t id = pending.back(); pending.pop_back();
    if (std::find(visited.begin(), visited.end(), id) != visited.end()) { continue; }
    visited.push_back(id);
    if ((ret = snapshot_roots(sql, id, root)) != OB_SUCCESS) { break; }
    ret = matches(root.directory);
    if (ret == OB_SUCCESS) { return ret; }
    if (ret != OB_ENTRY_NOT_EXIST) { break; }
    ret = OB_SUCCESS;
    if (root.parent_ref) { pending.push_back(root.parent_ref); }
  }
  return ret == OB_SUCCESS ? OB_ENTRY_NOT_EXIST : ret;
}
int release_lineage(ObISQLClient &trans, uint64_t id) {
  int ret = OB_SUCCESS;
  // Locks go from newer snapshots to older parents. A child owns exactly one
  // persistent reference to its parent, independent of any namespace tombstone.
  while (OB_SUCC(ret) && id) {
    Roots snapshot; ObSqlString q;
    if (OB_FAIL(snapshot_roots(trans, id, snapshot, true))) {
    } else if (snapshot.ref_count > 1) {
      if (OB_FAIL(q.assign_fmt("UPDATE %s SET ref_count=ref_count-1 WHERE snapshot_id=%lu", SNAPSHOTS, id))) {
      } else { ret = write_sql(trans, q); }
      break;
    } else {
      ObSnapshotInfo pin; ObSnapshotTableProxy pins; SCN scn; ObArray<ObTabletID> tablets;
      if (OB_FAIL(scn.convert_for_tx(snapshot.snapshot))) {
      } else if (OB_FAIL(pins.get_snapshot(trans, SNAPSHOT_FOR_MULTI_VERSION, scn, pin))) {
      } else if (pin.tablet_id_ != 0 || pin.schema_version_ != snapshot.schema_version) { ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(tablets.push_back(ObTabletID(0)))) {
      } else if (OB_FAIL(pins.batch_remove_snapshots(trans, SNAPSHOT_FOR_MULTI_VERSION,
          snapshot.schema_version, scn, tablets))) {
      } else if (OB_FAIL(q.assign_fmt("DELETE FROM %s WHERE snapshot_id=%lu", SNAPSHOTS, id))) {
      } else { ret = write_sql(trans, q); }
      LOG_INFO("PROTOTYPE_V8_RELEASE_SNAPSHOT_IN_TRANS", K(ret), K(id), "parent", snapshot.parent_ref);
      id = snapshot.parent_ref;
    }
  }
  return ret;
}

int collect_metadata() {
  if (metadata_depth || !GCTX.sql_proxy_) { return OB_STATE_NOT_MATCH; }
  std::unique_lock<std::shared_timed_mutex> exclusive(metadata_mutex, std::defer_lock);
  int ret = OB_SUCCESS;
  while (!exclusive.try_lock_for(std::chrono::milliseconds(1))) {
    if (OB_SUCCESS != (ret = THIS_WORKER.check_status())) { return ret; }
  }
  ObMySQLTransaction trans; ObSqlString q;
  std::vector<Ref> pending;
  std::unordered_set<uint64_t> reachable;
  std::vector<uint64_t> garbage;
  // External DDL owns its transaction beyond observe_schema/database(). Its root
  // row lock must also be gone. NOWAIT avoids waiting for an owner whose next
  // metadata callback is blocked by our exclusive guard. Retry the whole GC.
  if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(q.assign_fmt("SELECT catalog_page,directory_page FROM %s ORDER BY namespace_id FOR UPDATE NOWAIT", NAMESPACES))) {
  } else {
    ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
    if (OB_FAIL(trans.read(res, q.ptr()))) {
    } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret = r->next())) {
        Ref catalog, directory;
        if (OB_FAIL(r->get_uint(0L, catalog.page)) || OB_FAIL(r->get_uint(1L, directory.page))) { break; }
        if (catalog.page) { pending.push_back(catalog); }
        if (directory.page) { pending.push_back(directory); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  if (OB_SUCC(ret)) {
    ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
    if (OB_FAIL(q.assign_fmt("SELECT catalog_page,directory_page FROM %s WHERE ref_count>0", SNAPSHOTS))) {
    } else if (OB_FAIL(trans.read(res, q.ptr()))) {
    } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret = r->next())) {
        Ref catalog, directory;
        if (OB_FAIL(r->get_uint(0L, catalog.page)) || OB_FAIL(r->get_uint(1L, directory.page))) { break; }
        if (catalog.page) { pending.push_back(catalog); }
        if (directory.page) { pending.push_back(directory); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  while (OB_SUCC(ret) && !pending.empty()) {
    Ref ref = pending.back(); pending.pop_back();
    if (!reachable.insert(ref.page).second) { continue; }
    Node node;
    if (OB_FAIL(THIS_WORKER.check_status())) {
    } else if (OB_FAIL(read_node(trans, ref, node))) {
    } else if (!node.leaf) { pending.insert(pending.end(), node.children.begin(), node.children.end());
    } else {
      for (const auto &value : node.values) {
        uint64_t object = 0, table = 0, source = 0, bound = 0;
        if (!entry(value.data, object, table, source, bound) || !object) { ret = OB_CHECKSUM_ERROR; break; }
        if (reachable.insert(object).second) {
          std::string schema;
          if (OB_FAIL(blob(trans, object, schema))) { break; }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
    if (OB_FAIL(q.assign_fmt("SELECT id FROM %s ORDER BY id", PAGES))) {
    } else if (OB_FAIL(trans.read(res, q.ptr()))) {
    } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
    } else {
      // Bound deletions per request. Marking still scans all reachable metadata.
      while (garbage.size() < 256 && OB_SUCC(ret = r->next())) {
        uint64_t id = 0;
        if (OB_FAIL(r->get_uint(0L, id))) { break; }
        if (!reachable.count(id)) { garbage.push_back(id); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  if (OB_SUCC(ret) && !garbage.empty()) {
    if (OB_FAIL(q.assign_fmt("DELETE FROM %s WHERE id IN (", PAGES))) {
    } else {
      for (size_t i = 0; OB_SUCC(ret) && i < garbage.size(); ++i) {
        ret = q.append_fmt("%s%lu", i ? "," : "", garbage[i]);
      }
      int64_t affected = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(q.append(")"))) {
      } else if (OB_FAIL(trans.write(q.ptr(), affected))) {
      } else if (affected != int64_t(garbage.size())) { ret = OB_STATE_NOT_MATCH; }
    }
  }
  if (OB_SUCC(ret)) {
    DEBUG_SYNC(AFTER_UPDATE_TABLET_TO_LS);
    ret = THIS_WORKER.check_status();
  }
  if (trans.is_started()) { const int end = trans.end(ret == OB_SUCCESS); if (ret == OB_SUCCESS) { ret = end; } }
  LOG_INFO("PROTOTYPE_V9_METADATA_GC", K(ret), "reachable", reachable.size(), "deleted", garbage.size());
  return ret;
}

}

bool NamespaceForkKernelPrototype::enabled() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE");
    return s && (!std::strcmp(s, "2") || !std::strcmp(s, "3") || !std::strcmp(s, "4") || !std::strcmp(s, "5") || !std::strcmp(s, "6")); }();
  return on;
}
bool NamespaceForkKernelPrototype::namespace_mode() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE");
    return s && (!std::strcmp(s, "3") || !std::strcmp(s, "4") || !std::strcmp(s, "5") || !std::strcmp(s, "6")); }();
  return on;
}
bool NamespaceForkKernelPrototype::lifetime_mode() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE"); return s && (!std::strcmp(s, "4") || !std::strcmp(s, "5") || !std::strcmp(s, "6")); }();
  return on;
}
bool NamespaceForkKernelPrototype::lineage_mode() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE"); return s && (!std::strcmp(s, "5") || !std::strcmp(s, "6")); }();
  return on;
}
bool NamespaceForkKernelPrototype::metadata_gc_mode() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE"); return s && !std::strcmp(s, "6"); }();
  return on;
}
NamespaceSourceDropGuard::NamespaceSourceDropGuard(ObISQLClient &trans) : valid_(false) {
  const ObISQLClient *expected = nullptr;
  valid_ = NamespaceForkKernelPrototype::lifetime_mode()
      && source_drop_trans.compare_exchange_strong(expected, &trans);
}
NamespaceSourceDropGuard::~NamespaceSourceDropGuard() {
  if (valid_) { source_drop_trans.store(nullptr); }
}
int NamespaceForkKernelPrototype::begin_namespace_drop(const ObString &name, uint64_t &id, bool &done) {
  done = false; id = 0;
  if (!lifetime_mode() || !GCTX.sql_proxy_) { return OB_NOT_SUPPORTED; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  ObMySQLTransaction trans; Roots root; ObSqlString q;
  int ret = trans.start(GCTX.sql_proxy_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(namespace_named(trans, name, id))) {
  } else if (OB_FAIL(roots(trans, id, root, true, true))) {
  } else if (root.state == 2) { done = true;
  } else if (root.state != 0 && root.state != 1) { ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(q.assign_fmt("UPDATE %s SET state=1 WHERE namespace_id=%lu", NAMESPACES, id))) {
  } else { ret = write_sql(trans, q); }
  if (trans.is_started()) { const int end = trans.end(ret == OB_SUCCESS); if (ret == OB_SUCCESS) { ret = end; } }
  LOG_INFO("PROTOTYPE_V7_NAMESPACE_CLOSE", K(ret), K(id), K(done));
  return ret;
}
int NamespaceForkKernelPrototype::lock_namespace_drop(ObISQLClient &trans, uint64_t id,
    ObIArray<const ObTableSchema *> &bound_schemas) {
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; int ret = roots(trans, id, root, true, true);
  if (ret != OB_SUCCESS) { return ret; }
  if (root.state != 1) { return OB_STATE_NOT_MATCH; }
  if (id == 1) { return OB_SUCCESS; } // Native DROP owns its own enumeration.
  std::vector<Ref> pending; if (root.directory.page) { pending.push_back(root.directory); }
  while (OB_SUCC(ret) && !pending.empty()) {
    Ref ref = pending.back(); pending.pop_back(); Node node;
    if (OB_FAIL(read_node(trans, ref, node))) {
    } else if (!node.leaf) { pending.insert(pending.end(), node.children.begin(), node.children.end());
    } else {
      for (const auto &value : node.values) {
        uint64_t object = 0, table = 0, source = 0, bound = 0;
        const ObTableSchema *schema = nullptr;
        if (!entry(value.data, object, table, source, bound)) { ret = OB_CHECKSUM_ERROR;
        } else if (!bound) { continue; // Inherited input belongs to the snapshot, never this branch.
        } else if (lineage_mode() && is_encoded_id(bound) && database_of(bound) != id) {
          continue; // An inherited physical binding is owned by an ancestor snapshot.
        } else if (bound != encoded(id, source)) { ret = OB_STATE_NOT_MATCH;
        } else if (OB_FAIL(schema_from_value(id, value, schema))) {
        } else if (schema->get_tablet_id().id() != bound) { ret = OB_STATE_NOT_MATCH;
        } else { ret = bound_schemas.push_back(schema); }
        if (ret != OB_SUCCESS) { break; }
      }
    }
  }
  return ret;
}
int NamespaceForkKernelPrototype::finish_namespace_drop(ObISQLClient &trans, uint64_t id) {
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; ObSqlString q; int ret = roots(trans, id, root, true, true);
  if (OB_FAIL(ret)) {
  } else if (root.state != 1) { ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(q.assign_fmt("UPDATE %s SET state=2,source_id=0,snapshot_ref=0,catalog_page=0,catalog_cap=0,directory_page=0,directory_cap=0,snapshot=0,schema_version=0 WHERE namespace_id=%lu AND state=1", NAMESPACES, id))) {
  } else { ret = write_sql(trans, q); }
  if (OB_SUCC(ret) && root.snapshot_ref && lineage_mode()) {
    ret = release_lineage(trans, root.snapshot_ref);
  } else if (OB_SUCC(ret) && root.snapshot_ref) {
    // Lock the canonical snapshot before deciding whether its final owner is gone.
    // V7 creates one snapshot per fork; no API attaches new owners to an existing snapshot.
    bool referenced = false;
    {
      ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
      if (OB_FAIL(q.assign_fmt("SELECT snapshot_id FROM %s WHERE snapshot_id=%lu FOR UPDATE", SNAPSHOTS, root.snapshot_ref))) {
      } else if (OB_FAIL(trans.read(res, q.ptr()))) {
      } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
      } else { ret = r->next(); }
    }
    if (OB_SUCC(ret)) {
      ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
      if (OB_FAIL(q.assign_fmt("SELECT namespace_id FROM %s WHERE snapshot_ref=%lu LIMIT 1", NAMESPACES, root.snapshot_ref))) {
      } else if (OB_FAIL(trans.read(res, q.ptr()))) {
      } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
      } else if ((ret = r->next()) == OB_ITER_END) { ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) { referenced = true; }
    }
    if (OB_SUCC(ret) && !referenced) {
      Roots snapshot; ObSnapshotInfo pin; ObSnapshotTableProxy pins; SCN scn; ObArray<ObTabletID> tablets;
      if (OB_FAIL(snapshot_roots(trans, root.snapshot_ref, snapshot))) {
      } else if (snapshot.snapshot != root.snapshot || snapshot.schema_version != root.schema_version) { ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(scn.convert_for_tx(snapshot.snapshot))) {
      } else if (OB_FAIL(pins.get_snapshot(trans, SNAPSHOT_FOR_MULTI_VERSION, scn, pin))) {
      } else if (pin.tablet_id_ != 0 || pin.schema_version_ != snapshot.schema_version) { ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(tablets.push_back(ObTabletID(0)))) {
      } else if (OB_FAIL(pins.batch_remove_snapshots(trans, SNAPSHOT_FOR_MULTI_VERSION,
          snapshot.schema_version, scn, tablets))) {
      } else if (OB_FAIL(q.assign_fmt("DELETE FROM %s WHERE snapshot_id=%lu", SNAPSHOTS, root.snapshot_ref))) {
      } else { ret = write_sql(trans, q); }
      LOG_INFO("PROTOTYPE_V7_RELEASE_SNAPSHOT_IN_TRANS", K(ret), K(id), "snapshot", root.snapshot_ref);
    }
  }
  return ret;
}
int NamespaceForkKernelPrototype::check_baseline_access(const ObTabletID &tablet_id, bool &held) {
  if (!lineage_mode()) { return check_table_access(OB_INVALID_ID, tablet_id, held); }
  if (!held) { active_accesses.fetch_add(1); held = true; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  bool ready = false; Roots root; Value value;
  int ret = namespace_registry_ready(ready);
  if (ret != OB_SUCCESS || !ready) { return ret == OB_SUCCESS ? OB_EAGAIN : ret; }
  // A deleted parent may still serve a LIVE descendant. Requiring a LIVE owner in
  // the graph walk prevents a new DAG entering after the final descendant closes.
  return bound_value(*GCTX.sql_proxy_, tablet_id, root, value);
}
void NamespaceForkKernelPrototype::release_access(bool &held) {
  if (held) { held = false; active_accesses.fetch_sub(1); }
}
int NamespaceForkKernelPrototype::drain_access() {
  int ret = OB_SUCCESS;
  // Called after durable close, BEFORE taking the directory or DDL locks. Otherwise
  // an admitted cold-table materializer could wait on DROP while DROP waited on it.
  while (active_accesses.load() != 0 && OB_SUCC(ret = THIS_WORKER.check_status())) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_INFO("PROTOTYPE_V7_DRAIN_ACCESS", "active", active_accesses.load());
    }
    ob_usleep(10 * 1000);
  }
  return ret;
}
int NamespaceForkKernelPrototype::check_table_access(uint64_t table_id, const ObTabletID &tablet_id, bool &held) {
  if (!lifetime_mode() || tablet_id.is_inner_tablet()) { return OB_SUCCESS; }
  // Classify encoded storage by its actual tablet, including old plans and DML callers
  // without a schema parameter. Internal LOB scans must still bypass by owning tablet.
  const uint64_t id = is_encoded_id(tablet_id.id()) ? database_of(tablet_id.id()) : 1;
  int ret = OB_SUCCESS;
  if (id == 1) {
    if (is_inner_table(table_id) || table_id == OB_INVALID_ID) { return OB_SUCCESS; }
    ObSchemaGetterGuard guard; const ObTableSchema *table = nullptr; const ObDatabaseSchema *db = nullptr;
    if (OB_FAIL(GSCHEMASERVICE.get_runtime_schema_guard(guard))) {
    } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
    } else if (!table) { ret = OB_TABLE_NOT_EXIST;
    } else if (is_inner_db(table->get_database_id())) { return OB_SUCCESS;
    } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
    } else if (!db) { ret = OB_ERR_BAD_DATABASE;
    } else if (db->get_database_name_str().prefix_match("__fork_proto_meta")) { return OB_SUCCESS; }
  }
  if (OB_SUCC(ret)) {
    // Register BEFORE reading LIVE; release only after iterators/store contexts or
    // a baseline DAG have released their inputs. New work after close cannot enter.
    if (!held) { active_accesses.fetch_add(1); held = true; }
    bool ready = false; Roots root;
    if (OB_FAIL(namespace_registry_ready(ready))) {
    } else if (!ready) { ret = id == 1 ? OB_SUCCESS : OB_EAGAIN;
    } else if ((ret = roots(*GCTX.sql_proxy_, id, root, false, true)) == OB_ITER_END && id == 1) { ret = OB_SUCCESS;
    } else if (OB_SUCC(ret) && root.state != 0) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "access a closing or deleted prototype namespace");
    }
  }
  return ret;
}
int NamespaceForkKernelPrototype::protect_snapshot_tablets(ObIArray<ObTabletID> &candidates, bool &need_retry) {
  if (!lifetime_mode() || candidates.empty()) { return OB_SUCCESS; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  bool ready = false; int ret = namespace_registry_ready(ready);
  if (ret != OB_SUCCESS || !ready) { return ret == OB_SUCCESS ? OB_EAGAIN : ret; }
  // Candidates are committed deletions, collected BEFORE reading snapshot roots.
  // A closing source cannot publish another snapshot. Failed reads stop this GC pass.
  std::vector<Ref> directories;
  {
    ObSqlString q; ObMySQLProxy::MySQLResult res; sqlclient::ObMySQLResult *r = nullptr;
    if (OB_FAIL(lineage_mode()
        ? q.assign_fmt("SELECT directory_page,directory_cap FROM %s WHERE ref_count>0", SNAPSHOTS)
        : q.assign_fmt("SELECT s.directory_page,s.snapshot FROM %s s WHERE EXISTS(SELECT 1 FROM %s n WHERE n.snapshot_ref=s.snapshot_id)", SNAPSHOTS, NAMESPACES))) {
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, q.ptr()))) {
    } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret) && OB_SUCC(ret = r->next())) {
        Ref ref;
        if (OB_FAIL(r->get_uint(0L, ref.page)) || OB_FAIL(r->get_int(1L, ref.cap))) {
        } else { directories.push_back(ref); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  ObArray<ObTabletID> unreferenced;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    bool retained = false;
    for (const auto &ref : directories) {
      Value value;
      ret = find(*GCTX.sql_proxy_, ref, key_of(lineage_mode() ? local_of(candidates.at(i).id()) : candidates.at(i).id()), value);
      if (ret == OB_ENTRY_NOT_EXIST) { ret = OB_SUCCESS; continue; }
      if (ret != OB_SUCCESS) { break; }
      uint64_t object = 0, table = 0, source = 0, bound = 0;
      if (!entry(value.data, object, table, source, bound)) { ret = OB_CHECKSUM_ERROR;
      } else if (lineage_mode()) {
        retained = (bound ? bound : source) == candidates.at(i).id();
      } else if (source != candidates.at(i).id() || bound) { ret = OB_CHECKSUM_ERROR;
      } else { retained = true; }
      if (ret != OB_SUCCESS || retained) { break; }
    }
    if (OB_FAIL(ret)) {
    } else if (retained) {
      need_retry = true;
      LOG_INFO("PROTOTYPE_V6_RETAIN_SNAPSHOT_TABLET", "tablet_id", candidates.at(i));
    } else { ret = unreferenced.push_back(candidates.at(i)); }
  }
  if (OB_SUCC(ret)) { ret = candidates.assign(unreferenced); }
  return ret;
}
bool NamespaceForkKernelPrototype::is_encoded_id(uint64_t id) {
  return enabled() && id != OB_INVALID_ID && (id & (3ULL << 62)) == ID_MARK;
}
bool NamespaceForkKernelPrototype::is_namespace_address(const ObString &name) {
  return namespace_mode() && name.prefix_match("__fork_ns_");
}
int NamespaceForkKernelPrototype::database_by_address(const ObString &address, const ObDatabaseSchema *&schema) {
  schema = nullptr;
  if (!is_namespace_address(address)) { return OB_INVALID_ARGUMENT; }
  // Test transport for an explicit (namespace id, database name), not a real database alias.
  const std::string s(address.ptr() + 10, address.length() - 10);
  const size_t split = s.find("__"); uint64_t ns = 0;
  if (split == std::string::npos || split == 0 || split > 10 || split + 2 == s.size()) { return OB_INVALID_ARGUMENT; }
  for (size_t i = 0; i < split; ++i) {
    if (s[i] < '0' || s[i] > '9') { return OB_INVALID_ARGUMENT; }
    ns = ns * 10 + s[i] - '0';
  }
  return database_in_namespace(ns, ObString(s.size() - split - 2, s.data() + split + 2), schema);
}
int NamespaceForkKernelPrototype::database_in_namespace(uint64_t ns, const ObString &name,
                                                       const ObDatabaseSchema *&schema) {
  if (observer::namespace_worker_prototype::worker_catalog_fetch) { return remote_database('d', ns, name, schema); }
  schema = nullptr;
  if (!namespace_mode() || !GCTX.sql_proxy_ || !NamespaceObjectKey{ns, 1}.is_valid()) { return OB_INVALID_ARGUMENT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; Value value; int ret = roots(*GCTX.sql_proxy_, ns, root);
  if (ret == OB_SUCCESS) { ret = find(*GCTX.sql_proxy_, root.catalog, "D" + std::string(name.ptr(), name.length()), value); }
  if (ret == OB_ITER_END || ret == OB_ENTRY_NOT_EXIST) { return OB_SUCCESS; }
  return ret == OB_SUCCESS ? database_from_value(ns, value, schema) : ret;
}
int NamespaceForkKernelPrototype::database_by_id(uint64_t id, const ObDatabaseSchema *&schema) {
  if (observer::namespace_worker_prototype::worker_catalog_fetch) {
    { std::lock_guard<std::mutex> lock(schema_mutex);
      auto it = database_schemas.find(id); if (it != database_schemas.end()) { schema = &it->second->schema; return OB_SUCCESS; } }
    return remote_database('b', id, ObString(), schema);
  }
  schema = nullptr; Roots root; Value value;
  if (!namespace_mode() || !is_encoded_id(id) || !GCTX.sql_proxy_) { return OB_INVALID_ARGUMENT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  int ret = roots(*GCTX.sql_proxy_, database_of(id), root);
  if (ret == OB_SUCCESS) { ret = find(*GCTX.sql_proxy_, root.catalog, "@" + key_of(local_of(id)), value); }
  if (ret == OB_ITER_END || ret == OB_ENTRY_NOT_EXIST) { return OB_SUCCESS; }
  return ret == OB_SUCCESS ? database_from_value(database_of(id), value, schema) : ret;
}
int NamespaceForkKernelPrototype::database_by_id(uint64_t id, const ObSimpleDatabaseSchema *&schema) {
  schema = nullptr; const ObDatabaseSchema *full = nullptr;
  int ret = database_by_id(id, full);
  if (ret == OB_SUCCESS && full) {
    std::lock_guard<std::mutex> lock(schema_mutex);
    schema = &database_schemas.at(id)->simple;
  }
  return ret;
}
int NamespaceForkKernelPrototype::observe_database(ObISQLClient &trans, const ObDatabaseSchema &schema) {
  if (!namespace_mode() || is_inner_db(schema.get_database_id())
      || schema.get_database_name_str().prefix_match("__fork_proto_meta")) { return OB_SUCCESS; }
  if (is_namespace_address(schema.get_database_name_str())
      || !NamespaceObjectKey{1, schema.get_database_id()}.is_valid()) { return OB_NOT_SUPPORTED; }
  bool ready = false; int ret = namespace_registry_ready(ready);
  if (ret != OB_SUCCESS || !ready) { return ret; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; ret = roots(trans, 1, root, true);
  if (ret == OB_ITER_END) {
    if (lifetime_mode()) {
      ret = roots(trans, 1, root, false, true);
      if (ret == OB_SUCCESS) { return OB_OP_NOT_ALLOW; }
    }
    return ret == OB_ITER_END ? OB_SUCCESS : ret;
  }
  if (ret != OB_SUCCESS) { return ret; }
  std::string data(schema.get_serialize_size(), '\0'); int64_t pos = 0; uint64_t object = 0; Value value;
  if (OB_FAIL(schema.serialize(data.data(), data.size(), pos))) {
  } else if (OB_FAIL(save_blob(trans, data, object))) {
  } else {
    value.data = entry(object, schema.get_database_id(), 0);
    if (OB_FAIL(put(trans, root.catalog, "D" + std::string(schema.get_database_name()), value, root.catalog))) {
    } else if (OB_FAIL(put(trans, root.catalog, "@" + key_of(schema.get_database_id()), value, root.catalog))) {
    } else {
      root.schema_version = std::max(root.schema_version, schema.get_schema_version());
      ret = save_roots(trans, 1, root);
    }
  }
  return ret;
}
int NamespaceForkKernelPrototype::check_database_ddl(const ObDatabaseSchema &schema, const ObISQLClient *trans) {
  if (lifetime_mode() && trans && source_drop_trans.load() == trans) { return OB_SUCCESS; }
  if (!namespace_mode()) { return OB_SUCCESS; }
  if (is_encoded_id(schema.get_database_id())) { return OB_NOT_SUPPORTED; }
  if (is_inner_db(schema.get_database_id())
      || schema.get_database_name_str().prefix_match("__fork_proto_meta")) { return OB_SUCCESS; }
  bool ready = false; int ret = namespace_registry_ready(ready);
  if (ret != OB_SUCCESS || !ready) { return ret; }
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; Value existing;
  ret = roots(*GCTX.sql_proxy_, 1, root, false, true);
  if (ret == OB_ITER_END) { return OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  ret = find(*GCTX.sql_proxy_, root.catalog, "@" + key_of(schema.get_database_id()), existing);
  // Only databases captured in namespace metadata need the prototype's DDL protection.
  return ret == OB_ENTRY_NOT_EXIST ? OB_SUCCESS : ret == OB_SUCCESS ? OB_NOT_SUPPORTED : ret;
}
int NamespaceForkKernelPrototype::control_namespace(const ObString &source, const ObString &target, uint64_t &id) {
  if (!namespace_mode() || !GCTX.sql_proxy_ || target.empty() || target.length() > 128) { return OB_INVALID_ARGUMENT; }
  if (metadata_gc_mode() && source == "__gc__" && target == "__gc__") { id = OB_INVALID_ID; return collect_metadata(); }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  const int64_t begin_us = ObTimeUtility::current_time();
  const bool bootstrap = source == "__empty__";
  ObMySQLTransaction trans; Roots root; ObSchemaGetterGuard guard; uint64_t source_id = 0;
  int ret = trans.start(GCTX.sql_proxy_); ObSqlString q;
  if (ret != OB_SUCCESS) { return ret; }
  if (!bootstrap) {
    if (OB_FAIL(namespace_named(trans, source, source_id))) {
    } else if (source_id != 1 && !lineage_mode()) { ret = OB_NOT_SUPPORTED;
    } else { ret = roots(trans, source_id, root, true); }
  }
  if (OB_SUCC(ret)) { ret = GSCHEMASERVICE.get_runtime_schema_guard(guard); }
  if (OB_SUCC(ret)) { ret = guard.get_schema_version(root.schema_version); }
  if (OB_SUCC(ret)) {
    if (bootstrap) {
      ret = q.assign_fmt("INSERT INTO %s(namespace_id,name,source_id,catalog_page,catalog_cap,directory_page,directory_cap,snapshot,schema_version) VALUES(1,UNHEX('%s'),0,0,0,0,0,0,%ld)", NAMESPACES,
          hex(std::string(target.ptr(), target.length())).c_str(), root.schema_version);
    } else {
      ret = q.assign_fmt("INSERT INTO %s(name,source_id,catalog_page,catalog_cap,directory_page,directory_cap,snapshot,schema_version) VALUES(UNHEX('%s'),0,0,0,0,0,0,0)",
          NAMESPACES, hex(std::string(target.ptr(), target.length())).c_str());
    }
  }
  if (OB_SUCC(ret)) { ret = write_sql(trans, q); }
  if (OB_SUCC(ret)) { ret = namespace_named(trans, target, id); }
  if (OB_SUCC(ret) && !NamespaceObjectKey{id, 1}.is_valid()) { ret = OB_SIZE_OVERFLOW; }
  if (OB_SUCC(ret) && bootstrap) {
    // One-time enrollment of the native engine's existing user catalog, before any fork.
    // Future CREATEs maintain the source root in their own DDL transaction.
    ObArray<const ObDatabaseSchema *> databases;
    if (OB_FAIL(guard.get_database_schemas_in_runtime(databases))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < databases.count(); ++i) {
        const auto &db = *databases.at(i);
        if (is_inner_db(db.get_database_id()) || db.get_database_name_str().prefix_match("__fork_proto_meta")) { continue; }
        ObArray<const ObTableSchema *> tables;
        if (OB_FAIL(observe_database(trans, db))) {
        } else if (OB_FAIL(guard.get_table_schemas_in_database(db.get_database_id(), tables))) {
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < tables.count(); ++j) { ret = observe_schema(trans, *tables.at(j)); }
        }
      }
    }
  } else if (OB_SUCC(ret)) {
    ObSnapshotInfo pin; ObSnapshotTableProxy pins;
    if (OB_FAIL(rootserver::ObDDLTaskUtil::calc_snapshot_with_gts(root.snapshot))) {
    } else if (OB_FAIL(pin.snapshot_scn_.convert_for_tx(root.snapshot))) {
    } else {
      pin.snapshot_type_ = SNAPSHOT_FOR_MULTI_VERSION; pin.tablet_id_ = 0;
      pin.schema_version_ = root.schema_version; pin.comment_ = "PROTOTYPE namespace root; source retained";
      if (OB_FAIL(pins.add_snapshot(trans, pin))) {
      } else {
        root.catalog.cap = cap_min(root.catalog.cap, root.snapshot);
        root.directory.cap = cap_min(root.directory.cap, root.snapshot);
        if (lineage_mode() && root.snapshot_ref) {
          Roots parent;
          if (OB_FAIL(snapshot_roots(trans, root.snapshot_ref, parent, true))) {
          } else if (parent.ref_count == INT64_MAX) { ret = OB_SIZE_OVERFLOW;
          } else if (OB_FAIL(q.assign_fmt("UPDATE %s SET ref_count=ref_count+1 WHERE snapshot_id=%lu", SNAPSHOTS, root.snapshot_ref))) {
          } else { ret = write_sql(trans, q); }
        }
        if (OB_SUCC(ret) && lifetime_mode()) {
          if (OB_FAIL(lineage_mode()
              ? q.assign_fmt("INSERT INTO %s VALUES(%ld,%lu,%lu,%ld,%ld,%ld,%ld,%lu,1)", SNAPSHOTS,
                  root.snapshot, root.catalog.page, root.directory.page, root.snapshot, root.schema_version,
                  root.catalog.cap, root.directory.cap, root.snapshot_ref)
              : q.assign_fmt("INSERT INTO %s VALUES(%ld,%lu,%lu,%ld,%ld)", SNAPSHOTS,
                  root.snapshot, root.catalog.page, root.directory.page, root.snapshot, root.schema_version))) {
          } else if (OB_FAIL(write_sql(trans, q))) {
          } else if (OB_FAIL(q.assign_fmt("UPDATE %s SET snapshot_ref=%ld WHERE namespace_id=%lu", NAMESPACES, root.snapshot, id))) {
          } else { ret = write_sql(trans, q); }
        }
        root.source = lifetime_mode() ? 0 : source_id;
        if (OB_SUCC(ret)) { ret = save_roots(trans, id, root); }
      }
    }
  }
  if (OB_SUCC(ret) && !bootstrap && lineage_mode()) {
    DEBUG_SYNC(AFTER_UPDATE_TABLET_TO_LS);
    ret = THIS_WORKER.check_status();
  }
  const int end = trans.end(ret == OB_SUCCESS); if (ret == OB_SUCCESS) { ret = end; }
  const int64_t published_us = ObTimeUtility::current_time();
  if (ret == OB_SUCCESS && !bootstrap) {
    auto *freeze = share::server_service<ObFreezeInfoMgr>();
    ret = freeze ? freeze->reload_for_test() : OB_NOT_INIT;
  }
  LOG_INFO("PROTOTYPE_V5_NAMESPACE_REGISTER", K(ret), K(id), K(source_id), K(bootstrap),
      "snapshot", root.snapshot, "catalog_root", root.catalog.page, "directory_root", root.directory.page,
      "publish_us", published_us - begin_us, "reload_us", ObTimeUtility::current_time() - published_us);
  return ret;
}
int NamespaceForkKernelPrototype::observe_schema(ObISQLClient &trans, const ObTableSchema &schema) {
  if (!enabled() || !schema.is_user_table() || is_encoded_id(schema.get_table_id())) { return OB_SUCCESS; }
  if (namespace_mode() && is_inner_db(schema.get_database_id())) { return OB_SUCCESS; }
  int ret = OB_SUCCESS;
  if (namespace_mode()) {
    bool ready = false; ret = namespace_registry_ready(ready);
    if (ret != OB_SUCCESS || !ready) { return ret; }
  }
  { // Finish the result before issuing another statement on the same DDL connection.
  ObSqlString q; ObMySQLProxy::MySQLResult res; ObString name;
  sqlclient::ObMySQLResult *r = nullptr;
  if (OB_FAIL(q.assign_fmt("SELECT database_name FROM oceanbase.__all_database WHERE database_id=%lu", schema.get_database_id()))) {
  } else if (OB_FAIL(trans.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_varchar(0L, name))) {
  } else if (namespace_mode() ? name.prefix_match("__fork_proto_meta")
                             : !name.prefix_match("__fork_proto_a")) { return OB_SUCCESS;
  } else if (!supported(schema) || schema.get_database_id() >= (1ULL << 30)
      || schema.get_table_id() >= (1ULL << 32) || schema.get_tablet_id().id() >= (1ULL << 32)) { ret = OB_NOT_SUPPORTED; }
  }
  if (ret != OB_SUCCESS) { return ret; }
  {
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root;
  const uint64_t owner = namespace_mode() ? 1 : schema.get_database_id();
  ret = roots(trans, owner, root, true);
  if (namespace_mode() && (ret == OB_ITER_END || ret == OB_TABLE_NOT_EXIST || ret == OB_ERR_BAD_DATABASE)) {
    if (lifetime_mode() && ret == OB_ITER_END) {
      ret = roots(trans, 1, root, false, true);
      if (ret == OB_SUCCESS) { return OB_OP_NOT_ALLOW; }
      if (ret != OB_ITER_END) { return ret; }
    }
    return OB_SUCCESS; // Native bootstrap is enrolled once when namespace 1 is registered.
  }
  if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  if (root.snapshot != 0) { return OB_NOT_SUPPORTED; }
  std::string serialized(schema.get_serialize_size(), '\0'); int64_t pos = 0; uint64_t object = 0;
  Value value;
  if (OB_FAIL(schema.serialize(&serialized[0], serialized.size(), pos))) {
  } else if (OB_FAIL(save_blob(trans, serialized, object))) {
  } else {
    value.data = entry(object, schema.get_table_id(), schema.get_tablet_id().id());
    const std::string name_key = namespace_mode()
        ? "T" + key_of(schema.get_database_id()) + "/" + schema.get_table_name() : schema.get_table_name();
    if (OB_FAIL(put(trans, root.catalog, name_key, value, root.catalog))) {
    } else if (OB_FAIL(put(trans, root.catalog, "#" + key_of(schema.get_table_id()), value, root.catalog))) {
    } else if (OB_FAIL(put(trans, root.directory, key_of(schema.get_tablet_id().id()), value, root.directory))) {
    } else {
      root.schema_version = schema.get_schema_version();
      ret = save_roots(trans, owner, root);
      LOG_INFO("PROTOTYPE_V2_SOURCE_DIRECTORY", K(ret), "table_id", schema.get_table_id(), "directory_root", root.directory.page);
    }
  }
  } // Reader guard ends; the external native DDL transaction still owns the root.
  if (OB_SUCC(ret) && metadata_gc_mode()) {
    DEBUG_SYNC(BEFORE_CREATE_TABLE_TRANS_COMMIT);
    ret = THIS_WORKER.check_status();
  }
  return ret;
}
int NamespaceForkKernelPrototype::capture(ObISQLClient &trans, uint64_t source, uint64_t target,
                                          int64_t snapshot, int64_t schema_version) {
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; int ret = roots(trans, source, root, true);
  if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  if (root.snapshot || target >= (1ULL << 30) || root.schema_version > schema_version) { return OB_NOT_SUPPORTED; }
  root.source = source; root.snapshot = snapshot; root.schema_version = schema_version;
  root.catalog.cap = cap_min(root.catalog.cap, snapshot); root.directory.cap = cap_min(root.directory.cap, snapshot);
  ret = save_roots(trans, target, root);
  LOG_INFO("PROTOTYPE_V2_ROOT_CAPTURE", K(ret), K(source), K(target), K(snapshot),
           "catalog_root", root.catalog.page, "directory_root", root.directory.page);
  return ret;
}
int NamespaceForkKernelPrototype::schema_by_name(uint64_t db, const ObString &name, const ObTableSchema *&schema) {
  if (observer::namespace_worker_prototype::worker_catalog_fetch) { return remote_table('t', db, name, schema); }
  schema = nullptr; if (!enabled() || !GCTX.sql_proxy_) { return OB_NOT_INIT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  const uint64_t owner = namespace_mode() ? (is_encoded_id(db) ? database_of(db) : 1) : db;
  Roots root; Value value; int ret = roots(*GCTX.sql_proxy_, owner, root);
  if (ret == OB_ITER_END) { return OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  if (!root.snapshot && !namespace_mode()) { return OB_SUCCESS; }
  if (metadata_gc_mode()) {
    DEBUG_SYNC(BEFORE_FETCH_SIMPLE_TABLES);
    if (OB_FAIL(THIS_WORKER.check_status())) { return ret; }
  }
  const std::string name_key = (namespace_mode() ? "T" + key_of(local_of(db)) + "/" : "")
      + std::string(name.ptr(), name.length());
  ret = find(*GCTX.sql_proxy_, root.catalog, name_key, value);
  return ret == OB_ENTRY_NOT_EXIST ? OB_SUCCESS : ret != OB_SUCCESS ? ret : schema_from_value(owner, value, schema);
}
int NamespaceForkKernelPrototype::schema_by_id(uint64_t id, const ObTableSchema *&schema) {
  schema = nullptr; if (!is_encoded_id(id)) { return OB_INVALID_ARGUMENT; }
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto it = schemas.find(id); if (it != schemas.end()) { schema = &it->second->schema; return OB_SUCCESS; } }
  if (observer::namespace_worker_prototype::worker_catalog_fetch) { return remote_table('i', id, ObString(), schema); }
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; Value value; int ret = roots(*GCTX.sql_proxy_, database_of(id), root);
  if (ret != OB_SUCCESS) { return ret; }
  if (!root.snapshot) { return OB_TABLE_NOT_EXIST; }
  ret = find(*GCTX.sql_proxy_, root.catalog, "#" + key_of(local_of(id)), value);
  return ret != OB_SUCCESS ? ret : schema_from_value(database_of(id), value, schema);
}
int NamespaceForkKernelPrototype::table_id_for_tablet(const ObTabletID &tablet, int64_t schema_version,
                                                     uint64_t &table_id) {
  table_id = OB_INVALID_ID;
  if (!is_encoded_id(tablet.id()) || !GCTX.sql_proxy_) { return OB_INVALID_ARGUMENT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  const uint64_t db = database_of(tablet.id()); Roots root; Value value;
  if (lineage_mode()) {
    int result = bound_value(*GCTX.sql_proxy_, tablet, root, value);
    if (result == OB_ENTRY_NOT_EXIST) { return OB_SUCCESS; }
    if (result != OB_SUCCESS) { return result; }
    uint64_t object = 0, table = 0, source = 0, bound = 0;
    if (!entry(value.data, object, table, source, bound)) { return OB_CHECKSUM_ERROR; }
    table_id = encoded(db, table); return OB_SUCCESS;
  }
  int ret = roots(*GCTX.sql_proxy_, db, root);
  if (ret != OB_SUCCESS) { return ret; }
  if (!root.snapshot || schema_version < root.schema_version) { return OB_SUCCESS; }
  if ((ret = find(*GCTX.sql_proxy_, root.directory, key_of(local_of(tablet.id())), value)) != OB_SUCCESS) { return ret; }
  uint64_t object = 0, table = 0, source = 0, bound = 0;
  if (!entry(value.data, object, table, source, bound)) { return OB_CHECKSUM_ERROR; }
  if (bound == tablet.id()) { table_id = encoded(db, table); }
  return OB_SUCCESS;
}
int NamespaceForkKernelPrototype::list_schemas(uint64_t db, ObIArray<const ObTableSchema *> &out) {
  if (observer::namespace_worker_prototype::worker_catalog_fetch) { return OB_NOT_SUPPORTED; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  const uint64_t owner = namespace_mode() ? (is_encoded_id(db) ? database_of(db) : 1) : db;
  Roots root; int ret = roots(*GCTX.sql_proxy_, owner, root);
  if (ret == OB_ITER_END) { return OB_SUCCESS; }
  if (ret != OB_SUCCESS || !root.snapshot) { return ret; }
  std::vector<Ref> pending; if (root.catalog.page) { pending.push_back(root.catalog); }
  while (!pending.empty() && ret == OB_SUCCESS) {
    Ref ref = pending.back(); pending.pop_back(); Node node;
    if ((ret = read_node(*GCTX.sql_proxy_, ref, node)) != OB_SUCCESS) { break; }
    if (node.leaf) {
      for (size_t i = 0; i < node.keys.size() && ret == OB_SUCCESS; ++i) {
        // The catalog indexes each schema by id and by name. Enumerate only the id index.
        if (!node.keys[i].empty() && node.keys[i][0] == '#') {
          const ObTableSchema *schema = nullptr;
          if ((ret = schema_from_value(owner, node.values[i], schema)) == OB_SUCCESS
              && (!namespace_mode() || schema->get_database_id() == db)) { ret = out.push_back(schema); }
        }
      }
    } else { pending.insert(pending.end(), node.children.rbegin(), node.children.rend()); }
  }
  return ret;
}
int NamespaceForkKernelPrototype::check_ddl(const ObSimpleTableSchemaV2 &schema, const ObISQLClient *trans) {
  if (lifetime_mode() && trans && source_drop_trans.load() == trans) { return OB_SUCCESS; }
  if (!enabled() || !schema.is_user_table()) { return OB_SUCCESS; }
  if (is_encoded_id(schema.get_table_id())) { return OB_NOT_SUPPORTED; }
  if (namespace_mode()) {
    bool ready = false; const int ret = namespace_registry_ready(ready);
    if (ret != OB_SUCCESS || !ready) { return ret; }
  }
  ObSchemaGetterGuard guard; const ObDatabaseSchema *db = nullptr;
  int ret = GSCHEMASERVICE.get_runtime_schema_guard(guard);
  if (ret != OB_SUCCESS) { return ret; }
  if ((ret = guard.get_database_schema(schema.get_database_id(), db)) != OB_SUCCESS || !db) { return ret; }
  if (db->get_database_name_str().prefix_match("__fork_proto_b")) { return OB_NOT_SUPPORTED; }
  if (namespace_mode() ? db->get_database_name_str().prefix_match("__fork_proto_meta")
                       : !db->get_database_name_str().prefix_match("__fork_proto_a")) { return OB_SUCCESS; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  Roots root; Value existing;
  ret = roots(*GCTX.sql_proxy_, namespace_mode() ? 1 : schema.get_database_id(), root);
  if (ret == OB_ITER_END) { return OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  ret = find(*GCTX.sql_proxy_, root.catalog, "#" + key_of(schema.get_table_id()), existing);
  // Fixed physical definitions: new source tables are supported; existing ones cannot be altered/dropped.
  return ret == OB_ENTRY_NOT_EXIST ? OB_SUCCESS : ret == OB_SUCCESS ? OB_NOT_SUPPORTED : ret;
}
int NamespaceForkKernelPrototype::schedule_baseline(const ObTablet &tablet) {
  const auto &meta = tablet.get_tablet_meta();
  if (!is_encoded_id(meta.tablet_id_.id()) || tablet.is_empty_shell()
      || !meta.fork_info_.is_valid() || meta.fork_info_.is_complete()) { return OB_SUCCESS; }
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  int ret = OB_SUCCESS; Roots root; Value value; const ObTableSchema *schema = nullptr;
  const uint64_t db = database_of(meta.tablet_id_.id());
  uint64_t object = 0, table = 0, source = 0, bound = 0;
  if (OB_FAIL(lineage_mode() ? bound_value(*GCTX.sql_proxy_, meta.tablet_id_, root, value)
      : roots(*GCTX.sql_proxy_, db, root))) {
  } else if (!lineage_mode() && OB_FAIL(find(*GCTX.sql_proxy_, root.directory, key_of(local_of(meta.tablet_id_.id())), value))) {
  } else if (!entry(value.data, object, table, source, bound)) { ret = OB_CHECKSUM_ERROR;
  } else if (bound == 0) { // Physical CREATE MDS may not have committed its directory binding yet.
  } else if (bound != meta.tablet_id_.id() || (!lineage_mode()
      && (source != meta.fork_info_.get_fork_src_tablet_id().id()
          || root.snapshot != meta.fork_info_.get_fork_snapshot_version()))) { ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(schema_from_value(db, value, schema))) {
  } else {
    ObTabletForkParam param; bool ready = false;
    param.table_id_ = schema->get_table_id(); param.schema_version_ = schema->get_schema_version();
    // Stable DAG identity only; no rootserver DDL task is created.
    param.task_id_ = bound; param.source_tablet_id_ = meta.fork_info_.get_fork_src_tablet_id();
    param.dest_tablet_id_ = meta.tablet_id_; param.fork_snapshot_version_ = meta.fork_info_.get_fork_snapshot_version();
    param.data_format_version_ = DATA_CURRENT_VERSION;
    if (OB_FAIL(ObTabletForkUtil::check_satisfy_fork_condition(param, ready))) {
    } else if (ready) {
      ret = compaction::ObScheduleDagFunc::schedule_tablet_fork_dag(param, false);
      if (ret == OB_EAGAIN || ret == OB_SIZE_OVERFLOW) { ret = OB_SUCCESS; }
      else { LOG_INFO("PROTOTYPE_V3_BASELINE_SCHEDULE", K(ret), K(param)); }
    }
  }
  // A crash needs no independent task journal: the committed tablet's incomplete fork
  // mark retries here; the existing table-store update persists the completed baseline.
  return lineage_mode() && (ret == OB_ITER_END || ret == OB_ENTRY_NOT_EXIST) ? OB_SUCCESS : ret;
}
int NamespaceForkKernelPrototype::ensure_tablet(const ObTabletID &tablet_id) {
  if (!is_encoded_id(tablet_id.id())) { return OB_SUCCESS; }
  int ret = OB_SUCCESS; const uint64_t db = database_of(tablet_id.id()), local = local_of(tablet_id.id());
  {
    // Reuse the tablet manager and its existing committed-status cache. A valid
    // logical birth S alone is not proof that physical creation has committed.
    ObTabletHandle handle;
    ret = ObTabletCreateDeleteHelper::check_and_get_tablet(ObTabletMapKey(tablet_id), handle,
        0, ObMDSGetTabletMode::READ_READABLE_COMMITED, transaction::ObTransVersion::MAX_TRANS_VERSION);
    if (ret == OB_SUCCESS) { return ret; }
    if (ret != OB_TABLET_NOT_EXIST && ret != OB_EAGAIN) { return ret; }
  } // Do not pin an uncommitted tablet while waiting for its creator's root lock.
  LOG_INFO("PROTOTYPE_V4_DIRECTORY_SLOW_PATH", K(tablet_id), "lookup_ret", ret);
  ret = OB_SUCCESS;
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
  MetadataReadGuard access; if (access.error() != OB_SUCCESS) { return access.error(); }
  ObMySQLTransaction trans; Roots root; Value value;
  // The row lock joins concurrent requests before physical creation. The business transaction is untouched.
  if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(roots(trans, db, root, true))) {
  } else if (lifetime_mode() && OB_FAIL([&]() -> int {
      Roots snapshot;
      int result = snapshot_roots(trans, root.snapshot_ref, snapshot);
      if (result == OB_SUCCESS && (snapshot.snapshot != root.snapshot || snapshot.schema_version != root.schema_version
          || snapshot.catalog.page != root.catalog.page)) { result = OB_STATE_NOT_MATCH; }
      return result;
    }())) {
  } else if (OB_FAIL(find(trans, root.directory, key_of(local), value))) {
  } else {
    uint64_t object = 0, table = 0, local_tablet = 0, bound = 0;
    if (!entry(value.data, object, table, local_tablet, bound)) { ret = OB_CHECKSUM_ERROR;
    } else if (bound == tablet_id.id()) {
      // Already committed in the same authoritative directory; no DDL or snapshot reacquisition.
    } else if (local_tablet != local || value.cap <= 0 || value.cap > root.snapshot
        || (!lineage_mode() && (bound != 0 || value.cap != root.snapshot))
        || (bound && (!is_encoded_id(bound) || local_of(bound) != local
            || database_of(bound) == db))) { ret = OB_STATE_NOT_MATCH;
    } else {
      const uint64_t source_tablet = bound ? bound : local_tablet;
      const int64_t input_snapshot = value.cap;
      Roots inherited;
      uint64_t snapshot_id = root.snapshot_ref;
      const int64_t lineage_begin = ObTimeUtility::current_time();
      int64_t lineage_steps = 0;
      // Effective caps are one of the ancestor snapshot IDs. Keep that ancestor's
      // pin, not just the newest child's S (which would discard cold-table history).
      if (lineage_mode()) {
        while (OB_SUCC(ret)) {
          ++lineage_steps;
          if (snapshot_id < uint64_t(input_snapshot)) { ret = OB_STATE_NOT_MATCH; break; }
          if (OB_FAIL(snapshot_roots(trans, snapshot_id, inherited))) { break; }
          if (snapshot_id == uint64_t(input_snapshot)) { break; }
          snapshot_id = inherited.parent_ref;
        }
      } else { inherited.schema_version = root.schema_version; }
      if (metadata_gc_mode()) {
        LOG_INFO("PROTOTYPE_V9_LINEAGE_LOOKUP", K(ret), K(tablet_id), K(lineage_steps),
            "lineage_us", ObTimeUtility::current_time() - lineage_begin);
      }
      ObSnapshotInfo pin; ObSnapshotTableProxy pins; SCN scn; ObStorageSnapshotInfo reserved;
      const ObTableSchema *schema = nullptr;
      auto *freeze = share::server_service<ObFreezeInfoMgr>();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scn.convert_for_tx(input_snapshot))) {
      } else if (OB_FAIL(pins.get_snapshot(trans, SNAPSHOT_FOR_MULTI_VERSION, scn, pin))) {
      } else if (pin.tablet_id_ != 0 || pin.schema_version_ != inherited.schema_version || !freeze) { ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(freeze->reload_for_test())) {
      } else if (OB_FAIL(freeze->get_min_reserved_snapshot(ObTabletID(source_tablet), input_snapshot, reserved))) {
      } else if (reserved.snapshot_ > input_snapshot) { ret = OB_SNAPSHOT_DISCARDED;
      } else if (OB_FAIL(schema_from_value(db, value, schema))) {
      } else {
        rootserver::ObTabletCreator creator(SCN::min_scn(), trans); rootserver::ObTabletCreatorArg arg;
        ObArray<ObTabletID> ids; ObArray<const ObTableSchema *> definitions;
        ObArray<bool> empty_major; ObArray<int64_t> logical_birth; ObArray<ObForkTabletInfo> fork_infos;
        ObForkTabletInfo fork; fork.set_fork_snapshot_version(input_snapshot); fork.set_fork_src_tablet_id(ObTabletID(source_tablet));
        if (OB_FAIL(ids.push_back(tablet_id)) || OB_FAIL(definitions.push_back(schema))
            || OB_FAIL(empty_major.push_back(false)) || OB_FAIL(logical_birth.push_back(input_snapshot))
            || OB_FAIL(fork_infos.push_back(fork))) {
        } else if (OB_FAIL(arg.init(ids, tablet_id, definitions, false, DATA_CURRENT_VERSION,
                                   empty_major, logical_birth, fork_infos))) {
        } else if (OB_FAIL(creator.init(false))) {
        } else if (OB_FAIL(creator.add_create_tablet_arg(arg))) {
        } else if (FALSE_IT(creator.set_materialization_for_prototype())) {
        } else if (OB_FAIL(creator.execute())) {
        } else {
          ObArray<ObTabletTablePair> mappings;
          if (OB_FAIL(mappings.push_back(ObTabletTablePair(tablet_id, schema->get_table_id())))) {
          } else if (OB_FAIL(ObTabletMappingTableOperator::batch_update(trans, mappings))) {
          }
          value.data = entry(object, table, local_tablet, tablet_id.id()); value.cap = 0;
          const uint64_t previous_root = root.directory.page;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(put(trans, root.directory, key_of(local), value, root.directory))) {
          } else if (OB_FAIL(save_roots(trans, db, root))) {
          } else {
            // Reuse the existing mapping-update sync point in this isolated prototype.
            // It exposes a physical tablet plus uncommitted directory for crash/abort checks.
            DEBUG_SYNC(AFTER_UPDATE_TABLET_TO_LS);
            ret = THIS_WORKER.check_status();
            LOG_INFO("PROTOTYPE_V2_STORAGE_MATERIALIZE", K(tablet_id), K(source_tablet), "snapshot", input_snapshot, "namespace_snapshot", root.snapshot,
                     K(previous_root), "next_root", root.directory.page, "entry_layer", "ObAccessService", K(ret));
          }
        }
      }
    }
  }
  if (trans.is_started()) { int end = trans.end(ret == OB_SUCCESS); if (ret == OB_SUCCESS) { ret = end; } }
  if (ret != OB_SUCCESS) { LOG_WARN("prototype storage materialization failed", K(ret), K(tablet_id)); }
  return ret;
}
}
}
