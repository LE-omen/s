// PROTOTYPE: real immutable B+ tree pages stored in engine tables, one-engine transactions.
// Source retained, fixed two-integer-column schemas, no online page reclamation or source DROP.
#define USING_LOG_PREFIX STORAGE
#include "rootserver/fork_table/namespace_fork_kernel_prototype.h"
#include "rootserver/ob_tablet_creator.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/tablet/ob_tablet_mapping_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/rc/ob_server_runtime.h"
#include "storage/compaction/ob_freeze_info_mgr.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_tablet_fork_task.h"
#include "lib/checksum/ob_crc64.h"
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace oceanbase {
namespace storage {
using namespace common;
using namespace share;
using namespace share::schema;
namespace {
// Encodes (branch database, original local id) without changing production object keys.
// Both parts are checked; ids with the marker are reserved for this isolated experiment.
constexpr uint64_t ID_MARK = 1ULL << 62;
constexpr size_t FANOUT = 8;
const char *ROOTS = "__fork_proto_meta.roots";
const char *PAGES = "__fork_proto_meta.pages";
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

int64_t cap_min(int64_t a, int64_t b) { return a == 0 ? b : b == 0 ? a : std::min(a, b); }
uint64_t encoded(uint64_t db, uint64_t local) { return ID_MARK | (db << 32) | local; }
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
int roots(ObISQLClient &sql, uint64_t db, Roots &root, bool lock = false) {
  int ret = OB_SUCCESS; ObSqlString q; ObMySQLProxy::MySQLResult res;
  sqlclient::ObMySQLResult *r = nullptr;
  if (OB_FAIL(q.assign_fmt("SELECT source_id,catalog_page,catalog_cap,directory_page,directory_cap,snapshot,schema_version FROM %s WHERE database_id=%lu%s", ROOTS, db, lock ? " FOR UPDATE" : ""))) {
  } else if (OB_FAIL(sql.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_uint(0L, root.source)) || OB_FAIL(r->get_uint(1L, root.catalog.page))
      || OB_FAIL(r->get_int(2L, root.catalog.cap)) || OB_FAIL(r->get_uint(3L, root.directory.page))
      || OB_FAIL(r->get_int(4L, root.directory.cap)) || OB_FAIL(r->get_int(5L, root.snapshot))
      || OB_FAIL(r->get_int(6L, root.schema_version))) {
  }
  return ret;
}
int save_roots(ObISQLClient &sql, uint64_t db, const Roots &root) {
  ObSqlString q; int ret = q.assign_fmt("REPLACE INTO %s VALUES(%lu,%lu,%lu,%ld,%lu,%ld,%ld,%ld)",
      ROOTS, db, root.source, root.catalog.page, root.catalog.cap, root.directory.page,
      root.directory.cap, root.snapshot, root.schema_version);
  return ret == OB_SUCCESS ? write_sql(sql, q) : ret;
}
bool supported(const ObTableSchema &s) {
  const auto *id = s.get_column_schema("id"), *v = s.get_column_schema("v");
  return s.get_table_type() == USER_TABLE && !s.is_partitioned_table() && s.get_index_tid_count() == 0
      && !s.has_lob_aux_table() && s.get_foreign_key_infos().empty() && s.get_trigger_list().empty()
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
  copy->set_database_id(db); copy->set_table_id(id); copy->set_tablet_id(ObTabletID(encoded(db, tablet)));
  for (int64_t i = 0; i < copy->get_column_count(); ++i) {
    const_cast<ObColumnSchemaV2 *>(copy->get_column_schema_by_idx(i))->set_table_id(id);
  }
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto &slot = schemas[id]; if (!slot) { slot = std::move(holder); } schema = &slot->schema; }
  return OB_SUCCESS;
}
}

bool NamespaceForkKernelPrototype::enabled() {
  static bool on = [] { const char *s = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE"); return s && !std::strcmp(s, "2"); }();
  return on;
}
bool NamespaceForkKernelPrototype::is_encoded_id(uint64_t id) {
  return enabled() && id != OB_INVALID_ID && (id & (3ULL << 62)) == ID_MARK;
}
int NamespaceForkKernelPrototype::observe_schema(ObISQLClient &trans, const ObTableSchema &schema) {
  if (!enabled() || !schema.is_user_table() || is_encoded_id(schema.get_table_id())) { return OB_SUCCESS; }
  int ret = OB_SUCCESS;
  { // Finish the result before issuing another statement on the same DDL connection.
  ObSqlString q; ObMySQLProxy::MySQLResult res; ObString name;
  sqlclient::ObMySQLResult *r = nullptr;
  if (OB_FAIL(q.assign_fmt("SELECT database_name FROM oceanbase.__all_database WHERE database_id=%lu", schema.get_database_id()))) {
  } else if (OB_FAIL(trans.read(res, q.ptr()))) {
  } else if (OB_ISNULL(r = res.get_result())) { ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(r->next())) {
  } else if (OB_FAIL(r->get_varchar(0L, name))) {
  } else if (!name.prefix_match("__fork_proto_a")) { return OB_SUCCESS;
  } else if (!supported(schema) || schema.get_database_id() >= (1ULL << 30)
      || schema.get_table_id() >= (1ULL << 32) || schema.get_tablet_id().id() >= (1ULL << 32)) { ret = OB_NOT_SUPPORTED; }
  }
  if (ret != OB_SUCCESS) { return ret; }
  Roots root;
  if ((ret = roots(trans, schema.get_database_id(), root, true)) == OB_ITER_END) { ret = OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  if (root.snapshot != 0) { return OB_NOT_SUPPORTED; }
  std::string serialized(schema.get_serialize_size(), '\0'); int64_t pos = 0; uint64_t object = 0;
  Value value;
  if (OB_FAIL(schema.serialize(&serialized[0], serialized.size(), pos))) {
  } else if (OB_FAIL(save_blob(trans, serialized, object))) {
  } else {
    value.data = entry(object, schema.get_table_id(), schema.get_tablet_id().id());
    if (OB_FAIL(put(trans, root.catalog, std::string(schema.get_table_name()), value, root.catalog))) {
    } else if (OB_FAIL(put(trans, root.catalog, "#" + key_of(schema.get_table_id()), value, root.catalog))) {
    } else if (OB_FAIL(put(trans, root.directory, key_of(schema.get_tablet_id().id()), value, root.directory))) {
    } else {
      root.schema_version = schema.get_schema_version();
      ret = save_roots(trans, schema.get_database_id(), root);
      LOG_INFO("PROTOTYPE_V2_SOURCE_DIRECTORY", K(ret), "table_id", schema.get_table_id(), "directory_root", root.directory.page);
    }
  }
  return ret;
}
int NamespaceForkKernelPrototype::capture(ObISQLClient &trans, uint64_t source, uint64_t target,
                                          int64_t snapshot, int64_t schema_version) {
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
  schema = nullptr; if (!enabled() || !GCTX.sql_proxy_) { return OB_NOT_INIT; }
  Roots root; Value value; int ret = roots(*GCTX.sql_proxy_, db, root);
  if (ret == OB_ITER_END) { return OB_SUCCESS; }
  if (ret != OB_SUCCESS) { return ret; }
  if (!root.snapshot) { return OB_SUCCESS; }
  ret = find(*GCTX.sql_proxy_, root.catalog, std::string(name.ptr(), name.length()), value);
  return ret == OB_ENTRY_NOT_EXIST ? OB_SUCCESS : ret != OB_SUCCESS ? ret : schema_from_value(db, value, schema);
}
int NamespaceForkKernelPrototype::schema_by_id(uint64_t id, const ObTableSchema *&schema) {
  schema = nullptr; if (!is_encoded_id(id)) { return OB_INVALID_ARGUMENT; }
  { std::lock_guard<std::mutex> lock(schema_mutex);
    auto it = schemas.find(id); if (it != schemas.end()) { schema = &it->second->schema; return OB_SUCCESS; } }
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
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
  const uint64_t db = database_of(tablet.id()); Roots root; Value value;
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
  Roots root; int ret = roots(*GCTX.sql_proxy_, db, root);
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
          if ((ret = schema_from_value(db, node.values[i], schema)) == OB_SUCCESS) { ret = out.push_back(schema); }
        }
      }
    } else { pending.insert(pending.end(), node.children.rbegin(), node.children.rend()); }
  }
  return ret;
}
int NamespaceForkKernelPrototype::check_ddl(const ObSimpleTableSchemaV2 &schema) {
  if (!enabled() || !schema.is_user_table()) { return OB_SUCCESS; }
  if (is_encoded_id(schema.get_table_id())) { return OB_NOT_SUPPORTED; }
  ObSchemaGetterGuard guard; const ObDatabaseSchema *db = nullptr;
  int ret = GSCHEMASERVICE.get_runtime_schema_guard(guard);
  if (ret != OB_SUCCESS) { return ret; }
  if ((ret = guard.get_database_schema(schema.get_database_id(), db)) != OB_SUCCESS || !db) { return ret; }
  if (db->get_database_name_str().prefix_match("__fork_proto_b")) { return OB_NOT_SUPPORTED; }
  if (!db->get_database_name_str().prefix_match("__fork_proto_a")) { return OB_SUCCESS; }
  Roots root; Value existing;
  ret = roots(*GCTX.sql_proxy_, schema.get_database_id(), root);
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
  int ret = OB_SUCCESS; Roots root; Value value; const ObTableSchema *schema = nullptr;
  const uint64_t db = database_of(meta.tablet_id_.id());
  uint64_t object = 0, table = 0, source = 0, bound = 0;
  if (OB_FAIL(roots(*GCTX.sql_proxy_, db, root))) {
  } else if (OB_FAIL(find(*GCTX.sql_proxy_, root.directory, key_of(local_of(meta.tablet_id_.id())), value))) {
  } else if (!entry(value.data, object, table, source, bound)) { ret = OB_CHECKSUM_ERROR;
  } else if (bound == 0) { // Physical CREATE MDS may not have committed its directory binding yet.
  } else if (bound != meta.tablet_id_.id() || source != meta.fork_info_.get_fork_src_tablet_id().id()
      || root.snapshot != meta.fork_info_.get_fork_snapshot_version()) { ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(schema_from_value(db, value, schema))) {
  } else {
    ObTabletForkParam param; bool ready = false;
    param.table_id_ = schema->get_table_id(); param.schema_version_ = schema->get_schema_version();
    // Stable DAG identity only; no rootserver DDL task is created.
    param.task_id_ = bound; param.source_tablet_id_ = ObTabletID(source);
    param.dest_tablet_id_ = meta.tablet_id_; param.fork_snapshot_version_ = root.snapshot;
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
  return ret;
}
int NamespaceForkKernelPrototype::ensure_tablet(const ObTabletID &tablet_id) {
  if (!is_encoded_id(tablet_id.id())) { return OB_SUCCESS; }
  int ret = OB_SUCCESS; const uint64_t db = database_of(tablet_id.id()), local = local_of(tablet_id.id());
  if (!GCTX.sql_proxy_) { return OB_NOT_INIT; }
  ObMySQLTransaction trans; Roots root; Value value;
  // The row lock joins concurrent requests before physical creation. The business transaction is untouched.
  if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(roots(trans, db, root, true))) {
  } else if (OB_FAIL(find(trans, root.directory, key_of(local), value))) {
  } else {
    uint64_t object = 0, table = 0, source_tablet = 0, bound = 0;
    if (!entry(value.data, object, table, source_tablet, bound)) { ret = OB_CHECKSUM_ERROR;
    } else if (bound == tablet_id.id()) {
      // Already committed in the same authoritative directory; no DDL or snapshot reacquisition.
    } else if (bound != 0 || value.cap != root.snapshot || root.snapshot <= 0) { ret = OB_STATE_NOT_MATCH;
    } else {
      ObSnapshotInfo pin; ObSnapshotTableProxy pins; SCN scn; ObStorageSnapshotInfo reserved;
      const ObTableSchema *schema = nullptr;
      auto *freeze = share::server_service<ObFreezeInfoMgr>();
      if (OB_FAIL(scn.convert_for_tx(root.snapshot))) {
      } else if (OB_FAIL(pins.get_snapshot(trans, SNAPSHOT_FOR_MULTI_VERSION, scn, pin))) {
      } else if (pin.tablet_id_ != 0 || pin.schema_version_ != root.schema_version || !freeze) { ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(freeze->reload_for_test())) {
      } else if (OB_FAIL(freeze->get_min_reserved_snapshot(ObTabletID(source_tablet), root.snapshot, reserved))) {
      } else if (reserved.snapshot_ > root.snapshot) { ret = OB_SNAPSHOT_DISCARDED;
      } else if (OB_FAIL(schema_from_value(db, value, schema))) {
      } else {
        rootserver::ObTabletCreator creator(SCN::min_scn(), trans); rootserver::ObTabletCreatorArg arg;
        ObArray<ObTabletID> ids; ObArray<const ObTableSchema *> definitions;
        ObArray<bool> empty_major; ObArray<int64_t> logical_birth; ObArray<ObForkTabletInfo> fork_infos;
        ObForkTabletInfo fork; fork.set_fork_snapshot_version(root.snapshot); fork.set_fork_src_tablet_id(ObTabletID(source_tablet));
        if (OB_FAIL(ids.push_back(tablet_id)) || OB_FAIL(definitions.push_back(schema))
            || OB_FAIL(empty_major.push_back(false)) || OB_FAIL(logical_birth.push_back(root.snapshot))
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
          value.data = entry(object, table, source_tablet, tablet_id.id()); value.cap = 0;
          const uint64_t previous_root = root.directory.page;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(put(trans, root.directory, key_of(local), value, root.directory))) {
          } else if (OB_FAIL(save_roots(trans, db, root))) {
          } else {
            LOG_INFO("PROTOTYPE_V2_STORAGE_MATERIALIZE", K(tablet_id), K(source_tablet), "snapshot", root.snapshot,
                     K(previous_root), "next_root", root.directory.page, "entry_layer", "ObAccessService");
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
