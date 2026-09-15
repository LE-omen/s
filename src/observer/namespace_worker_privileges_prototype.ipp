// Native privilege lookups transported by value. The SQL privilege rules remain
// in ObSchemaGetterGuard; decoded values live only as long as that guard.
#include "share/schema/ob_priv_mgr.h"
#include <tuple>
#include <type_traits>
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
using namespace common;
using namespace share::schema;
int catalog_schema_guard(int64_t version, ObSchemaGetterGuard &guard);

template<class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
void priv_encode(Frame &frame, const T &value) { frame.number(value); }
inline void priv_encode(Frame &frame, const ObString &value) { frame.string(value); }
template<class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
void priv_decode(Frame &frame, T &value) {
  const uint64_t wire = frame.number();
  if constexpr (std::is_same_v<T, bool>) { if (wire > 1) { frame.ret = OB_INVALID_ARGUMENT; } }
  value = static_cast<T>(wire);
}
inline void priv_decode(Frame &frame, ObString &value) { value = frame.string(); }
template<class... T> void priv_encode_fields(Frame &frame, const T &...value) { (priv_encode(frame, value), ...); }
template<class... T> void priv_decode_fields(Frame &frame, T &...value) { (priv_decode(frame, value), ...); }
#define PRIV_KEY(Type, ...) \
  inline void priv_encode(Frame &frame, const Type &key) { priv_encode_fields(frame, __VA_ARGS__); } \
  inline void priv_decode(Frame &frame, Type &key) { priv_decode_fields(frame, __VA_ARGS__); }
PRIV_KEY(ObOriginalDBKey, key.user_id_, key.db_)
PRIV_KEY(ObTablePrivSortKey, key.user_id_, key.db_, key.table_)
PRIV_KEY(ObRoutinePrivSortKey, key.user_id_, key.db_, key.routine_, key.routine_type_)
PRIV_KEY(ObColumnPrivSortKey, key.user_id_, key.db_, key.table_, key.column_)
PRIV_KEY(ObSysPrivKey, key.grantee_id_)
PRIV_KEY(ObObjPrivSortKey, key.obj_id_, key.obj_type_, key.col_id_, key.grantor_id_, key.grantee_id_)
PRIV_KEY(ObObjMysqlPrivSortKey, key.user_id_, key.object_name_, key.object_type_)
#undef PRIV_KEY

template<class T> constexpr bool priv_output = std::is_lvalue_reference_v<T>
    && !std::is_const_v<std::remove_reference_t<T>>;
template<class T> struct PrivArgument { using Type = T; };
template<class T> struct PrivArgument<ObIArray<const T *>> { using Type = ObSEArray<const T *, 4>; };
template<class T> using PrivArgumentType = typename PrivArgument<std::remove_cv_t<std::remove_reference_t<T>>>::Type;

template<class T> void priv_encode_result(Frame &reply, T *value) {
  reply.number(value ? 1 : 0); if (value) { reply.append(*value); }
}
template<class T> void priv_encode_result(Frame &reply, const ObIArray<const T *> &values) {
  reply.number(values.count());
  for (int64_t i = 0; !reply.ret && i < values.count(); ++i) { priv_encode_result(reply, values.at(i)); }
}
template<class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
void priv_encode_result(Frame &reply, const T &value) { priv_encode(reply, value); }

template<class Formal, class Value> void priv_read_input(Frame &request, Value &value) {
  if constexpr (!priv_output<Formal>) { priv_decode(request, value); }
}
template<class Formal, class Value> void priv_write_output(Frame &reply, Value &value) {
  if constexpr (priv_output<Formal>) { priv_encode_result(reply, value); }
}
template<class... Args, size_t... I>
int dispatch_priv_read(int (ObPrivMgr::*method)(Args...) const, const ObPrivMgr &mgr,
                      Frame &request, Frame &reply, std::index_sequence<I...>) {
  static_assert((size_t(priv_output<Args>) + ...) == 1, "privilege reader has one output");
  std::tuple<PrivArgumentType<Args>...> args;
  (priv_read_input<Args>(request, std::get<I>(args)), ...);
  if (!request.consumed()) { reply.number(OB_INVALID_ARGUMENT); return reply.ret; }
  const int ret = std::apply([&](auto &...value) { return (mgr.*method)(value...); }, args);
  reply.number(ret);
  if (!ret) { (priv_write_output<Args>(reply, std::get<I>(args)), ...); }
  return reply.ret;
}
template<class... Args>
int dispatch_priv_read(int (ObPrivMgr::*method)(Args...) const, const ObPrivMgr &mgr,
                      Frame &request, Frame &reply) {
  return dispatch_priv_read(method, mgr, request, reply, std::index_sequence_for<Args...>{});
}

class RemotePrivMgr final : public ObPrivMgr {
  int64_t version_;
  mutable ObArenaAllocator allocator_{ObMemAttr("NsPrivilege")};
  mutable std::vector<ObSchema *> values_;
  template<class T> void decode_result(Frame &reply, T *&value) const {
    value = nullptr;
    const uint64_t present = reply.number();
    if (reply.ret || present > 1) { reply.ret = OB_INVALID_ARGUMENT; return; }
    if (present) {
      using Owned = std::remove_const_t<T>;
      void *memory = allocator_.alloc(sizeof(Owned));
      if (!memory) { reply.ret = OB_ALLOCATE_MEMORY_FAILED; return; }
      auto *owned = new (memory) Owned(&allocator_);
      reply.read(*owned);
      if (reply.ret) { owned->~Owned(); }
      else { values_.push_back(owned); value = owned; }
    }
  }
  template<class T> void decode_result(Frame &reply, ObIArray<const T *> &values) const {
    const uint64_t count = reply.number();
    if (reply.ret || count > reply.data.size() / 8) { reply.ret = OB_INVALID_ARGUMENT; return; }
    for (uint64_t i = 0; !reply.ret && i < count; ++i) {
      const T *value = nullptr; decode_result(reply, value);
      if (!reply.ret) { reply.ret = value ? values.push_back(value) : OB_INVALID_ARGUMENT; }
    }
  }
  template<class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  void decode_result(Frame &reply, T &value) const { priv_decode(reply, value); }
  template<class Formal, class Value> void write_input(Frame &request, const Value &value) const {
    if constexpr (!priv_output<Formal>) { priv_encode(request, value); }
  }
  template<class Formal, class Value> void read_output(Frame &reply, Value &value) const {
    if constexpr (priv_output<Formal>) { decode_result(reply, value); }
  }
  template<class... Formal, class... Actual, size_t... I>
  int read_impl(int (ObPrivMgr::*)(Formal...) const, const char *name, std::tuple<Actual &...> args,
                std::index_sequence<I...>) const {
    Frame input, reply; input.string(ObString::make_string(name));
    (write_input<Formal>(input, std::get<I>(args)), ...);
    int ret = input.ret;
    if (!ret) {
      ret = worker_catalog_fetch('p', 1, ObString(input.data.size() - Frame::HEADER_SIZE,
          input.data.data() + Frame::HEADER_SIZE), version_, reply);
    }
    if (!ret) { (read_output<Formal>(reply, std::get<I>(args)), ...); }
    return ret ? ret : reply.consumed() ? OB_SUCCESS : reply.ret ? reply.ret : OB_INVALID_ARGUMENT;
  }
  template<class... Formal, class... Actual>
  int read(int (ObPrivMgr::*method)(Formal...) const, const char *name, Actual &...args) const {
    return read_impl(method, name, std::tie(args...), std::index_sequence_for<Formal...>{});
  }
public:
  explicit RemotePrivMgr(int64_t version) : version_(version) {}
  ~RemotePrivMgr() override { for (auto *value : values_) { value->~ObSchema(); } }
  int get_db_priv(const ObOriginalDBKey &db_priv_key, const ObDBPriv *&db_priv, bool db_is_pattern) const override {
    return read(&ObPrivMgr::get_db_priv, "get_db_priv", db_priv_key, db_priv, db_is_pattern);
  }
  int get_table_priv(const ObTablePrivSortKey &table_priv_key, const ObTablePriv *&table_priv) const override {
    return read(&ObPrivMgr::get_table_priv, "get_table_priv", table_priv_key, table_priv);
  }
  int get_routine_priv(const ObRoutinePrivSortKey &routine_priv_key, const ObRoutinePriv *&routine_priv) const override {
    return read(&ObPrivMgr::get_routine_priv, "get_routine_priv", routine_priv_key, routine_priv);
  }
  int get_column_priv(const ObColumnPrivSortKey &column_priv_key, const ObColumnPriv *&column_priv) const override {
    return read(&ObPrivMgr::get_column_priv, "get_column_priv", column_priv_key, column_priv);
  }
  int get_column_priv_in_table(const uint64_t user_id, const ObString &db, const ObString &table, ObIArray<const ObColumnPriv *> &column_privs) const override {
    column_privs.reset(); return read(&ObPrivMgr::get_column_priv_in_table, "get_column_priv_in_table", user_id, db, table, column_privs);
  }
  int get_column_priv_by_id(const uint64_t priv_id, const ObColumnPriv *&column_priv) const override {
    return read(&ObPrivMgr::get_column_priv_by_id, "get_column_priv_by_id", priv_id, column_priv);
  }
  int get_column_priv_id(const uint64_t user_id, const ObString &db, const ObString &table, const ObString &column, uint64_t &column_priv_id) const override {
    return read(&ObPrivMgr::get_column_priv_id, "get_column_priv_id", user_id, db, table, column, column_priv_id);
  }
  int get_column_priv_in_db(const uint64_t user_id, const ObString &db, ObIArray<const ObColumnPriv *> &column_privs) const override {
    return read(&ObPrivMgr::get_column_priv_in_db, "get_column_priv_in_db", user_id, db, column_privs);
  }
  int table_grant_in_db(const uint64_t user_id, const common::ObString &db, bool &is_grant) const override {
    return read(&ObPrivMgr::table_grant_in_db, "table_grant_in_db", user_id, db, is_grant);
  }
  int routine_grant_in_db(const uint64_t user_id, const ObString &db, bool &is_grant) const override {
    return read(&ObPrivMgr::routine_grant_in_db, "routine_grant_in_db", user_id, db, is_grant);
  }
  int get_obj_priv(const ObObjPrivSortKey &obj_priv_key, const ObObjPriv *&obj_priv) const override {
    return read(&ObPrivMgr::get_obj_priv, "get_obj_priv", obj_priv_key, obj_priv);
  }
  int get_obj_privs_in_ur_and_obj(const ObObjPrivSortKey &obj_key, ObPackedObjPriv &obj_privs) const override {
    return read(&ObPrivMgr::get_obj_privs_in_ur_and_obj, "get_obj_privs_in_ur_and_obj", obj_key, obj_privs);
  }
  int get_obj_privs_in_grantor_ur_obj_id(const ObObjPrivSortKey &obj_key, common::ObIArray<const ObObjPriv *> &obj_privs) const override {
    obj_privs.reset(); return read(&ObPrivMgr::get_obj_privs_in_grantor_ur_obj_id, "get_obj_privs_in_grantor_ur_obj_id", obj_key, obj_privs);
  }
  int get_obj_privs_in_grantor_obj_id(const ObObjPrivSortKey &obj_key, common::ObIArray<const ObObjPriv *> &obj_privs) const override {
    obj_privs.reset(); return read(&ObPrivMgr::get_obj_privs_in_grantor_obj_id, "get_obj_privs_in_grantor_obj_id", obj_key, obj_privs);
  }
  int get_sys_priv(const ObSysPrivKey &sys_priv_key, const ObSysPriv *&sys_priv) const override {
    return read(&ObPrivMgr::get_sys_priv, "get_sys_priv", sys_priv_key, sys_priv);
  }
  int get_obj_mysql_priv(const ObObjMysqlPrivSortKey &obj_mysql_priv_key, const ObObjMysqlPriv *&obj_mysql_priv) const override {
    return read(&ObPrivMgr::get_obj_mysql_priv, "get_obj_mysql_priv", obj_mysql_priv_key, obj_mysql_priv);
  }
  int get_db_privs_in_runtime(common::ObIArray<const ObDBPriv *> &db_privs) const override {
    db_privs.reset(); return read(&ObPrivMgr::get_db_privs_in_runtime, "get_db_privs_in_runtime", db_privs);
  }
  int get_db_privs_in_user(const uint64_t user_id, common::ObIArray<const ObDBPriv *> &db_privs) const override {
    db_privs.reset(); return read(&ObPrivMgr::get_db_privs_in_user, "get_db_privs_in_user", user_id, db_privs);
  }
  int get_table_privs_in_runtime(common::ObIArray<const ObTablePriv *> &table_privs) const override {
    table_privs.reset(); return read(&ObPrivMgr::get_table_privs_in_runtime, "get_table_privs_in_runtime", table_privs);
  }
  int get_table_privs_in_user(const uint64_t user_id, common::ObIArray<const ObTablePriv *> &table_privs) const override {
    table_privs.reset(); return read(&ObPrivMgr::get_table_privs_in_user, "get_table_privs_in_user", user_id, table_privs);
  }
  int get_routine_privs_in_user(const uint64_t user_id, ObIArray<const ObRoutinePriv *> &routine_privs) const override {
    routine_privs.reset(); return read(&ObPrivMgr::get_routine_privs_in_user, "get_routine_privs_in_user", user_id, routine_privs);
  }
  int get_column_privs_in_user(const uint64_t user_id, ObIArray<const ObColumnPriv *> &column_privs) const override {
    column_privs.reset(); return read(&ObPrivMgr::get_column_privs_in_user, "get_column_privs_in_user", user_id, column_privs);
  }
  int get_obj_privs_in_grantee(const uint64_t grantee_id, common::ObIArray<const ObObjPriv *> &obj_privs) const override {
    obj_privs.reset(); return read(&ObPrivMgr::get_obj_privs_in_grantee, "get_obj_privs_in_grantee", grantee_id, obj_privs);
  }
  int get_obj_privs_in_grantor(const uint64_t grantor_id, common::ObIArray<const ObObjPriv *> &obj_privs, bool reset_flag) const override {
    if (reset_flag) { obj_privs.reset(); } return read(&ObPrivMgr::get_obj_privs_in_grantor, "get_obj_privs_in_grantor", grantor_id, obj_privs, reset_flag);
  }
  int get_obj_privs_in_obj(const uint64_t obj_id, const uint64_t obj_type, common::ObIArray<const ObObjPriv *> &obj_privs, bool reset_flag) const override {
    if (reset_flag) { obj_privs.reset(); } return read(&ObPrivMgr::get_obj_privs_in_obj, "get_obj_privs_in_obj", obj_id, obj_type, obj_privs, reset_flag);
  }
  int get_sys_privs_in_runtime(common::ObIArray<const ObSysPriv *> &sys_privs) const override {
    sys_privs.reset(); return read(&ObPrivMgr::get_sys_privs_in_runtime, "get_sys_privs_in_runtime", sys_privs);
  }
  int get_sys_priv_in_grantee(const uint64_t grantee_id, ObSysPriv *& sys_priv) const override {
    return read(&ObPrivMgr::get_sys_priv_in_grantee, "get_sys_priv_in_grantee", grantee_id, sys_priv);
  }
  int get_obj_mysql_privs_in_user(const uint64_t user_id, ObIArray<const ObObjMysqlPriv *> &obj_mysql_privs) const override {
    obj_mysql_privs.reset(); return read(&ObPrivMgr::get_obj_mysql_privs_in_user, "get_obj_mysql_privs_in_user", user_id, obj_mysql_privs);
  }
  int get_obj_mysql_privs_in_obj(const ObString &obj_name, const uint64_t obj_type, ObIArray<const ObObjMysqlPriv *> &obj_privs, bool reset_flag) const override {
    if (reset_flag) { obj_privs.reset(); } return read(&ObPrivMgr::get_obj_mysql_privs_in_obj, "get_obj_mysql_privs_in_obj", obj_name, obj_type, obj_privs, reset_flag);
  }
  int get_priv_schema_count(int64_t &priv_scheam_count) const override {
    return read(&ObPrivMgr::get_priv_schema_count, "get_priv_schema_count", priv_scheam_count);
  }
};
ObPrivMgr *make_remote_priv_mgr(int64_t version) { return new (std::nothrow) RemotePrivMgr(version); }
int process_privilege_read(int64_t version, const ObString &payload, Frame &reply) {
  Frame request;
  request.data.insert(request.data.end(), payload.ptr(), payload.ptr() + payload.length());
  const ObString method = request.string();
  ObSchemaGetterGuard guard;
  const ObPrivMgr *mgr = nullptr;
  int ret = catalog_schema_guard(version, guard);
  if (!ret) { ret = guard.get_priv_mgr(mgr); }
  reply = Frame('c');
  if (ret) { reply.number(ret); return reply.ret; }
#define PRIV_DISPATCH(name) \
  if (method == #name) { return dispatch_priv_read(&ObPrivMgr::name, *mgr, request, reply); }
  PRIV_DISPATCH(get_db_priv)
  PRIV_DISPATCH(get_table_priv)
  PRIV_DISPATCH(get_routine_priv)
  PRIV_DISPATCH(get_column_priv)
  PRIV_DISPATCH(get_column_priv_in_table)
  PRIV_DISPATCH(get_column_priv_by_id)
  PRIV_DISPATCH(get_column_priv_id)
  PRIV_DISPATCH(get_column_priv_in_db)
  PRIV_DISPATCH(table_grant_in_db)
  PRIV_DISPATCH(routine_grant_in_db)
  PRIV_DISPATCH(get_obj_priv)
  PRIV_DISPATCH(get_obj_privs_in_ur_and_obj)
  PRIV_DISPATCH(get_obj_privs_in_grantor_ur_obj_id)
  PRIV_DISPATCH(get_obj_privs_in_grantor_obj_id)
  PRIV_DISPATCH(get_sys_priv)
  PRIV_DISPATCH(get_obj_mysql_priv)
  PRIV_DISPATCH(get_db_privs_in_runtime)
  PRIV_DISPATCH(get_db_privs_in_user)
  PRIV_DISPATCH(get_table_privs_in_runtime)
  PRIV_DISPATCH(get_table_privs_in_user)
  PRIV_DISPATCH(get_routine_privs_in_user)
  PRIV_DISPATCH(get_column_privs_in_user)
  PRIV_DISPATCH(get_obj_privs_in_grantee)
  PRIV_DISPATCH(get_obj_privs_in_grantor)
  PRIV_DISPATCH(get_obj_privs_in_obj)
  PRIV_DISPATCH(get_sys_privs_in_runtime)
  PRIV_DISPATCH(get_sys_priv_in_grantee)
  PRIV_DISPATCH(get_obj_mysql_privs_in_user)
  PRIV_DISPATCH(get_obj_mysql_privs_in_obj)
  PRIV_DISPATCH(get_priv_schema_count)
#undef PRIV_DISPATCH
  reply.number(OB_NOT_SUPPORTED); return reply.ret;
}
} } }
