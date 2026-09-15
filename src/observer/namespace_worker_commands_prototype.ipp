// Typed transport for the existing Query -> Rootserver command interface.
// SQL parsing, privilege checks and command executors remain native in worker.
#include "query/command/ob_root_command_service.h"
#include <type_traits>
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
#define NS_ROOT_ONE(X) \
  X(modify_system_variable, obcall::ObModifySysVarArg, const) \
  X(alter_database, obcall::ObAlterDatabaseArg, const) \
  X(maintain_obj_dependency_info, obcall::ObDependencyObjDDLArg, const) \
  X(rename_table, obcall::ObRenameTableArg, const) \
  X(purge_index, obcall::ObPurgeIndexArg, const) \
  X(create_table_like, obcall::ObCreateTableLikeArg, const) \
  X(purge_table, obcall::ObPurgeTableArg, const) \
  X(restore_table_from_recyclebin, obcall::ObRecyclebinRestoreTableArg, const) \
  X(purge_database, obcall::ObPurgeDatabaseArg, const) \
  X(restore_database, obcall::ObRecyclebinRestoreDatabaseArg, const) \
  X(optimize_table, obcall::ObOptimizeTableArg, const) \
  X(set_passwd, obcall::ObSetPasswdArg, const) \
  X(grant, obcall::ObGrantArg, const) \
  X(revoke_user, obcall::ObRevokeUserArg, const) \
  X(alter_user_default_role, obcall::ObAlterUserRoleArg, const) \
  X(revoke_database, obcall::ObRevokeDBArg, const) \
  X(revoke_table, obcall::ObRevokeTableArg, const) \
  X(revoke_routine, obcall::ObRevokeRoutineArg, const) \
  X(alter_role, obcall::ObAlterRoleArg, const) \
  X(revoke_object, obcall::ObRevokeObjMysqlArg, const) \
  X(create_outline, obcall::ObCreateOutlineArg, const) \
  X(alter_outline, obcall::ObAlterOutlineArg, const) \
  X(drop_outline, obcall::ObDropOutlineArg, const) \
  X(create_routine, obcall::ObCreateRoutineArg, const) \
  X(drop_routine, obcall::ObDropRoutineArg, const) \
  X(alter_routine, obcall::ObCreateRoutineArg, const) \
  X(create_package, obcall::ObCreatePackageArg, const) \
  X(drop_package, obcall::ObDropPackageArg, const) \
  X(alter_trigger, obcall::ObAlterTriggerArg, const) \
  X(drop_trigger, obcall::ObDropTriggerArg, const) \
  X(create_ai_model, obcall::ObCreateAiModelArg, const) \
  X(drop_ai_model, obcall::ObDropAiModelArg, const) \
  X(root_minor_freeze, obcall::ObMinorFreezeArg, const) \
  X(tablet_major_freeze, common::ObTabletID, const) \
  X(admin_set_config, obcall::ObAdminSetConfigArg, )

#define NS_ROOT_TWO(X) \
  X(create_database, obcall::ObCreateDatabaseArg, const, obcall::UInt64) \
  X(parallel_create_table, obcall::ObCreateTableArg, const, obcall::ObCreateTableRes) \
  X(set_comment, obcall::ObSetCommentArg, const, obcall::ObParallelDDLRes) \
  X(alter_table, obcall::ObAlterTableArg, const, obcall::ObAlterTableRes) \
  X(fork_table, obcall::ObForkTableArg, const, obcall::ObDDLRes) \
  X(fork_database, obcall::ObForkDatabaseArg, const, obcall::ObDDLRes) \
  X(truncate_table, obcall::ObTruncateTableArg, const, obcall::ObDDLRes) \
  X(truncate_table_v2, obcall::ObTruncateTableArg, const, obcall::ObDDLRes) \
  X(exchange_partition, obcall::ObExchangePartitionArg, const, obcall::ObAlterTableRes) \
  X(create_index, obcall::ObCreateIndexArg, const, obcall::ObAlterTableRes) \
  X(parallel_create_index, obcall::ObCreateIndexArg, const, obcall::ObAlterTableRes) \
  X(drop_table, obcall::ObDropTableArg, const, obcall::ObDDLRes) \
  X(parallel_drop_table, obcall::ObDropTableArg, const, obcall::ObDropTableRes) \
  X(drop_database, obcall::ObDropDatabaseArg, const, obcall::ObDropDatabaseRes) \
  X(drop_index, obcall::ObDropIndexArg, const, obcall::ObDropIndexRes) \
  X(purge_expire_recycle_objects, obcall::ObPurgeRecycleBinArg, const, obcall::Int64) \
  X(create_user, obcall::ObCreateUserArg, , common::ObSArray<int64_t>) \
  X(drop_user, obcall::ObDropUserArg, const, common::ObSArray<int64_t>) \
  X(rename_user, obcall::ObRenameUserArg, const, common::ObSArray<int64_t>) \
  X(lock_user, obcall::ObLockUserArg, const, common::ObSArray<int64_t>) \
  X(create_trigger_with_res, obcall::ObCreateTriggerArg, const, obcall::ObCreateTriggerRes)

#define NS_ROOT_ZERO(X) \
  X(major_freeze) \
  X(suspend_merge) \
  X(resume_merge) \
  X(clear_merge_error)


Frame root_request(const char *method) {
  Frame request('M'); request.number(2); request.string(common::ObString::make_string(method)); return request;
}
int root_roundtrip(Frame &request, Frame &reply, int &command_ret) {
  int ret = worker_send(request);
  if (!ret) { ret = worker_read(reply); }
  if (!ret && reply.type() != 'g') { ret = common::OB_INVALID_ARGUMENT; }
  if (!ret) { ret = static_cast<int>(reply.number()); }
  if (!ret) { command_ret = static_cast<int>(reply.number()); ret = reply.ret; }
  return ret;
}
template<class T> void read_mutated_root_argument(Frame &reply, T &arg) {
  if constexpr (!std::is_const_v<T>) { reply.read(arg); }
}
int finish_root_reply(Frame &reply, int command_ret) {
  return reply.consumed() ? command_ret : common::OB_INVALID_ARGUMENT;
}
class RemoteRootCommands final : public query::ObIRootCommandService {
public:
#define ROOT_ONE(name, Arg, Const) \
  int name(Const Arg &arg) override { \
    Frame request = root_request(#name), reply; request.append(arg); \
    int command_ret = 0; const int ret = root_roundtrip(request, reply, command_ret); \
    if (ret) { return ret; } \
    read_mutated_root_argument(reply, arg); return finish_root_reply(reply, command_ret); \
  }
#define ROOT_TWO(name, Arg, Const, Result) \
  int name(Const Arg &arg, Result &result) override { \
    Frame request = root_request(#name), reply; request.append(arg); \
    int command_ret = 0; const int ret = root_roundtrip(request, reply, command_ret); \
    if (ret) { return ret; } \
    read_mutated_root_argument(reply, arg); reply.read(result); return finish_root_reply(reply, command_ret); \
  }
#define ROOT_ZERO(name) \
  int name() override { \
    Frame request = root_request(#name), reply; \
    int command_ret = 0; const int ret = root_roundtrip(request, reply, command_ret); \
    return ret ? ret : finish_root_reply(reply, command_ret); \
  }
  NS_ROOT_ONE(ROOT_ONE)
  NS_ROOT_TWO(ROOT_TWO)
  NS_ROOT_ZERO(ROOT_ZERO)
#undef ROOT_ONE
#undef ROOT_TWO
#undef ROOT_ZERO
  int check_partition_exchange_schema_for_user(const share::schema::ObTableSchema &base,
      const share::schema::ObTableSchema &inc, const common::ObString &name,
      share::schema::ObPartitionLevel level) override {
    Frame request = root_request("check_partition_exchange_schema_for_user"), reply;
    request.append(base); request.append(inc); request.string(name); request.number(level);
    int command_ret = 0; const int ret = root_roundtrip(request, reply, command_ret);
    return ret ? ret : finish_root_reply(reply, command_ret);
  }
};
int process_root_command(uint64_t ns, Frame &request, Frame &reply) {
  reply = Frame('g');
  if (ns != 1) { reply.number(common::OB_NOT_SUPPORTED); return reply.ret; }
  const common::ObString method = request.string();
  auto &service = ObServer::get_instance().get_local_management_service();
#define ROOT_ONE(name, Arg, Const) \
  if (method == common::ObString::make_string(#name)) { \
    auto arg = std::make_unique<Arg>(); request.read(*arg); \
    if (!request.consumed()) { reply.number(common::OB_INVALID_ARGUMENT); } \
    else { \
      const int ret = service.name(*arg); reply.number(0); reply.number(ret); \
      if constexpr (!std::is_const_v<Const Arg>) { reply.append(*arg); } \
    } \
    return reply.ret; \
  }
#define ROOT_TWO(name, Arg, Const, Result) \
  if (method == common::ObString::make_string(#name)) { \
    auto arg = std::make_unique<Arg>(); Result result; request.read(*arg); \
    if (!request.consumed()) { reply.number(common::OB_INVALID_ARGUMENT); } \
    else { \
      const int ret = service.name(*arg, result); reply.number(0); reply.number(ret); \
      if constexpr (!std::is_const_v<Const Arg>) { reply.append(*arg); } \
      reply.append(result); \
    } \
    return reply.ret; \
  }
#define ROOT_ZERO(name) \
  if (method == common::ObString::make_string(#name)) { \
    if (!request.consumed()) { reply.number(common::OB_INVALID_ARGUMENT); } \
    else { const int ret = service.name(); reply.number(0); reply.number(ret); } \
    return reply.ret; \
  }
  NS_ROOT_ONE(ROOT_ONE)
  NS_ROOT_TWO(ROOT_TWO)
  NS_ROOT_ZERO(ROOT_ZERO)
#undef ROOT_ONE
#undef ROOT_TWO
#undef ROOT_ZERO
  if (method == common::ObString::make_string("check_partition_exchange_schema_for_user")) {
    auto base = std::make_unique<share::schema::ObTableSchema>();
    auto inc = std::make_unique<share::schema::ObTableSchema>();
    request.read(*base); request.read(*inc);
    const common::ObString name = request.string();
    const auto level = static_cast<share::schema::ObPartitionLevel>(request.number());
    if (!request.consumed()) { reply.number(common::OB_INVALID_ARGUMENT); }
    else {
      const int ret = service.check_partition_exchange_schema_for_user(*base, *inc, name, level);
      reply.number(0); reply.number(ret);
    }
    return reply.ret;
  }
  reply.number(common::OB_NOT_SUPPORTED); return reply.ret;
}
#undef NS_ROOT_ONE
#undef NS_ROOT_TWO
#undef NS_ROOT_ZERO
} } }
