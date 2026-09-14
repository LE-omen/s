// Throwaway kernel experiment: immutable catalog and directory, storage-triggered materialization.
#ifndef OCEANBASE_NAMESPACE_FORK_KERNEL_PROTOTYPE_H_
#define OCEANBASE_NAMESPACE_FORK_KERNEL_PROTOTYPE_H_
#include "share/schema/ob_table_schema.h"
namespace oceanbase {
namespace common { class ObISQLClient; }
namespace share { namespace schema { class ObSimpleDatabaseSchema; } }
namespace storage {
class ObTablet;
// Bounded adapter for the prototype's existing 64-bit storage/cache/lock keys.
// Namespace 1 owns the original engine objects; other namespaces keep the same local ids.
struct NamespaceObjectKey
{
  uint64_t namespace_id;
  uint64_t local_id;
  bool is_valid() const { return namespace_id > 0 && namespace_id < (1ULL << 30)
      && local_id > 0 && local_id < (1ULL << 32); }
  uint64_t storage_id() const { return namespace_id == 1 ? local_id
      : (1ULL << 62) | (namespace_id << 32) | local_id; }
};
class NamespaceForkKernelPrototype final
{
public:
  static bool enabled();
  static bool namespace_mode();
  static bool is_namespace_address(const common::ObString &name);
  static int control_namespace(const common::ObString &source, const common::ObString &target,
                                uint64_t &namespace_id);
  static int observe_database(common::ObISQLClient &trans, const share::schema::ObDatabaseSchema &schema);
  static int check_database_ddl(const share::schema::ObDatabaseSchema &schema);
  static int database_in_namespace(uint64_t namespace_id, const common::ObString &name,
                                   const share::schema::ObDatabaseSchema *&schema);
  static int database_by_address(const common::ObString &address,
                                 const share::schema::ObDatabaseSchema *&schema);
  static int database_by_id(uint64_t id, const share::schema::ObDatabaseSchema *&schema);
  static int database_by_id(uint64_t id, const share::schema::ObSimpleDatabaseSchema *&schema);
  static bool is_encoded_id(uint64_t id);
  static int observe_schema(common::ObISQLClient &trans, const share::schema::ObTableSchema &schema);
  static int capture(common::ObISQLClient &trans, uint64_t source, uint64_t target,
                     int64_t snapshot, int64_t schema_version);
  static int schema_by_name(uint64_t database, const common::ObString &name,
                            const share::schema::ObTableSchema *&schema);
  static int schema_by_id(uint64_t table_id, const share::schema::ObTableSchema *&schema);
  static int table_id_for_tablet(const common::ObTabletID &tablet, int64_t schema_version,
                                 uint64_t &table_id);
  static int list_schemas(uint64_t database, common::ObIArray<const share::schema::ObTableSchema *> &schemas);
  static int check_ddl(const share::schema::ObSimpleTableSchemaV2 &schema);
  static int ensure_tablet(const common::ObTabletID &tablet_id);
  static int schedule_baseline(const ObTablet &tablet);
};
}
}
#endif
