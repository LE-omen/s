// Throwaway kernel experiment: immutable catalog and directory, storage-triggered materialization.
#ifndef OCEANBASE_NAMESPACE_FORK_KERNEL_PROTOTYPE_H_
#define OCEANBASE_NAMESPACE_FORK_KERNEL_PROTOTYPE_H_
#include "share/schema/ob_table_schema.h"
namespace oceanbase {
namespace common { class ObISQLClient; }
namespace storage {
class ObTablet;
class NamespaceForkKernelPrototype final
{
public:
  static bool enabled();
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
