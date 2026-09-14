// Throwaway, single-process experiment. See docs/research/namespace_fork_minimal_prototype.md.
#ifndef OCEANBASE_ROOTSERVER_NAMESPACE_FORK_PROTOTYPE_H_
#define OCEANBASE_ROOTSERVER_NAMESPACE_FORK_PROTOTYPE_H_

#include "share/schema/ob_table_schema.h"

namespace oceanbase {
namespace common { class ObISQLClient; }
namespace rootserver {

class NamespaceForkPrototype final
{
public:
  static bool enabled();
  static bool is_target(const common::ObString &name);
  static int publish(uint64_t source_id, uint64_t target_id, int64_t schema_version,
                     int64_t snapshot);
  static int get_snapshot(common::ObISQLClient &proxy,
                          const share::schema::ObTableSchema &source,
                          uint64_t target_id, int64_t &snapshot);
  static void note_materialization();
  static void note_tablet_collection();
  static void log_work(const char *stage, uint64_t database_id);
};

} // namespace rootserver
} // namespace oceanbase
#endif
