/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_ddl_common.h"
#include "share/rc/ob_server_runtime.h"
#include "rootserver/ddl_task/ob_ddl_task_util.h"
#include "share/ob_ddl_sim_point.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service.h"
#include "rootserver/fork_table/ob_fork_table_util.h"
#include "lib/utility/utility.h"
#include "rootserver/ob_local_management_service.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "rootserver/ob_domain_index_builder_util.h"
#include "sql/resolver/ddl/ob_fts_index_builder_util.h"
#include "query/vector/ob_vector_index_util.h"
#include "lib/hash/ob_hashset.h"
#include "rootserver/fork_table/namespace_fork_prototype.h"
#include "share/ob_snapshot_table_proxy.h"
#include "storage/compaction/ob_freeze_info_mgr.h"
#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>

using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

namespace {
struct PrototypeForkRecord
{
  uint64_t source_id = OB_INVALID_ID;
  uint64_t target_id = OB_INVALID_ID;
  int64_t schema_version = 0;
  int64_t snapshot = 0;
};
// ponytail: eight process-local experiment slots; persistent registry is outside this prototype.
std::array<PrototypeForkRecord, 8> prototype_forks;
std::mutex prototype_forks_mutex;
std::atomic<int64_t> prototype_materializations{0};
std::atomic<int64_t> prototype_tablet_collections{0};
}

bool NamespaceForkPrototype::enabled()
{
  static const bool enabled = [] {
    const char *value = std::getenv("SEEKDB_NAMESPACE_FORK_PROTOTYPE");
    return value != nullptr && (std::strcmp(value, "1") == 0 || std::strcmp(value, "2") == 0);
  }();
  return enabled;
}

bool NamespaceForkPrototype::is_target(const ObString &name)
{
  return enabled() && name.prefix_match("__fork_proto_b");
}

int NamespaceForkPrototype::publish(uint64_t source_id, uint64_t target_id,
                                    int64_t schema_version, int64_t snapshot)
{
  std::lock_guard<std::mutex> guard(prototype_forks_mutex);
  int ret = OB_SIZE_OVERFLOW;
  for (auto &record : prototype_forks) {
    if (record.target_id == source_id || record.target_id == target_id) {
      return OB_NOT_SUPPORTED;
    }
  }
  for (auto &record : prototype_forks) {
    if (record.snapshot == 0) {
      record.source_id = source_id;
      record.target_id = target_id;
      record.schema_version = schema_version;
      record.snapshot = snapshot;
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int NamespaceForkPrototype::get_snapshot(ObISQLClient &proxy, const ObTableSchema &source,
                                         uint64_t target_id, int64_t &snapshot)
{
  int ret = OB_SUCCESS;
  snapshot = 0;
  PrototypeForkRecord found;
  {
    std::lock_guard<std::mutex> guard(prototype_forks_mutex);
    for (const auto &record : prototype_forks) {
      if (record.target_id == target_id) { found = record; break; }
    }
  }
  // Only the fixed two-integer-column fixture is supported; no late schema cloning.
  const ObColumnSchemaV2 *id = source.get_column_schema("id");
  const ObColumnSchemaV2 *value = source.get_column_schema("v");
  ObSnapshotTableProxy snapshots;
  ObSnapshotInfo pin;
  SCN scn;
  storage::ObStorageSnapshotInfo reserved;
  auto *freeze_mgr = share::server_service<storage::ObFreezeInfoMgr>();
  if (!enabled() || found.snapshot <= 0 || found.source_id != source.get_database_id()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (source.get_schema_version() > found.schema_version
      || source.get_table_type() != USER_TABLE || source.is_partitioned_table()
      || source.get_index_tid_count() != 0 || source.has_lob_aux_table()
      || !source.get_foreign_key_infos().empty() || !source.get_trigger_list().empty()
      || source.has_generated_column() || source.get_autoinc_column_id() != 0
      || source.get_column_count() != 2 || source.get_rowkey_column_num() != 1
      || id == nullptr || value == nullptr || !ob_is_integer_type(id->get_data_type())
      || !ob_is_integer_type(value->get_data_type()) || !id->is_rowkey_column()) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(scn.convert_for_tx(found.snapshot))) {
  } else if (OB_FAIL(snapshots.get_snapshot(proxy, SNAPSHOT_FOR_MULTI_VERSION, scn, pin))) {
  } else if (pin.tablet_id_ != 0 || pin.schema_version_ != found.schema_version) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(freeze_mgr)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(freeze_mgr->reload_for_test())) {
  // Probe retention at S. Passing zero would select the special pre-major zero watermark.
  } else if (OB_FAIL(freeze_mgr->get_min_reserved_snapshot(source.get_tablet_id(), found.snapshot, reserved))) {
  } else if (reserved.snapshot_ > found.snapshot) {
    ret = OB_SNAPSHOT_DISCARDED;
  } else {
    snapshot = found.snapshot;
    LOG_INFO("PROTOTYPE_FORK_PIN_USED", K(target_id), K(snapshot), K(reserved),
             "gc_scn", freeze_mgr->get_snapshot_gc_ts(), "source_tablet", source.get_tablet_id());
  }
  return ret;
}

void NamespaceForkPrototype::note_materialization()
{
  if (enabled()) { ++prototype_materializations; }
}

void NamespaceForkPrototype::note_tablet_collection()
{
  if (enabled()) { ++prototype_tablet_collections; }
}

void NamespaceForkPrototype::log_work(const char *stage, uint64_t database_id)
{
  LOG_INFO("PROTOTYPE_FORK_WORK", K(stage), K(database_id),
           "materializations", prototype_materializations.load(),
           "tablet_collections", prototype_tablet_collections.load());
}

int ObForkTableUtil::collect_complete_domain_index_schemas(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema,
    hash::ObHashMap<uint64_t, ObTableSchema> &complete_index_schema_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObAuxTableMetaInfo> simple_index_infos;
  ObSArray<ObTableSchema> shared_schema_array;
  ObSArray<ObTableSchema> domain_schema_array;
  ObSArray<ObTableSchema> aux_schema_array;
  ObArenaAllocator allocator(lib::ObLabel("ForkTableIdx"));
  ObSArray<ObTableSchema> complete_index_schemas;

  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
  } else {
    if (complete_index_schema_map.created()) {
      if (OB_FAIL(complete_index_schema_map.clear())) {
      }
    } else if (OB_FAIL(complete_index_schema_map.create(simple_index_infos.count() * 2 + 8,
                                                        lib::ObLabel("ForkTableIdxMap")))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = nullptr;
      const uint64_t index_table_id = simple_index_infos.at(i).table_id_;
      if (OB_INVALID_ID == index_table_id) {
        continue;
      } else if (OB_FAIL(schema_guard.get_table_schema( index_table_id, index_schema))) {
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema is null", K(ret), K(index_table_id));
      } else if (index_schema->is_in_recyclebin() ||
                 INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
        continue;
      } else {
        HEAP_VAR(ObTableSchema, tmp_schema) {
          const ObIndexType index_type = index_schema->get_index_type();
          if (OB_FAIL(tmp_schema.assign(*index_schema))) {
          } else if (tmp_schema.is_rowkey_doc_id() ||
                     tmp_schema.is_doc_id_rowkey() ||
                     tmp_schema.is_vec_rowkey_vid_type() ||
                     tmp_schema.is_vec_vid_rowkey_type()) {
            if (OB_FAIL(shared_schema_array.push_back(tmp_schema))) {
            }
          } else if (tmp_schema.is_fts_index_aux() ||
                     tmp_schema.is_multivalue_index_aux() ||
                     tmp_schema.is_vec_domain_index()) {
            if (OB_FAIL(domain_schema_array.push_back(tmp_schema))) {
            }
          } else if (share::schema::is_fts_doc_word_aux(index_type) ||
                     share::schema::is_vec_index_id_type(index_type) ||
                     share::schema::is_vec_index_snapshot_data_type(index_type) ||
                     share::schema::is_built_in_vec_ivf_index(index_type) ||
                     share::schema::is_hybrid_vec_index_embedded_type(index_type)) {
            if (OB_FAIL(aux_schema_array.push_back(tmp_schema))) {
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !domain_schema_array.empty()) {
    bool need_doc_id = false;
    bool need_vid = false;
    if (OB_FAIL(ObFtsIndexBuilderUtil::check_need_doc_id(table_schema, need_doc_id))) {
    } else if (OB_FAIL(ObVectorIndexUtil::check_need_vid(table_schema, need_vid))) {
    } else if (OB_FAIL(rootserver::ObDomainIndexBuilderUtil::retrieve_complete_domain_index(
                 shared_schema_array,
                 domain_schema_array,
                 aux_schema_array,
                 allocator,
                 table_schema.get_table_id(),
                 complete_index_schemas,
                 need_doc_id,
                 need_vid))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < complete_index_schemas.count(); ++i) {
        const ObTableSchema &schema = complete_index_schemas.at(i);
        if (OB_FAIL(complete_index_schema_map.set_refactored(schema.get_table_id(), schema))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("fail to set complete index schema", K(ret), K(schema.get_table_id()));
          }
        }
      }
    }
  }

  return ret;
}

bool ObForkTableUtil::is_domain_or_aux_index(const ObTableSchema &index_schema)
{
  const ObIndexType index_type = index_schema.get_index_type();
  return index_schema.is_rowkey_doc_id()
         || index_schema.is_doc_id_rowkey()
         || index_schema.is_vec_rowkey_vid_type()
         || index_schema.is_vec_vid_rowkey_type()
         || index_schema.is_fts_index_aux()
         || index_schema.is_multivalue_index_aux()
         || index_schema.is_vec_domain_index()
         || share::schema::is_fts_doc_word_aux(index_type)
         || share::schema::is_vec_index_id_type(index_type)
         || share::schema::is_vec_index_snapshot_data_type(index_type)
         || share::schema::is_built_in_vec_ivf_index(index_type)
         || share::schema::is_hybrid_vec_index_embedded_type(index_type);
}

int ObForkTableUtil::collect_tablet_ids_from_table(
    schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t table_id,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  tablet_ids.reset();

  if (OB_FAIL(schema_guard.get_table_schema( table_id, table_schema))) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(table_id));
  } else if (OB_FAIL(ObForkTableUtil::collect_tablet_ids_from_table(schema_guard, *table_schema, tablet_ids))) {
  }

  return ret;
}

int ObForkTableUtil::collect_tablet_ids_from_table(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  NamespaceForkPrototype::note_tablet_collection();
  tablet_ids.reset();

  {
    // 1. Get main table tablet IDs
    ObSEArray<ObTabletID, 4> main_tablet_ids;
    if (OB_FAIL(table_schema.get_tablet_ids(main_tablet_ids))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < main_tablet_ids.count(); ++i) {
        if (OB_FAIL(tablet_ids.push_back(main_tablet_ids.at(i)))) {
        }
      }
    }

    // 2. Get all index table tablet IDs (including domain index auxiliary tables)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(collect_index_tablet_ids(schema_guard, table_schema, tablet_ids))) {
    }

    // 3. Get LOB auxiliary table tablet IDs
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(collect_lob_aux_tablet_ids(schema_guard, table_schema, tablet_ids))) {
    }
  }
  return ret;
}

int ObForkTableUtil::collect_index_tablet_ids(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObAuxTableMetaInfo> simple_index_infos;

  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
  } else {
    hash::ObHashMap<uint64_t, ObTableSchema> complete_index_schema_map;
    if (OB_FAIL(ObForkTableUtil::collect_complete_domain_index_schemas(
            schema_guard,
            table_schema,
            complete_index_schema_map))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = nullptr;
      const uint64_t index_table_id = simple_index_infos.at(i).table_id_;

      if (OB_FAIL(schema_guard.get_table_schema( index_table_id, index_schema))) {
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema is null", K(ret), K(index_table_id));
      } else if (index_schema->is_in_recyclebin() ||
                 INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
        // Skip indexes not yet built or already recycled to keep tablet counts aligned.
        continue;
      } else if (ObForkTableUtil::is_domain_or_aux_index(*index_schema)) {
        ObTableSchema aux_index_schema;
        int tmp_ret = complete_index_schema_map.get_refactored(index_table_id, aux_index_schema);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          // skip incomplete domain/aux index
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("failed to get complete domain index schema", K(ret), K(index_table_id));
        } else {
          ObSEArray<ObTabletID, 4> index_tablet_ids;
          if (OB_FAIL(aux_index_schema.get_tablet_ids(index_tablet_ids))) {
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_ids.count(); ++j) {
              if (OB_FAIL(tablet_ids.push_back(index_tablet_ids.at(j)))) {
              }
            }
          }
        }
        continue;
      } else {
        ObSEArray<ObTabletID, 4> index_tablet_ids;
        if (OB_FAIL(index_schema->get_tablet_ids(index_tablet_ids))) {
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_ids.count(); ++j) {
            if (OB_FAIL(tablet_ids.push_back(index_tablet_ids.at(j)))) {
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObForkTableUtil::collect_lob_aux_tablet_ids(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t lob_meta_tid = table_schema.get_aux_lob_meta_tid();
  const uint64_t lob_piece_tid = table_schema.get_aux_lob_piece_tid();

  if (OB_INVALID_ID != lob_meta_tid) {
    const ObTableSchema *lob_meta_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema( lob_meta_tid, lob_meta_schema))) {
    } else if (OB_ISNULL(lob_meta_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("LOB meta table schema is null", K(ret), K(lob_meta_tid));
    } else {
      ObSEArray<ObTabletID, 4> lob_meta_tablet_ids;
      if (OB_FAIL(lob_meta_schema->get_tablet_ids(lob_meta_tablet_ids))) {
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < lob_meta_tablet_ids.count(); ++j) {
          if (OB_FAIL(tablet_ids.push_back(lob_meta_tablet_ids.at(j)))) {
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != lob_piece_tid) {
    const ObTableSchema *lob_piece_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema( lob_piece_tid, lob_piece_schema))) {
    } else if (OB_ISNULL(lob_piece_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("LOB piece table schema is null", K(ret), K(lob_piece_tid));
    } else {
      ObSEArray<ObTabletID, 4> lob_piece_tablet_ids;
      if (OB_FAIL(lob_piece_schema->get_tablet_ids(lob_piece_tablet_ids))) {
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < lob_piece_tablet_ids.count(); ++j) {
          if (OB_FAIL(tablet_ids.push_back(lob_piece_tablet_ids.at(j)))) {
          }
        }
      }
    }
  }
  return ret;
}

int ObForkTableUtil::collect_table_ids_from_table(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &table_schema,
    common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();

  // 1. main table
  const uint64_t main_table_id = table_schema.get_table_id();
  if (OB_SUCC(ret) && OB_FAIL(table_ids.push_back(main_table_id))) {
    LOG_WARN("fail to push back main table id", K(ret), K(main_table_id));
  }

  // 2. index table
  if (OB_SUCC(ret)) {
    ObArray<ObAuxTableMetaInfo> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    } else {
      hash::ObHashMap<uint64_t, ObTableSchema> complete_index_schema_map;
      if (OB_FAIL(ObForkTableUtil::collect_complete_domain_index_schemas(
              schema_guard,
              table_schema,
              complete_index_schema_map))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
          const ObTableSchema *index_schema = nullptr;
          const uint64_t index_table_id = simple_index_infos.at(i).table_id_;
          if (OB_INVALID_ID == index_table_id) {
            continue;
          } else if (OB_FAIL(schema_guard.get_table_schema( index_table_id, index_schema))) {
          } else if (OB_ISNULL(index_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("index table schema is null", K(ret), K(index_table_id));
          } else if (index_schema->is_in_recyclebin() ||
                     INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
            continue;
          } else if (ObForkTableUtil::is_domain_or_aux_index(*index_schema)) {
            ObTableSchema aux_index_schema;
            int tmp_ret = complete_index_schema_map.get_refactored(index_table_id, aux_index_schema);
            if (OB_HASH_NOT_EXIST == tmp_ret) {
              // skip incomplete domain/aux index
            } else if (OB_SUCCESS != tmp_ret) {
              ret = tmp_ret;
              LOG_WARN("failed to get complete domain index schema", K(ret), K(index_table_id));
            } else {
              if (OB_FAIL(table_ids.push_back(index_table_id))) {
              }
            }
            continue;
          } else if (OB_FAIL(table_ids.push_back(index_table_id))) {
          }
        }
      }
    }
  }

  // 3. lob aux tables
  if (OB_SUCC(ret)) {
    const uint64_t lob_meta_table_id = table_schema.get_aux_lob_meta_tid();
    const uint64_t lob_piece_table_id = table_schema.get_aux_lob_piece_tid();

    if (OB_INVALID_ID != lob_meta_table_id && OB_FAIL(table_ids.push_back(lob_meta_table_id))) {
      LOG_WARN("fail to push back LOB meta table id", K(ret), K(lob_meta_table_id));
    } else if (OB_INVALID_ID != lob_piece_table_id && OB_FAIL(table_ids.push_back(lob_piece_table_id))) {
      LOG_WARN("fail to push back LOB piece table id", K(ret), K(lob_piece_table_id));
    }
  }

  return ret;
}

int ObForkTableUtil::get_tablet_ids(
    const common::ObIArray<share::schema::ObTableSchema> &table_schemas,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  tablet_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
    const share::schema::ObTableSchema &schema = table_schemas.at(i);
    if (OB_FAIL(schema.get_tablet_ids(tablet_ids))) {
    }
  }
  return ret;
}

// Obtain snapshot for multiple tables at once to ensure consistency
int ObForkTableUtil::obtain_snapshot(
    common::ObMySQLTransaction &trans,
    schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<const ObTableSchema*> &data_table_schemas,
    int64_t &new_fetched_snapshot)
{
  int ret = OB_SUCCESS;
  rootserver::ObDDLService &ddl_service = ::oceanbase::share::server_service<::oceanbase::rootserver::ObLocalManagementService>()->get_ddl_service();
  new_fetched_snapshot = 0;
  ObSEArray<ObTabletID, 16> tablet_ids;
  SCN snapshot_scn;
  int64_t max_schema_version = 0;

  if (OB_UNLIKELY(data_table_schemas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table_schemas is empty", K(ret));
  } else if (OB_FAIL(ObDDLTaskUtil::calc_snapshot_with_gts(new_fetched_snapshot))) {
  } else if (new_fetched_snapshot <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the snapshot is not valid", K(ret), K(new_fetched_snapshot));
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(new_fetched_snapshot))) {
  } else {
    // Collect tablet ids from all tables
    for (int64_t i = 0; OB_SUCC(ret) && i < data_table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = data_table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("table schema is null", K(ret), K(i));
      } else if (OB_FAIL(ObForkTableUtil::collect_tablet_ids_from_table(
                     schema_guard, *table_schema, tablet_ids))) {
      } else {
        max_schema_version = std::max(max_schema_version, table_schema->get_schema_version());
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t retry_interval_us = 10 * 1000; // 10ms
    int64_t retry_count = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.get_snapshot_mgr().batch_acquire_snapshot(
              trans, SNAPSHOT_FOR_DDL, max_schema_version,
              snapshot_scn, nullptr, tablet_ids))) {
        if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT == ret) {
          const bool has_timeout = THIS_WORKER.is_timeout_ts_valid();
          const bool timeouted = has_timeout ? THIS_WORKER.is_timeout() : true;
          if (timeouted) {
            LOG_WARN("batch acquire snapshot timeout on nowait conflict",
                     KR(ret), K(retry_count), K(has_timeout), K(tablet_ids));
          } else {
            if (REACH_TIME_INTERVAL(1000 * 1000)) { // 1s
              LOG_INFO("retry batch acquire snapshot on nowait conflict",
                       KR(ret), K(retry_count));
            }
            // clear last error to keep transaction usable for next retry
            trans.reset_last_error();
            ret = OB_SUCCESS;
            ++retry_count;
            ob_usleep(retry_interval_us);
            continue;
          }
        } else {
          LOG_WARN("batch acquire snapshot failed", K(ret), K(tablet_ids));
        }
      }
      break;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("hold snapshot finished for multiple tables", K(snapshot_scn), K(max_schema_version),
               "table_cnt", data_table_schemas.count(), "tablet_cnt", tablet_ids.count());
    }
  }
  return ret;
}

// Release snapshot for multiple tables at once
int ObForkTableUtil::release_snapshot(
    rootserver::ObDDLTask* task,
    schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 16> tablet_ids;
  if (OB_ISNULL(task)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("invalid argument", K(ret));
  } else if (!task->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("args have not been inited", K(ret), K(task->get_task_type()));
  } else if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ids is empty", K(ret));
  } else {
    int64_t schema_version = task->get_src_schema_version();
    if (OB_FAIL(DDL_SIM(task->get_task_id(), DDL_TASK_RELEASE_SNAPSHOT_FAILED))) {
    } else {
      // Collect tablet ids from all tables
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        const uint64_t table_id = table_ids.at(i);
        if (OB_FAIL(ObForkTableUtil::collect_tablet_ids_from_table(
                schema_guard, table_id, tablet_ids))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task->batch_release_snapshot(snapshot_version, tablet_ids))) {
      }
    }
    task->add_event_info("release snapshot finish");
    LOG_INFO("release snapshot finished for multiple tables", K(snapshot_version), K(schema_version),
        "table_cnt", table_ids.count(), "tablet_cnt", tablet_ids.count(), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()));
  }
  return ret;
}
