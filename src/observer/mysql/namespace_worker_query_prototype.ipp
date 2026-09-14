// Throwaway V10 result transport; keep MySQL encoding in the existing gateway.
#include "observer/namespace_worker_protocol_prototype.h"
#include "data_plane/transaction/ob_i_transaction_service.h"
#include "observer/mysql/obsm_row.h"
#include "rpc/obmysql/packet/ompk_row.h"
int oceanbase::observer::ObMPQuery::namespace_worker_query_prototype(ObSQLSessionInfo &session)
{
  using namespace namespace_worker_prototype;
  ObSMConnection *conn = get_conn();
  if (!conn || session.get_user_id() != OB_SYS_USER_ID || session.get_in_transaction()) { return OB_NOT_SUPPORTED; }
  if (!session.is_valid() || session.is_zombie()) { return OB_ERR_SESSION_INTERRUPTED; }
  auto *txs = data_plane::query_transaction_service();
  SCN snapshot;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30L * 1000 * 1000);
  int ret = txs ? txs->get_read_snapshot_version(THIS_WORKER.get_timeout_ts(), snapshot) : OB_NOT_INIT;
  if (ret) { return ret; }
  session.set_reserved_snapshot_version(snapshot);
  struct ResetSnapshot { ObSQLSessionInfo &s; ~ResetSnapshot() { s.reset_reserved_snapshot_version(); } } reset{session};
  Frame header;
  ObSEArray<ObField, 8> fields;
  bool got_header = false;
  ret = namespace_worker_prototype::query(conn->namespace_worker_id_, conn->namespace_worker_generation_,
      session.get_database_id(), snapshot.get_val_for_tx(), sql_, [&](Frame &frame) -> int {
    int ret = OB_SUCCESS;
    if (frame.type() == 'H' && !got_header) {
      header = std::move(frame);
      const uint64_t count = header.number();
      if (count == 0 || count > 64) { return OB_INVALID_ARGUMENT; }
      ObSEArray<ObMySQLField, 8> mysql_fields;
      for (uint64_t i = 0; !ret && i < count; ++i) {
        ObField field; header.read(field);
        ObMySQLField mysql;
        if (header.ret) { ret = header.ret; }
        else if ((ret = fields.push_back(field))) {}
        else if ((ret = ObMySQLResultSet::to_mysql_field(field, mysql))) {}
        else { ret = mysql_fields.push_back(mysql); }
      }
      if (!ret && !header.consumed()) { ret = OB_INVALID_ARGUMENT; }
      if (!ret) { ret = packet_sender_.response_resultset_metadata(mysql_fields, true, 0xfe, 0, 2); }
      got_header = !ret;
    } else if (frame.type() == 'R' && got_header) {
      const uint64_t count = frame.number();
      if (count != static_cast<uint64_t>(fields.count())) { return OB_INVALID_ARGUMENT; }
      std::vector<ObObj> cells(count);
      for (auto &cell : cells) {
        frame.read(cell);
        if (!cell.is_null() && !ob_is_numeric_type(cell.get_type())) { return OB_NOT_SUPPORTED; }
      }
      if (!frame.consumed()) { return OB_INVALID_ARGUMENT; }
      ObNewRow row; row.cells_ = cells.data(); row.count_ = count;
      const ObDataTypeCastParams casts = ObBasicSessionInfo::create_dtc_params(&session);
      ObSMRow text_row(obmysql::TEXT, row, casts, session, &fields, nullptr);
      obmysql::OMPKRow packet(text_row);
      ret = response_packet(packet);
    } else { ret = OB_INVALID_ARGUMENT; }
    return ret;
  });
  if (!ret && !got_header) { ret = OB_ERR_UNEXPECTED; }
  if (!ret) {
    obmysql::OMPKEOF eof;
    obmysql::ObServerStatusFlags flags; flags.flags_ = 2; eof.set_server_status(flags);
    ret = response_packet(eof);
  }
  return ret;
}
