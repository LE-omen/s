// Native driver output crosses IPC; the existing Rust encoder owns MySQL wire bytes.
#include "observer/namespace_worker_result_prototype.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "rpc/obmysql/packet/ompk_ok.h"
extern "C" int64_t namespace_proto_response_deadline(int64_t);
int oceanbase::observer::ObMPBase::namespace_worker_request_prototype(
    ObSQLSessionInfo &session, const ObString &text, bool change_database)
{
  using namespace namespace_worker_prototype;
  ObSMConnection *conn = get_conn();
  if (!conn || !conn->namespace_worker_binding_) { return OB_NOT_SUPPORTED; }
  if (!session.is_valid() || session.is_zombie()) { return OB_ERR_SESSION_INTERRUPTED; }
  int64_t timeout = 0;
  int ret = session.get_query_timeout(timeout);
  if (ret) { return ret; }
  const int64_t received = get_receive_timestamp();
  if (timeout <= 0 || received > INT64_MAX - timeout) { return OB_TIMEOUT; }
  const int64_t deadline = received + timeout;
  THIS_WORKER.set_timeout_ts(deadline);
  if (ObTimeUtility::current_time() >= deadline) { return OB_TIMEOUT; }
  // Scope this only to the namespace query's blocking MySQL result delivery.
  // Existing final/error flushing runs after this guard restores the old value.
  struct ResponseDeadline {
    int64_t previous;
    ~ResponseDeadline() { namespace_proto_response_deadline(previous); }
  } response_deadline{namespace_proto_response_deadline(deadline)};
  // Metadata and cell bytes are consumed synchronously while their IPC frame
  // is alive. No schema/result cache or second SQL conversion in the gateway.
  uint64_t columns = 0;
  Frame terminal;
  bool finished = false;
  ret = namespace_worker_prototype::query(*conn->namespace_worker_binding_,
      text, change_database, [&](Frame &frame) -> int {
    int ret = OB_SUCCESS;
    if (finished) { return OB_INVALID_ARGUMENT; }
    if (frame.type() == 'H' && !columns) {
      const uint64_t count = frame.number();
      const bool include_header = frame.number() != 0;
      const uint8_t eof_count = frame.number();
      const uint16_t warnings = frame.number(), status = frame.number();
      if (count == 0 || count > 64) { return OB_INVALID_ARGUMENT; }
      ObSEArray<ObMySQLField, 8> fields;
      for (uint64_t i = 0; !ret && i < count; ++i) {
        ObMySQLField field; read_field(frame, field);
        ret = frame.ret ? frame.ret : fields.push_back(field);
      }
      if (!ret && !frame.consumed()) { ret = OB_INVALID_ARGUMENT; }
      if (!ret) { ret = packet_sender_.response_resultset_metadata(fields, include_header, eof_count, warnings, status); }
      if (!ret) { columns = count; }
    } else if (frame.type() == 'R' && columns) {
      const uint64_t protocol = frame.number();
      const bool packed = frame.number() != 0;
      const uint64_t count = frame.number();
      if (protocol != obmysql::TEXT || (packed ? count != 1 : count != columns)) { return OB_INVALID_ARGUMENT; }
      class ProtocolRow final : public obmysql::ObMySQLRow {
      public:
        std::vector<obmysql::ObMySQLCellValue> cells;
        ObString blob;
        explicit ProtocolRow(size_t count) : ObMySQLRow(obmysql::TEXT), cells(count) {}
        int64_t get_cells_cnt() const override { return cells.size(); }
        int build_cell_value(int64_t index, ObIAllocator &, obmysql::ObMySQLCellValue &value) const override {
          if (index < 0 || index >= static_cast<int64_t>(cells.size())) { return OB_INVALID_ARGUMENT; }
          value = cells[index]; return OB_SUCCESS;
        }
        int get_packed_row_blob(const char *&data, int64_t &size) const override {
          data = blob.ptr(); size = blob.length(); return OB_SUCCESS;
        }
      } row(count);
      row.set_packed(packed);
      if (packed) { row.blob = frame.string(); }
      else { for (auto &cell : row.cells) { read_cell(frame, cell); } }
      if (!frame.consumed()) { return OB_INVALID_ARGUMENT; }
      obmysql::OMPKRow packet(row);
      ret = response_packet(packet);
    } else if ((frame.type() == 'e' && columns) || (frame.type() == 'o' && !columns)) {
      terminal = std::move(frame); finished = true;
    } else { ret = OB_INVALID_ARGUMENT; }
    return ret;
  });
  // A native success packet is published only after D and the engine's check
  // that statement contexts are closed. An explicit transaction may stay open.
  if (!ret && !finished) { ret = OB_ERR_UNEXPECTED; }
  if (!ret && terminal.type() == 'o') {
    ObOKPParam param;
    param.affected_rows_ = terminal.number(); param.lii_ = terminal.number();
    param.warnings_count_ = terminal.number(); param.has_more_result_ = terminal.number() != 0;
    param.cursor_exist_ = terminal.number() != 0; param.send_last_row_ = terminal.number() != 0;
    param.has_pl_out_ = terminal.number() != 0; param.take_trace_id_to_client_ = terminal.number() != 0;
    const ObString bytes = terminal.string();
    if (!terminal.consumed()) { ret = OB_INVALID_ARGUMENT; }
    else {
      std::string message(bytes.ptr(), bytes.length());
      param.message_ = message.empty() ? nullptr : &message[0];
      ret = packet_sender_.send_ok_packet(session, param);
    }
  } else if (!ret) {
    obmysql::OMPKEOF eof;
    eof.set_warning_count(terminal.number());
    obmysql::ObServerStatusFlags flags; flags.flags_ = terminal.number(); eof.set_server_status(flags);
    if (!terminal.consumed()) { ret = OB_INVALID_ARGUMENT; }
    else { ret = response_packet(eof); }
  }
  return ret;
}
