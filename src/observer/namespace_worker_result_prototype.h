// V16 transports the existing MySQL semantic values. SQL conversion stays in
// the native driver; the gateway's existing Rust encoder owns wire encoding.
#ifndef SEEKDB_NAMESPACE_WORKER_RESULT_PROTOTYPE_H_
#define SEEKDB_NAMESPACE_WORKER_RESULT_PROTOTYPE_H_
#include "observer/namespace_worker_protocol_prototype.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "rpc/obmysql/ob_mysql_row.h"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
inline void append_field(Frame &frame, const obmysql::ObMySQLField &field) {
  frame.string(field.dname_); frame.string(field.tname_); frame.string(field.org_tname_);
  frame.string(field.cname_); frame.string(field.org_cname_);
  frame.string(field.type_owner_); frame.string(field.type_name_);
  frame.number(field.type_); frame.number(field.flags_); frame.number(field.default_value_);
  frame.number(field.charsetnr_); frame.number(field.length_); frame.number(field.inout_mode_);
  frame.number(field.accuracy_.get_precision()); frame.number(field.accuracy_.get_scale());
}
inline void read_field(Frame &frame, obmysql::ObMySQLField &field) {
  field.dname_ = frame.string(); field.tname_ = frame.string(); field.org_tname_ = frame.string();
  field.cname_ = frame.string(); field.org_cname_ = frame.string();
  field.type_owner_ = frame.string(); field.type_name_ = frame.string();
  field.type_ = static_cast<obmysql::EMySQLFieldType>(frame.number()); field.flags_ = frame.number();
  field.default_value_ = static_cast<obmysql::EMySQLFieldType>(frame.number());
  field.charsetnr_ = frame.number(); field.length_ = frame.number(); field.inout_mode_ = frame.number();
  field.accuracy_.set_precision(frame.number()); field.accuracy_.set_scale(frame.number());
}
inline void append_cell(Frame &frame, const obmysql::ObMySQLCellValue &cell) {
  frame.number(static_cast<unsigned>(cell.kind_)); frame.number(cell.value_);
  frame.number(cell.days_); frame.number(cell.microseconds_); frame.number(cell.bit_len_);
  frame.number(cell.year_); frame.number(cell.month_); frame.number(cell.day_);
  frame.number(cell.hour_); frame.number(cell.minute_); frame.number(cell.second_); frame.number(cell.is_negative_);
  const int64_t size = cell.get_bytes_len();
  if (size < 0 || size > MAX_FRAME || (size && !cell.get_bytes())) { frame.ret = common::OB_INVALID_ARGUMENT; }
  else { frame.string(common::ObString(static_cast<int32_t>(size), cell.get_bytes())); }
}
inline void read_cell(Frame &frame, obmysql::ObMySQLCellValue &cell) {
  const uint64_t kind = frame.number();
  if (kind > static_cast<unsigned>(obmysql::ObMySQLCellValueKind::LEGACY_LENENC_NULL)) { frame.ret = common::OB_INVALID_ARGUMENT; }
  cell.value_ = frame.number(); cell.days_ = frame.number(); cell.microseconds_ = frame.number();
  cell.bit_len_ = frame.number(); cell.year_ = frame.number(); cell.month_ = frame.number(); cell.day_ = frame.number();
  cell.hour_ = frame.number(); cell.minute_ = frame.number(); cell.second_ = frame.number(); cell.is_negative_ = frame.number();
  const common::ObString bytes = frame.string();
  cell.set_borrowed_bytes(static_cast<obmysql::ObMySQLCellValueKind>(kind), bytes.ptr(), bytes.length());
}
} } }
#endif
