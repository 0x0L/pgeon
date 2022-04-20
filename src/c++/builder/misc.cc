// Copyright 2022 nullptr

#include "builder/misc.h"
#include "builder/common.h"

namespace pgeon {

NullBuilder::NullBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::NullBuilder>();
  ptr_ = reinterpret_cast<arrow::NullBuilder*>(arrow_builder_.get());
}

size_t NullBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  auto status = ptr_->AppendNull();
  return 4 + len;
}

TidBuilder::TidBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("block", arrow::int32()),
      arrow::field("offset", arrow::int16()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();

  block_builder_ = (arrow::Int32Builder*)ptr_->child(0);
  offset_builder_ = (arrow::Int16Builder*)ptr_->child(1);
}

size_t TidBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  int32_t block = unpack_int32(buf);
  int32_t offset = unpack_int16(buf + 4);
  buf += 6;

  block_builder_->Append(block);
  offset_builder_->Append(offset);

  return 4 + len;
}

PgSnapshotBuilder::PgSnapshotBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({arrow::field("xmin", arrow::int64()),
                              arrow::field("xmax", arrow::int64()),
                              arrow::field("xip", arrow::list(arrow::int64()))});

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();

  xmin_builder_ = (arrow::Int64Builder*)ptr_->child(0);
  xmax_builder_ = (arrow::Int64Builder*)ptr_->child(1);
  xip_builder_ = (arrow::ListBuilder*)ptr_->child(2);
  value_builder_ = (arrow::Int64Builder*)xip_builder_->value_builder();
}

size_t PgSnapshotBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();
  status = xip_builder_->Append();

  int32_t nxip = unpack_int32(buf);
  buf += 4;

  int64_t xmin = unpack_int64(buf);
  int64_t xmax = unpack_int64(buf + 8);
  buf += 16;

  xmin_builder_->Append(xmin);
  xmax_builder_->Append(xmax);

  for (size_t i = 0; i < nxip; i++) {
    int64_t xip = unpack_int64(buf);
    buf += 8;
    value_builder_->Append(xip);
  }

  return 4 + len;
}

}  // namespace pgeon
