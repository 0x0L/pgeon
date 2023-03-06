// Copyright 2022 nullptr

#include "builder/misc.h"
#include "builder/common.h"

namespace pgeon {

NullBuilder::NullBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::NullBuilder>();
  ptr_ = reinterpret_cast<arrow::NullBuilder*>(arrow_builder_.get());
}

arrow::Status NullBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  return ptr_->AppendNull();
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

arrow::Status TidBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  int32_t block = sb.ReadInt32();
  int32_t offset = sb.ReadInt16();

  status = block_builder_->Append(block);
  status = offset_builder_->Append(offset);

  return status;
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

arrow::Status PgSnapshotBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();
  status = xip_builder_->Append();

  int32_t nxip = sb.ReadInt32();
  int64_t xmin = sb.ReadInt64();
  int64_t xmax = sb.ReadInt64();

  status = xmin_builder_->Append(xmin);
  status = xmax_builder_->Append(xmax);

  for (int32_t i = 0; i < nxip; i++) {
    int64_t xip = sb.ReadInt64();
    status = value_builder_->Append(xip);
  }

  return status;
}

}  // namespace pgeon
