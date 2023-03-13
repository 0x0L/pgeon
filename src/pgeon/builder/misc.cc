// Copyright 2022 nullptr

#include "pgeon/builder/misc.h"

#include <memory>

#include "pgeon/builder/common.h"

namespace pgeon {

NullBuilder::NullBuilder(const SqlTypeInfo&, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::NullBuilder>();
  ptr_ = reinterpret_cast<arrow::NullBuilder*>(arrow_builder_.get());
}

arrow::Status NullBuilder::Append(StreamBuffer* sb) {
  int32_t len = sb->ReadInt32();
  return ptr_->AppendNull();
}

TidBuilder::TidBuilder(const SqlTypeInfo&, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("block", arrow::int32()),
      arrow::field("offset", arrow::int16()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
  block_builder_ = reinterpret_cast<arrow::Int32Builder*>(ptr_->child(0));
  offset_builder_ = reinterpret_cast<arrow::Int16Builder*>(ptr_->child(1));
}

arrow::Status TidBuilder::Append(StreamBuffer* sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  ARROW_RETURN_NOT_OK(block_builder_->Append(sb->ReadInt32()));
  return offset_builder_->Append(sb->ReadInt16());
}

PgSnapshotBuilder::PgSnapshotBuilder(const SqlTypeInfo&, const UserOptions&) {
  static const auto& type = arrow::struct_(
      {arrow::field("xmin", arrow::int64()), arrow::field("xmax", arrow::int64()),
       arrow::field("xip", arrow::list(arrow::int64()))});

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
  xmin_builder_ = reinterpret_cast<arrow::Int64Builder*>(ptr_->child(0));
  xmax_builder_ = reinterpret_cast<arrow::Int64Builder*>(ptr_->child(1));
  xip_builder_ = reinterpret_cast<arrow::ListBuilder*>(ptr_->child(2));
  value_builder_ = reinterpret_cast<arrow::Int64Builder*>(xip_builder_->value_builder());
}

arrow::Status PgSnapshotBuilder::Append(StreamBuffer* sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  ARROW_RETURN_NOT_OK(xip_builder_->Append());

  int32_t nxip = sb->ReadInt32();
  ARROW_RETURN_NOT_OK(xmin_builder_->Append(sb->ReadInt64()));
  ARROW_RETURN_NOT_OK(xmax_builder_->Append(sb->ReadInt64()));
  for (int32_t i = 0; i < nxip; i++) {
    ARROW_RETURN_NOT_OK(value_builder_->Append(sb->ReadInt64()));
  }
  return arrow::Status::OK();
}

}  // namespace pgeon
