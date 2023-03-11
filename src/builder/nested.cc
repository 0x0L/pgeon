// Copyright 2022 nullptr

#include <utility>

#include "builder/common.h"
#include "builder/nested.h"

namespace pgeon {

ListBuilder::ListBuilder(const SqlTypeInfo& info, const UserOptions&)
    : value_builder_(info.value_builder) {
  arrow_builder_ = std::make_unique<arrow::ListBuilder>(
      arrow::default_memory_pool(), std::move(value_builder_->arrow_builder_));
  ptr_ = reinterpret_cast<arrow::ListBuilder*>(arrow_builder_.get());
}

arrow::Status ListBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  int32_t ndim = sb.ReadInt32();
  int32_t hasnull = sb.ReadInt32();
  int32_t element_type = sb.ReadInt32();

  // Element will be flattened
  int32_t nitems = 1;
  for (int32_t i = 0; i < ndim; i++) {
    int32_t dim = sb.ReadInt32();
    int32_t lb = sb.ReadInt32();
    nitems *= dim;
  }

  ARROW_RETURN_NOT_OK(ptr_->Append());
  for (int32_t i = 0; i < nitems; i++) {
    ARROW_RETURN_NOT_OK(value_builder_->Append(sb));
  }
  return arrow::Status::OK();
}

StructBuilder::StructBuilder(const SqlTypeInfo& info, const UserOptions&) {
  FieldVector fields = info.field_builders;
  ncolumns_ = fields.size();

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders(ncolumns_);
  arrow::FieldVector fv(ncolumns_);
  for (size_t i = 0; i < ncolumns_; i++) {
    builders_.push_back(fields[i].second);
    builders[i] = std::move(fields[i].second->arrow_builder_);
    fv[i] = arrow::field(fields[i].first, builders[i]->type());
  }

  arrow_builder_ = std::make_unique<arrow::StructBuilder>(
      arrow::struct_(fv), arrow::default_memory_pool(), builders);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
}

arrow::Status StructBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  int32_t validcols = sb.ReadInt32();
  assert(validcols == ncolumns_);
  for (size_t i = 0; i < ncolumns_; i++) {
    int32_t column_type = sb.ReadInt32();
    ARROW_RETURN_NOT_OK(builders_[i]->Append(sb));
  }
  return arrow::Status::OK();
}

}  // namespace pgeon
