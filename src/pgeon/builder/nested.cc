// Copyright 2022 nullptr

#include <utility>

#include "pgeon/builder/common.h"
#include "pgeon/builder/nested.h"

namespace pgeon {

ListBuilder::ListBuilder(const SqlTypeInfo& info, const UserOptions&)
    : value_builder_(info.value_builder) {
  arrow_builder_ = std::make_unique<arrow::ListBuilder>(
      arrow::default_memory_pool(), std::move(value_builder_->arrow_builder_));
  ptr_ = (arrow::ListBuilder*)arrow_builder_.get();
}

size_t ListBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  int32_t ndim = unpack_int32(buf);
  buf += 4;
  // int32_t hasnull = unpack_int32(buf);
  buf += 4;
  // int32_t element_type = unpack_int32(buf);
  buf += 4;

  // Element will be flattened
  int32_t nitems = 1;
  for (size_t i = 0; i < ndim; i++) {
    int32_t dim = unpack_int32(buf);
    buf += 4;
    // int32_t lb = unpack_int32(buf);
    buf += 4;
    nitems *= dim;
  }

  auto status = ptr_->Append();
  for (size_t i = 0; i < nitems; i++) {
    buf += value_builder_->Append(buf);
  }

  return 4 + len;
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

  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t StructBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  int32_t validcols = unpack_int32(buf);
  buf += 4;

  assert(validcols == ncolumns_);

  for (size_t i = 0; i < ncolumns_; i++) {
    // int32_t column_type = unpack_int32(buf);
    buf += 4;

    buf += builders_[i]->Append(buf);
  }

  return 4 + len;
}

}  // namespace pgeon
