// Copyright 2022 nullptr

#include "pgeon/builder/geometric.h"
#include "pgeon/builder/common.h"

namespace pgeon {

BoxBuilder::BoxBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("high.x", arrow::float64()),
      arrow::field("high.y", arrow::float64()),
      arrow::field("low.x", arrow::float64()),
      arrow::field("low.y", arrow::float64()),
  });

  // std::unique_ptr<arrow::ArrayBuilder> out;
  // auto ss = arrow::MakeBuilder(
  //     arrow::default_memory_pool(), type->field(i)->type(), &out);

  field_builders_ = {
      std::make_shared<arrow::DoubleBuilder>(),
      std::make_shared<arrow::DoubleBuilder>(),
      std::make_shared<arrow::DoubleBuilder>(),
      std::make_shared<arrow::DoubleBuilder>(),
  };

  arrow_builder_ = std::make_unique<arrow::StructBuilder>(
      type, arrow::default_memory_pool(), field_builders_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t BoxBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();
  for (size_t i = 0; i < field_builders_.size(); i++) {
    double value = unpack_double(buf);
    buf += 8;

    auto status = ((arrow::DoubleBuilder*)field_builders_[i].get())->Append(value);
  }

  return 4 + len;
}

}  // namespace pgeon
