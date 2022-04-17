// Copyright 2022 nullptr

#include "pgeon/builder/network.h"
#include "pgeon/builder/common.h"

namespace pgeon {

InetBuilder::InetBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("family", arrow::uint8()),
      arrow::field("bits", arrow::uint8()),
      arrow::field("is_cidr", arrow::boolean()),
      arrow::field("ipaddr", arrow::binary()),
  });

  field_builders_ = {
      std::make_shared<arrow::UInt8Builder>(),
      std::make_shared<arrow::UInt8Builder>(),
      std::make_shared<arrow::BooleanBuilder>(),
      std::make_shared<arrow::BinaryBuilder>(),
  };

  arrow_builder_ = std::make_unique<arrow::StructBuilder>(
      type, arrow::default_memory_pool(), field_builders_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();
}

size_t InetBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  status = ((arrow::UInt8Builder*)field_builders_[0].get())->Append(*buf);
  buf += 1;

  status = ((arrow::UInt8Builder*)field_builders_[1].get())->Append(*buf);
  buf += 1;

  status = ((arrow::BooleanBuilder*)field_builders_[2].get())->Append(*buf != 0);
  buf += 1;

  uint8_t nb = *buf;
  buf += 1;

  status = ((arrow::BinaryBuilder*)field_builders_[3].get())->Append(buf, nb);

  return 4 + len;
}

}  // namespace pgeon
