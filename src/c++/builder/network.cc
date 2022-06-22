// Copyright 2022 nullptr

#include "builder/network.h"
#include "builder/common.h"

namespace pgeon {

InetBuilder::InetBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::struct_({
      arrow::field("family", arrow::uint8()),
      arrow::field("bits", arrow::uint8()),
      arrow::field("is_cidr", arrow::boolean()),
      arrow::field("ipaddr", arrow::binary()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = (arrow::StructBuilder*)arrow_builder_.get();

  family_builder_ = (arrow::UInt8Builder*)ptr_->child(0);
  bits_builder_ = (arrow::UInt8Builder*)ptr_->child(1);
  is_cidr_builder_ = (arrow::BooleanBuilder*)ptr_->child(2);
  ipaddr_builder_ = (arrow::BinaryBuilder*)ptr_->child(3);
}

size_t InetBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  status = family_builder_->Append(*buf);
  buf += 1;

  status = bits_builder_->Append(*buf);
  buf += 1;

  status = is_cidr_builder_->Append(*buf != 0);
  buf += 1;

  uint8_t nb = *buf;
  buf += 1;

  if (nb > -1) status = ipaddr_builder_->Append(buf, nb);

  return 4 + len;
}

}  // namespace pgeon
