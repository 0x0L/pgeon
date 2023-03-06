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

arrow::Status InetBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  status = family_builder_->Append(sb.ReadUInt8());
  status = bits_builder_->Append(sb.ReadUInt8());
  status = is_cidr_builder_->Append(sb.ReadUInt8() != 0);
  int8_t nb = sb.ReadUInt8();

  if (nb > -1) {
    auto value = sb.ReadBinary(nb);
    status = ipaddr_builder_->Append(value, nb);
  }

  return status;
}

}  // namespace pgeon
