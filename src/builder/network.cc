// Copyright 2022 nullptr

#include "builder/network.h"

#include "builder/common.h"

namespace pgeon {

InetBuilder::InetBuilder(const SqlTypeInfo& info, const UserOptions&) {
  static const auto& type = arrow::struct_({
      arrow::field("family", arrow::uint8()),
      arrow::field("bits", arrow::uint8()),
      arrow::field("is_cidr", arrow::boolean()),
      arrow::field("ipaddr", arrow::binary()),
  });

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);
  ptr_ = reinterpret_cast<arrow::StructBuilder*>(arrow_builder_.get());
  family_builder_ = reinterpret_cast<arrow::UInt8Builder*>(ptr_->child(0));
  bits_builder_ = reinterpret_cast<arrow::UInt8Builder*>(ptr_->child(1));
  is_cidr_builder_ = reinterpret_cast<arrow::BooleanBuilder*>(ptr_->child(2));
  ipaddr_builder_ = reinterpret_cast<arrow::BinaryBuilder*>(ptr_->child(3));
}

arrow::Status InetBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());
  ARROW_RETURN_NOT_OK(family_builder_->Append(sb.ReadUInt8()));
  ARROW_RETURN_NOT_OK(bits_builder_->Append(sb.ReadUInt8()));
  ARROW_RETURN_NOT_OK(is_cidr_builder_->Append(sb.ReadUInt8() != 0));
  int8_t size = sb.ReadUInt8();
  if (size > -1) {
    ARROW_RETURN_NOT_OK(ipaddr_builder_->Append(sb.ReadBinary(size), size));
  }
  return arrow::Status::OK();
}

}  // namespace pgeon
