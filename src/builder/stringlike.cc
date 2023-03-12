// Copyright 2022 nullptr

#include "builder/stringlike.h"

#include "builder/common.h"

namespace pgeon {

BinaryBuilder::BinaryBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto type = arrow::binary();
  if (info.typlen > -1) type = arrow::fixed_size_binary(info.typlen);

  auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::ArrayBuilder*>(arrow_builder_.get());
  binary_ptr_ = nullptr;
  fixed_size_binary_ptr_ = nullptr;

  if (info.typlen > -1) {
    fixed_size_binary_ptr_ =
        reinterpret_cast<arrow::FixedSizeBinaryBuilder*>(arrow_builder_.get());
  } else {
    binary_ptr_ = reinterpret_cast<arrow::BinaryBuilder*>(arrow_builder_.get());
  }
}

arrow::Status BinaryBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) return ptr_->AppendNull();

  auto value = sb.ReadBinary(len);
  return binary_ptr_ != nullptr ? binary_ptr_->Append(value, len)
                                : fixed_size_binary_ptr_->Append(value);
}

JsonbBuilder::JsonbBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::StringBuilder>();
  ptr_ = reinterpret_cast<arrow::StringBuilder*>(arrow_builder_.get());
}

arrow::Status JsonbBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) return ptr_->AppendNull();

  const char* buf = sb.ReadBinary(len);
  // First byte is format number
  return ptr_->Append(buf + 1, len - 1);
}

HstoreBuilder::HstoreBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(),
                         arrow::map(arrow::utf8(), arrow::utf8()), &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::MapBuilder*>(arrow_builder_.get());
  key_builder_ = reinterpret_cast<arrow::StringBuilder*>(ptr_->key_builder());
  item_builder_ = reinterpret_cast<arrow::StringBuilder*>(ptr_->item_builder());
}

arrow::Status HstoreBuilder::Append(StreamBuffer& sb) {
  APPEND_AND_RETURN_IF_EMPTY(sb, ptr_);
  ARROW_RETURN_NOT_OK(ptr_->Append());

  int32_t pcount = sb.ReadInt32();
  int32_t flen;
  for (int32_t i = 0; i < pcount; i++) {
    flen = sb.ReadInt32();
    ARROW_RETURN_NOT_OK(key_builder_->Append(sb.ReadBinary(flen), flen));

    flen = sb.ReadInt32();
    if (flen > -1) {
      ARROW_RETURN_NOT_OK(item_builder_->Append(sb.ReadBinary(flen), flen));
    } else {
      ARROW_RETURN_NOT_OK(item_builder_->AppendNull());
    }
  }
  return arrow::Status::OK();
}

}  // namespace pgeon
