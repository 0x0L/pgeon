// Copyright 2022 nullptr

#include <memory>

#include "builder/common.h"
#include "builder/stringlike.h"

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
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto value = sb.ReadBinary(len);
  auto status = binary_ptr_ != nullptr ? binary_ptr_->Append(value, len)
                                       : fixed_size_binary_ptr_->Append(value);
  return status;
}

JsonbBuilder::JsonbBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::StringBuilder>();
  ptr_ = reinterpret_cast<arrow::StringBuilder*>(arrow_builder_.get());
}

arrow::Status JsonbBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  const char* buf = sb.ReadBinary(len);
  return ptr_->Append(buf + 1, len - 1);
}

HstoreBuilder::HstoreBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(),
                         arrow::map(arrow::utf8(), arrow::utf8()), &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::MapBuilder*>(arrow_builder_.get());
  key_builder_ = (arrow::StringBuilder*)ptr_->key_builder();
  item_builder_ = (arrow::StringBuilder*)ptr_->item_builder();
}

arrow::Status HstoreBuilder::Append(StreamBuffer& sb) {
  int32_t len = sb.ReadInt32();
  if (len == -1) {
    return ptr_->AppendNull();
  }

  auto status = ptr_->Append();

  int32_t pcount = sb.ReadInt32();
  int32_t flen;
  for (int32_t i = 0; i < pcount; i++) {
    flen = sb.ReadInt32();

    auto value = sb.ReadBinary(flen);
    status = key_builder_->Append(value, flen);

    flen = sb.ReadInt32();
    if (flen > -1) {
      auto value = sb.ReadBinary(flen);
      status = item_builder_->Append(value, flen);
    } else {
      status = item_builder_->AppendNull();
    }
  }

  return status;
}

}  // namespace pgeon
