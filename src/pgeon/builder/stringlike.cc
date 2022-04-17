// Copyright 2022 nullptr

#include <memory>

#include "pgeon/builder/common.h"
#include "pgeon/builder/stringlike.h"

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

size_t BinaryBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = binary_ptr_ != nullptr ? binary_ptr_->Append(buf, len)
                                       : fixed_size_binary_ptr_->Append(buf);
  return 4 + len;
}

JsonbBuilder::JsonbBuilder(const SqlTypeInfo& info, const UserOptions&) {
  arrow_builder_ = std::make_unique<arrow::StringBuilder>();
  ptr_ = reinterpret_cast<arrow::StringBuilder*>(arrow_builder_.get());
}

size_t JsonbBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append(buf + 1, len);
  return 4 + len;
}

HstoreBuilder::HstoreBuilder(const SqlTypeInfo& info, const UserOptions&) {
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(),
                         arrow::map(arrow::utf8(), arrow::utf8()), &arrow_builder_);

  ptr_ = reinterpret_cast<arrow::MapBuilder*>(arrow_builder_.get());
  key_builder_ = (arrow::StringBuilder*)ptr_->key_builder();
  item_builder_ = (arrow::StringBuilder*)ptr_->item_builder();
}

size_t HstoreBuilder::Append(const char* buf) {
  int32_t len = unpack_int32(buf);
  buf += 4;

  if (len == -1) {
    auto status = ptr_->AppendNull();
    return 4;
  }

  auto status = ptr_->Append();

  int32_t pcount = unpack_int32(buf);
  buf += 4;

  int32_t flen;
  for (size_t i = 0; i < pcount; i++) {
    flen = unpack_int32(buf);
    buf += 4;

    key_builder_->Append(buf, flen);
    buf += flen;

    flen = unpack_int32(buf);
    buf += 4;

    if (flen > -1) {
      item_builder_->Append(buf, flen);
      buf += flen;
    } else {
      item_builder_->AppendNull();
    }
  }

  return 4 + len;
}

}  // namespace pgeon
