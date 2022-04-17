// Copyright 2022 nullptr

#include <memory>

#include "pgeon/builder/common.h"
#include "pgeon/builder/stringlike.h"

namespace pgeon {

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
  key_builder_ = std::make_shared<arrow::StringBuilder>();
  item_builder_ = std::make_shared<arrow::StringBuilder>();

  arrow_builder_ = std::make_unique<arrow::MapBuilder>(arrow::default_memory_pool(),
                                                       key_builder_, item_builder_);
  ptr_ = reinterpret_cast<arrow::MapBuilder*>(arrow_builder_.get());
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
