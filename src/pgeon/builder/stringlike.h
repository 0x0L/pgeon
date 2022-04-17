// Copyright 2022 nullptr

#pragma once

#include <memory>

#include "pgeon/builder/base.h"

namespace pgeon {

class JsonbBuilder : public ArrayBuilder {
 private:
  arrow::StringBuilder* ptr_;

 public:
  JsonbBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class HstoreBuilder : public ArrayBuilder {
 private:
    std::shared_ptr<arrow::StringBuilder> key_builder_;
    std::shared_ptr<arrow::StringBuilder> item_builder_;
    arrow::MapBuilder* ptr_;

 public:
  HstoreBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon
