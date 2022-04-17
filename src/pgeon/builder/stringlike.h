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
  arrow::MapBuilder* ptr_;
  arrow::StringBuilder* key_builder_;
  arrow::StringBuilder* item_builder_;

 public:
  HstoreBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon
