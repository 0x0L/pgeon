// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <vector>

#include "pgeon/builder/base.h"

namespace pgeon {

class ListBuilder : public ArrayBuilder {
 private:
  std::shared_ptr<ArrayBuilder> value_builder_;
  arrow::ListBuilder* ptr_;

 public:
  ListBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

class StructBuilder : public ArrayBuilder {
 private:
  std::vector<std::shared_ptr<ArrayBuilder>> builders_;
  arrow::StructBuilder* ptr_;
  size_t ncolumns_;

 public:
  StructBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon
