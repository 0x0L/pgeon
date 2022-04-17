// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <vector>

#include "pgeon/builder/base.h"

namespace pgeon {

class BoxBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders_;

 public:
  BoxBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon
