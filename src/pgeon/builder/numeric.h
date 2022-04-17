// Copyright 2022 nullptr

#pragma once

#include "pgeon/builder/base.h"

namespace pgeon {

class NumericBuilder : public ArrayBuilder {
 private:
  arrow::Decimal128Builder* ptr_;
  int precision_;
  int scale_;

 public:
  NumericBuilder(const SqlTypeInfo& info, const UserOptions&);
  size_t Append(const char* buf);
};

}  // namespace pgeon
