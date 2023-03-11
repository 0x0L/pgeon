// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <vector>

#include "builder/base.h"

namespace pgeon {

class InetBuilder : public ArrayBuilder {
 private:
  arrow::StructBuilder* ptr_;
  arrow::UInt8Builder* family_builder_;
  arrow::UInt8Builder* bits_builder_;
  arrow::BooleanBuilder* is_cidr_builder_;
  arrow::BinaryBuilder* ipaddr_builder_;

 public:
  InetBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer&);
};

}  // namespace pgeon
