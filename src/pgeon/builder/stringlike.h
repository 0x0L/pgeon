// Copyright 2022 nullptr

#pragma once

#include "pgeon/builder/base.h"

namespace pgeon {

class BinaryBuilder : public ArrayBuilder {
 private:
  arrow::ArrayBuilder* ptr_;
  arrow::BinaryBuilder* binary_ptr_;
  arrow::FixedSizeBinaryBuilder* fixed_size_binary_ptr_;

 public:
  BinaryBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

class JsonbBuilder : public ArrayBuilder {
 private:
  arrow::StringBuilder* ptr_;

 public:
  JsonbBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

class HstoreBuilder : public ArrayBuilder {
 private:
  arrow::MapBuilder* ptr_;
  arrow::StringBuilder* key_builder_;
  arrow::StringBuilder* item_builder_;

 public:
  HstoreBuilder(const SqlTypeInfo&, const UserOptions&);
  arrow::Status Append(StreamBuffer*);
};

}  // namespace pgeon
