// Copyright 2022 nullptr

#pragma once

#include <arrow/api.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace pgeon {

class ArrayBuilder {
 public:
  std::unique_ptr<arrow::ArrayBuilder> arrow_builder_;
  virtual ~ArrayBuilder() = default;

  // General format of a field is
  // int32 length
  // char[length] content if length > -1
  virtual size_t Append(const char* buffer) = 0;

  std::shared_ptr<arrow::DataType> type() { return arrow_builder_->type(); }

  std::shared_ptr<arrow::Array> Flush() {
    std::shared_ptr<arrow::Array> array;
    auto status = arrow_builder_->Finish(&array);
    return array;
  }
};

using Field = std::pair<std::string, std::shared_ptr<ArrayBuilder>>;
using FieldVector = std::vector<Field>;

struct SqlTypeInfo {
  std::string typreceive;
  int typmod = -1;
  int typlen = -1;

  // for ArrayBuilder
  std::shared_ptr<ArrayBuilder> value_builder;

  // for StructBuilder
  FieldVector field_builders;
};

struct UserOptions {
  bool string_as_dictionaries = false;
  int default_numeric_precision = 22;  // TODO(xav) max precision of 128 decimal ?
  int default_numeric_scale = 6;
  int monetary_fractional_precision = 2;  // TODO(xav) lc_monetary

  struct UserOptions static Defaults() {
    return UserOptions();
  }
};

}  // namespace pgeon
