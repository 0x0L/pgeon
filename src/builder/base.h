// Copyright 2022 nullptr

#pragma once

#include <pgeon.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "util/streambuffer.h"

namespace pgeon {

class ArrayBuilder {
 public:
  std::unique_ptr<arrow::ArrayBuilder> arrow_builder_;
  virtual ~ArrayBuilder() = default;
  virtual arrow::Status Append(StreamBuffer& sb) = 0;

  inline std::shared_ptr<arrow::DataType> type() { return arrow_builder_->type(); }

  inline arrow::Result<std::shared_ptr<arrow::Array>> Flush() {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(arrow_builder_->Finish(&array));
    return array;
  }
};

using Field = std::pair<std::string, std::shared_ptr<ArrayBuilder>>;
using FieldVector = std::vector<Field>;

struct SqlTypeInfo {
  std::string typreceive;
  int typmod = -1;
  int typlen = -1;

  // for ListBuilder
  std::shared_ptr<ArrayBuilder> value_builder;

  // for StructBuilder
  FieldVector field_builders;
};

}  // namespace pgeon
