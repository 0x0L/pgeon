// Copyright 2022 nullptr

#pragma once

#include "builder/base.h"

namespace pgeon {

class TableBuilder {
 private:
  FieldVector fields_;
  std::vector<ArrayBuilder*> builders_;
  std::shared_ptr<arrow::Schema> schema_;

 public:
  explicit TableBuilder(const FieldVector&);

  arrow::Status Append(StreamBuffer&);

  arrow::Result<std::shared_ptr<arrow::Table>> Flush();
};

}  // namespace pgeon
