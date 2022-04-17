// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <vector>

#include "builder/base.h"

namespace pgeon {

class TableBuilder {
 private:
  FieldVector fields_;
  std::vector<ArrayBuilder*> builders_;
  std::shared_ptr<arrow::Schema> schema_;
  // std::vector<int32_t> fields_offsets_;  // omp tentative

 public:
  explicit TableBuilder(const FieldVector& fields);

  int32_t Append(const char* cursor);

  std::shared_ptr<arrow::Table> Flush();
};

}  // namespace pgeon
