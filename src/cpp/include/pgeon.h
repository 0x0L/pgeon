// Copyright 2022 nullptr

#pragma once

#include <arrow/api.h>
#include <memory>

namespace pgeon {

struct UserOptions {
  bool string_as_dictionaries = false;
  int default_numeric_precision = 22;  // TODO(xav) max precision of 128 decimal ?
  int default_numeric_scale = 6;
  int monetary_fractional_precision = 2;  // TODO(xav) lc_monetary

  struct UserOptions static Defaults() {
    return UserOptions();
  }
};

std::shared_ptr<arrow::Table> CopyQuery(
    const char* conninfo, const char* query,
    const UserOptions& options = UserOptions::Defaults());

}  // namespace pgeon
