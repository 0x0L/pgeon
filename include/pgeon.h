// Copyright 2022 nullptr

#pragma once

#include <arrow/api.h>

#include <memory>

namespace pgeon {

std::shared_ptr<arrow::Table> CopyQuery(const char* conninfo, const char* query);

}  // namespace pgeon
