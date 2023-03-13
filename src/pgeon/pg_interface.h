// Copyright 2022 nullptr

#pragma once

#include <libpq-fe.h>

#include <memory>

#include "pgeon/table_builder.h"

namespace pgeon {

arrow::Result<std::shared_ptr<TableBuilder>> MakeTableBuilder(PGconn*, const char*,
                                                              const UserOptions&);

arrow::Status CopyQuery(PGconn*, const char*, std::shared_ptr<TableBuilder>);

}  // namespace pgeon
