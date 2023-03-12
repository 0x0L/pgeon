// Copyright 2022 nullptr

#pragma once

#include <libpq-fe.h>

#include "table_builder.h"

namespace pgeon {

arrow::Result<std::shared_ptr<TableBuilder>> MakeTableBuilder(PGconn* conn,
                                                              const char* query,
                                                              const UserOptions& options);

arrow::Status CopyQuery(PGconn* conn, const char* query,
                        std::shared_ptr<TableBuilder> builder);

}  // namespace pgeon
