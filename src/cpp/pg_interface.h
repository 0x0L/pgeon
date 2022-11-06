// Copyright 2022 nullptr

#pragma once

#include <libpq-fe.h>

#include <memory>

#include "table_builder.h"

namespace pgeon {

std::shared_ptr<ArrayBuilder> MakeColumnBuilder(PGconn* conn, Oid oid, int mod);

std::shared_ptr<TableBuilder> MakeTableBuilder(PGconn* conn, const char* query);

void CopyQuery(PGconn* conn, const char* query, std::shared_ptr<TableBuilder> builder);

}  // namespace pgeon
