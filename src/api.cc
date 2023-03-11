// Copyright 2022 nullptr

#include <libpq-fe.h>
#include <pgeon.h>

#include <iostream>

#include "pg_interface.h"

namespace pgeon {

UserOptions UserOptions::Defaults() { return UserOptions(); }

arrow::Status UserOptions::Validate() const {
  if (default_numeric_precision < 1) {
    // Min is 1 because some tests use really small block sizes
    return arrow::Status::Invalid(
        "UserOptions: default_numeric_precision must be at least 1",
        std::to_string(default_numeric_precision));
  }
  // TODO
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> CopyQuery(const char* conninfo,
                                                       const char* query,
                                                       const UserOptions& options) {
  auto status = arrow::Status::OK();

  auto conn = PQconnectdb(conninfo);
  if (PQstatus(conn) != CONNECTION_OK) {
    status = arrow::Status::IOError("[libpq] ", PQerrorMessage(conn));
    PQfinish(conn);
    return status;
  }

  auto res = PQexec(conn, "BEGIN READ ONLY");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));

  PQclear(res);
  if (!status.ok()) {
    PQfinish(conn);
    return status;
  }

  auto builder = MakeTableBuilder(conn, query, options);
  if (!builder.ok()) {
    PQfinish(conn);
    return builder.status();
  }

  status = CopyQuery(conn, query, *builder);
  if (!status.ok()) {
    PQfinish(conn);
    return status;
  }

  auto table = (*builder)->Flush();
  if (!table.ok()) {
    PQfinish(conn);
    return table.status();
  }

  res = PQexec(conn, "END");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));

  PQclear(res);
  PQfinish(conn);
  return status.ok() ? table : status;
}

// void CopyBatch(const char* conninfo, const char* query, size_t batch_size,
//                void (*callback)(std::shared_ptr<arrow::RecordBatch>)) {}

}  // namespace pgeon
