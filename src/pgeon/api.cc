// Copyright 2022 nullptr

#include <libpq-fe.h>
#include <pgeon.h>

#include <iostream>

#include "pgeon/sql_interface.h"

namespace pgeon {

std::shared_ptr<arrow::Table> CopyTable(const char* conninfo, const char* query) {
  auto conn = PQconnectdb(conninfo);
  if (PQstatus(conn) != CONNECTION_OK)
    std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn) << std::endl;

  auto res = PQexec(conn, "BEGIN READ ONLY");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
              << std::endl;
  PQclear(res);

  auto builder = MakeTableBuilder(conn, query);
  CopyTable(conn, query, builder);
  auto table = builder->Flush();

  res = PQexec(conn, "END");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    std::cout << "unable to end transaction: " << PQresultErrorMessage(res) << std::endl;
  PQclear(res);
  PQfinish(conn);

  return table;
}

// void CopyBatch(const char* conninfo, const char* query, size_t batch_size,
//                void (*callback)(std::shared_ptr<arrow::RecordBatch>)) {}

}  // namespace pgeon
