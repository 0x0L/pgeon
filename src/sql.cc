// Copyright 2022 nullptr

#include <pgeon.h>

#include <stdio.h>
#include <iostream>

namespace pgeon {
using ColumnVector = std::vector<std::tuple<std::string, Oid, int>>;

ColumnVector ColumnTypesForQuery(PGconn* conn, const char* query) {
  const auto descr_query = "SELECT * FROM (" + std::string(query) + ") AS FOO LIMIT 0;";
  PGresult* res = PQexec(conn, descr_query.c_str());

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cout << PQresultErrorMessage(res) << std::endl;
  }

  int n = PQnfields(res);
  ColumnVector fields(n);

  for (size_t i = 0; i < n; i++) {
    const char* name = PQfname(res, i);
    Oid oid = PQftype(res, i);
    int mod = PQfmod(res, i);
    fields[i] = {name, oid, mod};
  }

  PQclear(res);
  return fields;
}

ColumnVector RecordTypeInfo(PGconn* conn, Oid oid) {
  char query[4096];
  snprintf(query, sizeof(query), R"(
SELECT
    attnum, attname, atttypid, atttypmod, atttyplen
FROM
    pg_catalog.pg_attribute a,
    pg_catalog.pg_type t,
    pg_catalog.pg_namespace n
WHERE
    t.typnamespace = n.oid
    AND a.atttypid = t.oid
    AND a.attrelid = %u
;)",
           oid);

  auto res = PQexec(conn, query);

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cout << PQresultErrorMessage(res) << std::endl;
  }

  int nfields = PQntuples(res);
  std::vector<std::tuple<std::string, Oid, int>> fields(nfields);

  for (size_t i = 0; i < nfields; i++) {
    int attnum = atoi(PQgetvalue(res, i, 0));
    const char* attname = PQgetvalue(res, i, 1);
    Oid atttypid = atooid(PQgetvalue(res, i, 2));
    int atttypmod = atoi(PQgetvalue(res, i, 3));

    fields[attnum - 1] = {attname, atttypid, atttypmod};
  }

  PQclear(res);
  return fields;
}

extern std::map<std::string, std::shared_ptr<ColumnBuilder> (*)(const SqlTypeInfo&,
                                                                const UserOptions&)>
    gDecoderFactory;

// attname, attnum, atttypid, atttypmod, attlen,
// attbyval, attalign, typtype, typrelid, typelem,
// nspname, typname

std::shared_ptr<ColumnBuilder> MakeColumnBuilder(PGconn* conn, Oid oid, int mod) {
  const UserOptions& options = UserOptions::Defaults();

  char query[4096];
  snprintf(query, sizeof(query), R"(
SELECT
    typreceive, typelem, typrelid
FROM
    pg_catalog.pg_type t,
    pg_catalog.pg_namespace n
WHERE
    t.typnamespace = n.oid
    AND t.oid = %u
;)",
           oid);

  auto res = PQexec(conn, query);

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cout << "error in copy command: " << PQresultErrorMessage(res) << std::endl;
  }

  std::string typreceive = PQgetvalue(res, 0, 0);
  Oid typelem = atooid(PQgetvalue(res, 0, 1));
  Oid typrelid = atooid(PQgetvalue(res, 0, 2));

  SqlTypeInfo sql_info = {.typmod = mod};

  PQclear(res);

  if (typreceive == "anyarray_recv" || typreceive == "anycompatiblearray_recv" ||
      typreceive == "array_recv") {
    sql_info.value_builder = MakeColumnBuilder(conn, typelem, mod);
  } else if (typreceive == "record_recv") {
    auto fields_info = RecordTypeInfo(conn, typrelid);
    FieldVector fields;
    for (size_t i = 0; i < fields_info.size(); i++) {
      auto [name, oid, mod] = fields_info[i];
      fields.push_back({name, MakeColumnBuilder(conn, oid, mod)});
    }
    sql_info.field_builders = fields;
  }

  if (gDecoderFactory.count(typreceive) == 0)
    return gDecoderFactory["bytearecv"](sql_info, options);

  return gDecoderFactory[typreceive](sql_info, options);
}

std::shared_ptr<TableBuilder> MakeQueryBuilder(PGconn* conn, const char* query) {
  auto columns = ColumnTypesForQuery(conn, query);
  FieldVector fields;
  for (auto& [name, oid, mod] : columns) {
    auto builder = MakeColumnBuilder(conn, oid, mod);
    fields.push_back({name, builder});
  }
  return std::make_shared<TableBuilder>(fields);
}

void CopyQuery(PGconn* conn, const char* query, std::shared_ptr<TableBuilder> builder) {
  auto copy_query = std::string("COPY (") + query + ") TO STDOUT (FORMAT binary)";
  auto res = PQexec(conn, copy_query.c_str());
  if (PQresultStatus(res) != PGRES_COPY_OUT)
    std::cout << "error in copy command: " << PQresultErrorMessage(res) << std::endl;
  PQclear(res);

  TableBuilder* builder_ = builder.get();

  char* tuple;

  auto status = PQgetCopyData(conn, &tuple, 0);
  if (status > 0) {
    const int kBinaryHeaderSize = 19;
    builder_->Append(tuple + kBinaryHeaderSize);

    PQfreemem(tuple);
  }

  while (true) {
    status = PQgetCopyData(conn, &tuple, 0);
    if (status < 0) break;

    builder_->Append(tuple);
    PQfreemem(tuple);
  }

  res = PQgetResult(conn);
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
  PQclear(res);
}

std::shared_ptr<arrow::Table> GetTable(const char* conninfo, const char* query) {
  auto conn = PQconnectdb(conninfo);
  if (PQstatus(conn) != CONNECTION_OK)
    std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn) << std::endl;

  auto res = PQexec(conn, "BEGIN READ ONLY");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
              << std::endl;
  PQclear(res);

  auto builder = MakeQueryBuilder(conn, query);
  CopyQuery(conn, query, builder);
  auto table = builder->Flush();

  res = PQexec(conn, "END");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    std::cout << "unable to end transaction: " << PQresultErrorMessage(res) << std::endl;
  PQclear(res);
  PQfinish(conn);

  return table;
}

}  // namespace pgeon
