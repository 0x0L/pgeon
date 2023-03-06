// Copyright 2022 nullptr

#include <libpq-fe.h>

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include "builder.h"
#include "pg_interface.h"

namespace pgeon {

using ColumnVector = std::vector<std::tuple<std::string, Oid, int>>;

ColumnVector ColumnTypesForQuery(PGconn* conn, const char* query) {
  const auto descr_query = "SELECT * FROM (" + std::string(query) + ") AS FOO LIMIT 0;";
  PGresult* res = PQexec(conn, descr_query.c_str());

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cout << "error in ColumnTypesForQuery (query " << query
              << "): " << PQresultErrorMessage(res) << std::endl;
  }

  int n = PQnfields(res);
  ColumnVector fields(n);

  for (int i = 0; i < n; i++) {
    const char* name = PQfname(res, i);
    Oid oid = PQftype(res, i);
    int mod = PQfmod(res, i);
    fields[i] = {name, oid, mod};
    // std::cout << "field " << name << " len " << PQfsize(res, i) << std::endl;
  }

  PQclear(res);
  return fields;
}

ColumnVector RecordTypeInfo(PGconn* conn, Oid oid) {
  char query[4096];
  snprintf(query, sizeof(query), R"(
SELECT
    attnum, attname, atttypid, atttypmod
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
    std::cout << "error in RecordTypeInfo (Oid " << oid
              << "): " << PQresultErrorMessage(res) << std::endl;
  }

  int nfields = PQntuples(res);
  std::vector<std::tuple<std::string, Oid, int>> fields(nfields);

  for (int i = 0; i < nfields; i++) {
    int attnum = atoi(PQgetvalue(res, i, 0));
    const char* attname = PQgetvalue(res, i, 1);
    Oid atttypid = atooid(PQgetvalue(res, i, 2));
    int atttypmod = atoi(PQgetvalue(res, i, 3));

    fields[attnum - 1] = {attname, atttypid, atttypmod};
  }

  PQclear(res);
  return fields;
}

std::shared_ptr<ArrayBuilder> MakeColumnBuilder(PGconn* conn, Oid oid, int mod,
                                                const UserOptions& options) {
  char query[4096];
  snprintf(query, sizeof(query), R"(
SELECT
    typreceive, typelem, typrelid, typlen
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
    std::cout << "error in MakeColumnBuilder (Oid " << oid
              << "): " << PQresultErrorMessage(res) << std::endl;
  }

  if (PQntuples(res) == 0) {
    // this happens with attmissingval (anyarrayrecv) in pg_attribute
    PQclear(res);
    return MakeBuilder({.typreceive = "void_recv"}, options);
  }

  std::string typreceive = PQgetvalue(res, 0, 0);
  Oid typelem = atooid(PQgetvalue(res, 0, 1));
  Oid typrelid = atooid(PQgetvalue(res, 0, 2));
  int typlen = atoi(PQgetvalue(res, 0, 3));

  SqlTypeInfo sql_info{.typreceive = typreceive, .typmod = mod, .typlen = typlen};

  PQclear(res);

  if (typreceive == "anyarray_recv" || typreceive == "anycompatiblearray_recv" ||
      typreceive == "array_recv") {
    sql_info.value_builder = MakeColumnBuilder(conn, typelem, mod, options);
  } else if (typreceive == "record_recv") {
    auto fields_info = RecordTypeInfo(conn, typrelid);
    FieldVector fields;
    for (size_t i = 0; i < fields_info.size(); i++) {
      auto [name, oid, mod] = fields_info[i];
      fields.push_back({name, MakeColumnBuilder(conn, oid, mod, options)});
    }
    sql_info.field_builders = fields;
  }

  return MakeBuilder(sql_info, options);
}

std::shared_ptr<TableBuilder> MakeTableBuilder(PGconn* conn, const char* query,
                                               const UserOptions& options) {
  auto columns = ColumnTypesForQuery(conn, query);
  FieldVector fields;
  for (auto& [name, oid, mod] : columns) {
    auto builder = MakeColumnBuilder(conn, oid, mod, options);
    fields.push_back({name, builder});
  }
  return std::make_shared<TableBuilder>(fields);
}

void CopyQuery(PGconn* conn, const char* query, std::shared_ptr<TableBuilder> builder) {
  auto copy_query = std::string("COPY (") + query + ") TO STDOUT (FORMAT binary)";
  auto res = PQexec(conn, copy_query.c_str());
  if (PQresultStatus(res) != PGRES_COPY_OUT) {
    std::cout << "error in copy command: " << PQresultErrorMessage(res) << std::endl;
  }
  PQclear(res);

  TableBuilder* builder_ = builder.get();

  char* tuple;

  arrow::Status status;
  auto pg_status = PQgetCopyData(conn, &tuple, 0);
  if (pg_status > 0) {
    const int kBinaryHeaderSize = 19;

    StreamBuffer sb = StreamBuffer(tuple);
    auto header = sb.ReadBinary(kBinaryHeaderSize);
    status = builder_->Append(sb);

    PQfreemem(tuple);
  }

  while (true) {
    pg_status = PQgetCopyData(conn, &tuple, 0);
    if (pg_status < 0) break;

    StreamBuffer sb(tuple);
    status = builder_->Append(sb);
    PQfreemem(tuple);
  }

  res = PQgetResult(conn);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    // not really an issue...
    // pg_attribute gives "ERROR:  no binary output function available for type aclitem"
    std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
  }
  PQclear(res);
}

}  // namespace pgeon
