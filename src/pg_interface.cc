// Copyright 2022 nullptr

#include "pg_interface.h"

#include "builder.h"

namespace pgeon {

using ColumnVector = std::vector<std::tuple<std::string, Oid, int>>;

arrow::Result<std::shared_ptr<ColumnVector>> ColumnTypesForQuery(PGconn* conn,
                                                                 const char* query) {
  auto descr_query =
      arrow::util::StringBuilder("SELECT * FROM (", query, ") AS foo LIMIT 0;");

  PGresult* res = PQexec(conn, descr_query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    auto status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));
    PQclear(res);
    return status;
  }

  int n = PQnfields(res);
  ColumnVector fields(n);

  for (int i = 0; i < n; i++) {
    const char* name = PQfname(res, i);
    Oid oid = PQftype(res, i);
    int mod = PQfmod(res, i);
    fields[i] = {name, oid, mod};
  }

  PQclear(res);
  return std::make_shared<ColumnVector>(fields);
}

arrow::Result<std::shared_ptr<ColumnVector>> RecordTypeInfo(PGconn* conn, Oid oid) {
  auto query = arrow::util::StringBuilder(
      "SELECT attnum, attname, atttypid, atttypmod ",
      "FROM pg_catalog.pg_attribute a, pg_catalog.pg_type t, pg_catalog.pg_namespace n ",
      "WHERE t.typnamespace = n.oid AND a.atttypid = t.oid AND a.attrelid = ",
      std::to_string(oid), ";");

  auto res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    auto status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));
    PQclear(res);
    return status;
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
  return std::make_shared<ColumnVector>(fields);
}

arrow::Result<std::shared_ptr<ArrayBuilder>> MakeColumnBuilder(
    PGconn* conn, Oid oid, int mod, const UserOptions& options) {
  auto query = arrow::util::StringBuilder(
      "SELECT typreceive, typelem, typrelid, typlen ",
      "FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n ",
      "WHERE t.typnamespace = n.oid AND t.oid = ", std::to_string(oid), ";");

  auto res = PQexec(conn, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    auto status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));
    PQclear(res);
    return status;
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
  PQclear(res);

  std::shared_ptr<ArrayBuilder> builder;
  SqlTypeInfo sql_info{.typreceive = typreceive, .typmod = mod, .typlen = typlen};
  if (typreceive == "anyarray_recv" || typreceive == "anycompatiblearray_recv" ||
      typreceive == "array_recv") {
    ARROW_ASSIGN_OR_RAISE(builder, MakeColumnBuilder(conn, typelem, mod, options));
    sql_info.value_builder = builder;
  } else if (typreceive == "record_recv") {
    std::shared_ptr<ColumnVector> fields_info;
    ARROW_ASSIGN_OR_RAISE(fields_info, RecordTypeInfo(conn, typrelid));

    FieldVector fields;
    for (size_t i = 0; i < fields_info->size(); i++) {
      auto [name, oid, mod] = (*fields_info)[i];
      ARROW_ASSIGN_OR_RAISE(builder, MakeColumnBuilder(conn, oid, mod, options));
      fields.push_back({name, builder});
    }
    sql_info.field_builders = fields;
  }

  return MakeBuilder(sql_info, options);
}

arrow::Result<std::shared_ptr<TableBuilder>> MakeTableBuilder(
    PGconn* conn, const char* query, const UserOptions& options) {
  std::shared_ptr<ColumnVector> columns;
  ARROW_ASSIGN_OR_RAISE(columns, ColumnTypesForQuery(conn, query));

  FieldVector fields;
  std::shared_ptr<ArrayBuilder> builder;
  for (auto& [name, oid, mod] : *columns) {
    ARROW_ASSIGN_OR_RAISE(builder, MakeColumnBuilder(conn, oid, mod, options));
    fields.push_back({name, builder});
  }
  return std::make_shared<TableBuilder>(fields);
}

arrow::Status CopyQuery(PGconn* conn, const char* query,
                        std::shared_ptr<TableBuilder> builder) {
  arrow::Status status = arrow::Status::OK();
  auto copy_query =
      arrow::util::StringBuilder("COPY (", query, ") TO STDOUT (FORMAT binary);");

  PGresult* res = PQexec(conn, copy_query.c_str());
  if (PQresultStatus(res) != PGRES_COPY_OUT) {
    status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));
  }
  PQclear(res);
  ARROW_RETURN_NOT_OK(status);

  // Attempts to obtain another row of data from the server during a COPY.
  // Data is always returned one data row at a time; if only a partial row
  // is available, it is not returned. Successful return of a data row involves
  // allocating a chunk of memory to hold the data. The buffer parameter must
  // be non-NULL. *buffer is set to point to the allocated memory, or to NULL
  // in cases where no buffer is returned. A non-NULL result buffer should be
  // freed using PQfreemem when no longer needed.

  // When a row is successfully returned, the return value is the number of
  // data bytes in the row (this will always be greater than zero). The returned
  // string is always null-terminated, though this is probably only useful for
  // textual COPY. A result of zero indicates that the COPY is still in progress,
  // but no row is yet available (this is only possible when async is true).
  // A result of -1 indicates that the COPY is done. A result of -2 indicates
  // that an error occurred (consult PQerrorMessage for the reason).

  // After PQgetCopyData returns -1, call PQgetResult to obtain the final result
  // status of the COPY command. One can wait for this result to be available
  // in the usual way. Then return to normal operation.
  char* tuple = nullptr;
  auto tuple_size = PQgetCopyData(conn, &tuple, 0);
  StreamBuffer sb = StreamBuffer(tuple);

  if (tuple_size > 0) {
    const int kBinaryHeaderSize = 19;
    const char* header = sb.ReadBinary(kBinaryHeaderSize);
  }

  TableBuilder* builder_ = builder.get();
  while (tuple_size > 0) {
    status = builder_->Append(sb);
    if (tuple != nullptr) PQfreemem(tuple);
    ARROW_RETURN_NOT_OK(status);

    tuple_size = PQgetCopyData(conn, &tuple, 0);
    sb = StreamBuffer(tuple);
  }

  if (tuple != nullptr) PQfreemem(tuple);

  res = PQgetResult(conn);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    // not really an issue...
    // pg_attribute gives "ERROR:  no binary output function available for type aclitem"
    status = arrow::Status::IOError("[libpq] ", PQresultErrorMessage(res));
  }
  PQclear(res);
  return status;
}

}  // namespace pgeon
