#include "sql.h"

#include <iostream>
#include <stdio.h>

namespace pgeon
{

ColumnVector ColumnTypesForQuery(PGconn *conn, const char *query)
{
    // TODO: make descr query work with limit query...
    const auto descr_query = std::string(query) + " limit 0";
    PGresult *res = PQexec(conn, descr_query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        std::cout << PQresultErrorMessage(res) << std::endl;
    }

    int n = PQnfields(res);
    ColumnVector fields(n);

    for (size_t i = 0; i < n; i++)
    {
        const char *name = PQfname(res, i);
        Oid oid = PQftype(res, i);
        fields[i] = {name, oid};
    }

    PQclear(res);
    return fields;
}

ColumnVector RecordTypeInfo(PGconn *conn, Oid oid)
{
    char query[4096];
    snprintf(
        query, sizeof(query), R"(
SELECT
    attnum, attname, atttypid
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

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        std::cout << PQresultErrorMessage(res) << std::endl;
    }

    int nfields = PQntuples(res);
    std::vector<std::pair<std::string, Oid>> fields(nfields);

    for (size_t i = 0; i < nfields; i++)
    {
        int attnum = atoi(PQgetvalue(res, i, 0));
        const char *attname = PQgetvalue(res, i, 1);
        Oid atttypid = atooid(PQgetvalue(res, i, 2));

        fields[attnum - 1] = {attname, atttypid};
    }

    PQclear(res);
    return fields;
}

std::shared_ptr<ColumnBuilder> MakeColumnBuilder(PGconn *conn, Oid oid)
{
    char query[4096];
    snprintf(
        query, sizeof(query), R"(
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

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        auto ss = PQresultErrorMessage(res);
    }

    std::string typreceive = PQgetvalue(res, 0, 0);
    Oid typelem = atooid(PQgetvalue(res, 0, 1));
    Oid typrelid = atooid(PQgetvalue(res, 0, 2));

    PQclear(res);

    if (typreceive == "array_recv")
    {
        auto value_receiver = MakeColumnBuilder(conn, typelem);
        return std::make_shared<ArrayBuilder>(value_receiver);
    }
    else if (typreceive == "record_recv")
    {
        auto fields_info = RecordTypeInfo(conn, typrelid);
        FieldVector fields;
        for (size_t i = 0; i < fields_info.size(); i++)
        {
            auto [field_name, field_oid] = fields_info[i];
            fields.push_back({field_name, MakeColumnBuilder(conn, field_oid)});
        }
        return std::make_shared<RecordBuilder>(fields);
    }
    return DecoderFactory[typreceive]();
}

std::shared_ptr<TableBuilder> MakeQueryBuilder(PGconn *conn, const char *query)
{
    auto columns = ColumnTypesForQuery(conn, query);
    FieldVector fields;
    for (auto &[name, oid] : columns)
    {
        auto builder = MakeColumnBuilder(conn, oid);
        fields.push_back(std::make_pair(name, builder));
    }
    return std::make_shared<TableBuilder>(fields);
}

void CopyQuery(PGconn *conn, const char *query, std::shared_ptr<TableBuilder> builder)
{
    auto copy_query = std::string("COPY (") + query + ") TO STDOUT (FORMAT binary)";
    auto res = PQexec(conn, copy_query.c_str());
    if (PQresultStatus(res) != PGRES_COPY_OUT)
        std::cout << "error in copy command: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    TableBuilder *builder_ = builder.get();
    char *tuple;

    auto status = PQgetCopyData(conn, &tuple, 0);
    if (status > 0)
    {
        const int kBinaryHeaderSize = 19;
        builder_->Append(tuple + kBinaryHeaderSize);
        PQfreemem(tuple);
    }

    while (true)
    {
        status = PQgetCopyData(conn, &tuple, 0);
        if (status < 0)
            break;

        builder_->Append(tuple);
        PQfreemem(tuple);
    }

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "copy command failed: " << PQresultErrorMessage(res) << std::endl;
    PQclear(res);
}

} // namespace pgeon
