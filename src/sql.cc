#include "decoders.h"
#include <libpq-fe.h>

#include <stdio.h>

namespace pgeon
{
template <typename... Args>
std::string string_format(const std::string &format, Args... args)
{
    // Extra space for '\0'
    int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) + 1;
    if (size_s <= 0)
    {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    auto buf = std::make_unique<char[]>(size);
    std::snprintf(buf.get(), size, format.c_str(), args...);

    // We don't want the '\0' inside
    return std::string(buf.get(), buf.get() + size - 1);
}

using ColumnVector = std::vector<std::pair<std::string, Oid>>;

ColumnVector ColumnsForQuery(PGconn *conn, const char *query)
{
    // TODO: make descr query work with limit query...
    const auto descr_query = std::string(query) + " limit 0";
    PGresult *res = PQexec(conn, descr_query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        // std::cout << PQresultErrorMessage(res) << std::endl;
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

ColumnVector RecordInfo(PGconn *conn, Oid oid)
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

    //     const char *QUERY = R"(
    // SELECT
    //     attnum, attname, atttypid
    // FROM
    //     pg_catalog.pg_attribute a,
    //     pg_catalog.pg_type t,
    //     pg_catalog.pg_namespace n
    // WHERE
    //     t.typnamespace = n.oid
    //     AND a.atttypid = t.oid
    //     AND a.attrelid = {}
    // ;)";

    //     const auto query = string_format(QUERY, oid);
    auto res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        // std::cout << PQresultErrorMessage(res) << std::endl;
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

using FieldVector = std::vector<std::pair<std::string, std::shared_ptr<ColumnBuilder>>>;

std::shared_ptr<ColumnBuilder> MakeBuilder(PGconn *conn, Oid oid)
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

//     const char *_TYPE_INFO = R"(
// SELECT
//     typreceive, typelem, typrelid
// FROM
//     pg_catalog.pg_type t,
//     pg_catalog.pg_namespace n
// WHERE
//     t.typnamespace = n.oid
//     AND t.oid = {}
// ;)";

//     const auto query = string_format(_TYPE_INFO, oid);
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
        auto value_receiver = MakeBuilder(conn, typelem);
        return std::make_shared<ArrayBuilder>(value_receiver);
    }
    else if (typreceive == "record_recv")
    {
        auto fields_info = RecordInfo(conn, typrelid);
        FieldVector fields;
        for (size_t i = 0; i < fields_info.size(); i++)
        {
            auto [field_name, field_oid] = fields_info[i];
            fields.push_back({field_name, MakeBuilder(conn, field_oid)});
        }
        return std::make_shared<RecordBuilder>(fields);
    }
    // else if (typreceive == "timestamp_recv")
    // {
    //     return std::make_shared<TimestampReceiver>();
    // }

    return DecoderFactory[typreceive]();
}
} // namespace pgeon
