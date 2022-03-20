#include "decoders.h"

#include <libpq-fe.h>

using FieldVector = std::vector<std::pair<std::string, std::shared_ptr<FieldReceiver>>>;

std::vector<std::pair<std::string, Oid>> RecordInfo(PGconn *conn, Oid oid)
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
        )",
        oid);

    auto res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        assert(false);
    // std::cout << "get composite descr failed: " << PQresultErrorMessage(res)
    //           << std::endl;

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

std::shared_ptr<FieldReceiver> MakeReceiver(PGconn *conn, Oid oid)
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
        )",
        oid);

    auto res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        assert(false);

    std::string typreceive = PQgetvalue(res, 0, 0);
    Oid typelem = atooid(PQgetvalue(res, 0, 1));
    Oid typrelid = atooid(PQgetvalue(res, 0, 2));

    PQclear(res);

    if (typreceive == "array_recv")
    {
        auto value_receiver = MakeReceiver(conn, typelem);
        return std::make_shared<ArrayReceiver>(value_receiver);
    }
    else if (typreceive == "record_recv")
    {
        auto fields_info = RecordInfo(conn, typrelid);
        FieldVector fields;
        for (size_t i = 0; i < fields_info.size(); i++)
        {
            auto [field_name, field_oid] = fields_info[i];
            fields.push_back({field_name, MakeReceiver(conn, field_oid)});
        }
        return std::make_shared<StructReceiver>(fields);
    }
    else if (typreceive == "timestamp_recv")
    {
        return std::make_shared<TimestampReceiver>();
    }
    else if (typreceive == "date_recv")
    {
        return std::make_shared<DateReceiver>();
    }
    else if (typreceive == "time_recv")
    {
        return std::make_shared<TimeReceiver>();
    }
    else if (typreceive == "interval_recv")
    {
        return std::make_shared<IntervalReceiver>();
    }
    else if (typreceive == "boolrecv")
    {
        return std::make_shared<BoolReceiver>();
    }
    else if (typreceive == "float4recv")
    {
        return std::make_shared<Float4Receiver>();
    }
    else if (typreceive == "float8recv")
    {
        return std::make_shared<Float8Receiver>();
    }
    else if ((typreceive == "int2recv") || (typreceive == "serial2"))
    {
        return std::make_shared<Int2Receiver>();
    }
    else if ((typreceive == "int4recv") || (typreceive == "serial4"))
    {
        return std::make_shared<Int4Receiver>();
    }
    else if ((typreceive == "int8recv") || (typreceive == "serial8"))
    {
        return std::make_shared<Int8Receiver>();
    }
    else if (
        (typreceive == "bpcharrecv") || (typreceive == "json_recv") ||
        (typreceive == "textrecv") || (typreceive == "xml_recv"))
    {
        return std::make_shared<TextReceiver>();
    }
    else if ((typreceive == "enum_recv") || (typreceive == "varcharrecv"))
    {
        return std::make_shared<DictionaryReceiver>();
    }
    else if (typreceive == "uuid_recv")
    {
        return std::make_shared<UuidReceiver>();
    }
    else if ((typreceive == "jsonb_recv") || (typreceive == "jsonpath_recv"))
    {
        return std::make_shared<JsonbReceiver>();
    }
    return std::make_shared<BinaryReceiver>();
}

FieldVector GetQuerySchema(PGconn *conn, const char *query)
{
    auto descr_query = std::string(query) + " limit 0";
    PGresult *res = PQexec(conn, descr_query.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        assert(false);
    // std::cout << "get descr failed: " << PQresultErrorMessage(res) << std::endl;

    int nfields = PQnfields(res);
    FieldVector fields(nfields);
    for (size_t i = 0; i < nfields; i++)
    {
        const char *name = PQfname(res, i);
        Oid oid = PQftype(res, i);
        fields[i] = {name, MakeReceiver(conn, oid)};
    }

    PQclear(res);
    return fields;
}
