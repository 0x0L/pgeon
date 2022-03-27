#include "sql.h"

#include <iostream>

#include <libpq-fe.h>

using namespace pgeon;

int main(int argc, char const *argv[])
{
    const char *conninfo = "postgresql://localhost/mytests";

    auto conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
        std::cout << "failed on PostgreSQL connection: " << PQerrorMessage(conn)
                  << std::endl;

    auto res = PQexec(conn, "BEGIN READ ONLY");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to begin transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);

    const char *query = "select * from minute_bars";
    // query = "select sum(volume) as my_sum from minute_bars";
    // query = "select * from numerical";

    auto builder = MakeQueryBuilder(conn, query);
    CopyQuery(conn, query, builder);
    auto table = builder->Flush();

    // std::cout << table->ToString() << std::endl;
    std::cout << table->num_rows() << " rows fetched" << std::endl;

    res = PQexec(conn, "END");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        std::cout << "unable to end transaction: " << PQresultErrorMessage(res)
                  << std::endl;
    PQclear(res);
    PQfinish(conn);

    return 0;
}
