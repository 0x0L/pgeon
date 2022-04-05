#include "sql.h"

#include <iostream>

using namespace pgeon;

int main(int argc, char const *argv[])
{
    const char *conninfo = "postgresql://localhost/mytests";
    const char *query = "select * from minute_bars";
    // query = "select sum(volume) as my_sum from minute_bars";
    // query = "select * from numeric";

    auto table = GetTable(conninfo, query);

    std::cout << table->ToString() << std::endl;
    // std::cout << table->num_rows() << " rows fetched" << std::endl;

    return 0;
}
