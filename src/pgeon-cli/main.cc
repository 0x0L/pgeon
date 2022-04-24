// Copyright 2022 nullptr

#include <pgeon.h>

#include <iostream>

#include "./CLI11.hpp"

int main(int argc, char** argv) {
  CLI::App app{"pgeon.\n"};

  std::string dbname = "";
  std::string query = "";
  app.add_option("-d,--dbname", dbname, "database name to connect to");
  app.add_option("query", query, "the query");

  CLI11_PARSE(app, argc, argv);
  // std::cout << "dbname " << dbname << std::endl;
  // std::cout << "query " << query << std::endl;

  auto tbl = pgeon::CopyQuery(dbname.c_str(), query.c_str());

  // std::cout << tbl->ToString() << std::endl;
  std::cout << tbl->num_rows() << " rows fetched" << std::endl;

  return 0;
}
