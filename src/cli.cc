// Copyright 2022 nullptr

#include <pgeon.h>
#include <iostream>

// #include <arrow/io/api.h>
// #include <parquet/arrow/writer.h>

int main(int argc, char const* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " DB QUERY" << std::endl;
    return 1;
  }

  auto table = pgeon::CopyQuery(argv[1], argv[2]).ValueOrDie();
  std::cout << table->ToString() << std::endl;

  // std::shared_ptr<arrow::io::OutputStream> output =
  // arrow::io::FileOutputStream::Open("toto.parquet").ValueOrDie();
  // parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output);
  return 0;
}
