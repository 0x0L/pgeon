// Copyright 2022 nullptr

#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <pgeon.h>

#include <iostream>

int main(int argc, char const* argv[]) {
  if ((argc != 3) && (argc != 4)) {
    std::cerr << "Usage: " << argv[0] << " DB QUERY [OUTPUT]" << std::endl;
    return EXIT_FAILURE;
  }

  auto table = pgeon::CopyQuery(argv[1], argv[2]);
  if (!table.ok()) {
    std::cerr << table.status() << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << (*table)->schema()->ToString() << std::endl;
  std::cout << "Fetched " << (*table)->num_rows() << " rows" << std::endl;

  if (argc == 3) return EXIT_SUCCESS;

  auto output = arrow::io::FileOutputStream::Open(argv[3]);
  if (!output.ok()) {
    std::cerr << output.status() << std::endl;
    return EXIT_FAILURE;
  }

  auto status =
      parquet::arrow::WriteTable(**table, arrow::default_memory_pool(), *output);
  if (!status.ok()) {
    std::cerr << status << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
