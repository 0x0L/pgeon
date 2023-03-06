// Copyright 2022 nullptr

#include <pgeon.h>
#include <iostream>

int main(int argc, char const *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " DB QUERY" << std::endl;
        return 1;
    }

    argv[1] = "postgresql://localhost:5432/postgres";
    argv[2] = "SELECT '(1.2, 4.3)'::point";

    auto table = pgeon::CopyQuery(argv[1], argv[2]);
    std::cout << table->ToString() << std::endl;
    return 0;
}
