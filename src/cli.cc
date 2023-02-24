#include <pgeon.h>
#include <iostream>

int main(int argc, char const *argv[])
{
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " DB QUERY" << std::endl;
        return 1;
    }

    auto table = pgeon::CopyQuery(argv[1], argv[2]);
    std::cout << table->ToString() << std::endl;
    return 0;
}
