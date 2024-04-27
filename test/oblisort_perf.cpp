#include "emp-sh2pc/emp-sh2pc.h"
#include "util/oblisort.hpp"
#include <iostream>
#include <chrono>

using namespace emp;

// Utility function to initialize an example set of tuples
void init_tuples(Tuple* tuples, int size) {
    for(int i = 0; i < size; i++) {
        tuples[i].value = Integer(32, rand() % 1000, PUBLIC);  // Random integer value
        tuples[i].flag = Integer(32, rand() % 2, PUBLIC);     // Random flag (0 or 1)
    }
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    int size = 1 << 20;  // 2^12 elements, modify as needed
    Tuple* tuples = new Tuple[size];

    init_tuples(tuples, size);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Sort based on flags
    ObliviousSorting::bitonic_sort(tuples, 0, size, true);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_flag_sort = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    init_tuples(tuples, size);

    start_time = std::chrono::high_resolution_clock::now();

    // Sort based on values
    ObliviousSorting::bitonic_sort_by_value(tuples, 0, size, true);

    end_time = std::chrono::high_resolution_clock::now();
    auto duration_value_sort = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Sorting by Flag Time: " << duration_flag_sort << " ms" << std::endl;
    std::cout << "Sorting by Value Time: " << duration_value_sort << " ms" << std::endl;

    delete[] tuples;
    delete io;
    return 0;
}
