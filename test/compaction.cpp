#include "emp-sh2pc/emp-sh2pc.h"
#include "util/oblisort.hpp"
#include <iostream>
#include <chrono>

using namespace emp;

// Utility function to initialize an example set of tuples
void init_tuples(Tuple* tuples, int size) {
    for(int i = 0; i < size; i++) {
        tuples[i].value = Integer(32, rand() % 1000, PUBLIC);
        tuples[i].flag = Integer(32, rand() % 2, PUBLIC);
    }
}

// Utility function to print tuples' details
void print_tuples(const std::string& label, Tuple* tuples, int size) {
    std::cout << label;
    for(int i = 0; i < size; i++) {
        std::cout << "(" << tuples[i].value.reveal<int>() << ", " << tuples[i].flag.reveal<int>() << ") ";
    }
    std::cout << std::endl;
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    int size = 16;  // Reduced size for printing details
    Tuple* tuples = new Tuple[size];
    Tuple* tuples_copy = new Tuple[size];

    init_tuples(tuples, size);
    std::copy(tuples, tuples + size, tuples_copy);  // Create an exact copy of the tuples

    print_tuples("Original Tuples: ", tuples, size);

    auto start_time = std::chrono::high_resolution_clock::now();

    ObliviousSorting::bitonic_compaction(tuples, 0, size);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_compaction = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    print_tuples("After Bitonic Compaction: ", tuples, size);

    start_time = std::chrono::high_resolution_clock::now();

    ObliviousSorting::bitonic_sort(tuples_copy, 0, size, true);

    end_time = std::chrono::high_resolution_clock::now();
    auto duration_sort = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    print_tuples("After Traditional Bitonic Sort: ", tuples_copy, size);

    std::cout << "Bitonic Compaction Time: " << duration_compaction << " ms" << std::endl;
    std::cout << "Traditional Bitonic Sort Time: " << duration_sort << " ms" << std::endl;

    delete[] tuples;
    delete[] tuples_copy;
    delete io;
    return 0;
}
