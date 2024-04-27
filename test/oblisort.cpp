#include "emp-sh2pc/emp-sh2pc.h"
#include "util/oblisort.hpp"
#include <iostream>

using namespace emp;

// Utility function to initialize an example set of tuples
void init_tuples(Tuple* tuples, int size) {
    for(int i = 0; i < size; i++) {
        tuples[i].value = Integer(32, rand() % 1000, PUBLIC);  // Random integer value
        tuples[i].flag = Integer(32, rand() % 2, PUBLIC);     // Random flag (0 or 1)
    }
}

// Utility function to print revealed values of tuples
void print_tuples(const std::string& label, Tuple* tuples, int size) {
    std::cout << label;
    for(int i = 0; i < size; i++) {
        std::cout << "(" << tuples[i].value.reveal<int>() << ", " << tuples[i].flag.reveal<int>() << ") ";
    }
    std::cout << "\n";
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    int size = 8;  // For simplicity, choose a power of 2 for bitonic sort
    Tuple* tuples = new Tuple[size];

    init_tuples(tuples, size);
    print_tuples("Original Tuples:\n", tuples, size);

    // Sort based on flags
    ObliviousSorting::bitonic_sort(tuples, 0, size, true);
    print_tuples("Tuples Sorted by Flag:\n", tuples, size);

    // Re-initialize the tuples and sort based on values
    init_tuples(tuples, size);
    print_tuples("Re-initialized Tuples:\n", tuples, size);

    ObliviousSorting::bitonic_sort_by_value(tuples, 0, size, true);
    print_tuples("Tuples Sorted by Value:\n", tuples, size);

    delete[] tuples;
    delete io;
    return 0;
}
