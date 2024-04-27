#include "core/relation.hpp"
#include "emp-sh2pc/emp-sh2pc.h"
#include <ctime>
#include <cstdlib>
#include <iostream>

using namespace emp;

// Utility to print the contents of the SecureRelation
void print_relation(const SecureRelation& relation) {
    for (size_t row = 0; row < relation.flags.size(); ++row) {
        for (size_t col = 0; col < relation.columns.size(); ++col) {
            std::cout << relation.columns[col][row].reveal<int>() << "\t";
        }
        std::cout << "| " << relation.flags[row].reveal<int>() << std::endl;
    }
    std::cout << "----------------------\n";
}

int main(int argc, char** argv) {
    // Set up the networking (localhost for this test)
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    std::srand(std::time(nullptr));

    int num_cols = 3;
    int num_rows = 1 << 8; 

    SecureRelation relation(num_cols, num_rows);

    // Fill the relation with random values
    for (int col = 0; col < num_cols; ++col) {
        for (int row = 0; row < num_rows; ++row) {
            relation.columns[col][row] = Integer(32, std::rand() % 1000, ALICE);  // Random values
        }
    }

    for (int row = 0; row < num_rows; ++row) {
        relation.flags[row] = Integer(1, std::rand() % 2, ALICE);  // Random binary flags
    }

    io->flush();

    std::cout << "Original Relation:" << std::endl;
    //print_relation(relation);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Sort by column 1
    relation.sort_by_column(0);
    std::cout << "Sorted by Column 1:" << std::endl;
    //print_relation(relation);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Execution time: " 
              << duration 
              << " milliseconds\n\n";

    // Sort by flag
    relation.sort_by_flag();
    std::cout << "Sorted by Flag:" << std::endl;
    //print_relation(relation);

    delete io;
    return 0;
}
