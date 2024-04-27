#include "emp-sh2pc/emp-sh2pc.h"
#include "core/relation.hpp"
#include "core/op_filter.hpp"
#include <iostream>
#include <chrono>

using namespace emp;

// Utility function to initialize a relation with random values
void init_relation(SecureRelation& relation, int num_cols, int num_rows) {
    for (int col = 0; col < num_cols; ++col) {
        for (int row = 0; row < num_rows; ++row) {
            relation.columns[col][row] = Integer(32, rand() % 1000, ALICE);  // Random values
        }
    }

    for (int row = 0; row < num_rows; ++row) {
        relation.flags[row] = Integer(1, 1, ALICE);  // Set all flags to 1 initially
    }
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    // Create and initialize a large relation
    const int num_cols = 3;  // 3 columns
    const int num_rows = 1 << 12;  // Around a million rows
    SecureRelation relation(num_cols, num_rows);
    init_relation(relation, num_cols, num_rows);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Filter based on a fixed value
    FilterOperator filter_by_fixed_value(1, Integer(32, 500, ALICE), "lt");
    SecureRelation filtered_relation1 = filter_by_fixed_value.execute(relation);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Filter by Fixed Value Time: " << duration_filter_by_fixed_value << " ms" << std::endl;
    io->flush();

    SecureRelation relation2(num_cols, num_rows);
    init_relation(relation2, num_cols, num_rows);

    start_time = std::chrono::high_resolution_clock::now();

    // Filter based on an input column (comparing first and second columns)
    FilterOperator filter_by_column(0, relation2.columns[1], "gt");
    SecureRelation filtered_relation2 = filter_by_column.execute(relation2);

    end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_column = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Filter by Input Column Time: " << duration_filter_by_column << " ms" << std::endl;
    io->flush();

    // Two times selection over a base relation
    SecureRelation relation3(num_cols, num_rows);
    init_relation(relation3, num_cols, num_rows);

    start_time = std::chrono::high_resolution_clock::now();

    FilterOperator first_filter(0, Integer(32, 300, ALICE), "lt");
    SecureRelation filtered_relation3_1 = first_filter.execute(relation3);
    io->flush();

    FilterOperator second_filter(1, Integer(32, 700, ALICE), "gt");
    SecureRelation filtered_relation3_2 = second_filter.execute(filtered_relation3_1);

    end_time = std::chrono::high_resolution_clock::now();
    auto duration_two_times_filter = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Two Times Selection Time: " << duration_two_times_filter << " ms" << std::endl;
    io->flush();

    delete io;
    return 0;
}
