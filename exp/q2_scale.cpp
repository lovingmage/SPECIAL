//Q2 -eps
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
#include "core/op_agg_count.hpp"
#include "core/op_filter.hpp"
#include "core/relation.hpp"

// Utility function to initialize a relation with random values and flag bits
void init_relation(SecureRelation& relation, int num_cols, int num_rows, bool mixed_flags = false) {
    for (int col = 0; col < num_cols; ++col) {
        for (int row = 0; row < num_rows; ++row) {
            relation.columns[col][row] = Integer(32, rand() % 100, ALICE);  // Random values
        }
    }

    for (int row = 0; row < num_rows; ++row) {
        if (mixed_flags) {
            relation.flags[row] = Integer(1, rand() % 2, ALICE);  // Random binary flags
        } else {
            relation.flags[row] = Integer(1, 1, ALICE);  // All flags set to 1
        }
    }
}

// Utility function to get the memory size of a SecureRelation object in bytes
size_t getRelationMemorySize(const SecureRelation& relation) {
    size_t memorySize = 0;

    // Calculate the memory size used by the columns
    for (const auto& column : relation.columns) {
        // Integer object size multiplied by the number of integers in the column
        memorySize += column.size() * sizeof(emp::Integer); 
    }

    // Calculate the memory size used by the flags
    memorySize += relation.flags.size() * sizeof(emp::Integer);

    return memorySize;
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    // Setup filter
    FilterOperator filter_by_fixed_value(0, Integer(32, 1, ALICE), "eq");

    // Setup count operator
    CountOperator count_op;

    // Input sizes for different values of epsilon
    std::vector<int> input_sizes = {43, 64*43, 128*43, 256*43};
    std::vector<double> epsilons = {1, 64, 128, 256};

    for (size_t i = 0; i < input_sizes.size(); ++i) {
        // Initialize relation with random values
        SecureRelation relationA(1, input_sizes[i]);
        init_relation(relationA, 1, input_sizes[i]);

        auto start_time = std::chrono::high_resolution_clock::now();

        // Step 1. Filter Loan table
        SecureRelation filtered_relationA = filter_by_fixed_value.execute(relationA);

        // Step 2. Count
        SecureRelation result = count_op.execute(filtered_relationA);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // Print out the results
        std::cout << "Results for scale = " << epsilons[i] << "x:\n";
        std::cout << "---------------------\n";
        std::cout << "Memory size: " 
                  << getRelationMemorySize(relationA) + getRelationMemorySize(filtered_relationA) + getRelationMemorySize(result)
                  << " bytes\n";
        std::cout << "Execution time: " 
                  << duration 
                  << " milliseconds\n\n";
    }

    delete io;
    return 0;
}
