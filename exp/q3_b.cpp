// exp - query 3
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


    // Q3 - Baseline 2
    // Sim SeqAcc of Disp data [sized 5369]
    SecureRelation relationA(1, 5369);
    init_relation(relationA, 1, 5369);
    
    // Sim SeqAcc of Client data [sized 5369]
    SecureRelation relationB(1, 5369);
    init_relation(relationB, 1, 5369);

    // Setup filter
    FilterOperator filter_by_fixed_value(0, Integer(32, 1, ALICE), "eq");

    // Setup count operator
    CountOperator count_op;

    // Naive nested loop join (EquiJoin)
    EquiJoinOperator equijoin_op(0, 0);

    
    auto start_time = std::chrono::high_resolution_clock::now();

    //Step 1. Filter Disp table - resize to size of 841 (DP resized)
    SecureRelation filtered_relationA = filter_by_fixed_value.execute(relationA);
    SecureRelation filtered_relationB = filter_by_fixed_value.execute(relationB);
    size_t mem_filter = getRelationMemorySize(relationA) + getRelationMemorySize(relationB);
    filtered_relationA.compact(811);
    filtered_relationB.compact(95);
    
    //Step 2. Nested loop join 
    SecureRelation equi_join_result = equijoin_op.execute(filtered_relationA, filtered_relationB);
    size_t mem_join = getRelationMemorySize(equi_join_result);
    equi_join_result.compact(12);
   

    //Step 3. Count distinct (already sorted, directly count)
    SecureRelation result = count_op.execute(equi_join_result);
    size_t mem_cnt = getRelationMemorySize(equi_join_result);


    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size: " 
              << mem_filter + mem_join + mem_cnt
              << " bytes\n";
    std::cout << "Index EquiJoin execution time: " 
              << duration_index_join 
              << " milliseconds\n\n";

    delete io;
    return 0;
}
