// exp - query 3
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
#include "core/op_agg_count.hpp"
#include "core/op_filter.hpp"
#include "core/op_project.hpp"
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


    // Q4 - Baseline 1
    // Sim SeqAcc of Order data [sized 6471]
    SecureRelation relationA(1, 6471);
    init_relation(relationA, 1, 6471);
    
    // Sim SeqAcc of Trans data [sized 1056322]
    SecureRelation relationB(1, 1056322);
    init_relation(relationB, 1, 1056322);

    // Sim SeqAcc of Disp data [sized 5426]
    SecureRelation relationC(1, 5426);
    init_relation(relationC, 1, 5426);

    // Setup filter
    FilterOperator filter_by_fixed_value(0, Integer(32, 1, ALICE), "eq");

    // Setup count operator
    CountOperator count_op;

    // Naive nested loop join (EquiJoin)
    EquiJoinOperator equijoin_op(0, 0);
    EquiJoinOperator equijoin_op_2(0, 0);
    std::cout << "Init completed \n";
    
    auto start_time = std::chrono::high_resolution_clock::now();

    //Step 1. Filter 
    SecureRelation filtered_relationA = filter_by_fixed_value.execute(relationA);
    filtered_relationA.compact(379);

    SecureRelation filtered_relationB = filter_by_fixed_value.execute(relationB);
    filtered_relationB.compact(8059);

    size_t mem_filter = getRelationMemorySize(relationA) + \
                        getRelationMemorySize(relationB) + \
                        getRelationMemorySize(relationC) + \
                        getRelationMemorySize(filtered_relationA) + \
                        getRelationMemorySize(filtered_relationB);

    std::cout << "Filter completed \n";


    //Step 2. Equi join with result compaction (sorting push down)
    auto join_start = std::chrono::high_resolution_clock::now();

    SecureRelation equi_join_result = equijoin_op.execute(filtered_relationB, relationC);
    size_t mem_join_1 = getRelationMemorySize(equi_join_result);
    std::cout << "Join I total memory " << mem_join_1 << " bytes\n";
    equi_join_result.compact(9271);

    // After join operation
    auto join_end = std::chrono::high_resolution_clock::now();
    auto join_duration = std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_start).count();

    std::cout << "Join I completed in " << join_duration << " milliseconds\n";

    SecureRelation equi_join_result_2 = equijoin_op.execute(equi_join_result, filtered_relationA);
    size_t mem_join_2 = getRelationMemorySize(equi_join_result_2);

    //Step 3. Count 
    SecureRelation result = count_op.execute(equi_join_result_2);
     size_t mem_cnt = getRelationMemorySize(equi_join_result_2);


    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): " 
              << mem_filter + mem_join_1 + mem_join_2 + mem_cnt
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_index_join 
              << " milliseconds\n\n";

    delete io;
    return 0;
}
