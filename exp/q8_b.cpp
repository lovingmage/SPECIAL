// exp - query 8
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
    // Sim SeqAcc of account data [sized 4502]
    SecureRelation relationA(1, 4502);
    init_relation(relationA, 1, 4502);
    
    // Sim SeqAcc of Trans data [sized 1056322]
    SecureRelation relationB(1, 1056322);
    init_relation(relationB, 1, 1056322);

    // Sim SeqAcc of Disp data [sized 5426]
    SecureRelation relationC(1, 5426);
    init_relation(relationC, 1, 5426);

    // Sim SeqAcc of Order [sized 6472]
    SecureRelation relationD(1, 6472);
    init_relation(relationD, 1, 6472);

    // Sim SeqAcc of Loan [sized 683]
    SecureRelation relationE(1, 683);
    init_relation(relationE, 1, 683);


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
    filtered_relationA.compact(91);

    SecureRelation filtered_relationB = filter_by_fixed_value.execute(relationB);
    filtered_relationB.compact(8036);

    SecureRelation filtered_relationD = filter_by_fixed_value.execute(relationD);
    filtered_relationD.compact(364);

    SecureRelation filtered_relationE = filter_by_fixed_value.execute(relationE);
    filtered_relationE.compact(179);

    std::cout << "Filter completed \n";


    //Step 2. Equi join with result compaction B-C
    
    // Join I will be same as Q6, reuse testing restul to save time
    // Join I total memory 3140612208 bytes
    // Join I total memory 3140612208 bytes
    // Join I completed in 57816317 milliseconds
    // Join I completed in 57835056 milliseconds
#ifdef FULL_BENCH
    SecureRelation equi_join_result = equijoin_op.execute(filtered_relationB, relationC);
    size_t mem_join_1 = getRelationMemorySize(equi_join_result);
    equi_join_result.compact(9271);
#else
    SecureRelation equi_join_result(1, 9271);
    init_relation(equi_join_result, 1, 9271);
    int rtime_ms = 57816317;
    size_t mem_join_1 = 3140612208;
#endif
    std::cout << "Join I completed in \n";

    //Step 3. Equi join with result compaction (B-C)-D
    SecureRelation equi_join_result_2 = equijoin_op.execute(equi_join_result, filtered_relationD);
    size_t mem_join_2 = getRelationMemorySize(equi_join_result_2);
    equi_join_result_2.compact(801); // true cnt - 790
    std::cout << "Join II completed in \n";

    //Step 4. Equi join ((B-C)-D)-A
    SecureRelation equi_join_result_3 = equijoin_op.execute(equi_join_result_2, filtered_relationA);
    size_t mem_join_3 = getRelationMemorySize(equi_join_result_3);
    equi_join_result_3.compact(17);

    //Step 5. Equi join (((B-C)-D)-A)-E sorting push down no compactions
    equi_join_result_3.sort_by_column(0);
    filtered_relationE.sort_by_column(0);
    SecureRelation equi_join_result_4 = equijoin_op.execute(equi_join_result_3, filtered_relationE);
    size_t mem_join_4 = getRelationMemorySize(equi_join_result_4);
    equi_join_result_4.compact(9);

    //Step 6. Count
    SecureRelation result = count_op.execute(equi_join_result_4);
    size_t mem_cnt = getRelationMemorySize(equi_join_result_4);

    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): " 
              << mem_join_1 + mem_join_2 + mem_join_3 + mem_join_4 + mem_cnt + \
                 getRelationMemorySize(filtered_relationA) + \
                 getRelationMemorySize(filtered_relationB) + \
                 getRelationMemorySize(filtered_relationD) +\
                 getRelationMemorySize(filtered_relationE) +\
                 getRelationMemorySize(relationA) + \
                 getRelationMemorySize(relationB) + \
                 getRelationMemorySize(relationC) + \
                 getRelationMemorySize(relationD) + \
                 getRelationMemorySize(relationE)
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_index_join + rtime_ms
              << " milliseconds\n\n";

    delete io;
    return 0;
}
