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

    
    // Q4 - Baseline 1
    // Sim SeqAcc of account data [sized 4502]
    SecureRelation relationA(1, 45);
    init_relation(relationA, 1, 45);
    
    // Sim SeqAcc of Trans data [sized 1056322]
    SecureRelation relationB(1, 10563);
    init_relation(relationB, 1, 10563);

    // Sim SeqAcc of Order data [sized 6472]
    SecureRelation relationC(1, 64);
    init_relation(relationC, 1, 64);

    // Setup filter
    FilterOperator filter_by_fixed_value(0, Integer(32, 1, ALICE), "eq");

    // Setup count operator
    CountOperator count_op;

    // Naive nested loop join (EquiJoin)
    EquiJoinOperator equijoin_op(0, 0);
    EquiJoinOperator equijoin_op_2(0, 0);
    std::cout << "Init completed \n";
    
    // Start the overall timer
    auto overall_start = std::chrono::high_resolution_clock::now();

    //Step 1. Filter Account and Trans tables resize to size of 49, 7997, respectively (DP resized)
    SecureRelation filtered_relationA = filter_by_fixed_value.execute(relationA);
    SecureRelation filtered_relationB = filter_by_fixed_value.execute(relationB);
    SecureRelation filtered_relationC = filter_by_fixed_value.execute(relationC);

    //Step 2. Equi join 
    // Start the timer for the first join
    auto join1_start = std::chrono::high_resolution_clock::now();

    SecureRelation equi_join_result = equijoin_op.execute(filtered_relationA, filtered_relationB);    
    // Stop the timer for the first join and start the timer for the second join
    auto join1_end = std::chrono::high_resolution_clock::now();

    auto join2_start = std::chrono::high_resolution_clock::now();
    SecureRelation equi_join_result_2 = equijoin_op.execute(equi_join_result, filtered_relationC);

    // Stop the timer for the second join
    auto join2_end = std::chrono::high_resolution_clock::now();

    // Stop the overall timer
    auto overall_end = std::chrono::high_resolution_clock::now();

    //Step 3. Get distinct (we ignore the distinct time for baseline, otherwise the execution is too slow)
    //SecureRelation result = count_op.execute(equi_join_result);

    // Calculate durations
    auto duration_first_join = std::chrono::duration_cast<std::chrono::milliseconds>(join1_end - join1_start).count();
    auto duration_second_join = std::chrono::duration_cast<std::chrono::milliseconds>(join2_end - join2_start).count();
    auto overall_duration = std::chrono::duration_cast<std::chrono::milliseconds>(overall_end - overall_start).count();

    // Assuming linear scaling for the first join and a potential larger factor for the second join
    auto projected_time_first_join = duration_first_join * 100;
    auto scaling_factor_second_join = 100; // This might be larger than 100, adjust based on your data
    auto projected_time_second_join = duration_second_join * scaling_factor_second_join;
    auto projected_total_time = projected_time_first_join + projected_time_second_join;

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size for the join: " 
              << getRelationMemorySize(equi_join_result) + getRelationMemorySize(equi_join_result_2) 
              << " bytes\n";
    std::cout << "Execution time: " << std::endl;
    std::cout << "First Join Duration: " << duration_first_join << " milliseconds\n";
    std::cout << "Second Join Duration: " << duration_second_join << " milliseconds\n";
    std::cout << "Overall Duration: " << overall_duration << " milliseconds\n";


    std::cout << "Projected times:\n";
    // Print projected durations
    std::cout << "Projected First Join Duration for Full Data: " << projected_time_first_join << " milliseconds\n";
    std::cout << "Projected Second Join Duration for Full Data: " << projected_time_second_join << " milliseconds\n";
    std::cout << "Projected Total Duration for Full Data: " << projected_total_time << " milliseconds\n";
    std::cout << "Projected memory size for the join: " 
              << 100*getRelationMemorySize(equi_join_result) + 100*getRelationMemorySize(equi_join_result_2) 
              << " bytes\n";

    delete io;
    return 0;
}
