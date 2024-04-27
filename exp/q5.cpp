// exp - query 3
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
#include "core/op_agg_count.hpp"
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


    // Q4 - Caplan
    // Sim SargAcc of Account.account_id=18 data [sized 46]
    SecureRelation relationA(1, 106);
    init_relation(relationA, 1, 106);
    
    // Sim SeqAcc of Trans data with operation='VYBER KARTOU' [sized 8036]
    SecureRelation relationB(1, 8096);
    init_relation(relationB, 1, 8096);

    // Sim SeqAcc of Order data with k_symbol='LEASING' [sized 394]
    SecureRelation relationC(1, 394);
    init_relation(relationC, 1, 394);

    // DP indexes
    std::vector<std::pair<int, int>> indexA = { {0, 18}, {6, 41}, {16, 65}, {25, 72}, {25, 79}, {25, 89}, {25, 97}, {25, 105}};
    std::vector<std::pair<int, int>> indexB = { {0, 2644}, {2630, 5080}, {5055, 7058}, {7022, 7404}, {7354, 7543}, {7481, 7746}, {7671, 7900}, {7808, 8095} };
    std::vector<std::pair<int, int>> indexC = { {0, 101}, {87, 208}, {179, 302}, {262, 341}, {286, 355}, {289, 369}, {290, 382}, {291, 393} };

    // Setup count operator
    CountOperator count_op;

    //Step 1. Bypass filters

    auto start_time = std::chrono::high_resolution_clock::now();

    //Step 2. Index join A-C
    relationA.sort_by_column(0);
    relationC.sort_by_column(0);

    // Index (non-expanding) join of Account and Order on account_id
    // Join order (A-C)-B
    IndexEquiJoinOperator index_join_op(indexA, indexC, 0, 0, IndexEquiJoinOperator::SMALLER_REL); 
    SecureRelation index_join_result = index_join_op.execute(relationA, relationC);

    //Step 3. Reconstruct indexes and join (A-C) with B
    auto newIndex = index_join_op.rebuild_index();
#ifdef DEBUG_LOG
    // Printing out the rebuilt index
        for (const auto& indexPair : newIndex) {
        std::cout << "Start Index: " << indexPair.first << ", End Index: " << indexPair.second << std::endl;
    }
#endif
    relationB.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_2(newIndex, indexB, 0, 0, IndexEquiJoinOperator::LARGER_REL); 
    SecureRelation index_join_result_2 = index_join_op_2.execute(index_join_result, relationB);
    

    //Step 4. Distinct (provactive distinct - no secure computing costs)
    index_join_result_2.sort_by_column(0);
    SecureRelation result = count_op.execute(index_join_result_2);


    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): " 
              << getRelationMemorySize(index_join_result) + \
                 getRelationMemorySize(index_join_result_2) + \
                 getRelationMemorySize(relationA) + \
                 getRelationMemorySize(relationB) + getRelationMemorySize(relationC)
              << " bytes\n";
    std::cout << "Index EquiJoin execution time: " 
              << duration_index_join 
              << " milliseconds\n\n";

    
    /*
    // Naive nested loop join (EquiJoin)
    EquiJoinOperator equijoin_op(0, 0);

    start_time = std::chrono::high_resolution_clock::now();
    SecureRelation equi_join_result = equijoin_op.execute(relationA, relationB);
        // Print out the memory size of the result relation
    std::cout << "Memory size of the standard join result relation: " 
              << getRelationMemorySize(equi_join_result) 
              << " bytes" << std::endl;

    end_time = std::chrono::high_resolution_clock::now();
    auto duration_equijoin = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Naive Nested Loop Join (EquiJoin) time: " << duration_equijoin << " milliseconds" << std::endl;
    */

    delete io;
    return 0;
}
