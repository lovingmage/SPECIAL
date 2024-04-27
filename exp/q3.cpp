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


    // Q3 - Caplan
    // Sim SargAcc of Disp data [sized 869]
    SecureRelation relationA(1, 870);
    init_relation(relationA, 1, 870);
    
    // Sim SeqAcc of Client data [sized 5369]
    SecureRelation relationB(1, 112);
    init_relation(relationB, 1, 112);

    size_t mem_filter = getRelationMemorySize(relationA) + getRelationMemorySize(relationB);


    CountOperator count_op;

    // DP indexes
    std::vector<std::pair<int, int>> indexA = { {0, 292}, {213, 581}, {502, 800}, {721, 834}, {755, 854}, {775, 869}, {808, 869}, {846, 869} };
    std::vector<std::pair<int, int>> indexB = { {0, 16}, {7, 44}, {21, 73}, {35, 81}, {35, 90}, {35, 96}, {35, 104}, {35, 111} };

    // Index (non-expanding) join of Disp and Client on client ID
    IndexEquiJoinOperator index_join_op(indexA, indexB, 0, 0, IndexEquiJoinOperator::SMALLER_REL); 


    //Step 1. Bypass filters

    auto start_time = std::chrono::high_resolution_clock::now();
    //Step 2. Index join
    SecureRelation index_join_result = index_join_op.execute(relationA, relationB);
    size_t mem_join = getRelationMemorySize(index_join_result);

    //Step 3. Count distinct (already sorted)
    SecureRelation result = count_op.execute(index_join_result);
    size_t mem_cnt = getRelationMemorySize(index_join_result);


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
