// exp - query 4
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

    CountOperator count_op;

    // DP indexes
    std::vector<std::pair<int, int>> indexA = { {0, 18}, {6, 41}, {16, 65}, {25, 72}, {25, 79}, {25, 89}, {25, 97}, {25, 105}};
    std::vector<std::pair<int, int>> indexB = { {0, 2644}, {2630, 5080}, {5055, 7058}, {7022, 7404}, {7354, 7543}, {7481, 7746}, {7671, 7900}, {7808, 8095} };
    size_t mem_filter = getRelationMemorySize(relationA) + getRelationMemorySize(relationB);


    // Index (non-expanding) join of Disp and Client on client ID
    IndexEquiJoinOperator index_join_op(indexA, indexB, 0, 0, IndexEquiJoinOperator::SMALLER_REL); 


    //Step 1. Bypass filters

    auto start_time = std::chrono::high_resolution_clock::now();

    //Step 2. Index join
    relationA.sort_by_column(0);
    SecureRelation index_join_result = index_join_op.execute(relationA, relationB);
    size_t mem_join = getRelationMemorySize(index_join_result);

    //Step 3. Simulate groupby perf using sorting + count
    index_join_result.sort_by_column(0);
    SecureRelation result = count_op.execute(index_join_result);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size for the query plan: " 
              << mem_filter + mem_join
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_index_join 
              << " milliseconds\n\n";



    delete io;
    return 0;
}
