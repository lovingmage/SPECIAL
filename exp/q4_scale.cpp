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

void runQuery(int relationASize, int relationBSize, const std::vector<std::pair<int, int>>& indexA, const std::vector<std::pair<int, int>>& indexB) {
    SecureRelation relationA(1, relationASize);
    init_relation(relationA, 1, relationASize);
    
    SecureRelation relationB(1, relationBSize);
    init_relation(relationB, 1, relationBSize);

    CountOperator count_op;
    IndexEquiJoinOperator index_join_op(indexA, indexB, 0, 0, IndexEquiJoinOperator::SMALLER_REL); 
    size_t mem_filter = getRelationMemorySize(relationA) + getRelationMemorySize(relationB);

    auto start_time = std::chrono::high_resolution_clock::now();
    relationA.sort_by_column(0);
    SecureRelation index_join_result = index_join_op.execute(relationA, relationB);
    size_t mem_join = getRelationMemorySize(index_join_result);

    index_join_result.sort_by_column(0);
    SecureRelation result = count_op.execute(index_join_result);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size for the query plan: " << mem_filter + mem_join << " bytes\n";
    std::cout << "Execution time: " << duration_index_join << " milliseconds\n\n";
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);
    
    // 2x sizes
    int relationASize = 211;
    int relationBSize = 16191;
    std::vector<std::pair<int, int>> indexA = {{0, 36}, {12, 82}, {32, 130}, {50, 144}, {50, 158}, {50, 178}, {50, 194}, {50, 210}};
    std::vector<std::pair<int, int>> indexB = {{0, 5288}, {5260, 10160}, {10110, 14116}, {14044, 14808}, {14708, 15086}, {14962, 15492}, {15342, 15800}, {15616, 16190}};
    runQuery(relationASize, relationBSize, indexA, indexB);

    // 4x sizes
    relationASize = 421;
    relationBSize = 32385;
    indexA = {{0, 72}, {24, 164}, {64, 260}, {100, 288}, {100, 316}, {100, 356}, {100, 388}, {100, 420}};
    indexB = {{0, 10576}, {10520, 20320}, {20220, 28232}, {28088, 29616}, {29416, 30172}, {29924, 30984}, {30684, 31600}, {31232, 32384}};
    runQuery(relationASize, relationBSize, indexA, indexB);


    // 8x sizes
    relationASize = 841;
    relationBSize = 64769;
    indexA = {{0, 144}, {48, 328}, {128, 520}, {200, 576}, {200, 632}, {200, 712}, {200, 776}, {200, 840}};
    indexB = {{0, 21152}, {21040, 40640}, {40440, 56464}, {56176, 59232}, {58832, 60344}, {59848, 61968}, {61368, 63200}, {62464, 64768}};
    runQuery(relationASize, relationBSize, indexA, indexB);


    delete io;
    return 0;
}
