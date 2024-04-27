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

    // eps = 0.1 (hardcoded config)
    int relationASize = 139;
    int relationBSize = 8152;
    std::vector<std::pair<int, int>> indexA = {{0, 19}, {0, 44}, {0, 77}, {9, 102}, {9, 109}, {9, 113}, {9, 131}, {9, 138}};
    std::vector<std::pair<int, int>> indexB = {{0, 2647}, {2617, 5089}, {5039, 7069}, {7010, 7420}, {7314, 7559}, {7437, 7767}, {7622, 7926}, {7748, 8151}};
    runQuery(relationASize, relationBSize, indexA, indexB);

    // eps = 0.2 (hardcoded config)
    relationASize = 111;
    relationBSize = 8123;
    indexA = {{0, 24}, {0, 46}, {3, 64}, {16, 71}, {16, 80}, {16, 102}, {16, 107}, {16, 110}};
    indexB = {{0, 2653}, {2630, 5091}, {5048, 7087}, {7009, 7441}, {7334, 7577}, {7448, 7774}, {7637, 7925}, {7776, 8122}};
    runQuery(relationASize, relationBSize, indexA, indexB);

    // eps = 0.5 (hardcoded config)
    relationASize = 103;
    relationBSize = 8100;
    indexA = {{0, 19}, {3, 43}, {10, 66}, {19, 75}, {19, 83}, {19, 89}, {19, 96}, {19, 102}};
    indexB = {{0, 2645}, {2633, 5078}, {5055, 7054}, {7021, 7399}, {7355, 7537}, {7482, 7743}, {7673, 7894}, {7812, 8099}};
    runQuery(relationASize, relationBSize, indexA, indexB);

    // eps = 1 (hardcoded config)
    relationASize = 106;
    relationBSize = 8096;
    indexA = {{0, 18}, {6, 41}, {16, 65}, {25, 72}, {25, 79}, {25, 89}, {25, 97}, {25, 105}};
    indexB = {{0, 2644}, {2630, 5080}, {5055, 7058}, {7022, 7404}, {7354, 7543}, {7481, 7746}, {7671, 7900}, {7808, 8095}};
    runQuery(relationASize, relationBSize, indexA, indexB);

    // eps = 10 (hardcoded config)
    relationASize = 103;
    relationBSize = 8090;
    indexA = {{0, 18}, {4, 41}, {13, 64}, {22, 72}, {22, 79}, {22, 86}, {22, 94}, {22, 102}};
    indexB = {{0, 2645}, {2631, 5081}, {5053, 7060}, {7018, 7406}, {7350, 7545}, {7475, 7748}, {7664, 7898}, {7800, 8089}};
    runQuery(relationASize, relationBSize, indexA, indexB);


    delete io;
    return 0;
}
