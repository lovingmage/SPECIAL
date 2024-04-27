#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
#include "core/op_agg_count.hpp"
#include "core/relation.hpp"

void init_relation(SecureRelation& relation, int num_cols, int num_rows, bool mixed_flags = false) {
    for (int col = 0; col < num_cols; ++col) {
        for (int row = 0; row < num_rows; ++row) {
            relation.columns[col][row] = Integer(32, rand() % 100, ALICE);
        }
    }
    for (int row = 0; row < num_rows; ++row) {
        if (mixed_flags) {
            relation.flags[row] = Integer(1, rand() % 2, ALICE);
        } else {
            relation.flags[row] = Integer(1, 1, ALICE);
        }
    }
}

size_t getRelationMemorySize(const SecureRelation& relation) {
    size_t memorySize = 0;
    for (const auto& column : relation.columns) {
        memorySize += column.size() * sizeof(emp::Integer);
    }
    memorySize += relation.flags.size() * sizeof(emp::Integer);
    return memorySize;
}

void runQuery(int relationASize, int relationBSize, int relationCSize, int relationDSize, int relationESize,
              const std::vector<std::pair<int, int>>& indexA, 
              const std::vector<std::pair<int, int>>& indexB, 
              const std::vector<std::pair<int, int>>& indexC, 
              const std::vector<std::pair<int, int>>& indexD, 
              const std::vector<std::pair<int, int>>& indexE,
              size_t mf_order, size_t mf_trans, size_t mf_disp) {
    SecureRelation relationA(1, relationASize);
    init_relation(relationA, 1, relationASize);
    SecureRelation relationB(1, relationBSize);
    init_relation(relationB, 1, relationBSize);
    SecureRelation relationC(1, relationCSize);
    init_relation(relationC, 1, relationCSize);
    SecureRelation relationD(1, relationDSize);
    init_relation(relationD, 1, relationDSize);
    SecureRelation relationE(1, relationESize);
    init_relation(relationE, 1, relationESize);

    CountOperator count_op;

    auto start_time = std::chrono::high_resolution_clock::now();

    relationA.sort_by_column(0);
    relationE.sort_by_column(0);
    IndexEquiJoinOperator index_join_op(indexA, indexE, 0, 0, IndexEquiJoinOperator::SMALLER_REL);
    SecureRelation index_join_result = index_join_op.execute(relationA, relationE);

    auto newIndex = index_join_op.rebuild_index();
    relationD.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_2(newIndex, indexD, 0, 0, IndexEquiJoinOperator::LARGER_REL);
    SecureRelation index_join_result_2 = index_join_op_2.execute(index_join_result, relationD);

    auto newIndex_2 = index_join_op_2.rebuild_index();
    relationC.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_3(newIndex_2, indexC, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order, mf_disp);
    SecureRelation index_join_result_3 = index_join_op_3.execute(index_join_result_2, relationC);

    auto newIndex2 = index_join_op_3.rebuild_index();
    relationB.sort_by_column(0);
    

#ifndef EFFICIENT_MODE
    IndexEquiJoinOperator index_join_op_4(newIndex2, indexB, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order * mf_disp, mf_trans);
    SecureRelation index_join_result_4 = index_join_op_4.execute(index_join_result_3, relationB);
    SecureRelation result = count_op.execute(index_join_result_4);
    size_t mem_index_join_result_4 = getRelationMemorySize(index_join_result_4);
#else
    /* This efficient implementation method saves actively recycle unused memory from Join 4 */
    
    size_t mem_index_join_result_4 = 0;
    for (size_t i = 0; i < newIndex2.size(); ++i) {
    // Fetch bins from the third join result and relation B based on the given indexes
    SecureRelation binA(1, newIndex2[i].second - newIndex2[i].first + 1);
    for (int j = newIndex2[i].first; j <= newIndex2[i].second; ++j) {
        binA.columns[0][j - newIndex2[i].first] = index_join_result_3.columns[0][j];
    }

    SecureRelation binB(1, indexB[i].second - indexB[i].first + 1);
    for (int j = indexB[i].first; j <= indexB[i].second; ++j) {
        binB.columns[0][j - indexB[i].first] = relationB.columns[0][j];
    }

    // Perform a join between the fetched bins 
    EquiJoinOperator bin_join_op(0, 0);
    SecureRelation bin_join_result = bin_join_op.execute(binA, binB);
    compaction_size = std::min({static_cast<int>(mf_trans * binA.columns[0].size()),
                                static_cast<int>(mf_order * mf_disp * binB.columns[0].size()),
                                static_cast<int>(binA.columns[0].size() * binB.columns[0].size())});

    // Compact the join result based on the computed size
    mem_index_join_result_4 += getRelationMemorySize(bin_join_result);

    // Aggregation
    SecureRelation result = count_op.execute(bin_join_result);

    // Free memory by clearing the temporary bin relations and the join result
    binA.columns.clear();
    binA.flags.clear();
    binB.columns.clear();
    binB.flags.clear();
    bin_join_result.columns.clear();
    bin_join_result.flags.clear();
}
#endif
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): "
              << getRelationMemorySize(index_join_result) + getRelationMemorySize(index_join_result_2) +
                     getRelationMemorySize(index_join_result_3) + mem_index_join_result_4 + getRelationMemorySize(relationA) +
                     getRelationMemorySize(relationB) + getRelationMemorySize(relationC) + getRelationMemorySize(relationD) + getRelationMemorySize(relationE)
              << " bytes\n";
    std::cout << "Index EquiJoin execution time: " << duration_index_join << " milliseconds\n\n";
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    // 2x data
    int relationASize = 212;
    int relationBSize = 16192;
    int relationCSize = 10852;
    int relationDSize = 788;
    int relationESize = 382;
    std::vector<std::pair<int, int>> indexA = {{0, 36}, {12, 82}, {32, 130}, {50, 144}, {50, 158}, {50, 178}, {50, 194}, {50, 210}};
    std::vector<std::pair<int, int>> indexB = {{0, 5288}, {5260, 10160}, {10110, 14116}, {14044, 14808}, {14708, 15086}, {14962, 15492}, {15342, 15800}, {15616, 16190}};
    std::vector<std::pair<int, int>> indexC = {{0, 3316}, {3286, 6664}, {6604, 9392}, {9302, 9808}, {9694, 10046}, {9902, 10292}, {10122, 10582}, {10386, 10850}};
    std::vector<std::pair<int, int>> indexD = {{0, 202}, {174, 416}, {358, 604}, {524, 682}, {572, 710}, {578, 738}, {580, 764}, {582, 786}};
    std::vector<std::pair<int, int>> indexE = {{0, 42}, {6, 100}, {40, 140}, {54, 188}, {74, 230}, {90, 276}, {104, 318}, {118, 380}};
    size_t mf_order = 4;
    size_t mf_trans = 141;
    size_t mf_disp = 4;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);

    // 4x data
    relationASize = 421;
    relationBSize = 32385;
    relationCSize = 21705;
    relationDSize = 1573;
    relationESize = 761;
    indexA = {{0, 72}, {24, 164}, {64, 260}, {100, 288}, {100, 316}, {100, 356}, {100, 388}, {100, 420}};
    indexB = {{0, 10576}, {10520, 20320}, {20220, 28232}, {28088, 29616}, {29416, 30172}, {29924, 30984}, {30684, 31600}, {31232, 32384}};
    indexC = {{0, 6632}, {6572, 13328}, {13208, 18784}, {18604, 19616}, {19388, 20092}, {19804, 20584}, {20244, 21164}, {20772, 21704}};
    indexD = {{0, 404}, {348, 832}, {716, 1208}, {1048, 1364}, {1144, 1420}, {1156, 1476}, {1160, 1528}, {1164, 1572}};
    indexE = {{0, 84}, {12, 200}, {80, 280}, {108, 376}, {148, 460}, {180, 552}, {208, 636}, {236, 760}};
    mf_order = 9;
    mf_trans = 288;
    mf_disp = 11;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);

    delete io;
    return 0;
}
