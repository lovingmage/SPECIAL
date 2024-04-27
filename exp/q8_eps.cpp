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
    IndexEquiJoinOperator index_join_op_2(newIndex, indexD, 0, 0, IndexEquiJoinOperator::MF, 0, 1, 2);
    SecureRelation index_join_result_2 = index_join_op_2.execute(index_join_result, relationD);

    auto newIndex_2 = index_join_op_2.rebuild_index();
    relationC.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_3(newIndex_2, indexC, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order, mf_disp);
    SecureRelation index_join_result_3 = index_join_op_3.execute(index_join_result_2, relationC);

    auto newIndex2 = index_join_op_3.rebuild_index();
    relationB.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_4(newIndex2, indexB, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order * mf_disp, mf_trans);
    SecureRelation index_join_result_4 = index_join_op_4.execute(index_join_result_3, relationB);

    SecureRelation result = count_op.execute(index_join_result_4);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): "
              << getRelationMemorySize(index_join_result) + getRelationMemorySize(index_join_result_2) +
                     getRelationMemorySize(index_join_result_3) + getRelationMemorySize(index_join_result_4) + getRelationMemorySize(relationA) +
                     getRelationMemorySize(relationB) + getRelationMemorySize(relationC) + getRelationMemorySize(relationD) + getRelationMemorySize(relationE)
              << " bytes\n";
    std::cout << "Index EquiJoin execution time: " << duration_index_join << " milliseconds\n\n";
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    // eps = 0.1 (hardcoded config)
    int relationASize = 139;
    int relationBSize = 8152;
    int relationCSize = 5493;
    int relationDSize = 453;
    int relationESize = 228;
    std::vector<std::pair<int, int>>  indexA = {{0, 19}, {0, 44}, {0, 77}, {9, 102}, {9, 109}, {9, 113}, {9, 131}, {9, 138}};
    std::vector<std::pair<int, int>>  indexB = {{0, 2647}, {2617, 5089}, {5039, 7069}, {7010, 7420}, {7314, 7559}, {7437, 7767}, {7622, 7926}, {7748, 8151}};
    std::vector<std::pair<int, int>>  indexC = {{0, 1635}, {1629, 3279}, {3249, 4676}, {4611, 4903}, {4828, 5013}, {4930, 5195}, {5064, 5344}, {5196, 5492}};
    std::vector<std::pair<int, int>>  indexD = {{0, 104}, {83, 233}, {160, 329}, {238, 370}, {268, 390}, {272, 437}, {275, 446}, {275, 452}};
    std::vector<std::pair<int, int>>  indexE = {{0, 19}, {0, 44}, {6, 96}, {13, 124}, {30, 149}, {30, 184}, {42, 204}, {42, 227}};
    int mf_order = 4;
    int mf_trans = 87;
    int mf_disp = 5;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);


    // eps = 0.2 (hardcoded config)
    relationASize = 111;
    relationBSize = 8123;
    relationCSize = 5445;
    relationDSize = 398;
    relationESize = 199;
    indexA = {{0, 24}, {0, 46}, {3, 64}, {16, 71}, {16, 80}, {16, 102}, {16, 107}, {16, 110}};
    indexB = {{0, 2653}, {2630, 5091}, {5048, 7087}, {7009, 7441}, {7334, 7577}, {7448, 7774}, {7637, 7925}, {7776, 8122}};
    indexC = {{0, 1633}, {1623, 3268}, {3230, 4655}, {4599, 4878}, {4807, 4991}, {4908, 5133}, {5042, 5274}, {5170, 5442}};
    indexD = {{0, 102}, {86, 208}, {175, 299}, {261, 345}, {284, 358}, {284, 367}, {284, 380}, {289, 397}};
    indexE = {{0, 18}, {12, 45}, {27, 70}, {35, 94}, {44, 111}, {45, 127}, {46, 162}, {59, 198}};
    mf_order = 4;
    mf_disp = 4;
    mf_trans = 77;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);

    // eps = 0.5 (hardcoded config)
    relationASize = 103;
    relationBSize = 8100;
    relationCSize = 5431;
    relationDSize = 384;
    relationESize = 192;
    indexA = {{0, 19}, {3, 43}, {10, 66}, {19, 75}, {19, 83}, {19, 89}, {19, 96}, {19, 102}};
    indexB = {{0, 2645}, {2633, 5078}, {5055, 7054}, {7021, 7399}, {7355, 7537}, {7482, 7743}, {7673, 7894}, {7812, 8099}};
    indexC = {{0, 1634}, {1625, 3267}, {3244, 4662}, {4624, 4888}, {4840, 5001}, {4942, 5151}, {5073, 5295}, {5203, 5430}};
    indexD = {{0, 97}, {86, 204}, {178, 294}, {259, 336}, {284, 350}, {284, 365}, {285, 379}, {285, 383}};
    indexE = {{0, 17}, {4, 47}, {22, 72}, {29, 96}, {44, 120}, {52, 134}, {57, 155}, {63, 191}};
    mf_order = 4;
    mf_disp = 3;
    mf_trans = 72;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);

     // eps = 1 (hardcoded config)
    relationASize = 106;
    relationBSize = 8096;
    relationCSize = 5426;
    relationDSize = 394;
    relationESize = 191;
    indexA = {{0, 18}, {6, 41}, {16, 65}, {25, 72}, {25, 79}, {25, 89}, {25, 97}, {25, 105}};
    indexB = {{0, 2644}, {2630, 5080}, {5055, 7058}, {7022, 7404}, {7354, 7543}, {7481, 7746}, {7671, 7900}, {7808, 8095}};
    indexC = {{0, 1658}, {1643, 3332}, {3302, 4696}, {4651, 4904}, {4847, 5023}, {4951, 5146}, {5061, 5291}, {5193, 5425}};
    indexD = {{0, 101}, {87, 208}, {179, 302}, {262, 341}, {286, 355}, {289, 369}, {290, 382}, {291, 393}};
    indexE = {{0, 21}, {3, 50}, {20, 70}, {27, 94}, {37, 115}, {45, 138}, {52, 159}, {59, 190}};
    mf_order = 2;
    mf_disp = 3;
    mf_trans = 71;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);

    // eps = 10 (hardcoded config)
    relationASize = 103;
    relationBSize = 8090;
    relationCSize = 5426;
    relationDSize = 398;
    relationESize = 187;
    indexA = {{0, 18}, {4, 41}, {13, 64}, {22, 72}, {22, 79}, {22, 86}, {22, 94}, {22, 102}};
    indexB = {{0, 2645}, {2631, 5081}, {5053, 7060}, {7018, 7406}, {7350, 7545}, {7475, 7748}, {7664, 7898}, {7800, 8089}};
    indexC = {{0, 1637}, {1623, 3271}, {3243, 4663}, {4621, 4890}, {4834, 5005}, {4935, 5148}, {5064, 5291}, {5193, 5425}};
    indexD = {{0, 101}, {87, 207}, {179, 303}, {261, 343}, {287, 357}, {287, 372}, {288, 386}, {288, 397}};
    indexE = {{0, 19}, {5, 49}, {21, 70}, {28, 94}, {38, 116}, {46, 135}, {51, 156}, {58, 186}};
    mf_order = 2;
    mf_trans = 70;
    mf_disp = 2;
    runQuery(relationASize, relationBSize, relationCSize, relationDSize, relationESize, indexA, indexB, indexC, indexD, indexE, mf_order, mf_trans, mf_disp);


    delete io;
    return 0;
}
