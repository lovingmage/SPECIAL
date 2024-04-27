// exp - query 8
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
    
    // Sim SargAcc of Trans data with operation='VYBER KARTOU' [sized 8036]
    SecureRelation relationB(1, 8096);
    init_relation(relationB, 1, 8096);

    // Sim SeqAcc of Disp [sized 5426]
    SecureRelation relationC(1, 5426);
    init_relation(relationC, 1, 5426);

    // Sim SargAcc of Order with k_symbol = 'LEASING' [sized 394]
    SecureRelation relationD(1, 394);
    init_relation(relationD, 1, 394);

    // Sim SargAcc of Loan with duration = 36 [sized 191]
    SecureRelation relationE(1, 191);
    init_relation(relationE, 1, 191);

    // DP indexes
    std::vector<std::pair<int, int>> indexA = { {0, 18}, {6, 41}, {16, 65}, {25, 72}, {25, 79}, {25, 89}, {25, 97}, {25, 105}};
    std::vector<std::pair<int, int>> indexB = { {0, 2644}, {2630, 5080}, {5055, 7058}, {7022, 7404}, {7354, 7543}, {7481, 7746}, {7671, 7900}, {7808, 8095} };
    std::vector<std::pair<int, int>> indexC = { {0, 1658}, {1643, 3332}, {3302, 4696}, {4651, 4904}, {4847, 5023}, {4951, 5146}, {5061, 5291}, {5193, 5425} };
    std::vector<std::pair<int, int>> indexD = { {0, 101}, {87, 208}, {179, 302}, {262, 341}, {286, 355}, {289, 369}, {290, 382}, {291, 393} };
    std::vector<std::pair<int, int>> indexE = { {0, 21}, {3, 50}, {20, 70}, {27, 94}, {37, 115}, {45, 138}, {52, 159}, {59, 190}};
    size_t mf_order = 2;
    size_t mf_disp = 3;
    size_t mf_trans = 72;

    // Setup count operator
    CountOperator count_op;

    //Step 1. Bypass filters 
    
    auto start_time = std::chrono::high_resolution_clock::now();

    //Step 2. Index join A-E
    relationA.sort_by_column(0);
    relationE.sort_by_column(0);
    IndexEquiJoinOperator index_join_op(indexA, indexE, 0, 0, IndexEquiJoinOperator::SMALLER_REL); 
    SecureRelation index_join_result = index_join_op.execute(relationA, relationE);

    //Step 3. Reconstruct indexes and join (A-E) with D 
    auto newIndex = index_join_op.rebuild_index();
    relationD.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_2(newIndex, indexD, 0, 0, IndexEquiJoinOperator::MF, 0, 1, mf_order); 
    SecureRelation index_join_result_2 = index_join_op_2.execute(index_join_result, relationD);

    //Step 4. Reconstruct indexes and join ((A-E)-D) with C 
    auto newIndex_2 = index_join_op_2.rebuild_index();
    relationC.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_3(newIndex_2, indexC, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order, mf_disp); 
    SecureRelation index_join_result_3 = index_join_op_3.execute(index_join_result_2, relationC);
    
    //Step 5. Reconstruct indexes and join (((A-E)-D)-C) with B
    auto newIndex2 = index_join_op_3.rebuild_index();
    relationB.sort_by_column(0);
    IndexEquiJoinOperator index_join_op_4(newIndex2, indexB, 0, 0, IndexEquiJoinOperator::MF, 0, mf_order*mf_disp, mf_trans); 
    SecureRelation index_join_result_4 = index_join_op_4.execute(index_join_result_3, relationB);

    //Step 5. Max (simulate max using count, same imp)
    SecureRelation result = count_op.execute(index_join_result_4);


    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_index_join = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Results:\n";
    std::cout << "---------\n";
    std::cout << "Memory size (query plan): " 
              << getRelationMemorySize(index_join_result) + \
                 getRelationMemorySize(index_join_result_2) + \
                 getRelationMemorySize(index_join_result_3) + \
                 getRelationMemorySize(index_join_result_4) + \
                 getRelationMemorySize(relationA) + \
                 getRelationMemorySize(relationB) + \
                 getRelationMemorySize(relationC) + \
                 getRelationMemorySize(relationD) + \
                 getRelationMemorySize(relationE)
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
