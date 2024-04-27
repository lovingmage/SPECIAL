#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
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

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    SecureRelation relation1(3, 16);
    init_relation(relation1, 3, 16);
    relation1.print_relation("Relation 1 with all flags set to 1:");

    SecureRelation relation2(3, 16);
    init_relation(relation2, 3, 16);
    relation2.print_relation("Relation 2 with all flags set to 1:");

    std::vector<std::pair<int, int>> index1 = { {0,5}, {4,9} };
    std::vector<std::pair<int, int>> index2 = { {0,5}, {4,9} };

    // No Compaction
    IndexEquiJoinOperator index_join_op(index1, index2, 1, 1);
    SecureRelation index_join_result = index_join_op.execute(relation1, relation2);
    index_join_result.print_relation("Index Join Result (No Compaction):");

    // Compaction to the size of the smaller input relation
    IndexEquiJoinOperator index_join_op_small(index1, index2, 1, 1, IndexEquiJoinOperator::SMALLER_REL);
    SecureRelation index_join_result_small = index_join_op_small.execute(relation1, relation2);
    index_join_result_small.print_relation("Index Join Result (Compact to Smaller Relation):");

    // Compaction to the size of the larger input relation
    IndexEquiJoinOperator index_join_op_large(index1, index2, 1, 1, IndexEquiJoinOperator::LARGER_REL);
    SecureRelation index_join_result_large = index_join_op_large.execute(relation1, relation2);
    index_join_result_large.print_relation("Index Join Result (Compact to Larger Relation):");

    // Compaction to a fixed size
    int fixed_size = 3;
    IndexEquiJoinOperator index_join_op_fixed(index1, index2, 1, 1, IndexEquiJoinOperator::FIXED_SIZE, fixed_size);
    SecureRelation index_join_result_fixed = index_join_op_fixed.execute(relation1, relation2);
    index_join_result_fixed.print_relation("Index Join Result (Compact to Fixed Size):");

    // MF Compaction with specified v1 and v2
    double mf1 = 2;  // Example value for v1
    double mf2 = 3;  // Example value for v2
    IndexEquiJoinOperator index_join_op_mf(index1, index2, 1, 1, IndexEquiJoinOperator::MF, 0, mf1, mf2);
    SecureRelation index_join_result_mf = index_join_op_mf.execute(relation1, relation2);
    index_join_result_mf.print_relation("Index Join Result (MF Compaction):");


    delete io;
    return 0;
}
