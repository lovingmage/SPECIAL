#include "emp-sh2pc/emp-sh2pc.h"
#include "core/relation.hpp"
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"  // Including the new header file
#include <string>
#include <cstdlib>
#include <ctime>

using namespace emp;

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

// Utility function to print the relation's details
void print_relation(const std::string& label, const SecureRelation& relation) {
    std::cout << label << "\n";
    for (size_t row = 0; row < relation.columns[0].size(); ++row) {
        for (size_t col = 0; col < relation.columns.size(); ++col) {
            std::cout << relation.columns[col][row].reveal<int>() << "\t";
        }
        std::cout << "| Flag: " << relation.flags[row].reveal<int>() << "\n";
    }
    std::cout << "\n";
}

int main(int argc, char** argv) {
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    // Create and initialize a relation with all flags set to 1
    SecureRelation relation_all_one(3, 16);
    init_relation(relation_all_one, 3, 16);
    relation_all_one.print_relation("Relation with all flags set to 1:");

    EquiJoinOperator join_op(1, 1); // Joining on second column for both relations
    SecureRelation join_result_all_one = join_op.execute(relation_all_one, relation_all_one);
    join_result_all_one.print_relation("Join Result (All Flags 1):");

    // Create and initialize a relation with mixed flags
    SecureRelation relation_mixed_flags(3, 16);
    init_relation(relation_mixed_flags, 3, 16, true);
    relation_mixed_flags.print_relation("Relation with mixed flags:");

    SecureRelation join_result_mixed = join_op.execute(relation_mixed_flags, relation_mixed_flags);
    join_result_mixed.print_relation("Join Result (Mixed Flags):");

    // Sort the join result by flag
    join_result_mixed.sort_by_flag();
    join_result_mixed.print_relation("Join Result Sorted by Flag (Mixed Flags):");


    delete io;
    return 0;
}
