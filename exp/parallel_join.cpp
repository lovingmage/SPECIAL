#include "core/op_equijoin.hpp"
#include "core/op_agg_count.hpp"
#include "core/relation.hpp"

// Utility function to initialize a relation with random values
void init_relation(SecureRelation& relation, int num_cols, int num_rows) {
    for (int col = 0; col < num_cols; ++col) {
        for (int row = 0; row < num_rows; ++row) {
            relation.columns[col][row] = Integer(32, rand() % 100, ALICE);  // Random values
        }
    }

    for (int row = 0; row < num_rows; ++row) {
        relation.flags[row] = Integer(1, 1, ALICE);  // All flags set to 1
    }
}

int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <party> <port> <left_size> <right_size> <compaction_size>" << std::endl;
        return 1;
    }

    int party = std::atoi(argv[1]);
    int port = std::atoi(argv[2]);
    int left_size = std::atoi(argv[3]);
    int right_size = std::atoi(argv[4]);
    int compaction_size = std::atoi(argv[5]);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    // Initialize relations
    SecureRelation relationA(1, left_size);
    SecureRelation relationB(1, right_size);
    init_relation(relationA, 1, left_size);
    init_relation(relationB, 1, right_size);

    // Perform equijoin
    EquiJoinOperator equijoin_op(0, 0);
    SecureRelation join_result = equijoin_op.execute(relationA, relationB);
    join_result.compact(compaction_size);

    delete io;
    return 0;
}
