// exp - selection
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"
#include "core/op_agg_count.hpp"
#include "core/relation.hpp"
#include "core/op_filter.hpp"



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

    int rel_sz = 1 << 18;
    int sel_sz = 1 << 14;
    // Setup filter
    FilterOperator filter_by_fixed_value(0, Integer(32, 1, ALICE), "eq");
    // Naive nested loop join (EquiJoin)
    EquiJoinOperator equijoin_op(0, 0);
    
    // Sim SeqAcc of account data [sized 4502]
    SecureRelation relationA(1, rel_sz);
    init_relation(relationA, 1, rel_sz);

    //Micro benchmark. Selections

    auto start_time = std::chrono::high_resolution_clock::now();
    //1. Standard selection
    SecureRelation filtered_relationA = filter_by_fixed_value.execute(relationA);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Print out the results
    std::cout << "Standard Selection Results:\n";
    std::cout << "---------\n";
    std::cout << "Output size: (Memory size of the total input data): " 
              << getRelationMemorySize(filtered_relationA) 
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration 
              << " milliseconds\n\n";

    auto start_time_2 = std::chrono::high_resolution_clock::now();
    //2. OPac selection
    SecureRelation filtered_relationA_2 = filter_by_fixed_value.execute(relationA);
    filtered_relationA_2.compact(sel_sz);
    auto end_time_2 = std::chrono::high_resolution_clock::now();
    auto duration_2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_2 - start_time_2).count();

    // Print out the results
    std::cout << "OPac Selection Results:\n";
    std::cout << "---------\n";
    std::cout << "Output size: (Memory size of the total input data): " 
              << getRelationMemorySize(filtered_relationA_2) 
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_2 
              << " milliseconds\n\n";

    auto start_time_3 = std::chrono::high_resolution_clock::now();
    //3. DC selection
    SecureRelation filtered_relationA_3(1, sel_sz);
    init_relation(filtered_relationA_3, 1, sel_sz);
    auto end_time_3 = std::chrono::high_resolution_clock::now();
    auto duration_3 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_3 - start_time_3).count();

    // Print out the results
    std::cout << "DC Selection Results:\n";
    std::cout << "---------\n";
    std::cout << "Output size: (Memory size of the total input data): " 
              << getRelationMemorySize(filtered_relationA_3) 
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_3 
              << " milliseconds\n\n";

    SecureRelation tmp(1, sel_sz);
    init_relation(tmp, 1, sel_sz);
    auto start_time_4 = std::chrono::high_resolution_clock::now();
    //3. SP selection (simulated using crossproduct) - size same as OPac
    equijoin_op.execute(relationA, tmp);

    auto end_time_4 = std::chrono::high_resolution_clock::now();
    auto duration_4 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_4 - start_time_4).count();

    // Print out the results
    std::cout << "SP Selection Results:\n";
    std::cout << "---------\n";
    std::cout << "Output size: (Memory size of the total input data): " 
              << getRelationMemorySize(filtered_relationA_2) 
              << " bytes\n";
    std::cout << "Execution time: " 
              << duration_4 
              << " milliseconds\n\n";


    delete io;
    return 0;
}
