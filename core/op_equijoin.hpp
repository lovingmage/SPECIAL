// equijoin.hpp

#ifndef EQUIJOIN_OPERATOR_HPP
#define EQUIJOIN_OPERATOR_HPP

#include "core/_op_binary.hpp"
#include <vector>

class EquiJoinOperator : public BinaryOperator {
public:
    int column_index1;  // The join column index for the first relation
    int column_index2;  // The join column index for the second relation

    EquiJoinOperator(int col_idx1, int col_idx2);

protected:
    SecureRelation operation(const SecureRelation& rel1, const SecureRelation& rel2) override;

};

// Definitions

EquiJoinOperator::EquiJoinOperator(int col_idx1, int col_idx2) 
    : column_index1(col_idx1), column_index2(col_idx2) {}

SecureRelation EquiJoinOperator::operation(const SecureRelation& rel1, const SecureRelation& rel2) {
    int result_rows = rel1.columns[0].size() * rel2.columns[0].size();
    int result_cols = rel1.columns.size() + rel2.columns.size();

    SecureRelation result(result_cols, result_rows);

    for (int i = 0; i < rel1.columns[0].size(); i++) {
        for (int j = 0; j < rel2.columns[0].size(); j++) {
            int result_row_index = i * rel2.columns[0].size() + j;
            
            // Copy the columns from the first relation
            for (int k = 0; k < rel1.columns.size(); k++) {
                result.columns[k][result_row_index] = rel1.columns[k][i];
            }

            // Copy the columns from the second relation
            for (int k = 0; k < rel2.columns.size(); k++) {
                result.columns[rel1.columns.size() + k][result_row_index] = rel2.columns[k][j];
            }

            // Set the join flag. 1 if the join condition is satisfied, 0 otherwise.
            emp::Bit join_condition = (rel1.columns[column_index1][i] == rel2.columns[column_index2][j]) 
                                      & (rel1.flags[i] == emp::Integer(1, 1, ALICE)) 
                                      & (rel2.flags[j] == emp::Integer(1, 1, ALICE));
                                      
            result.flags[result_row_index] = emp::If(join_condition, emp::Integer(1, 1, ALICE), emp::Integer(1, 0, ALICE));
        }
    }

    return result;
}


#endif // EQUIJOIN_HPP
