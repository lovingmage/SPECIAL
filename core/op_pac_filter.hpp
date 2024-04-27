#ifndef PACFILTER_OPERATOR_HPP
#define PACFILTER_OPERATOR_HPP

#include "core/_op_unary.hpp"
#include <string>
#include <vector>

class PACFilterOperator : public UnaryOperator {
public:
    int column_index; // The index of the column on which the filter is applied
    emp::Integer target_value; // Target value for comparison, if it's a single value
    std::vector<emp::Integer> target_column; // Column for comparison, if applicable
    std::string condition; // Condition: "gt", "geq", "lt", "leq", "eq", "neq"
    int truncation_size; // Desired size of the output relation after filtering

    emp::Bit compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition);
    SecureRelation operation(const SecureRelation& input) override;

    // Constructor when target is a single value
    PACFilterOperator(int col_idx, const emp::Integer& target, const std::string& cnd, int trunc_size);

    // Constructor when target is a column
    PACFilterOperator(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd, int trunc_size);
};

// Definitions

PACFilterOperator::PACFilterOperator(int col_idx, const emp::Integer& target, const std::string& cnd, int trunc_size) 
    : column_index(col_idx), target_value(target), condition(cnd), truncation_size(trunc_size) {}

PACFilterOperator::PACFilterOperator(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd, int trunc_size) 
    : column_index(col_idx), target_column(target_col), condition(cnd), truncation_size(trunc_size) {}

emp::Bit PACFilterOperator::compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition) {
    if (condition == "gt") return a > b;
    if (condition == "geq") return a >= b;
    if (condition == "lt") return a < b;
    if (condition == "leq") return a <= b;
    if (condition == "eq") return a == b;
    if (condition == "neq") return a != b;
    return emp::Bit(false); // Default to false for unsupported conditions
}

SecureRelation PACFilterOperator::operation(const SecureRelation& input) {
    SecureRelation output(input.columns.size(), truncation_size); // Initialize output relation with truncation size

    emp::Integer last_written_index(32, -1, ALICE);  // -1 indicates that no actual write has been done yet
    emp::Integer actual_writes_counter(32, 0, ALICE); // Counts the actual writes made

    // Initialize output's flags to all zeros
    for (int j = 0; j < truncation_size; j++) {
        output.flags[j] = Integer(1, 0, ALICE);
    }

    for (int i = 0; i < input.columns[0].size(); i++) {
        emp::Bit satisfies_condition;
        if (target_column.empty()) { // If the target is a single value
            satisfies_condition = compare(input.columns[column_index][i], target_value, condition) & (input.flags[i] == Integer(1, 1, ALICE));
        } else { // If the target is a column
            satisfies_condition = compare(input.columns[column_index][i], target_column[i], condition) & (input.flags[i] == Integer(1, 1, ALICE));
        }

        emp::Bit is_write_position = (last_written_index + Integer(32, 1, ALICE) < Integer(32, truncation_size, ALICE)) & satisfies_condition;

        for (int j = 0; j < truncation_size; j++) {
            emp::Bit is_current_position = (Integer(32, j, ALICE) == (last_written_index + Integer(32, 1, ALICE))) & is_write_position;

            for (int col = 0; col < input.columns.size(); col++) {
                output.columns[col][j] = If(is_current_position, input.columns[col][i], output.columns[col][j]);
            }
            output.flags[j] = If(is_current_position, Integer(1, 1, ALICE), output.flags[j]);
        }
        // Update last_written_index if an actual write happened
        last_written_index = If(is_write_position, last_written_index + Integer(32, 1, ALICE), last_written_index);

        // Increment the actual writes counter if an actual write happened
        actual_writes_counter = If(is_write_position, actual_writes_counter + Integer(32, 1, ALICE), actual_writes_counter);
    }

    return output;
}

#endif // PACFILTER_OPERATOR_HPP
