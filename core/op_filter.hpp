#ifndef FILTER_OPERATOR_HPP
#define FILTER_OPERATOR_HPP

#include "core/_op_unary.hpp"
#include <string>

class FilterOperator : public UnaryOperator {
public:
    int column_index;  // The index of the column on which the filter is applied
    emp::Integer target_value;  // A target value for comparison if it's not a column
    std::vector<emp::Integer> target_column; // A column for comparison, if applicable
    std::string condition;  // One of the conditions: "gt, geq, lt, leq, eq, neq"

    emp::Bit compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition);

    // Constructor when target is a single value
    FilterOperator(int col_idx, const emp::Integer& target, const std::string& cnd);

    // Constructor when target is a column
    FilterOperator(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd);

    SecureRelation operation(const SecureRelation& input) override;
};


FilterOperator::FilterOperator(int col_idx, const emp::Integer& target, const std::string& cnd) 
    : column_index(col_idx), target_value(target), condition(cnd) {}

FilterOperator::FilterOperator(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd) 
    : column_index(col_idx), target_column(target_col), condition(cnd) {}

emp::Bit FilterOperator::compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition) {
    if (condition == "gt") return a > b;
    if (condition == "geq") return a >= b;
    if (condition == "lt") return a < b;
    if (condition == "leq") return a <= b;
    if (condition == "eq") return a == b;
    if (condition == "neq") return a != b;
    return emp::Bit(false); // Default to false for unsupported conditions
}

SecureRelation FilterOperator::operation(const SecureRelation& input) {
    SecureRelation output = input; // Make a copy of the input relation

    if (target_column.empty()) { // If the target is a single value
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_value, condition).reveal<bool>(), ALICE);
        }
    } else { // If the target is a column
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_column[i], condition).reveal<bool>(), ALICE);
        }
    }
    return output;
}

#endif // FILTER_OPERATOR_HPP
