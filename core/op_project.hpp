#ifndef PROJECTION_OPERATOR_HPP
#define PROJECTION_OPERATOR_HPP

#include "relation.hpp"  // Assuming relation.hpp is in the same directory or include path
#include <vector>
#include <iostream>

class ProjectionOperator : public UnaryOperator {
public:
    std::vector<int> column_indexes;  // Indices of columns to be projected

    // Constructor
    ProjectionOperator(const std::vector<int>& col_indexes) : column_indexes(col_indexes) {}

protected:
    // Operation method
    SecureRelation operation(const SecureRelation& input) override {
        // Determine the number of rows from the input
        int rowCount = input.columns.empty() ? 0 : input.columns[0].size();

        // Initialize the output SecureRelation with the required number of columns and rows
        SecureRelation output(column_indexes.size() + 1, rowCount); // +1 for the flag column

        // Copy the selected columns to the output
        for (size_t i = 0; i < column_indexes.size(); ++i) {
            if (column_indexes[i] < 0 || column_indexes[i] >= input.columns.size()) {
                std::cerr << "Error: Invalid column index " << column_indexes[i] << std::endl;
                throw std::invalid_argument("Invalid column index " + std::to_string(column_indexes[i]));
            }
            output.columns[i] = input.columns[column_indexes[i]];
        }

        // Copy the flag column to the output
        output.columns.back() = input.flags;

        return output;
    }
};

#endif // PROJECTION_OPERATOR_HPP
