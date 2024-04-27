// count_operator.hpp

#ifndef COUNT_OPERATOR_HPP
#define COUNT_OPERATOR_HPP

#include "_op_unary.hpp"

class CountOperator : public UnaryOperator {
protected:
    SecureRelation operation(const SecureRelation& relation) override {
        emp::Integer count(32, 0, emp::PUBLIC); // Initialize count to zero with size of 32 bits.
        
        // Sum up the flag bits.
        for(const auto& flag : relation.flags) {
            count = count + flag;
        }

        // Create a result relation with only one row.
        SecureRelation result(1, 1);
        result.columns[0][0] = count;
        result.flags[0] = emp::Integer(1, 1, emp::PUBLIC); // Set the flag to 1.

        return result;
    }
};

#endif // COUNT_OPERATOR_HPP
