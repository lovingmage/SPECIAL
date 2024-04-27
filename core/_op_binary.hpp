// binary_operator.hpp

#ifndef BINARY_OPERATOR_HPP
#define BINARY_OPERATOR_HPP

#include "core/relation.hpp"

class BinaryOperator {
public:
    // The primary interface for the binary operator
    SecureRelation execute(const SecureRelation& input1, const SecureRelation& input2) {
        return operation(input1, input2);
    }

protected:
    // Pure virtual function for the actual operation. Subclasses must provide an implementation.
    virtual SecureRelation operation(const SecureRelation& input1, const SecureRelation& input2) = 0;

    virtual ~BinaryOperator() {}  // Virtual destructor for proper cleanup
};

#endif // BINARY_OPERATOR_HPP
