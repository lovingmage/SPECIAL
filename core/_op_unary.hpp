// unary_operator.hpp

#ifndef UNARY_OPERATOR_HPP
#define UNARY_OPERATOR_HPP

#include "core/relation.hpp"

emp::Bit mux(const emp::Bit& condition, const emp::Bit& A, const emp::Bit& B) {
    return condition.select(A, B);
}

class UnaryOperator {
public:
    // The primary interface for the unary operator
    SecureRelation execute(const SecureRelation& input) {
        return operation(input);
    }

protected:
    // Pure virtual function for the actual operation. Subclasses must provide an implementation.
    virtual SecureRelation operation(const SecureRelation& input) = 0;
    
    virtual ~UnaryOperator() {}  // Virtual destructor for proper cleanup
};

#endif // UNARY_OPERATOR_HPP