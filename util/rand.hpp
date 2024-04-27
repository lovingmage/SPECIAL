#ifndef RANDOM_VECTOR_GENERATOR_HPP
#define RANDOM_VECTOR_GENERATOR_HPP

#include <vector>
#include <random>

class RandomVectorGenerator {
public:
    RandomVectorGenerator(unsigned int seed = std::random_device{}()) : gen(seed) {}
    std::vector<int> generate(size_t size, int min_val = 0, int max_val = 100) const {
        std::uniform_int_distribution<> dis(min_val, max_val);
        std::vector<int> result(size);
        for (size_t i = 0; i < size; ++i) {
            result[i] = dis(gen);
        }
        return result;
    }

private:
    mutable std::mt19937 gen;
};


#endif // RANDOM_VECTOR_GENERATOR_HPP
