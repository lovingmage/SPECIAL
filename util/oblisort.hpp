// util/oblisort.hpp

#ifndef OBLISORT_HPP
#define OBLISORT_HPP

#include "emp-sh2pc/emp-sh2pc.h"

struct Tuple {
    emp::Integer value;
    emp::Integer flag;

    Tuple() : value(32, 0, emp::PUBLIC), flag(32, 0, emp::PUBLIC) {}
};

namespace ObliviousSorting {

    // Forward declarations for single array functions
    void bitonic_sort(emp::Integer array[], int left, int right, bool ascending);
    void bitonic_merge(emp::Integer array[], int left, int right, bool ascending);
    void swap_data(emp::Integer* a, emp::Integer* b, emp::Bit to_swap);

    // Forward declarations for Tuple functions
    void bitonic_sort(Tuple array[], int left, int right, bool ascending);
    void bitonic_merge(Tuple array[], int left, int right, bool ascending);
    void swap_data(Tuple* a, Tuple* b, emp::Bit to_swap);

    // For Tuple functions sorted by integer value
    void bitonic_sort_by_value(Tuple array[], int left, int right, bool ascending);
    void bitonic_merge_by_value(Tuple array[], int left, int right, bool ascending);

    // Forward declarations for the bitonic compaction functions
    void bitonic_compaction(Tuple array[], int left, int right);
    void binary_bitonic_merge_by_flag(Tuple array[], int left, int right);
    int greatest_power_of_two_less_than(int n);

    /* Implementations */

    // Functions for sorting a single secure array
    void bitonic_sort(emp::Integer array[], int left, int right, bool ascending) {
        if (right > 1) {
            int m = right / 2;
            bitonic_sort(array, left, m, true);
            bitonic_sort(array, left + m, m, false);
            bitonic_merge(array, left, right, ascending);
        }
    }

    void bitonic_merge(emp::Integer array[], int left, int right, bool ascending) {
        int i, j;
        for (int k = right / 2; k > 0; k /= 2) {
            for (i = left, j = left + k; j < left + right; i++, j++) {
                emp::Bit to_swap = ((array[i] > array[j]) == ascending);
                swap_data(&array[i], &array[j], to_swap);
            }
        }
    }

    void swap_data(emp::Integer* a, emp::Integer* b, emp::Bit to_swap) {
        emp::Integer temp = *a;
        *a = emp::If(to_swap, *b, *a);
        *b = emp::If(to_swap, temp, *b);
    }

    // Functions for sorting the Tuple structure
    void bitonic_sort(Tuple array[], int left, int right, bool ascending) {
        if (right > 1) {
            int m = right / 2;
            bitonic_sort(array, left, m, true);
            bitonic_sort(array, left + m, m, false);
            bitonic_merge(array, left, right, ascending);
        }
    }

    void bitonic_merge(Tuple array[], int left, int right, bool ascending) {
        int i, j;
        for (int k = right / 2; k > 0; k /= 2) {
            for (i = left, j = left + k; j < left + right; i++, j++) {
                emp::Bit to_swap = ((array[i].flag > array[j].flag) == ascending);
                swap_data(&array[i], &array[j], to_swap);
            }
        }
    }

    void swap_data(Tuple* a, Tuple* b, emp::Bit to_swap) {
        Tuple temp = *a;
        a->flag = emp::If(to_swap, b->flag, a->flag);
        a->value = emp::If(to_swap, b->value, a->value);
        b->flag = emp::If(to_swap, temp.flag, b->flag);
        b->value = emp::If(to_swap, temp.value, b->value);
    }

    void bitonic_sort_by_value(Tuple array[], int left, int right, bool ascending) {
        if (right > 1) {
            int m = right / 2;
            bitonic_sort_by_value(array, left, m, true);
            bitonic_sort_by_value(array, left + m, m, false);
            bitonic_merge_by_value(array, left, right, ascending);
        }
    }

    void bitonic_merge_by_value(Tuple array[], int left, int right, bool ascending) {
        int i, j;
        for (int k = right / 2; k > 0; k /= 2) {
            for (i = left, j = left + k; j < left + right; i++, j++) {
                emp::Bit to_swap = ((array[i].value > array[j].value) == ascending);
                swap_data(&array[i], &array[j], to_swap);
            }
        }
    }

    // Compaction related implementations (Binary Bitonic Sort for Tuples based on flag)
    void bitonic_compaction(Tuple array[], int left, int right) {
        if (right <= 1) return;

        int m = right / 2;
        
        // Create bitonic sequence
        bitonic_compaction(array, left, m);
        bitonic_compaction(array, left + m, m);
        
        // Merge the sequence
        binary_bitonic_merge_by_flag(array, left, right);
    }

    void binary_bitonic_merge_by_flag(Tuple array[], int left, int right) {
        if (right <= 1) return;

        int m = greatest_power_of_two_less_than(right);
        
        for(int i = left; i < left + right - m; i++) {
            emp::Bit to_swap = (array[i].flag > array[i+m].flag);
            swap_data(&array[i], &array[i+m], to_swap);
        }

        // Recursively merge both halves
        binary_bitonic_merge_by_flag(array, left, m);
        binary_bitonic_merge_by_flag(array, left + m, right - m);
    }

    int greatest_power_of_two_less_than(int n) {
        int k = 1;
        while(k < n) {
            k <<= 1;
        }
        return k >> 1;
    }


} // namespace ObliviousSorting

#endif // OBLISORT_HPP
