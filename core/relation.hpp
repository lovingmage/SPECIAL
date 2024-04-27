// relation.hpp

#ifndef RELATION_HPP
#define RELATION_HPP

#include "emp-sh2pc/emp-sh2pc.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <iostream>


class SecureRelation {
public:
    std::vector<std::vector<emp::Integer>> columns;
    std::vector<emp::Integer> flags;

    // Constructor to initialize the relation with specified column count and row count
    SecureRelation() : SecureRelation(0, 0) {} // Default constructor
    SecureRelation(int column_count, int row_count);

    // Methods to sort the relation based on a given column index or the flag
    void sort_by_column(int column_index);
    void sort_by_flag();
    void sort_by_two_columns(int primary_column_index, int secondary_column_index);

    // Utility methods for bitonic sort
    void bitonic_sort(int low, int high, bool ascending, std::vector<emp::Integer>& key_column);
    void bitonic_merge(int low, int high, bool ascending, std::vector<emp::Integer>& key_column);
    void swap_rows(int i, int j, emp::Bit condition);

    // Goldreich's Bitonic Compaction methods:
    void sort_by_flag_goldreich();
    void goldreich_compaction(int low, int high);
    void goldreich_merge(int low, int mid, int high);

    // The compact function
    void compact(int K);

    // Utility function to print the relation's details
    void print_relation(const std::string& label) const;
};

// Implementations

SecureRelation::SecureRelation(int column_count, int row_count) {
    columns.resize(column_count, std::vector<emp::Integer>(row_count, emp::Integer(32, 0, emp::PUBLIC)));
    flags.resize(row_count, emp::Integer(1, 1, emp::PUBLIC));
}

void SecureRelation::sort_by_column(int column_index) {
    if(column_index < 0 || column_index >= columns.size()) {
        std::cerr << "Error: Invalid column index!" << std::endl;
        return;
    }
    bitonic_sort(0, flags.size(), true, columns[column_index]);
}

void SecureRelation::sort_by_flag() {
    bitonic_sort(0, flags.size(), true, flags);
}

void SecureRelation::bitonic_sort(int low, int high, bool ascending, std::vector<emp::Integer>& key_column) {
    if (high <= 1) return;

    int mid = high / 2;
    bitonic_sort(low, mid, true, key_column);
    bitonic_sort(low + mid, mid, false, key_column);
    bitonic_merge(low, high, ascending, key_column);
}

void SecureRelation::bitonic_merge(int low, int high, bool ascending, std::vector<emp::Integer>& key_column) {
    if (high <= 1) return;

    int mid = high / 2;
    for (int i = low; i < low + mid; i++) {
        emp::Bit condition = (key_column[i] > key_column[i+mid]) == ascending;
        swap_rows(i, i+mid, condition);
    }

    bitonic_merge(low, mid, ascending, key_column);
    bitonic_merge(low + mid, mid, ascending, key_column);
}

void SecureRelation::swap_rows(int i, int j, emp::Bit condition) {
    for (auto& column : columns) {
        emp::Integer temp = column[i];
        column[i] = emp::If(condition, column[j], column[i]);
        column[j] = emp::If(condition, temp, column[j]);
    }

    emp::Integer temp_flag = flags[i];
    flags[i] = emp::If(condition, flags[j], flags[i]);
    flags[j] = emp::If(condition, temp_flag, flags[j]);
}

// Implementation of the Goldreich's Bitonic Compaction methods:

void SecureRelation::sort_by_flag_goldreich() {
    goldreich_compaction(0, flags.size());
}

void SecureRelation::goldreich_compaction(int low, int high) {
    if (high - low <= 1) return; // Base case

    int mid = (low + high) / 2;
    goldreich_compaction(low, mid);  // Recursively compact left half
    goldreich_compaction(mid, high); // Recursively compact right half
    goldreich_merge(low, mid, high); // Merge the two compacted halves
}

void SecureRelation::goldreich_merge(int low, int mid, int high) {
    int i = mid - 1;
    int j = mid;

    // This process locates the end of the 1s in the left half (i) and the start of the 1s in the right half (j)
    while (i >= low && j < high) {
        emp::Bit condition = (flags[i] < flags[j]);
        swap_rows(i, j, condition);
        if (flags[i].reveal<int>() == 1) {
            i--;
        }
        if (flags[j].reveal<int>() == 1) {
            j++;
        }
    }
}

//sort on two columns
void SecureRelation::sort_by_two_columns(int primary_column_index, int secondary_column_index) {
    if(primary_column_index < 0 || primary_column_index >= columns.size() || 
       secondary_column_index < 0 || secondary_column_index >= columns.size()) {
        std::cerr << "Error: Invalid column index!" << std::endl;
        return;
    }
    sort_by_column(secondary_column_index);
    sort_by_column(primary_column_index);
}

// Compaction function
void SecureRelation::compact(int K) {
    // First, sort the relation by the flag.
    sort_by_flag();

    // Check if the relation has more than K rows. 
    if (columns[0].size() > K) {
        // Resize each column to have only K rows.
        for (auto& column : columns) {
            column.resize(K);
        }
        // Resize the flags vector to have only K entries.
        flags.resize(K);
    }
}

// Helper function 
void SecureRelation::print_relation(const std::string& label) const {
    std::cout << label << "\n";
    for (size_t row = 0; row < columns[0].size(); ++row) {
        for (size_t col = 0; col < columns.size(); ++col) {
            std::cout << columns[col][row].reveal<int>() << "\t";
        }
        std::cout << "| Flag: " << flags[row].reveal<int>() << "\n";
    }
    std::cout << "\n";
}

#endif // RELATION_HPP
