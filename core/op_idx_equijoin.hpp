// index_equijoin.hpp

#ifndef INDEX_EQUIJOIN_OPERATOR_HPP
#define INDEX_EQUIJOIN_OPERATOR_HPP

#include "core/_op_binary.hpp"
#include "op_equijoin.hpp"
#include <vector>
#include <utility>

class IndexEquiJoinOperator : public BinaryOperator {
public:
    enum CompactionMode {
        NONE,          // No compaction
        SMALLER_REL,   // Compact to the size of the smaller input relation
        LARGER_REL,    // Compact to the size of the larger input relation
        FIXED_SIZE,    // Compact based on a fixed size MF
        MF             // Compact to the MF upper bound
    };

    std::vector<std::pair<int, int>> index1;
    std::vector<std::pair<int, int>> index2;

    int column_index1;
    int column_index2;

    CompactionMode mode;
    int fixed_size;  // Used only when mode is FIXED_SIZE
    int mf1, mf2;

    IndexEquiJoinOperator(const std::vector<std::pair<int, int>>& idx1, const std::vector<std::pair<int, int>>& idx2, int col_idx1, int col_idx2, CompactionMode mode = NONE, int fixed_size = 0, int mf1=1, int mf2=1);
    
    // Rebuilding the index
    std::vector<std::pair<int, int>> rebuild_index();

protected:
    SecureRelation operation(const SecureRelation& rel1, const SecureRelation& rel2) override;

private:
    // bucketize large join
    std::vector<SecureRelation> bucketize(const SecureRelation& rel, const std::vector<std::pair<int, int>>& index);
   
    // compact bucket join output
    SecureRelation compact_result(SecureRelation& bucket_result, const SecureRelation& rel1, const SecureRelation& rel2);
};

// Implementations
IndexEquiJoinOperator::IndexEquiJoinOperator(const std::vector<std::pair<int, int>>& idx1, const std::vector<std::pair<int, int>>& idx2, int col_idx1, int col_idx2, CompactionMode mode, int fixed_size, int mf1, int mf2)
    : index1(idx1), index2(idx2), column_index1(col_idx1), column_index2(col_idx2), mode(mode), fixed_size(fixed_size), mf1(mf1), mf2(mf2) {}

#ifndef MULTI_THREAD
SecureRelation IndexEquiJoinOperator::operation(const SecureRelation& rel1, const SecureRelation& rel2) {
    EquiJoinOperator join_op(column_index1, column_index2);

    // To hold the final result
    std::vector<SecureRelation> final_results;

    for (size_t i = 0; i < index1.size(); i++) {
        // Bucketize current pair of indices
        std::vector<SecureRelation> bucket1 = bucketize(rel1, {index1[i]});
        std::vector<SecureRelation> bucket2 = bucketize(rel2, {index2[i]});
        
        // Perform the equijoin on current pair of buckets
        SecureRelation join_result = join_op.execute(bucket1[0], bucket2[0]);
        
        // Compact the result
        SecureRelation compacted_result = compact_result(join_result, bucket1[0], bucket2[0]);
        
        // Append to the final results
        final_results.push_back(compacted_result);
    }

    // Merge all the results
    int total_rows = 0;
    for (const auto& res : final_results) {
        total_rows += res.columns[0].size();
    }

    SecureRelation result(rel1.columns.size() + rel2.columns.size(), total_rows);
    int offset = 0;
    for (const auto& res : final_results) {
        for (size_t j = 0; j < res.columns[0].size(); j++) {
            for (size_t k = 0; k < result.columns.size(); k++) {
                result.columns[k][offset + j] = res.columns[k][j];
            }
            result.flags[offset + j] = res.flags[j];
        }
        offset += res.columns[0].size();
    }

    return result;
}
#else
SecureRelation IndexEquiJoinOperator::operation(const SecureRelation& rel1, const SecureRelation& rel2) {
    EquiJoinOperator join_op(column_index1, column_index2);

    // Preallocate space for the final results
    std::vector<SecureRelation> final_results(index1.size());
    std::vector<std::thread> threads;

    for (size_t i = 0; i < index1.size(); i++) {
        threads.emplace_back([&, i] {
            // Bucketize current pair of indices
            std::vector<SecureRelation> bucket1 = bucketize(rel1, {index1[i]});
            std::vector<SecureRelation> bucket2 = bucketize(rel2, {index2[i]});

            // Perform the equijoin on current pair of buckets
            SecureRelation join_result = join_op.execute(bucket1[0], bucket2[0]);

            // Compact the result
            final_results[i] = compact_result(join_result, bucket1[0], bucket2[0]);
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge all the results
    int total_rows = 0;
    for (const auto& res : final_results) {
        total_rows += res.columns[0].size();
    }

    SecureRelation result(rel1.columns.size() + rel2.columns.size(), total_rows);
    int offset = 0;
    for (const auto& res : final_results) {
        for (size_t j = 0; j < res.columns[0].size(); j++) {
            for (size_t k = 0; k < result.columns.size(); k++) {
                result.columns[k][offset + j] = res.columns[k][j];
            }
            result.flags[offset + j] = res.flags[j];
        }
        offset += res.columns[0].size();
    }

    return result;
}

#endif

std::vector<SecureRelation> IndexEquiJoinOperator::bucketize(const SecureRelation& rel, const std::vector<std::pair<int, int>>& index) {
    std::vector<SecureRelation> buckets;
    for (const auto& idx_pair : index) {
        int start_idx = idx_pair.first;
        int end_idx = idx_pair.second;
        int bucket_size = end_idx - start_idx + 1;
        
        SecureRelation bucket(rel.columns.size(), bucket_size);

        
        for (size_t col = 0; col < rel.columns.size(); col++) {
            for (int row = start_idx; row <= end_idx; row++) {
                bucket.columns[col][row - start_idx] = rel.columns[col][row];
            }
        }
        for (int row = start_idx; row <= end_idx; row++) {
            bucket.flags[row - start_idx] = rel.flags[row];
        }
        buckets.push_back(bucket);
    }
    return buckets;
}

SecureRelation IndexEquiJoinOperator::compact_result(SecureRelation& bucket_result, const SecureRelation& rel1, const SecureRelation& rel2) {
    int compact_size = bucket_result.columns[0].size(); 
    switch (mode) {
        case SMALLER_REL:
            compact_size = std::min(rel1.columns[0].size(), rel2.columns[0].size());
            break;
        case LARGER_REL:
            compact_size = std::max(rel1.columns[0].size(), rel2.columns[0].size());
            break;
        case FIXED_SIZE:
            compact_size = fixed_size;
            break;
        case MF:
            compact_size = std::min({static_cast<int>(rel1.columns[0].size() * mf2),
                                     static_cast<int>(rel2.columns[0].size() * mf1),
                                     static_cast<int>(rel1.columns[0].size() * rel2.columns[0].size())});
            break;
        default:
            break;
    }

    // Sort the bucket result by flag
    bucket_result.sort_by_flag();

    // Create a new relation to store the compacted result
    SecureRelation compacted_rel(bucket_result.columns.size(), compact_size);

    for (size_t col = 0; col < bucket_result.columns.size(); col++) {
        for (int row = 0; row < compact_size && row < bucket_result.columns[col].size(); row++) {
            compacted_rel.columns[col][row] = bucket_result.columns[col][row];
        }
    }
    for (int row = 0; row < compact_size && row < bucket_result.flags.size(); row++) {
        compacted_rel.flags[row] = bucket_result.flags[row];
    }

    return compacted_rel;
}


std::vector<std::pair<int, int>> IndexEquiJoinOperator::rebuild_index() {
    std::vector<std::pair<int, int>> new_index;
    int start_idx = 0;

    for (size_t i = 0; i < index1.size(); i++) {
        int original_size = index1[i].second - index1[i].first + 1;
        int compacted_size = 0;

        // Determine compacted size based on compaction mode
        switch (mode) {
            case SMALLER_REL:
                compacted_size = std::min(index1[i].second - index1[i].first + 1, index2[i].second - index2[i].first + 1);
                break;
            case LARGER_REL:
                compacted_size = std::max(index1[i].second - index1[i].first + 1, index2[i].second - index2[i].first + 1);
                break;
            case FIXED_SIZE:
                compacted_size = fixed_size;
                break;
            case MF:
                compacted_size = std::min(static_cast<int>(original_size * mf2),
                                          static_cast<int>(original_size * mf1));
                break;
            default:
                compacted_size = original_size; // For NONE mode or any other unspecified mode
                break;
        }

        int end_idx = start_idx + compacted_size - 1;
        new_index.push_back({start_idx, end_idx});
        start_idx = end_idx + 1;
    }

    return new_index;
}

#endif // INDEX_EQUIJOIN_OPERATOR_HPP
